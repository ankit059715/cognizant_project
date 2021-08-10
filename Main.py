from data_generator import DataGenerator
from hadoop_operations import HadoopOperations
from hive_operations import HiveOperations
from data_validator import DataValidator
import app_logger
import os
import csv
import argparse


class Main:
    def __init__(self, filepath):
        self.__filepath = filepath
        self.hdfs_path = None
        self.error_file = None
        self.data_generator = DataGenerator()
        self.data_validator = DataValidator(filename=self.__filepath)
        self.hadoop_operations = HadoopOperations()
        self.hive_operations = HiveOperations(database="project_db",
                                              table_name="test_table")
        self.logger = app_logger.get_logger(__name__)
        self.logger.info("---------------New Execution---------------")

    def generate_data_to_file(self):
        """
        Generates Random Data for Given Amount and Writes to file
        """
        print("\nGenerate data")
        self.logger.info("Starting Generate Data Function")
        self.data_generator.generate_data(filename=self.__filepath)
        self.logger.info("Generate Data Function Complete")

    def validate_data(self):
        print("\nValidate data")
        self.logger.info("Starting Validate Data Function")
        self.__filepath, self.error_file = self.data_validator.validate_data_main()
        self.logger.info("Data Validation Complete")

    def upload_file(self):
        """
        Uploads file to hadoop file system
        """
        print("\nUpload directory to hadoop")
        self.logger.info("Starting File Upload to Hadoop")
        self.hadoop_operations.upload_file_hadoop(filepath=self.__filepath)
        self.hdfs_path = self.hadoop_operations.hdfs_path
        self.logger.info("File Upload to Hadoop completed")

    def delete_directory(self):
        """
        Delete directory from hadoop file system
        """
        print("\nDelete directory from hadoop\n")
        if self.hadoop_operations.is_directory_exist():
            self.logger.info("Deleting Existing Directory From Hadoop")
            self.hadoop_operations.delete_directory_hadoop()
            self.logger.info("Deleted Existing Directory From Hadoop")

    def hive_create_db(self):
        """
        Create hive database
        """
        self.logger.info("Creating Hive Database")
        self.hive_operations.create_hive_database()
        self.logger.info("Successfully Created Hive Database")

    def create_table_with_data(self, file_given=False):
        """
        Creates Table and load it with data
        """
        print("\nCreate table and add data:")
        key_type = {}
        if file_given:
            column_names, complete_data = self.data_validator.return_data_keys_from_file
            for ind in range(0, len(column_names)):
                key_type[column_names[ind]] = str(type(complete_data[0][ind])).split("'")[1]
        else:
            column_names = self.data_generator.get_keys
            for key in column_names:
                key_type[key] = self.data_generator.data_type_key(key=key)
        # print(key_type)
        self.hive_operations.create_hive_table(table_keys_type=key_type)
        self.hive_operations.insert_data_to_table_from_file(filepath=self.hdfs_path)

    def save_data(self, data, destination_file, file_given):
        if file_given:
            keys, _ = self.data_validator.return_data_keys_from_file
        else:    
            keys = self.data_generator.get_keys
        data = [data_row.split(',') for data_row in data]
        with open(destination_file, 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=",")
            writer.writerows([keys])
            writer.writerows(data)

    def get_data_from_hive(self, file_given=False):
        print("\nGetting data from hive:\n")

        data_with_char, data_rest = self.hive_operations.get_data_from_hive_with_first_letter(first_letter='v')
        data_with_char = data_with_char.replace('\t', ",").strip().split("\n")
        data_rest = data_rest.replace('\t', ",").strip().split("\n")
        v_file = os.path.join(os.path.dirname(self.__filepath), "v.csv")
        not_v_file = os.path.join(os.path.dirname(self.__filepath), "not_v.csv")
        self.save_data(data=data_with_char, destination_file=v_file, file_given=file_given)
        self.save_data(data=data_rest, destination_file=not_v_file, file_given=file_given)
        return (v_file, not_v_file)

    def upload_final_file_hadoop(self, v_file, not_v_file, file_given=False):
        """
        Uploads final files to hadoop file system
        """
        print("\nUploading Final files to hadoop\n")
        self.logger.info("Starting %s File Upload to Hadoop" % v_file)
        self.hadoop_operations.upload_file_hadoop(filepath=v_file)
        self.logger.info("File Upload to Hadoop completed for %s" % v_file)
        
        self.logger.info("Starting %s File Upload to Hadoop" % not_v_file)
        self.hadoop_operations.upload_file_hadoop(filepath=not_v_file)
        self.logger.info("File Upload to Hadoop completed for %s" % not_v_file)
        if not file_given:
            self.logger.info("Starting %s File Upload to Hadoop" % self.error_file)
            self.hadoop_operations.upload_file_hadoop(filepath=self.error_file)
            self.logger.info("File Upload to Hadoop completed for %s" % self.error_file)

        self.logger.info("Completed Program Execution!!")


if __name__ == "__main__":

    my_parser = argparse.ArgumentParser(description='Cognizant Project',
                                        epilog='Enjoy the program! :)',
                                        formatter_class=argparse.RawTextHelpFormatter)

    my_parser.add_argument('-f',
                           '--filename',
                           type=str,
                           help='The path to read csv file from',
                           required=False)
    args = my_parser.parse_args()
    file_given = False
    try:
        if args.filename is None or len(args.filename) == 0:
            filename = os.path.join(os.getcwd(), "project_data.csv")
        else:
            filename = args.filename
            if '/' in filename or '\\' in filename:
                pass
            else:
                filename = os.path.join(os.getcwd(), filename)
            file_given = True
    except Exception as exp:
        raise Exception(exp)

    main = Main(filename)
    if not file_given:
        main.generate_data_to_file()
        main.validate_data()
    main.delete_directory()
    main.upload_file()
    main.hive_create_db()
    main.create_table_with_data(file_given)
    v_file, not_v_file = main.get_data_from_hive(file_given)
    main.upload_final_file_hadoop(v_file, not_v_file, file_given)


