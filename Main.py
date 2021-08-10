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
        self.data_generator = DataGenerator()
        self.data_validator = None
        self.hadoop_operations = HadoopOperations()
        self.hive_operations = HiveOperations(database="project_db",
                                              table_name="test_table")
        self.logger = app_logger.get_logger(__name__)
        self.logger.info("---------------New Execution---------------")

    def generate_data_to_file(self):
        """
        Generates Random Data for Given Amount and Writes to file
        """
        print("\nGenerate data\n")
        self.logger.info("Starting Generate Data Function")
        self.data_generator.generate_data(filename=self.__filepath)
        self.logger.info("Generate Data Function Complete")

    def validate_data(self):
        print("\nValidate data\n")
        self.logger.info("Starting Validate Data Function")
        self.data_validator = DataValidator(filename=self.__filepath)
        self.__filepath = self.data_validator.validate_data_main()
        self.logger.info("Data Validation Complete")

    def upload_file(self):
        """
        Uploads file to hadoop file system
        """
        print("\nUpload directory to hadoop\n")
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

    def create_table_with_data(self):
        """
        Creates Table and load it with data
        """
        print("\nCreate table and add data:\n")
        column_names = self.data_generator.get_keys
        key_type = {}
        for key in column_names:
            key_type[key] = self.data_generator.data_type_key(key=key)
        # print(key_type)
        self.hive_operations.create_hive_table(table_keys_type=key_type)
        self.hive_operations.insert_data_to_table_from_file(filepath=self.hdfs_path)

    def save_data(self, data, destination_file):
        keys = self.data_generator.get_keys
        data = [data_row.split(',') for data_row in data]
        with open(destination_file, 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=",")
            writer.writerows([keys])
            writer.writerows(data)

    def get_data_from_hive(self):
        print("\nGetting data from hive:\n")

        data_with_char, data_rest = self.hive_operations.get_data_from_hive_with_first_letter(first_letter='v')
        data_with_char = data_with_char.replace('\t', ",").strip().split("\n")
        data_rest = data_rest.replace('\t', ",").strip().split("\n")
        
        self.save_data(data=data_with_char, destination_file="v.csv")
        self.save_data(data=data_rest, destination_file="not_v.csv")


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
            file_given = True
    except Exception as exp:
        raise Exception(exp)

    main = Main(filename)
    if not file_given:
        main.generate_data_to_file()
        main.validate_data()
    # main.delete_directory()
    # main.upload_file()
    # main.hive_create_db()
    # main.create_table_with_data()
    # main.get_data_from_hive()

    # Phone number special character validation
    # pin special character validation
    # email -> @ must be in it
    #
    # main.delete_directory()
