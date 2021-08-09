from data_generator import DataGenerator
from hadoop_operations import HadoopOperations
from hive_operations import HiveOperations
import os
import csv


class Main:
    def __init__(self):
        self.__filepath = None
        self.hdfs_path = None
        self.data_generator = DataGenerator()
        self.hadoop_operations = HadoopOperations()
        self.hive_operations = HiveOperations(database="project_db",
                                              table_name="test_table")

    def generate_data_to_file(self, filename="project_data.csv"):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File to save data to.
                - default: project_data.csv
        """
        print("\nGenerate data\n")
        self.__filepath = os.path.join(os.getcwd(), filename)
        self.data_generator.generate_data(filename=self.__filepath)

    def upload_file(self):
        """
        Uploads file to hadoop file system
        """
        print("\nUpload directory to hadoop\n")
        self.hadoop_operations.upload_file_hadoop(filepath=self.__filepath)
        self.hdfs_path = self.hadoop_operations.hdfs_path

    def delete_directory(self):
        """
        Delete directory from hadoop file system
        """
        print("\nDelete directory from hadoop\n")
        if self.hadoop_operations.is_directory_exist():
            self.hadoop_operations.delete_directory_hadoop()

    def hive_create_db(self):
        """
        Create hive database
        """
        self.hive_operations.create_hive_database()

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
            writer = csv.writer(output_file, delimiter = ",")
            writer.writerows([keys])
            writer.writerows(data)

    def get_data_from_hive(self):
        print("\nGetting data from hive:\n")

        data_with_char, data_rest = self.hive_operations.get_data_from_hive_with_first_letter(first_letter='v')
        data_with_char = data_with_char.replace('\t',",").strip().split("\n")
        data_rest = data_rest.replace('\t',",").strip().split("\n")
        
        self.save_data(data=data_with_char,destination_file="v.csv")
        self.save_data(data=data_rest,destination_file="not_v.csv")


if __name__ == "__main__":
    main = Main()
    main.generate_data_to_file()

    #main.delete_directory()
    #main.upload_file()
    #main.hive_create_db()
    #main.create_table_with_data()
    main.get_data_from_hive()

    # Phone number special character validation
    # pin special character validation
    # email -> @ must be in it
    #
    #main.delete_directory()
