from data_generator import DataGenerator
from hadoop_operations import HadoopOperations
from hive_operations import HiveOperations
import os


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
        # self.hive_operations.create_hive_table()
        self.hive_operations.insert_data_to_table_from_file(filepath=self.hdfs_path)

    @staticmethod
    def save_data(data, destination_file):
        with open(destination_file, 'w', newline='') as output_file:
            output_file.writelines(data)

    def get_data_from_hive(self):
        print("\nGetting data from hive:\n")

        for i in range(0, 2):
            data = self.hive_operations.get_data_from_hive_with_first_letter(first_letter=chr(ord('a')+i))
            self.save_data(data=data,
                           destination_file=chr(ord('a')+i)+".txt")


if __name__ == "__main__":
    main = Main()
    # main.generate_data_to_file()

    # main.delete_directory()
    # main.upload_file()
    # main.hive_create_db()
    main.create_table_with_data()
    # main.get_data_from_hive()

    # Phone number special character validation
    # pin special character validation
    # email -> @ must be in it
    #
    #main.delete_directory()
