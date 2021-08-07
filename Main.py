from data_generator import DataGenerator
from hadoop_operations import HadoopOperations
import os


class Main:
    def __init__(self):
        self.__filepath = None
        self.hdfs_path = None
        self.hadoop_operations = HadoopOperations()
        self.data_generator = DataGenerator()

    def generate_data_to_file(self, filename="project_data.csv"):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File to save data to.
                - default: project_data.csv
        """
        self.__filepath = os.path.join(os.getcwd(), filename)
        self.data_generator.generate_data(filename=self.__filepath)

    def upload_file(self):
        """
        Uploads file to hadoop file system
        """
        self.hadoop_operations.upload_file_hadoop(filepath=self.__filepath)
        self.hdfs_path = self.hadoop_operations.hdfs_path

    def delete_directory(self):
        """
                Delete directory from hadoop file system
        """
        if self.hadoop_operations.is_directory_exist():
            self.hadoop_operations.delete_directory_hadoop()


if __name__ == "__main__":
    main = Main()
    main.generate_data_to_file()

    main.delete_directory()
    main.upload_file()
    # TODO: Hive Code
    main.delete_directory()
