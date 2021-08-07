from data_generator import DataGenerator
from subprocess import PIPE, Popen
import os


class Main:
    def __init__(self):
        self.filepath = None
        self.hdfs_path = None

    def generate_data_to_file(self, filename="project_data.csv"):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File to save data to.
                - default: project_data.csv
        """
        self.filepath = os.path.join(os.getcwd(), filename)
        data_generator = DataGenerator()
        data_generator.generate_data(filename=filename)

    def upload_file_hadoop(self):
        """
        Uploads file to hadoop file system
        Raises:
            Exception:
                If any issue when uploading file or creating directory
        """
        self.hdfs_path = os.path.join(os.sep, 'user', 'cloudera', 'project_file')
        print("Hdoop path: ", self.hdfs_path)
        print("File path: ", self.filepath)
        try:
            put = Popen(["hadoop", "fs", "-mkdir", self.hdfs_path], stdin=PIPE, bufsize=-1)
            put.communicate()
            put = Popen(["hadoop", "fs", "-put", self.filepath, self.hdfs_path], stdin=PIPE, bufsize=-1)
            put.communicate()
        except Exception as exp:
            raise Exception(exp)


if __name__ == "__main__":
    main = Main()
    main.generate_data_to_file()
