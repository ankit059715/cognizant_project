from data_generator import DataGenerator
from subprocess import PIPE, Popen
import os


class Main:
    def __init__(self):
        self.filename = None

    def generate_data_to_file(self, filename="project_data.csv"):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File to save data to.
                - default: project_data.csv
        """
        self.filename = filename
        data_generator = DataGenerator()
        data_generator.generate_data(filename=self.filename)

    def upload_file_hadoop(self):
        """
        Uploads file to hadoop file system
        Raises:
            Exception:
                If any issue when uploading file
        """
        hdfs_path = os.path.join(os.sep, 'user', 'cloudera', self.filename)
        try:
            put = Popen(["hadoop", "fs", "-put", self.filename, hdfs_path], stdin=PIPE, bufsize=-1)
            put.communicate()
        except Exception as exp:
            raise Exception(exp)


if __name__ == "__main__":
    main = Main()
    main.generate_data_to_file()
