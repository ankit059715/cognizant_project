from subprocess import PIPE, Popen
import os


class HadoopOperations:
    def __init__(self):
        self.__hdfs_path = None

    def upload_file_hadoop(self, filepath):
        """
        Uploads file to hadoop file system
        Args:
            filepath(str) ---> Path to file to upload
        Raises:
            Exception:
                If any issue when uploading file or creating directory
        """
        self.__hdfs_path = os.path.join(os.sep, 'user', 'cloudera', 'project_file')

        try:
            put = Popen(["hadoop", "fs", "-mkdir", self.__hdfs_path], stdin=PIPE, stdout=PIPE, bufsize=-1)
            put.communicate()
            put = Popen(["hadoop", "fs", "-put", filepath, self.__hdfs_path], stdin=PIPE, bufsize=-1)
            put.communicate()
        except Exception as exp:
            raise Exception(exp)

    @staticmethod
    def is_directory_exist():
        """
        Check if directory exists in hadoop file system
        Raises:
            Exception:
                If any issue when listing directory
        """
        try:
            put = Popen(["hadoop", "fs", "-ls"], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, err = put.communicate()

            if "project_file" in str(out).lower():
                return True
            return False
        except Exception as exp:
            raise Exception(exp)

    def delete_directory_hadoop(self):
        """
        Delete directory from hadoop file system
        Raises:
            Exception:
                If any issue when deleting directory
        """
        self.__hdfs_path = os.path.join(os.sep, 'user', 'cloudera', 'project_file')
        try:
            put = Popen(["hadoop", "fs", "-rm", "-r", self.__hdfs_path], stdin=PIPE, stdout=PIPE, bufsize=-1)
            put.communicate()
        except Exception as exp:
            raise Exception(exp)

    @property
    def hdfs_path(self):
        return self.__hdfs_path
