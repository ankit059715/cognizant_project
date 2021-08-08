from subprocess import PIPE, Popen


class HiveOperations:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
        self.base_cmd = "hive -S -e \"use {0};".format(self.database)

    @staticmethod
    def hive_output_formatter(message):
        """
        Returns Formatted Output message
        Args:
            message(str) ---> Output message
        Returns:
            str -> Formatted Message
        """
        index = message.find("warn")
        return message[:index]

    def is_database_exist(self):
        try:
            put = Popen(["hive", "-S", "-e", "show databases"], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, err = put.communicate()
            out = self.hive_output_formatter(str(out).lower())
            if self.database.lower() in out:
                return True
            return False
        except Exception as exp:
            raise Exception(exp)

    def drop_hive_database(self):
        try:
            put = Popen(["hive", "-S", "-e", "drop", "database", self.database], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                raise Exception("Failed to delete database")
        except Exception as exp:
            raise Exception(exp)

    def create_hive_database(self):
        """
        Creates Hive database
        Raises:
            Exception:
                If any problem while connecting to Hive Database
        """
        try:
            if self.is_database_exist():
                self.drop_hive_database()
            put = Popen(["hive", "-S", "-e", "'create", "database", self.database, "'"],
                        stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create database")
        except Exception as exp:
            raise Exception(exp)

    def create_hive_table(self):
        """
        Creates a table in hive database
        Raises:
            Exception:
                If any issue when creating table
        """
        try:
            cmd = "create table {0}(year INT, quarter INT, revenue DOUBLE,seats INT) " \
                  "row format delimited fields terminated by ',' stored as textfile\";".format(self.table_name)

            put = Popen([self.base_cmd, cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create table")
        except Exception as exp:
            raise Exception(exp)


    def insert_data_to_table_from_file(self, filepath):
        """
        Insert data into table created with file content
        Args:
            filepath(str) ---> Hadoop filepath to upload data from
        Raises:
            Exception:
                If any issue when loading data from file
        """
        try:
            cmd = "load data INPATH '{0}' INTO TABLE {1}\";".format(filepath, self.table_name)

            put = Popen([self.base_cmd, cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create table")
        except Exception as exp:
            raise Exception(exp)

    def get_data_from_hive_with_first_letter(self, first_letter="a"):
        """
        Get all data beginning with given character
        Args:
            first_letter(str) ---> First letter to get data from
        Returns:
            list -> List of data
        Raises:
            Exception:
                If any issue when performing select operation
        """
        query = "select * from {0} where name like '%{1}' or name like '%{2}'".format(self.table_name,
                                                                                      first_letter,
                                                                                      first_letter.upper())

        try:
            put = Popen([self.base_cmd, query], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create table")
            message = self.hive_output_formatter(str(out))
            print(message)
            print("\n-----------------------------------------------------------\n")
            return message
        except Exception as exp:
            raise Exception(exp)
