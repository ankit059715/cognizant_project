from subprocess import PIPE, Popen


class HiveOperations:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name

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
            print("\nChecking database exist:\n")
            put = Popen(["hive", "-S", "-e", "show databases;"], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, err = put.communicate()
            out = self.hive_output_formatter(str(out).lower())
            print(out)
            if self.database.lower() in out:
                return True
            return False
        except Exception as exp:
            raise Exception(exp)

    def drop_hive_database(self):
        try:
            print("\nDropping database\n")
            put = Popen(["hive", "-S", "-e", "drop database {0};".format(self.database)], stdin=PIPE, stdout=PIPE, bufsize=-1)
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
            print("\nCreating database :\n")
            if self.is_database_exist():
                print("Exists")
                self.drop_hive_database()
            else:
                print("Not Exists")
            put = Popen(["hive", "-S", "-e", "create database {0};".format(self.database)], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create database")
        except Exception as exp:
            raise Exception(exp)

    def create_hive_table(self, table_keys_type):
        """
        Creates a table in hive database
        Raises:
            Exception:
                If any issue when creating table
        """
        data_type_map = {'int': "int",
                         'str': "string",
                         'float': "double"
                         }
        table_struct = ""
        for key, data_type in table_keys_type.items():
            table_struct += "{0} {1}, ".format(key, data_type_map[data_type])
        table_struct = table_struct[:-2]
        try:
            print("\nCreating Table:\n")
            cmd = "use {0};create table {1}({2}) " \
                  "row format delimited fields terminated by \",\" stored as textfile".format(self.database,
                                                                                              self.table_name,
                                                                                              table_struct)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
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
            print("\nLoading data to file\n")
            cmd = "use {0};load data INPATH '{0}' INTO TABLE {1}".format(filepath, self.table_name)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                print(str(out).lower())
                raise Exception("Failed to create table")

            cmd = 'use {0};alter table {1} set tblproperties("skip.header.line.count"="1");'.format(self.database,
                                                                                                    self.table_name)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            print(str(out).lower())

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
        query = "use {0};select * from {1} where name like '%{2}' or name like '%{3}'".format(self.database,
                                                                                              self.table_name,
                                                                                              first_letter,
                                                                                              first_letter.upper())

        try:
            put = Popen(["hive", "-S", "-e", query], stdin=PIPE, stdout=PIPE, bufsize=-1)
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
