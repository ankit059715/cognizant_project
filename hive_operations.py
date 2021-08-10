from subprocess import PIPE, Popen
import app_logger

class HiveOperations:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
        self.logger = app_logger.get_logger(__name__)

    def hive_output_formatter(self, message):
        """
        Returns Formatted Output message
        Args:
            message(str) ---> Output message
        Returns:
            str -> Formatted Message
        """
        index = message.lower().find("warn")
        return message[:index]

    def is_database_exist(self):
        self.logger.info("Checking if database exist")
        try:
            print("\nChecking database exist:\n")
            put = Popen(["hive", "-S", "-e", "show databases;"], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, err = put.communicate()
            out = self.hive_output_formatter(str(out))
            
            if self.database.lower() in out:
                return True
            return False
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def drop_hive_database(self):
        self.logger.info("Dropping Database")
        try:
            print("\nDropping database\n")
            if self.is_table_exist():
                print("Exists")
                self.drop_hive_table()
            put = Popen(["hive", "-S", "-e", "drop database {0};".format(self.database)], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                self.logger("Failed to delete database %s!" % self.database)
                raise Exception("Failed to delete database")
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def create_hive_database(self):
        """
        Creates Hive database
        Raises:
            Exception:
                If any problem while connecting to Hive Database
        """
        self.logger.info("Creating Database")
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
                self.logger("Failed to create Hive Database %s!" % self.database)
                raise Exception("Failed to create database")
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def create_hive_table(self, table_keys_type):
        """
        Creates a table in hive database
        Raises:
            Exception:
                If any issue when creating table
        """
        self.logger.info("Creating Hive Table")
        data_type_map = {'int': "bigint",
                         'str': "string",
                         'float': "double"
                         }
        table_struct = ""
        for key, data_type in table_keys_type.items():
            table_struct += "{0} {1}, ".format(key, data_type_map[data_type])
        table_struct = table_struct[:-2]                                            #------------------->
        try:
            print("\nCreating Table:\n")
            if self.is_table_exist():
                print("Exists")
                self.drop_hive_table()

            cmd = "use {0};create table {1}({2}) " \
                  "row format delimited fields terminated by \",\" stored as textfile".format(self.database,
                                                                                              self.table_name,
                                                                                              table_struct)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                self.logger("Failed to create Table %s in Hive Database %s!" % (self.database, self.table_name))
                raise Exception("Failed to create table")
        except Exception as exp:
            self.logger.error(exp)
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
        self.logger.info("Inserting Data")
        try:
            print("\nLoading data to file\n")
            cmd = "use {0};load data INPATH \"{1}\" INTO TABLE {2}".format(self.database, filepath, self.table_name)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                self.logger("Failed to insert data in table %s!" % self.table_name)
                raise Exception("Failed to insert data in table")

            cmd = 'use {0};alter table {1} set tblproperties("skip.header.line.count"="1");'.format(self.database,
                                                                                                    self.table_name)

            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()

        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def is_table_exist(self):
        self.logger.info("Checking if table exist")
        try:
            table_cmd = 'use {0};show tables;'.format(self.database)
            print("\nChecking table exist:\n")
            put = Popen(["hive", "-S", "-e", table_cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, err = put.communicate()
            out = self.hive_output_formatter(str(out))
            
            if self.table_name.lower() in out:
                return True
            return False
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def drop_hive_table(self):
        self.logger.info("Dropping Hive Table")
        try:
            cmd = 'use {0};drop table {1};'.format(self.database,self.table_name)
            print("\nDropping Table")
            put = Popen(["hive", "-S", "-e", cmd], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                self.logger("Failed to delete Table %s from Hive Database %s!" % (self.database, self.table_name))
                raise Exception("Failed to delete Table")
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    def get_data_from_hive_with_first_letter(self, first_letter):
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
        self.logger.info("Performing operation of V and not_V")
        query = "use {0};select * from {1} where name like '{2}%' or name like '{3}%'".format(self.database,
                                                                                              self.table_name,
                                                                                              first_letter,
                                                                                              first_letter.upper())
        not_query = "use {0};select * from {1} where name not like '{2}%' or name not like '{3}%'".format(self.database,
                                                                                                          self.table_name,
                                                                                                          first_letter,
                                                                                                          first_letter.upper())

        try:
            put = Popen(["hive", "-S", "-e", query], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                raise Exception("Failed to create table")
            message = self.hive_output_formatter(out.decode("utf-8"))
            
            put = Popen(["hive", "-S", "-e", not_query], stdin=PIPE, stdout=PIPE, bufsize=-1)
            out, exp = put.communicate()
            if "failed" in str(out).lower():
                raise Exception("Failed to create table")
            message2 = self.hive_output_formatter(out.decode("utf-8"))
            
            return (message, message2)
        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)
