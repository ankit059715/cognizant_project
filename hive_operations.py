from pyhive import hive


class HiveOperations:
    def __init__(self, hostname, port, user, password, database):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cur = None
        self.table_name = None

    def connect_hive_db(self):
        """
        Connects to Hive database
        Raises:
            Exception:
                If any problem while connecting to Hive Database
        """
        self.conn = hive.Connection(host=self.hostname,
                                    port=self.port,
                                    username=self.user,
                                    password=self.password,
                                    database=self.database,
                                    auth='CUSTOM')
        self.cur = self.conn.cursor()

    def create_hive_table(self, table_name):
        """
        Creates a table in hive database
        Args:
            table_name(str) ---> Name of table to create
        Raises:
            Exception:
                If any issue when creating table
        """
        self.table_name = table_name

    def insert_data_to_table_from_file(self, filepath):
        """
        Insert data into table created with file content
        Args:
            filepath(str) ---> Hadoop filepath to upload data from
        Raises:
            Exception:
                If any issue when loading data from file
        """
        pass

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
        query = 'select * from {0} where name like "%{1}" or name like "%{2}"'.format(self.table_name,
                                                                                      first_letter,
                                                                                      first_letter.upper())
        self.cur.execute(query)
        result = self.cur.fetchall()
        return result

    def close_connections(self):
        self.cur.close()
        self.conn.close()
