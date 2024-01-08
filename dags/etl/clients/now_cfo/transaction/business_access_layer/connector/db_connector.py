"""
This Db_connector gets executed when any db operations are needed.

"""

from urllib.parse import quote
import pyodbc
from sqlalchemy import create_engine


class Db_Connector():
    def __init__(self, sql_driver, server_name, database_name, user_name, password):
        """
        Initializes a new instance of the class.

        Args:
            sql_driver (str): driver name
            server_name (str): server name
            database_name (str): name of the database
            user_name (str): user ID for database connection
            password (str): password for database connection
        """
        self.sql_driver = sql_driver
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password= password
    
    def connect(self, connection_engine="cursor"):
        """
        This function connects to the database either through cursor or engine
        Args:
            connection_engine (str, optional): engine name, _default_ : cursor.
        Descriptions:
            1. Check which db to be connected
            2. create database connection and return its object
        Returns:
            database_connection (object): cursor or engine object
        """
        # TODO: this will be later placed in model_dict and accessed with their properties
        # 1. Check which db to be connected
        if connection_engine.lower() == "cursor":
            try:
                conn = pyodbc.connect(f'Driver={self.sql_driver};'
                f'Server={self.server_name};'
                f'Database={self.database_name};'
                f'UID={self.user_name};'
                f'PWD={self.password};'
                'TrustServerCertificate=yes;'
                )
                # 2. create connection and return its object
                database_connection = conn.cursor()
                return database_connection
            except Exception as ex:
                print(f"Unable to fetch Database credentials with Engine name: {connection_engine}. \
                    Database connection failed. {ex}")
                return False
                # TODO: Raise Airflow Exception
        elif connection_engine.lower() == "engine":
            try:
                param = f'DRIVER={self.sql_driver};SERVER={self.server_name};DATABASE={self.database_name};UID={self.user_name};PWD={quote(self.password)};TrustServerCertificate=yes;'
                print(param)
                conn_str = "mssql+pyodbc:///?odbc_connect={}".format(param)
                database_connection = create_engine(conn_str, echo=False)
                # 2. create connection and return its object
                return database_connection
            except Exception as ex:
                print(f"Unable to fetch Database credentials with Engine name: {connection_engine}. \
                    Database connection failed. {ex}")
                return False
                # TODO: Raise Airflow Exception
        else:
            print("Database engine does not exists.")
            # TODO: Raise Airflow Exception
            return False
            
    
    
    def disconnect(self, database_connection):
        """
        This function commits and end the database connection.

        Args:
            database_connection (object): Database connection object.
        
        Descriptions:
            1. Commit the change in database.
            2. End/Close the database connection.
        """
        # 1. Commit the change in database.
        database_connection.commit()
        # 2. End/Close the database connection.
        database_connection.close()