import mysql.connector
from mysql.connector import Error


def sql_connection(host, user, passwd, database = None):
    '''Connects to MySQL'''

    connection = None

    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
        print("MySQL Database connection successful")

    except Error as err:
        print(f"Error: '{err}'")

    return connection