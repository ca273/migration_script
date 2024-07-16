import psycopg2
from psycopg2 import Error


class DBManagerBase:
    """ Initiate a DB connection for use in various repositories """

    def __init__(self, db_user: str, password: str, host: str, port: int, db_name: str):
        self.db_user = db_user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name

    def create_conn(self):
        try:
            connection = psycopg2.connect(
                user=self.db_user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db_name
            )
            return connection
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return None

    def test_conn(self, conn) -> bool:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("Connected to", db_version)
        cursor.close()
        return True

    def close_connection(self, connection):
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")
