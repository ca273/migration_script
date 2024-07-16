from psycopg2 import extras
from settings import config


class UserRepository:
    """ DB queries for users part of the application """

    @classmethod
    def create_users_table(cls, cursor):
        # here it is ok to use f-string to do concatenation for table name
        # as we provide the value, not an outside user (no sql-injections)
        query = (f"""
            CREATE TABLE {config.USERS_TABLE_NAME} (
                first_name VARCHAR(60),
                last_name VARCHAR(60),
                address VARCHAR(60),
                age INT
            )
        """)
        cursor.execute(query)

    @classmethod
    def users_table_exists(cls, cursor, table_name="users") -> bool:
        # Check if the table exists in the database
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))
        return cursor.fetchone()[0]

    @classmethod
    def bulk_insert(cls, cursor, chuck):
        """
        data = [
            ('John Doe', 28),
            ('Jane Smith', 32),
            ('Mike Johnson', 25)
        ]
        :param db_connection:
        :param chuck:
        :return:
        """
        insert_query = f"INSERT INTO {config.USERS_TABLE_NAME} (first_name, last_name, address, age) VALUES %s"
        extras.execute_values(cursor, insert_query, chuck)
