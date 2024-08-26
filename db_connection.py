import mysql.connector
from mysql.connector import Error
import logging
import config


class DatabaseConnection:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=config.DB_HOST,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME
            )
            if self.connection.is_connected():
                logging.info("Connected to the database")
        except Error as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")

    def get_cursor(self):
        if not self.connection or not self.connection.is_connected():
            self.connect()
        return self.connection.cursor()

    def commit(self):
        if self.connection and self.connection.is_connected():
            self.connection.commit()
        else:
            logging.warning("No active database connection to commit.")

    def __enter__(self):
        if not self.connection:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


