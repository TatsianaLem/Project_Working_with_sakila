import pymysql
import logging
from pymysql.cursors import DictCursor


class DBConnection:
    """
        Class for managing the connection to the database.
        Supports connection, executing queries, and logging.
    """
    def __init__(self, dbconfig: dict, log_file: str):
        self._dbconfig = dbconfig
        self._connection = None
        self._cursor = None
        self._setup_logging(log_file)
        self._connect()

    def _setup_logging(self, log_file: str):
        """
            Configures the logging system.
        """
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(filename=log_file, level=logging.INFO,
                                format="%(asctime)s - %(levelname)s - %(message)s")

    def _connect(self):
        """
            Creates a connection to the database and a cursor. Logs errors in case of a failed connection.
        """
        try:
            self._connection = pymysql.connect(**self._dbconfig, cursorclass=DictCursor)
            self._cursor = self._connection.cursor()
        except pymysql.Error as e:
            logging.error(f"Database connection error: {e}")
            self._connection = None

    def execute_query(self, query: str, params=None):
        """Executes an SQL query and returns the result (if any)."""

        if not self._connection or not self._connection.open:
            logging.warning("No connection available, attempting to reconnect...")
            self._connect()

        if not self._connection:
            logging.error("Failed to restore the connection to the database.")
            return None

        self.log_query(query)
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().upper().startswith("SELECT"):
                    return cursor.fetchall()
                self._connection.commit()
        except pymysql.Error as e:
            logging.error(f"Error executing the query: {e}")
            self._connection.rollback()

    def log_query(self, query: str):
        """ Logs the executed SQL query. """
        logging.info(f"SQL Query: {query}")

    def get_connection(self):
        """ Returns the current connection to the database, reconnecting if necessary. """
        if not self._connection or not self._connection.open:
            self._connect()
        return self._connection

    def get_cursor(self):
        """ Returns a new cursor for each call. """
        if not self._connection or not self._connection.open:
            self._connect()
        return self._connection.cursor() if self._connection else None

    def close(self):
        """ Closes the connection to the database and the cursor."""
        if self._cursor:
            self._cursor.close()
        if self._connection and self._connection.open:
            self._connection.close()

    def __enter__(self):
        """ Method for using the class in a context manager (with). """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
            Method for properly exiting the context manager.
            Closes the connection when exiting the with block.
        """
        self.close()