import os
import dotenv


class DBConfig:
    """ Class for loading and providing database configuration from environment variables."""
    def __init__(self):
        """ Initializes the DBConfig object and loads environment variables from the .env file."""
        dotenv.load_dotenv()

        # path_to_env = os.path.join(os.getcwd(), '.env')
        # dotenv.load_dotenv(dotenv_path=path_to_env)
    def get_dbconfig(self):
        """ Returns a dictionary with the database configuration. """
        dbconfig = {
            'host': os.getenv("HOST"),
            'user': os.getenv("USER"),
            'password': os.getenv("PASSWORD"),
            'database': os.getenv("DATABASE"),
            'charset': os.getenv("CHARSET"),
        }
        return dbconfig
