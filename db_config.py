import os
import dotenv


class DBConfig:
    """ Класс для загрузки и предоставления конфигурации базы данных из переменных окружения."""
    def __init__(self):
        """ Инициализирует объект DBConfig и загружает переменные окружения из файла .env."""
        dotenv.load_dotenv()

        # path_to_env = os.path.join(os.getcwd(), '.env')
        # dotenv.load_dotenv(dotenv_path=path_to_env)
    def get_dbconfig(self):
        """ Возвращает словарь с конфигурацией базы данных. """
        dbconfig = {
            'host': os.getenv("HOST"),
            'user': os.getenv("USER"),
            'password': os.getenv("PASSWORD"),
            'database': os.getenv("DATABASE"),
            'charset': os.getenv("CHARSET"),
        }
        return dbconfig
