import os
from pydantic_settings import BaseSettings, SettingsConfigDict
import psycopg2
import dotenv

dotenv.load_dotenv()


# ENV_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
# print(ENV_PATH)

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str

    COMPANIES: str
    # model_config = SettingsConfigDict(env_file=ENV_PATH,
    #                                   extra='ignore')


try:
    settings = Settings()
except Exception as e:
    exit(f"error in module: {__name__}: {e}")


class DatabaseConnection:
    __instance = None

    @staticmethod
    def get_instance():
        if not DatabaseConnection.__instance:
            DatabaseConnection()
        return DatabaseConnection.__instance

    def __init__(self):
        if DatabaseConnection.__instance:
            raise Exception("Этот класс является Singleton, используйте метод get_instance()")
        else:
            self.connection = psycopg2.connect(database=settings.DB_NAME, user=settings.DB_USER,
                                               password=settings.DB_PASS,
                                               host=settings.DB_HOST, port=settings.DB_PORT)
            DatabaseConnection.__instance = self

    def get_connection(self):
        return self.connection


if __name__ == '__main__':
    db_connection = DatabaseConnection.get_instance()
    print(db_connection)
    query = (
        f"""
                select 
                    *
                from jaffle_shop.orders
                """
    )
    with db_connection.get_connection().cursor() as cursor:
        cursor.execute(query)
        res = cursor.fetchall()
    print(res)
