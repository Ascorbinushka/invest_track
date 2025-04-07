from typing import Any
from sqlalchemy import create_engine
from pydantic_settings import BaseSettings
import psycopg2
import dotenv

dotenv.load_dotenv()


class Settings(BaseSettings):
    PG_HOST: str
    PG_PORT: int
    PG_USER: str
    PG_PASSWORD: str
    PG_DATABASE: str

    COMPANIES: str
    API_KEY: str

    START_DATE: str
    END_DATE: str

    def gp_engine(self) -> Any | None:
        db_string = f"postgresql+psycopg2://{self.PG_USER}:{self.PG_PASSWORD}@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"

        try:
            engine = create_engine(db_string)  # Создаем движок SQLAlchemy
            # engine.connect()  # Проверяем подключение (необязательно)
            print("Движок SQLAlchemy успешно создан.")
            return engine
        except Exception as e:
            print(f"Ошибка при создании движка SQLAlchemy: {e}")
            return None


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
            self.connection = psycopg2.connect(database=settings.PG_DATABASE, user=settings.PG_USER,
                                               password=settings.PG_PASSWORD,
                                               host=settings.PG_HOST, port=settings.PG_PORT)
            DatabaseConnection.__instance = self

    def get_connection(self):
        return self.connection


if __name__ == '__main__':
    db_connection = DatabaseConnection.get_instance()
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
