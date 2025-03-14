from typing import Any

from equity_data_extractor.config import DatabaseConnection
import psycopg2


class Generate_DDL:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def create_postgres_table(self, ddl_file_name: str) -> None:
        """Создает таблицы из DDL скрипта."""
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                with open(ddl_file_name, "r") as file:
                    cursor.execute(file.read())
            self.db_connection.get_connection().commit()
            print(f"Таблицы из {ddl_file_name} успешно созданы.")

        except psycopg2.Error as e:
            self.db_connection.get_connection().rollback()
            print(f"Ошибка при создании таблиц: {e}")


class Stocks:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_id_stock(self, ticker: str) -> str | None:
        """Получает ticker_id для заданного ticker."""
        query = "SELECT ticker_id FROM financial_models.stocks WHERE ticker = %s"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query, (ticker,))
                return cursor.fetchone()
        except psycopg2.Error as e:
            print(f"Ошибка при получении ticker_id: {e}")
            return None

    def insert_stock(self, ticker: str) -> None:
        """Вставляет новый ticker в таблицу stocks."""
        query = "INSERT INTO financial_models.stocks (ticker) VALUES (%s)"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query, (ticker,))
            self.db_connection.get_connection().commit()
            print(f"Вставлен новый ticker: {ticker}")
        except psycopg2.Error as e:
            self.db_connection.get_connection().rollback()
            print(f"Ошибка при вставке ticker: {e}")

    def get_stock_id_stock(self):
        """Получает ticker_id и ticker."""
        query = "SELECT ticker_id, ticker FROM financial_models.stocks"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetchall()
                return res
        except psycopg2.Error as e:
            print(f"Ошибка при получении ticker_id: {e}")
            return None


class StocksDaily:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def insert_stocks_daily(self, ticker_id: str, date: str, open: float, high: float, low: float,
                            close: float) -> None:
        """Вставляет данные котировок в таблицу stocks_daily по одной строке."""
        query = """
                INSERT INTO financial_models.stocks_daily (ticker_id, date, open, high, low, close)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker_id, date) DO NOTHING;
            """
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query, (ticker_id, date, open, high, low, close))
                row_count = cursor.rowcount
            self.db_connection.get_connection().commit()
            if row_count > 0:
                print(f"Вставлена запись в stocks_daily для ticker_id: {ticker_id} на дату: {date}")
            else:
                print(
                    f"Запись для ticker_id: {ticker_id} на дату: {date} уже существует и не была добавлена (ON CONFLICT).")
        except psycopg2.Error as e:
            self.db_connection.get_connection().rollback()
            print(f"Ошибка при вставке данных в stocks_daily: {e}")

    def get_date(self):
        query = "SELECT date FROM financial_models.stocks_daily"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query)
                dates = cursor.fetchall()
                return dates
        except psycopg2.Error as e:
            print(f"Ошибка при получении ticker_id: {e}")
            return None


class Users:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def insert_user(self, first_name: str, last_name: str, email: str, country: str) -> None:
        """Вставляем пользователей из csv"""
        query = "INSERT INTO financial_models.users (first_name, last_name, email, country) VALUES (%s, %s, %s, %s) ON CONFLICT (first_name, last_name) DO NOTHING"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query, (first_name, last_name, email, country))
                row_count = cursor.rowcount
            self.db_connection.get_connection().commit()
            if row_count > 0:
                print(f"Вставлен новый пользователь: {first_name}, {last_name},{email},{country}")
            else:
                print(f"Такой пользователь уже есть: {first_name}, {last_name}")
        except psycopg2.Error as e:
            self.db_connection.get_connection().rollback()
            print(f"Ошибка при вставке пользователя: {first_name}, {last_name},{email},{country}. Ошибка {e}")

    def get_user(self, first_name: str, last_name: str):
        query = f"SELECT users_id FROM financial_models.users WHERE first_name = %s AND last_name = %s"
        try:
            with self.db_connection.get_connection().cursor() as cursor:
                cursor.execute(query, (first_name, last_name))
                res = cursor.fetchall()
                if res:
                    return res
                else:
                    return None
        except psycopg2.Error as e:
            print(f"Данные уже вставлены: {first_name}, {last_name}")
            return None


if __name__ == '__main__':
    db_connection = DatabaseConnection.get_instance()
    ddl_generator = Generate_DDL(db_connection)
    ddl_generator.create_postgres_table("DDL.sql")
    stocks_daily = StocksDaily(db_connection)
    print([date[0] for date in stocks_daily.get_date()])
