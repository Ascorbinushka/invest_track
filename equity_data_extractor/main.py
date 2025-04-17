from config import Settings, DatabaseConnection
from fmp_data_provider import get_data_company
from base_config import Stocks, StocksDaily, Users
from stock_trading import generate_transactions, process_and_save_transactions
import csv
import os
from typing import List
from global_gender_predictor import GlobalGenderPredictor
from datetime import datetime
import random


def _load_stock_data(db_connection, companies: List[str], start_date: str, end_date: str, api_key: str) -> None:
    """Загружает данные об акциях в базу данных."""
    # Получение данных об акциях
    data = get_data_company(COMPANIES=companies, start_date=start_date, end_date=end_date, API_KEY=api_key)
    for company_data in data:
        ticker = company_data.symbol
        # Поиск id тикер акции
        id_stock = Stocks(db_connection).get_id_stock(ticker)
        # Если тикер не найден в базе, добавляем его. Выносим добавление тикера за цикл.
        if not id_stock:
            Stocks(db_connection).insert_stock(ticker)
            id_stock = Stocks(db_connection).get_id_stock(ticker)
            if not id_stock:  # Обработка ошибки, если не удалось добавить акцию.
                print(f"Error: Failed to insert stock {ticker} into the database.")
                continue  # Переходим к следующей компании.

        ticker_id = id_stock[0]  # Извлекаем ID (предполагается, что get_id_stock возвращает список или кортеж)
        # Загрузка данных в бд
        for daily_data in company_data.historical:
            StocksDaily(db_connection).insert_stocks_daily(
                ticker_id=ticker_id,
                date=daily_data.date,
                open=daily_data.open,
                high=daily_data.high,
                low=daily_data.low,
                close=daily_data.close
            )


def generate_random_birthdate(start_year=1980, end_year=2000):
    """
    Генерирует случайную дату рождения в заданном диапазоне лет
    с использованием datetime и random, преобразуя даты в timestamp.

    Returns:
        Строка с датой рождения в формате YYYY-MM-DD.
    """
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31, 23, 59, 59)  # Включаем последний день года

    # Преобразуем даты в timestamp (количество секунд с начала эпохи)
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    # Генерируем случайный timestamp в заданном диапазоне
    random_timestamp = random.randint(start_timestamp, end_timestamp)

    # Преобразуем timestamp обратно в datetime
    random_date = datetime.fromtimestamp(random_timestamp)
    return random_date.strftime("%Y-%m-%d")


def _load_users_from_csv(db_connection, csv_file: str) -> None:
    """Загружает данные о пользователях из CSV-файла в базу данных."""
    user_load = Users(db_connection)  # Инициализируем объект Users
    predictor = GlobalGenderPredictor()
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)  # Используем DictReader для доступа к данным по названию колонок
            for row in csv_reader:
                # Извлекаем значения из текущей строки CSV-файла
                first_name = row["first_name"].strip()
                last_name = row['last_name'].strip()
                email = f"{first_name.lower()}_{last_name.lower()}@gmail.com"
                country = row['country'].strip()
                gender = predictor.predict_gender(row["first_name"].strip())
                birthdate = generate_random_birthdate()
                print(birthdate)
                # Проверяем, существует ли пользователь
                if not user_load.get_user(first_name=first_name, last_name=last_name):
                    # Добавляем нового пользователя
                    user_load.insert_user(
                        first_name=first_name,
                        last_name=last_name,
                        email=email,
                        country=country,
                        gender=gender,
                        birthdate=birthdate
                    )
    except FileNotFoundError:
        print(f"Error: File not found: {csv_file}")
    except Exception as e:
        print(f"Error loading users: {e}")


def _generate_and_save_transactions(db_connection, companies: List[str], csv_file_transactions: str,
                                    num_transactions: int = 10000) -> None:
    """
        Генерирует искусственные транзакции, сохраняет их в CSV-файл.

        Args:
            db_connection: Объект подключения к базе данных.
            companies: Список тикеров акций.
            num_transactions: Количество транзакций на каждую компанию.
        """
    try:
        stocks_daily = StocksDaily(db_connection=db_connection)
        df = generate_transactions(companies=companies * num_transactions,
                                   dates=[date[0] for date in stocks_daily.get_date()])
        process_and_save_transactions(df, csv_file_transactions)
    except Exception as e:
        print(f"Error generating and saving transactions: {e}")


def main():
    db_connection = DatabaseConnection.get_instance()
    # Список компаний из переменной окружения
    settings = Settings()
    companies = settings.COMPANIES
    companies_list = companies.split(",")
    # Ключ
    api_key = settings.API_KEY
    # start_date
    start_date = settings.START_DATE
    # end_date
    end_date = settings.END_DATE
    current_directory = os.path.dirname(os.path.abspath(__file__))

    try:
        # # 1. Генерация данных об акциях
        # _load_stock_data(db_connection, companies_list, start_date, end_date, api_key)
        #
        # 2. Загрузка users из Excel
        csv_file = os.path.join(current_directory, 'users.csv')

        _load_users_from_csv(db_connection, csv_file)

        # 3. Генерация искусственных транзакций и сохранение в csv
        # filename = os.path.join(current_directory, 'transactions.csv')
        # Сохранение транзакций в csv
        # _generate_and_save_transactions(db_connection=db_connection, companies=companies_list,
        #                                 csv_file_transactions=filename)
    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()
