from config import DatabaseConnection, Settings
from get_yf_data import get_data_company
from datetime import datetime, timedelta
from base_config import Stocks, StocksDaily, Generate_DDL, Users
from stock_trading import generate_transactions, save_transactions_to_csv
import csv
import os

db_connection = DatabaseConnection.get_instance()


def generation_stock_prices(COMPANIES: list[str], start_date: str, end_date: str, API_KEY) -> None:
    """
    Генерация первичных данных котировок акций API и загрузка их в БД.

    :param COMPANIES: Список тикеров компаний.
    :param start_date: Начальная дата для запроса котировок (YYYY-MM-DD).
    :param end_date: Конечная дата для запроса котировок (YYYY-MM-DD).
    :param API_KEY: Ключ API для доступа к данным.
    :return: None
    """

    DATA = get_data_company(COMPANIES=COMPANIES, start_date=start_date, end_date=end_date, API_KEY=API_KEY)

    for company_data in DATA:
        ticker = company_data.symbol
        id_stock = Stocks(db_connection).get_id_stock(ticker)

        # Если тикер не найден в базе, добавляем его.  Выносим добавление тикера за цикл.
        if not id_stock:
            Stocks(db_connection).insert_stock(ticker)
            id_stock = Stocks(db_connection).get_id_stock(ticker)
            if not id_stock:  #Обработка ошибки, если не удалось добавить акцию.
                print(f"Error: Failed to insert stock {ticker} into the database.")
                continue  #Переходим к следующей компании.

        ticker_id = id_stock[0]  #Извлекаем ID  (предполагается, что get_id_stock возвращает список или кортеж)

        for daily_data in company_data.historical:
            StocksDaily(db_connection).insert_stocks_daily(
                ticker_id=ticker_id,
                date=daily_data.date,
                open=daily_data.open,
                high=daily_data.high,
                low=daily_data.low,
                close=daily_data.close
            )


def main() -> None:
    """Загрузка первичных данных и генерация тестовых транзакций"""
    # Загрузка пользователей
    # current_directory = os.path.dirname(os.path.abspath(__file__))
    # csv_file = os.path.join(current_directory, 'users.csv')
    # with open(csv_file, 'r', encoding='utf-8') as f:
    #     csv_reader = csv.DictReader(f)  # Используем DictReader для доступа к данным по названию колонок
    #     user_load = Users(db_connection)  # Инициализируем объект Users
    #     for row in csv_reader:
    #         # Извлекаем значения из текущей строки CSV-файла
    #         first_name = row["first_name"]
    #         last_name = row['last_name']
    #         email = f"{row["first_name"].lower()}_{row['last_name'].lower()}@gmail.com"
    #         country = row['country']
    #         data = user_load.get_user(first_name=first_name, last_name=last_name)
    #         if data is None:
    #             user_load.insert_user(first_name=first_name, last_name=last_name, email=email, country=country)

    # # Генерация данных акций и загрузка их в БД
    COMPANIES = Settings().COMPANIES
    # API_KEY = Settings().API_KEY
    companies_list = COMPANIES.split(",")
    #
    # start_date = (datetime.today() - timedelta(days=2000)).strftime("%Y-%m-%d")
    # end_date = datetime.today().strftime("%Y-%m-%d")
    #
    # generation_stock_prices(COMPANIES=companies_list, start_date=start_date, end_date=end_date, API_KEY=API_KEY)
    #
    # Генерация транзакций
    stocks_daily = StocksDaily(db_connection)

    df = generate_transactions(companies=companies_list*30000, dates=[date[0] for date in stocks_daily.get_date()])
    current_directory = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(current_directory, 'transactions.csv')
    # Сохранение транзакций в csv
    save_transactions_to_csv(df, filename)


if __name__ == '__main__':
    main()
