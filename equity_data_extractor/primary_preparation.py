from config import DatabaseConnection, Settings
from get_yf_data import get_data_company
from datetime import datetime, timedelta
from base_config import Stocks, StocksDaily, Generate_DDL
from stock_trading import generate_transactions, save_transactions_to_excel

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
    print(DATA)

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
            print(daily_data.date)
            StocksDaily(db_connection).insert_stocks_daily(
                ticker_id=ticker_id,
                date=daily_data.date,
                open=daily_data.open,
                high=daily_data.high,
                low=daily_data.low,
                close=daily_data.close
            )


def main(COMPANIES: list[str], start_date: str, end_date: str, API_KEY) -> None:
    """Загрузка первичных данных и генерация тестовых транзакций"""
    Generate_DDL(db_connection).create_postgres_table("DDL.sql")

    # generation_stock_prices(COMPANIES=COMPANIES, start_date=start_date, end_date=end_date, API_KEY=API_KEY)
    stocks_daily = StocksDaily(db_connection)

    df = generate_transactions(companies=COMPANIES, dates=[date[0] for date in stocks_daily.get_date()])
    filename = 'transactions.xlsx'
    save_transactions_to_excel(df, filename)


if __name__ == '__main__':
    # COMPANIES = Settings().COMPANIES
    # companies_list = COMPANIES.strip("[]").replace(" ", "").split(",")
    #
    # API_KEY = 'aQwbfnw3sTcopw8gl2D6EE83uWAGYNWb'
    #
    # start_date = (datetime.today() - timedelta(days=100)).strftime("%Y-%m-%d")
    # end_date = datetime.today().strftime("%Y-%m-%d")
    # main(COMPANIES=companies_list, start_date=start_date, end_date=end_date, API_KEY=API_KEY)
    from pyspark.sql import SparkSession
    import pandas as pd
    import os

    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    # spark = SparkSession.builder \
    #     .master("local[*]") \
    #     .appName('PySpark_Tutorial') \
    #     .getOrCreate()
    # excel_file = 'transactions.xlsx'
    # pandas_df = pd.read_excel(excel_file)
    # spark_df = spark.createDataFrame(pandas_df)
    # # Чтение CSV файла
    # spark_df.printSchema()
    # print(spark_df.show(5))

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    print(spark)
