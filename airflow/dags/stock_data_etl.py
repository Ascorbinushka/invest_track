import pandas as pd
from sqlalchemy import create_engine
import time
import csv
from io import StringIO
from equity_data_extractor.base_config import Stocks, DatabaseConnection
from equity_data_extractor.config import Settings

import os
import json


def save_ticker_to_json():
    db_connection = DatabaseConnection.get_instance()
    rows = Stocks(db_connection).get_stock_id_stock()
    data = [{"ticker": row[1], "ticker_id": row[0]} for row in rows]
    filename = 'stocks.json'

    # 4. Сохранение в JSON-файл
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)  # indent для красивого форматирования

    print(f"Данные ticker и ticker_id успешно выгружены в файл: {filename}")


def replace_ticker_with_id_from_json(df: pd.DataFrame, json_file: str) -> pd.DataFrame:
    """
    Заменяет столбец 'ticker' на 'ticker_id' в DataFrame, используя соответствие из JSON-файла.
    """

    # Загружаем JSON и создаем словарь
    with open(json_file, 'r', encoding='utf-8') as f:
        ticker_mapping = json.load(f)
    ticker_dict = {item['ticker'].strip().upper(): item['ticker_id'] for item in ticker_mapping}

    # Очистка данных в DataFrame
    df['ticker'] = df['ticker'].str.strip().str.upper()

    # Отображение тикеров на ID
    df['ticker_id'] = df['ticker'].map(ticker_dict)

    # Проверка отсутствующих тикеров
    if df['ticker_id'].isna().any():
        missing_tickers = df[df['ticker_id'].isna()]['ticker'].unique()
        print(f"Предупреждение! Следующие тикеры не найдены в JSON: {missing_tickers}")

    # Удаление строк с отсутствующими ID и преобразование в int
    df = df.dropna(subset=['ticker_id'])
    df['ticker_id'] = df['ticker_id'].astype(int)

    # Удаление столбца 'ticker'
    df = df.drop('ticker', axis=1)
    return df


def df_to_db(df, schema, table, force=False):
    def psql_insert_copy(table, conn, keys, data_iter):  #функция передаваемая в мотод
        # получаем подключение DBAPI, которое может обеспечить курсор
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    engine = Settings().gp_engine()  # gp_engine функция возвращающая движок от psycopg2
    start_time = time.time()  # получаем время начала до вставки
    df.to_sql(con=engine,
              index=False,
              schema=schema,
              name=table,
              if_exists='append',
              method=psql_insert_copy)
    end_time = time.time()  # получаем время окончания после вставки
    total_time = end_time - start_time  # вычисляем время
    print(f"Время вставки: {total_time} секунд")  # печатаем время


if __name__ == '__main__':
    save_ticker_to_json()
    target_folder = "equity_data_extractor"
    target_file = 'transactions.csv'
    current_directory = os.path.dirname(os.path.abspath(__file__))
    grandparent_dir = os.path.dirname(os.path.dirname(current_directory))
    # Добавляем целевую папку и имя файла
    target_dir = os.path.join(grandparent_dir, target_folder)
    file_path = os.path.join(target_dir, target_file)
    data = os.path.abspath(file_path)
    df = pd.read_csv(data)
    file_path = 'stocks.json'

    transactions_df = replace_ticker_with_id_from_json(df=df, json_file=file_path)
    print(transactions_df)
    df_to_db(df=transactions_df, schema='financial_models', table='trade_execution')
