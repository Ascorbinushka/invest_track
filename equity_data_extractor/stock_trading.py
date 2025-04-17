import pandas as pd
from datetime import datetime
import random


def generate_transactions(companies: list[str], dates: list[datetime], sell_probability: float = 0.65):
    """
        Генерирует DataFrame транзакций (покупка и продажа) с учетом хронологического порядка дат и вероятности продажи.

        Args:
            companies: Список тикеров акций (строки).
            dates: Список объектов datetime (отсортированных по возрастанию).
            user_id: Идентификатор пользователя (int).
            amount: Количество акций в сделке (int).
            sell_probability: Вероятность продажи (от 0.0 до 1.0).  По умолчанию 0.65.

        Returns:
            pandas.DataFrame: DataFrame с транзакциями.
        """
    transactions = []
    num_dates = len(dates)


    for current_company in companies:
        start_index = random.randint(0, num_dates - 1)
        start_date = dates[start_index]
        amount = random.randint(1, 1000)
        user_id = random.randint(1, 93951)

        # Добавляем запись о покупке
        transactions.append({
            'ticker': current_company,
            'cnt_stock': amount,
            'trade_time': start_date,
            'users_id': user_id,
            'trade_type': 'buy'
        })

        # Проверяем вероятность продажи
        if random.random() < sell_probability:
            # Выбираем случайный индекс даты продажи (не раньше start_index)
            end_index = random.randint(start_index, num_dates - 1)
            end_date = dates[end_index]

            # Добавляем запись о продаже
            transactions.append({
                'ticker': current_company,
                'cnt_stock': amount,
                'trade_time': end_date,
                'users_id': user_id,
                'trade_type': 'sell'
            })

    transactions_df = pd.DataFrame(transactions)
    return transactions_df


def load_existing_transactions(filename: str) -> pd.DataFrame:
    """Загружает существующие транзакции из CSV-файла или возвращает пустой DataFrame, если файл не найден."""
    try:
        existing_df = pd.read_csv(filename)
        print("Существующие данные в файле:\n", existing_df)
        return existing_df
    except FileNotFoundError:
        print("Файл не найден. Будет создан новый файл.")
        return pd.DataFrame()


def find_and_remove_duplicates(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """Находит и удаляет дубликаты из новых данных на основе существующих данных."""
    if existing_df.empty:
        print("Существующие данные отсутствуют. Дубликаты не ищутся.")
        return new_df

    duplicates = new_df.merge(existing_df,
                              on=['trade_time', 'ticker', 'trade_type'],
                              how='inner',
                              suffixes=('_new', '_existing'))

    if not duplicates.empty:
        print("Найдены дубликаты. Удаляем их из новых данных.")
        new_df = new_df.drop(duplicates.index)
        print("Новые данные после удаления дубликатов:\n", new_df)
    else:
        print("Дубликаты не найдены.")
    return new_df


def generate_trade_ids(df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """Генерирует trade_id для новых данных, обеспечивая последовательную нумерацию."""
    if df.empty:
        print("Нет новых данных для генерации trade_id.")
        return df

    if 'trade_id' in existing_df.columns:
        max_id = existing_df['trade_id'].max()
        max_id = 0 if pd.isna(max_id) else int(max_id)  # Handle NaN
    else:
        max_id = 0

    start_id = int(max_id + 1)
    df['trade_id'] = range(start_id, start_id + len(df))
    print("Новые данные с сгенерированным trade_id:\n", df)
    return df


def combine_dataframes(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """Объединяет новые данные с существующими данными."""
    if new_df.empty:
        print("Новые данные отсутствуют. Возвращаются существующие данные.")
        return existing_df

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    print("Объединённые данные:\n", combined_df)
    return combined_df


def save_transactions_to_csv(df: pd.DataFrame, filename: str):
    """
    Сохраняет DataFrame с транзакциями в CSV-файл.

    Args:
        df: DataFrame для сохранения.
        filename: Имя CSV-файла.
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Транзакции успешно сохранены в файл: {filename}")
    except Exception as e:
        print(f"Ошибка при сохранении в CSV-файл: {e}")


def process_and_save_transactions(df: pd.DataFrame, filename: str):
    """
    Координирует процесс загрузки, обработки и сохранения транзакций.

    Args:
        df: DataFrame с новыми транзакциями.
        filename: Имя CSV-файла.
    """
    try:
        existing_df = load_existing_transactions(filename)
        df_remove_duplicates = find_and_remove_duplicates(df, existing_df)
        df = generate_trade_ids(df_remove_duplicates, existing_df)
        combined_df = combine_dataframes(existing_df, df)
        save_transactions_to_csv(combined_df, filename)
    except Exception as e:
        print(f"Ошибка в процессе обработки и сохранения транзакций: {e}")


if __name__ == '__main__':
    stocks = ["AAPL", "MSFT", "GOOG"]
    dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 5),
        datetime(2023, 1, 10),
        datetime(2023, 1, 15),
        datetime(2023, 1, 20),
        datetime(2023, 1, 25),
        datetime(2023, 2, 1)

    ]
    print(stocks * len(dates))
    df = generate_transactions(companies=stocks * len(dates), dates=dates)
    print(df)
    filename = 'transactions.csv'
    process_and_save_transactions(df=df, filename=filename)
