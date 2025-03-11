import pandas as pd
from datetime import datetime
import os
import random
from random import randint


def generate_transactions(companies: list[str], dates: list[datetime]):
    if not dates or len(dates) < 2:
        print("Недостаточно дат для выбора покупки и продажи.")
        return None

    if len(companies) > len(dates) // 2:
        print(
            "Недостаточно дат для всех компаний. Каждая компания должна иметь как минимум одну покупку и одну продажу.")
        return None

        # Сортируем даты для обеспечения хронологии
    dates.sort()

    transactions = []
    for company in companies:
        if len(dates) >= 2:
            # Выбираем случайные даты для покупки и продажи
            purchase_index = random.randint(0, len(dates) - 2)
            sell_index = random.randint(purchase_index + 1, len(dates) - 1)

            purchase_date = dates.pop(purchase_index)
            sell_date = dates.pop(sell_index - 1)

            # Количество акций
            amount = random.randint(1, 1000)

            transactions.append({'trade_time': purchase_date, 'ticker': company, 'trade_type': 'buy', 'cnt_stock': amount})
            transactions.append({'trade_time': sell_date, 'ticker': company, 'trade_type': 'sell', 'cnt_stock': amount})

    # Создаем DataFrame
    transactions_df = pd.DataFrame(transactions)

    return transactions_df


def save_transactions_to_excel(df: pd.DataFrame, filename: str):
    """
    Сохраняет DataFrame с транзакциями в Excel-файл, дополняя существующие данные и избегая дублирования строк.

    Args:
        df: DataFrame с транзакциями.
        filename: Имя файла Excel.
    """
    try:
        # Проверяем, существует ли файл
        try:
            existing_df = pd.read_excel(filename)
            print("Существующие данные в файле:\n", existing_df)
        except FileNotFoundError:
            existing_df = pd.DataFrame()
            print("Файл не найден. Будет создан новый файл.")

        # Объединяем существующий DataFrame с новым
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        # Удаляем дублирующие строки по полям 'dt', 'company', 'transaction_type'
        combined_df = combined_df.drop_duplicates(subset=['trade_time', 'ticker', 'trade_type'])

        print("Объединённые данные:\n", combined_df)

        # Сохраняем обновлённый DataFrame в Excel
        combined_df.to_excel(filename, index=False)
        print(f"Транзакции успешно сохранены в файл: {filename}")
    except Exception as e:
        print(f"Ошибка при сохранении в Excel-файл: {e}")



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
    df = generate_transactions(companies=stocks, dates=dates)
    filename = 'transactions.xlsx'
    save_transactions_to_excel(df, filename)
