import pandas as pd
from datetime import datetime
import os
import random
from random import randint


def generate_transactions1(companies: list[str], dates: list[datetime]):
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
    num_transactions = len(dates)  # Определяем, что записей будет примерно половина всех дат

    for i in range(num_transactions):
        # Проверяем, что ещё есть доступные компании и даты
        if len(companies) > i and len(dates) >= 2:
            # Берём первую доступную дату для покупки
            purchase_date = dates.pop(0)
            # Берём последнюю доступную дату для продажи
            sell_date = dates.pop(-1)

            # Количество акций
            amount = random.randint(1, 1000)

            user_id = random.randint(1, 93951)

            transactions.append(
                {'ticker': companies[i], 'cnt_stock': amount, 'trade_time': purchase_date, 'user_id': user_id,
                 'trade_type': 'buy'})
            transactions.append(
                {'ticker': companies[i], 'cnt_stock': amount, 'trade_time': sell_date, 'user_id': user_id,
                 'trade_type': 'sell'})

    # Создаем DataFrame
    transactions_df = pd.DataFrame(transactions)
    # Удаляем половину транзакций с типом "sell"
    sell_indices = transactions_df[transactions_df['trade_type'] == 'sell'].index.tolist()
    num_to_remove = len(sell_indices) // 2
    remove_indices = random.sample(sell_indices, num_to_remove)
    transactions_df = transactions_df.drop(remove_indices)
    return transactions_df
def generate_transactions(companies: list[str], dates: list[datetime]):
    # Сортируем даты для обеспечения хронологии
    dates.sort()
    transactions = []

    while len(dates) > 0 and len(companies) > 0:
        # Берём первую дату для покупки
        purchase_date = dates.pop(0)

        # Проверяем, есть ли третья дата для продажи
        sell_date = None
        if len(dates) > 2:  # Достаточно дат для +3
            sell_date = dates.pop(2)  # Убираем третью дату в списке

        # Для текущей компании создаём транзакции
        current_company = companies.pop(0)  # Убираем текущую компанию из списка

        # Случайные параметры для транзакции
        amount = random.randint(1, 1000)
        user_id = random.randint(1, 93951)

        # Добавляем запись о покупке
        transactions.append({
            'ticker': current_company,
            'cnt_stock': amount,
            'trade_time': purchase_date,
            'user_id': user_id,
            'trade_type': 'buy'
        })

        # Добавляем запись о продаже, если есть дата продажи
        if sell_date:
            transactions.append({
                'ticker': current_company,
                'cnt_stock': amount,
                'trade_time': sell_date,
                'user_id': user_id,
                'trade_type': 'sell'
            })

    # Создаём DataFrame из транзакций
    transactions_df = pd.DataFrame(transactions)
    return transactions_df

    # transactions = []
    # num_transactions = len(dates)  # Определяем, что записей будет примерно половина всех дат
    #
    # for i in range(num_transactions):
    #     # Проверяем, что ещё есть доступные компании и даты
    #     if len(companies) > i and len(dates) >= 2:
    #         # Берём первую доступную дату для покупки
    #         purchase_date = dates.pop(0)
    #         # Берём последнюю доступную дату для продажи
    #         sell_date = dates.pop(-1)
    #
    #         # Количество акций
    #         amount = random.randint(1, 1000)
    #
    #         user_id = random.randint(1, 93951)
    #
    #         transactions.append(
    #             {'ticker': companies[i], 'cnt_stock': amount, 'trade_time': purchase_date, 'user_id': user_id,
    #              'trade_type': 'buy'})
    #         transactions.append(
    #             {'ticker': companies[i], 'cnt_stock': amount, 'trade_time': sell_date, 'user_id': user_id,
    #              'trade_type': 'sell'})
    #
    # # Создаем DataFrame
    # transactions_df = pd.DataFrame(transactions)
    # # Удаляем половину транзакций с типом "sell"
    # sell_indices = transactions_df[transactions_df['trade_type'] == 'sell'].index.tolist()
    # num_to_remove = len(sell_indices) // 2
    # remove_indices = random.sample(sell_indices, num_to_remove)
    # transactions_df = transactions_df.drop(remove_indices)
    # return transactions_df

def save_transactions_to_csv(df: pd.DataFrame, filename: str):
    """
    Сохраняет DataFrame с транзакциями в Excel-файл, дополняя существующие данные и избегая дублирования строк.

    Args:
        df: DataFrame с транзакциями.
        filename: Имя файла Excel.
    """
    try:
        # Проверяем, существует ли файл
        try:
            existing_df = pd.read_csv(filename)
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
        combined_df.to_csv(filename, index=False)
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
    print(stocks*len(dates))
    df = generate_transactions(companies=stocks*len(dates), dates=dates)
    filename = 'transactions.xlsx'
    save_transactions_to_csv(df, filename)
