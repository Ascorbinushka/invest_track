import pandas as pd
import requests
from pandas import DataFrame
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import List


class HistoricalData(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float


class StockData(BaseModel):
    symbol: str
    historical: List[HistoricalData]


def get_data_company(COMPANIES: list[str], start_date: str, end_date: str, API_KEY) -> DataFrame | None:
    """
        Получает данные по акциям для списка компаний за заданный период.

        Args:
            COMPANIES (list[str]): Список тикеров акций (например, ["AAPL", "GOOG"]).
            start_date (str): Начальная дата в формате строки (например, "2023-01-01").
            end_date (str): Конечная дата в формате строки (например, "2023-12-31").

        Returns:
            pd.DataFrame: DataFrame с данными по акциям для всех компаний.
        """
    for company in COMPANIES:
        url = "https://financialmodelingprep.com/api/v3/historical-price-full/{}?from={}&to={}&apikey={}".format(
            company, start_date, end_date, API_KEY)
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            if response_json:
                parsed_data = StockData(**response_json)
                yield parsed_data


if __name__ == '__main__':
    API_KEY = 'API_KEY'
    COMPANIES = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "JPM", "BAC", "WFC", "JNJ", "UNH",
        "MRK", "PFE", "PG", "KO", "PEP", "XOM", "CVX", "GE", "HON", "LMT",
        "META", "VZ", "T", "DIS", "AMT", "PLD", "TSM", "SHEL", "BRK.B", "NKE"
    ]

    start_date = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")

    data = get_data_company(COMPANIES=COMPANIES, start_date=start_date, end_date=end_date, API_KEY=API_KEY)
    print(data)

