import datetime

import yfinance as yf
import pandas as pd

from src.data_sources.abstract import AbstractDataSource


class PricesDataSource(AbstractDataSource):
    __dir_name__ = 'prices'

    @classmethod
    def load(
            cls,
            from_date: datetime.date,
            to_date: datetime.date,
            interval='1d',
    ) -> pd.DataFrame:
        data = cls._read_from_csv(cls._build_file_name(from_date, to_date))
        if data is None:
            data = cls._load(
                from_date=from_date,
                to_date=to_date,
                interval=interval,
            )
            cls._save_to_csv(data, name=cls._build_file_name(from_date, to_date), index=False)
        return data

    @classmethod
    def _load(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
        interval: str,
        pairs: list[str],
    ) -> pd.DataFrame:
        prices = pd.DataFrame()
        for pair in pairs:
            pair_df = pd.DataFrame()
            try:
                pair_df = yf.download('BTC-USD', start=from_date, end=to_date, interval=interval, multi_level_index=False)
                pair_df.reset_index(inplace=True)
                pair_df.rename(
                    columns={
                        "Date": f"date",
                        "Close": f"{pair.lower().replace('-', '')}_close",
                        "High": f"{pair.lower().replace('-', '')}_high",
                        "Low": f"{pair.lower().replace('-', '')}_low",
                        "Open": f"{pair.lower().replace('-', '')}_open",
                        "Volume": f"{pair.lower().replace('-', '')}_volume"
                    },
                    inplace=True
                )
            except Exception as e:
                print(f"Ошибка загрузки цен {pair}: {e}")
            prices = pd.merge(prices, pair_df, on='date', how='left')
        return prices
