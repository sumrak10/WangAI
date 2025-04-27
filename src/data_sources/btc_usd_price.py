import datetime

import yfinance as yf
import pandas as pd

from src.data_sources.abstract import AbstractDataSource


class BTCUSDPriceDataSource(AbstractDataSource):
    __dir_name__ = 'btc_usd_price'

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
    ) -> pd.DataFrame:
        btc: pd.DataFrame = pd.DataFrame()
        try:
            btc = yf.download('BTC-USD', start=from_date, end=to_date, interval=interval, multi_level_index=False)
            btc.reset_index(inplace=True)
            btc.rename(
                columns={
                    "Date": "date",
                    "Close": "close",
                    "High": "high",
                    "Low": "low",
                    "Open": "open",
                    "Volume": "volume"
                },
                inplace=True
            )
        except Exception as e:
            print(f"Ошибка загрузки цен BTC: {e}")
        return btc
