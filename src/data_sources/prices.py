import datetime
import logging
import time
from typing import Sequence

import yfinance as yf
import pandas as pd

from src.data_sources.abstract import AbstractDataSource
from src.logger import LoggerConfig


class PricesDataSource(AbstractDataSource):
    __dir_name__ = 'prices'

    @classmethod
    def load(
            cls,
            from_date: datetime.date,
            to_date: datetime.date,
            interval: str = '1h',
            tickers: Sequence[str] = ('BTC-USD',)
    ) -> pd.DataFrame:
        cache_key = cls._build_cache_key(from_date, to_date, interval, tickers)
        data = cls._read_from_csv(cache_key)
        if data is None:
            data = cls._load(
                from_date=from_date,
                to_date=to_date,
                interval=interval,
                tickers=tickers,
            )
            cls._save_to_csv(data, name=cache_key, index=False)
        return data

    @classmethod
    def _load(
            cls,
            from_date: datetime.date,
            to_date: datetime.date,
            interval: str,
            tickers: Sequence[str],
    ) -> pd.DataFrame:
        logging.info("Загружаем данные из API с разбивкой на части")

        try:
            df = yf.download(
                tickers=list(tickers),
                start=from_date.strftime("%Y-%m-%d"),
                end=to_date.strftime("%Y-%m-%d"),
                interval=interval,
                group_by='ticker',
                auto_adjust=False,
                progress=False,
                threads=True,
            )

            if df.empty:
                logging.warning(f"Пустой ответ для периода {from_date} - {to_date}")
            else:
                df.reset_index(inplace=True)

            time.sleep(2)  # чтобы избежать бана

        except Exception as e:
            logging.error(f"Ошибка загрузки {tickers} за {from_date} - {to_date}: {e}")
            df = pd.DataFrame()

        return cls.clean_yfinance_columns(df)

    @staticmethod
    def clean_yfinance_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Преобразует мультииндекс yfinance в нормальные имена колонок типа 'btc_usd_close'
        """
        if isinstance(df.columns, pd.MultiIndex):
            new_columns = []
            for ticker, param in df.columns:
                if ticker == '':
                    new_columns.append(param.lower())  # индекс типа 'Date'
                else:
                    clean_ticker = ticker.lower().replace('-', '_')
                    clean_param = param.lower().replace(' ', '_')
                    new_columns.append(f"{clean_ticker}_{clean_param}")
            df.columns = new_columns
        return df


if __name__ == '__main__':
    LoggerConfig.setup_logger()
    ANALYSIS_START_DATE = datetime.date.fromisoformat("2014-09-17")
    ANALYSIS_END_DATE = datetime.date.fromisoformat("2025-04-27")
    prices_data = PricesDataSource.load(
        from_date=ANALYSIS_START_DATE,
        to_date=ANALYSIS_END_DATE,
        interval="1d",
        tickers=[
            'BTC-USD',
            'ETH-USD',
            'SOL-USD',
            'BNB-USD',
            'ADA-USD',
            'SPY',  # индекс акций
            'GLD',  # золото
        ]
    )
    print(prices_data.head())
