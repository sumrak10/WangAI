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
        # data = cls._read_from_csv(cache_key)
        data = None
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

        max_days_per_request = 300  # Лимит Yahoo
        dataframes = []

        start = from_date
        end = to_date

        current_start = start
        while current_start < end:
            current_end = min(current_start + datetime.timedelta(days=max_days_per_request), end)
            logging.info(f"Загружаем период {current_start} -> {current_end}")

            try:
                df = yf.download(
                    tickers=list(tickers),
                    start=current_start.strftime("%Y-%m-%d"),
                    end=current_end.strftime("%Y-%m-%d"),
                    interval=interval,
                    group_by='ticker',
                    auto_adjust=False,
                    progress=False,
                    threads=True,
                )

                if df.empty:
                    logging.warning(f"Пустой ответ для периода {current_start} - {current_end}")
                else:
                    df.reset_index(inplace=True)
                    dataframes.append(df)

                time.sleep(2)  # чтобы избежать бана

            except Exception as e:
                logging.error(f"Ошибка загрузки {tickers} за {current_start} - {current_end}: {e}")

            current_start = current_end + datetime.timedelta(days=1)

        if not dataframes:
            raise ValueError("Не удалось загрузить ни одного блока данных.")

        full_data = pd.concat(dataframes)
        full_data = full_data.drop_duplicates(subset=['Datetime']).sort_values('Datetime').reset_index(drop=True)

        return full_data


if __name__ == '__main__':
    LoggerConfig.setup_logger()
    ANALYSIS_START_DATE = datetime.date.fromisoformat("2014-09-17")
    ANALYSIS_END_DATE = datetime.date.fromisoformat("2025-04-27")
    prices_data = PricesDataSource.load(
        from_date=ANALYSIS_START_DATE,
        to_date=ANALYSIS_END_DATE,
        interval="1h",
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
