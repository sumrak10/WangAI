import datetime
import logging
import time
from typing import Sequence

import pandas as pd
from pytrends.request import TrendReq

from src.data_sources.abstract import AbstractDataSource


class GoogleTrendsDataSource(AbstractDataSource):
    __dir_name__ = 'google_trends'

    @classmethod
    def load(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
        *,
        keywords: Sequence[str] = ('btc',),
    ) -> pd.DataFrame:
        cache_key = cls._build_cache_key(from_date, to_date, keywords)
        data = cls._read_from_csv(cache_key)
        if data is None:
            data = cls._load(
                from_date=from_date,
                to_date=to_date,
                keywords=keywords,
            )
            cls._save_to_csv(data, name=cache_key, index=False)
        cls.__check_dead_keywords(data)
        return data

    @classmethod
    def _load(
            cls,
            from_date: datetime.date,
            to_date: datetime.date,
            keywords: Sequence[str],
    ) -> pd.DataFrame:
        logging.info("Загружаем данные из API")
        all_data = []
        pytrends = TrendReq()
        sleep_time = 15

        # Разбиваем keywords по 5 штук
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        keyword_batches = list(chunks(keywords, 5))

        for batch in keyword_batches:
            logging.info(f"Собираем тренды для: {batch}")
            timeframe = f"{from_date.strftime('%Y-%m-%d')} {to_date.strftime('%Y-%m-%d')}"

            pytrends.build_payload(batch, timeframe=timeframe)
            trends_batch: pd.DataFrame = pytrends.interest_over_time()
            trends_batch.reset_index(inplace=True)
            trends_batch['date'] = pd.to_datetime(trends_batch['date'])

            all_data.append(trends_batch)

            logging.info(f"Ожидание времени по избежание 429: {sleep_time}")
            time.sleep(sleep_time)

        # Склеиваем все батчи по колонкам (по дате)
        merged = all_data[0][['date']].copy()
        for df in all_data:
            # Убираем isPartial, оставляем только интересы по словам
            df = df.drop(columns=['isPartial'], errors='ignore')
            merged = pd.merge(merged, df, on='date', how='outer')

        # Сортируем даты по порядку
        merged = merged.sort_values('date').reset_index(drop=True)

        # Приводим даты к дневному формату, даже если были пробелы
        full_dates = pd.DataFrame({'date': pd.date_range(from_date, to_date)})
        merged = pd.merge(full_dates, merged, on='date', how='left')
        return merged.ffill().bfill()

    @classmethod
    def __check_dead_keywords(cls, trends: pd.DataFrame):
        dead_keywords = []
        for column in trends.columns:
            if column == 'date':
                continue
            if trends[column].fillna(0).sum() == 0:
                dead_keywords.append(column)

        if dead_keywords:
            logging.info("⚠️ Слова без интереса за всё время (всё 0):")
            for word in dead_keywords:
                logging.info(f"- {word}")
        else:
            logging.info("✅ Все слова имеют хотя бы какие-то данные.")


if __name__ == '__main__':
    ANALYSIS_START_DATE = datetime.date.fromisoformat("2014-09-17")
    ANALYSIS_END_DATE = datetime.date.fromisoformat("2025-04-27")

    google_trends_data = GoogleTrendsDataSource.load(
        from_date=ANALYSIS_START_DATE,
        to_date=ANALYSIS_END_DATE,
        keywords=[
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto',
            'buy bitcoin', 'sell bitcoin', 'bitcoin price', 'crypto trading', 'crypto price',
            'crypto crash', 'bitcoin crash', 'crypto fear', 'crypto bull run', 'crypto bear market',
            'gold', 'usd', 'usdt', 'usdc',
            'web3', 'nft', 'defi', 'blockchain',
            'binance', 'coinbase', 'crypto.com', 'kraken', 'okx',
            'bitcoin investment', 'how to buy bitcoin', 'bitcoin price',
        ]
    )
