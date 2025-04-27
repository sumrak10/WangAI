import datetime
import time

import pandas as pd
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException

from config.newsapi_source import news_api_settings
from src.data_sources.abstract import AbstractDataSource


class NewsAPIDataSource(AbstractDataSource):
    __dir_name__ = 'newsapi'

    @classmethod
    def load(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
    ) -> pd.DataFrame:
        data = cls._read_from_csv(cls._build_file_name(from_date, to_date))
        if data is None:
            data = cls._load(
                from_date=from_date,
                to_date=to_date,
            )
            cls._save_to_csv(data, name=cls._build_file_name(from_date, to_date), index=False)
        return data

    @classmethod
    def _load(
        cls,
        from_date: datetime.date,
        to_date: datetime.date,
    ) -> pd.DataFrame:
        client = NewsApiClient(news_api_settings.API_KEY)
        query = "Bitcoin"

        articles = []
        page_size = 100
        from_param = from_date
        while from_param <= to_date:
            try:
                data = client.get_everything(
                    q=query,
                    from_param=from_param,
                    to=from_param + datetime.timedelta(days=1),
                    language='en',
                    sort_by="publishedAt",
                )
            except NewsAPIException as e:
                raise RuntimeError(e)
            founded_articles = data.get('articles', [])
            articles.extend(founded_articles)
            print(f"Date {from_param}. {len(founded_articles)} articles found. but total {data['totalResults']}")
            from_param += datetime.timedelta(days=1)

        def prepare_text(text: str | None) -> str:
            if text is None:
                return ""
            text = (
                text
                .replace("\n", " ")
                .replace("\r", " ")
                .replace(",", " ")
            )
            return text

        news_df: pd.DataFrame = pd.DataFrame([{
            'date': datetime.date.fromisoformat(article['publishedAt'].split('T')[0]),
            'author': prepare_text(article['author']),
            'title': prepare_text(article['title']),
            'description': prepare_text(article['description']),
            'content': prepare_text(article['content']),
            'source': prepare_text(article['source']['name']),
            'url': article['url']
        } for article in articles])

        return news_df


if __name__ == '__main__':
    ANALYSIS_START_DATE = datetime.date.fromisoformat("2025-03-27")
    ANALYSIS_END_DATE = datetime.date.fromisoformat("2025-04-27")

    news_data = NewsAPIDataSource.load(
        from_date=ANALYSIS_START_DATE,
        to_date=ANALYSIS_END_DATE,
    )
