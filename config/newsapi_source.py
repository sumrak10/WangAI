from pydantic_settings import BaseSettings, SettingsConfigDict


class NewsApiSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        env_prefix="NEWSAPI_",
        case_sensitive=True,
    )
    API_KEY: str


news_api_settings = NewsApiSettings()
