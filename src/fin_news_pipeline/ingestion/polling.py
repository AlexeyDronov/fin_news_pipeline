from fin_news_pipeline.ingestion.providers.base import NewsProvider
from fin_news_pipeline.models import RawArticle
from .providers import cnbc, yahoo

def poll_all_providers() -> list[RawArticle]:
    providers: list[NewsProvider] = [cnbc.CNBCProvider(), yahoo.YahooProvider()]
    all_articles = []
    for provider in providers:
        provider_articles = provider.fetch()
        all_articles.extend(provider_articles)
    return all_articles