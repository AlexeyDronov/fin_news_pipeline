import logging
from fin_news_pipeline.ingestion.providers.base import NewsProvider
from fin_news_pipeline.models import RawArticle
from .providers import cnbc, yahoo

logger = logging.getLogger(__name__)

def poll_all_providers() -> list[RawArticle]:
    providers: list[NewsProvider] = [cnbc.CNBCProvider(), yahoo.YahooProvider()]
    all_articles = []
    for provider in providers:
        try:
            provider_articles = provider.fetch()
            all_articles.extend(provider_articles)
        except Exception as e:
            logger.error(f"Provider {provider.__class__.__name__} failed: {e}")
    return all_articles