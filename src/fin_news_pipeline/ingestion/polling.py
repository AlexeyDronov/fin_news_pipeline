import logging
from fin_news_pipeline.ingestion.providers.base import NewsProvider
from fin_news_pipeline.models import RawArticle
from .providers import cnbc, yahoo

logger = logging.getLogger(__name__)

def poll_all_providers() -> list[RawArticle]:
    providers: list[NewsProvider] = [cnbc.CNBCProvider(), yahoo.YahooProvider()]
    all_articles: list[RawArticle] = []
    for provider in providers:
        logger.info(f"Fetching articles for provider {provider.__class__.__name__}...")
        try:
            provider_articles = provider.fetch()
            logger.info(f"Successfully fetched {len(provider_articles)} articles from {provider.__class__.__name__}.")
            all_articles.extend(provider_articles)
        except Exception:
            logger.exception(f"Provider {provider.__class__.__name__} failed:")
    return all_articles