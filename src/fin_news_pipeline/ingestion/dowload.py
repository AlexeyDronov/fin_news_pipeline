import logging
from newspaper import Article

logger = logging.getLogger(__name__)

def download_article(url: str) -> str | None:
    article = Article(url)
    try:
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        logger.warning(f"Unable to download body text for {url}. Error: {e}")
        return None