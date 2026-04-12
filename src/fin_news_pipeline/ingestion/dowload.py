import logging
from newspaper import Article

from fin_news_pipeline.models import RawArticle, Status
from fin_news_pipeline.storage.sqlite_store import DBManager

logger = logging.getLogger(__name__)

class ArticleDownloader:
    def download_single_article(self, url: str) -> str | None:
        article = Article(url)
        try:
            article.download()
            article.parse()
            return article.text
        except Exception as e:
            logger.warning(f"Unable to download body text for {url}. Error: {e}")
            return None
        
    def download_article_batch(self, articles: list[RawArticle], db: DBManager) -> list[RawArticle]:
        """Runs newspaper's Article downloader on a batch of articles. 

        Args:
            articles (list[tuple[str, str, str]]): List of tuples containing `[(canonical_id, headline, url)]`

        Returns:
            out (list[tuple[str, str]]): A list of tuples containing `[(canonical_id, body_text)]` for successfully downloaded articles
        """
        for article in articles:
            article_to_download = Article(article.url)
            try:
                article_to_download.download()
                article_to_download.parse()
                if article_to_download.text:
                    article.body = article_to_download.text
            except Exception as e:
                logger.warning(f"Unable to download body text for {article.url}. Error: {e}")
                article.body_attempts += 1
                article.body_last_error = e
                article.status = Status.FAILED
                # TODO: push failed articles back to db and remove them from the pipeline
                continue
        return articles