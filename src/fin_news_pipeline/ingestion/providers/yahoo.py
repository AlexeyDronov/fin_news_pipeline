import logging
from email.utils import parsedate_to_datetime

import feedparser

from fin_news_pipeline.models import RawArticle, Source
from fin_news_pipeline.utils import build_article_id
from .base import NewsProvider

logger = logging.getLogger(__name__)

class YahooProvider(NewsProvider):
    """
    Yahoo RSS news provider.

    Data flow:
    1. `fetch()` -> retrieves articles via feedparser
    2. For each entry:
        - `_download_article()` -> uses newspaper4k to fetch full body text
        - `_adapt()` -> maps raw feed fields to RawArticle dataclass

    Expected feed structure:
    {
        "status": int,                              # HTTP status
        "bozo": bool,                               # malformed XML flag
        "entries": [
            {
                "id": str,                          # Yahoo unique article id
                "link": str,                        # URL of the article
                "title": str,
                "summary": str,
                "published": str                    # Email format (e.g. 'Thu, 02 Apr 2026 18:37:15 GMT')
                "published_parsed": struct_time     # uses time.struct_time format without tz info
            }
        ]
    }
    """
    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or "https://feeds.finance.yahoo.com/rss/2.0/headline?s=yhoo,goog&region=US&lang=en-US"
        
    def _adapt(
            self,
            entry: dict
    ) -> RawArticle | None:
        """Maps a single raw RSS feed entry to a RawArticle dataclass."""
        try:
            pub_date_str = entry.get("published")
            published_at = parsedate_to_datetime(pub_date_str) if pub_date_str else None

            entry_id = entry.get("id") or entry.get("link")
            title = entry.get("title", "No Title")
            link = entry.get("link", "")

            if not entry_id:
                logger.warning("Skipping entry missing both 'id' and 'link'.")
                return None
            
            return RawArticle(
                provider_id=f"Yahoo_{entry_id}",
                canonical_id=build_article_id(link, title),
                source=Source.YAHOO,
                headline=title,
                summary=entry.get("summary", "No Summary"),
                body=None,
                url=link,
                published_at=published_at,
            )
        except Exception as e:
            logger.error(f"Failed to adapt feed entry: {entry.get('title', 'No Title')}. Error: {e}")
            return None
        
    def fetch(self) -> list[RawArticle]:
        feed = feedparser.parse(self.base_url)

        status= feed.get("status")
        if isinstance(status, int) and not (200 <= status < 300):
            logger.error(f"Server error fetching Yahoo feed. HTTP Status: {status}")
            return []
        
        if feed.get("bozo") == 1:
            logger.warning(f"Malformed XML detected in Yahoo feed. Attempting to parse anyway...")

        articles = []
        entries = feed.get("entries", [])
        if not isinstance(entries, list): entries = [] # silence pylance

        for entry in entries:
            article = self._adapt(entry)
            if article:
                articles.append(article)

        return articles