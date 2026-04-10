import logging
from email.utils import parsedate_to_datetime
from typing import Literal

import feedparser

from fin_news_pipeline.models import RawArticle, Source
from fin_news_pipeline.utils import build_article_id
from .base import NewsProvider

logger = logging.getLogger(__name__)

CNBCBranch = Literal["business", "earnings", "finance"]

class CNBCProvider(NewsProvider):
    """
    CNBC RSS news provider.

    Data flow:
    1. `fetch()` -> calls `fetch_one_branch()` for each financial branch (business, earnings, finance)
    2. `fetch_one_branch()` -> downloads raw RSS feeds via feedparser
    3. For each entry `_adapt()` -> maps raw feed fields to RawArticle dataclass

    Expected feed structure:
    {
        "status": int,                              # HTTP status
        "bozo": bool,                               # malformed XML flag
        "entries": [
            {
                "id": str,                          # CNBC unique article id
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
        self.base_url = base_url or "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=" 
        self.cnbc_branches: dict[CNBCBranch, str] = dict(
            business="10001147", 
            earnings="15839135", 
            finance="10000664"
        )
        
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
                canonical_id=build_article_id(link, title),
                provider_id=f"CNBC_{entry_id}",
                source=Source.CNBC,
                headline=title,
                summary=entry.get("summary", "No Summary"),
                body=None,
                url=link,
                is_downloadable=True,
                published_at=published_at,
            )
        except Exception as e:
            logger.error(f"Failed to adapt feed entry: {entry.get('title', 'No Title')}. Error: {e}")
            return None

    def fetch_one_branch(
            self, 
            feed_branch: Literal["business", "earnings", "finance"]
    ) -> list[RawArticle]:
        feed_url = self.base_url + self.cnbc_branches[feed_branch]
        feed = feedparser.parse(feed_url)

        status= feed.get("status")
        if isinstance(status, int) and not (200 <= status < 300):
            logger.error(f"Server error fetching CNBC {feed_branch} feed. HTTP Status: {status}")
            return []
        
        if feed.get("bozo") == 1:
            logger.warning(f"Malformed XML detected in {feed_branch} feed. Attempting to parse anyway...")

        articles = []
        entries = feed.get("entries", [])
        if not isinstance(entries, list): entries = [] # silence pylance

        for entry in entries:
            article = self._adapt(entry)
            if article:
                articles.append(article)

        return articles
    
    def fetch(self) -> list[RawArticle]:
        """Iterates over CNBC RSS feeds' financial endpoints and returns a flat list of all articles."""
        all_articles = []
        for branch in self.cnbc_branches:
            all_articles.extend(self.fetch_one_branch(branch))
        return all_articles