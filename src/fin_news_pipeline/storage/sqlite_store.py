import logging
from pathlib import Path
import sqlite3
from typing import Optional, Union

from fin_news_pipeline.models import EnrichedArticle, RawArticle

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, db_path: Optional[Union[str, Path]] = None) -> None:
        if db_path is None:
            current_dir = Path(__file__).parent.resolve()
            self.db_path = current_dir / "article_storage.db"
        else:
            self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row # allow column access by name: row['headline']

    def init_db(self) -> None:
        schema_path = Path(__file__).parent.resolve() / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema path not found at {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as fp:
            self.connection.executescript(fp.read())
    
    def save_articles_metadata(self, articles: list[RawArticle]) -> None:
        if not articles:
            return
        
        data_to_insert = [a.to_dict() for a in articles]
        query = """
        INSERT INTO raw_articles (
            canonical_id, provider_id, source, headline, summary, body, url,
            is_downloadable, published_at, fetched_at, status, body_attempts, body_last_error
        ) VALUES (
            :canonical_id, :provider_id, :source, :headline, :summary, :body, :url,
            :is_downloadable, :published_at, :fetched_at, :status, :body_attempts, :body_last_error
        )
        ON CONFLICT (canonical_id) DO NOTHING;
        """
        try:
            with self.connection:
                self.connection.executemany(query, data_to_insert)
        except sqlite3.Error as e:
            logger.error(f"Database error during batch insert: {e}")

    def pull_pending_articles(self, N: int) -> dict[str, list[RawArticle]] | None:
        """Extract `N` articles whose status is `pending`. Returns a list of `RawArticle`s."""
        query = """
        SELECT * FROM raw_articles
        WHERE status="pending"
        LIMIT ?;
        """
        lock_query = """
        UPDATE raw_articles SET
            status='processing'
        WHERE canonical_id=?;
        """
        try:
            with self.connection:
                cur = self.connection.cursor()
                rows = cur.execute(query, (N,)).fetchall()

            if not rows:
                return None
            
            all_articles = [RawArticle(**dict(row)) for row in rows] # TODO: figure out what to do with status and source

            canoncial_ids = [(row['canonical_id'],) for row in rows]
            cur.executemany(lock_query, canoncial_ids) # lock the raws to avoid re-fetching same articles

            if all_articles:
                return {
                    "downloadable": [a for a in all_articles if a.is_downloadable],
                    "summary_only": [a for a in all_articles if not a.is_downloadable]
                }
        except sqlite3.Error as e:
            logger.error(f"Database error during article fetching: {e}")
            return None
        
    def push_enriched_articles(self, enriched_articles: list[EnrichedArticle]) -> None:
        update_query = """
        UPDATE raw_articles SET
            body=:body,
            status='processed'
        WHERE canonical_id=:canonical_id;
        """
        insert_query = """
        INSERT INTO article_enrichments (
            canonical_id, tickers, sentiment_score,
            sentiment_label, entities, processed_at
        ) VALUES (
            :canonical_id, :tickers, :sentiment_score,
            :sentiment_label, :entities, :processed_at
        )
        ON CONFLICT (canonical_id) DO UPDATE SET
            tickers=excluded.tickers,
            sentiment_score=excluded.sentiment_score,
            sentiment_label=excluded.sentiment_label,
            entities=excluded.entities,
            processed_at=excluded.processed_at;
        """
        enriched_data = [a.to_enriched_dict() for a in enriched_articles]
        body_data = [
            {"body": a.article.body, "canonical_id": a.article.canonical_id} 
            for a in enriched_articles
        ]

        try:
            with self.connection:
                cur = self.connection.cursor()
                cur.executemany(update_query, body_data) # body update
                cur.executemany(insert_query, enriched_data)
        except sqlite3.Error as e:
            logger.error(f"Database error during enriched article push: {e}")