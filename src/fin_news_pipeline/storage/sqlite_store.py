import logging
from pathlib import Path
import sqlite3
from typing import Optional, Union

from fin_news_pipeline.models import RawArticle

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
            published_at, fetched_at, body_status, body_attempts
        ) VALUES (
            :canonical_id, :provider_id, :source, :headline, :summary, :body, :url,
            :published_at, :fetched_at, :body_status, :body_attempts
        )
        ON CONFLICT (canonical_id) DO NOTHING;
        """
        try:
            with self.connection:
                self.connection.executemany(query, data_to_insert)
        except sqlite3.Error as e:
            logger.error(f"Database error during batch insert: {e}")
            

    def clean_up(self) -> None:
        if self.connection:
            self.connection.close()