from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import json

from .utils.utils import utc_now

class Source(str, Enum):
    CNBC = "Cnbc"
    YAHOO = "Yahoo"

class Status(str, Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    FAILED = "failed"

@dataclass
class RawArticle:
    canonical_id: str
    provider_id: str
    
    source: Source
    headline: str
    summary: str | None
    body: str | None
    url: str

    is_downloadable: bool

    published_at: datetime | None
    fetched_at: datetime = field(default_factory=utc_now)

    status: Status = Status.PENDING
    body_attempts: int = 0
    body_last_error: str = "" # last error when trying to download

    def to_dict(self) -> dict:
        d = asdict(self)
        d["published_at"] = self.published_at.isoformat() if self.published_at else None
        d["fetched_at"] = self.fetched_at.isoformat()
        d["source"] = self.source.value
        d["status"] = self.status.value
        return d
    
@dataclass
class EnrichedArticle:
    article: RawArticle

    # NLP step
    tickers: list[str] = field(default_factory=list)
    sentiment_score: float | None = None
    sentiment_label: str | None = None
    entities: list[str] = field(default_factory=list)

    processed_at: datetime = field(default_factory=utc_now)

    def to_enriched_dict(self) -> dict:
        return {
            "canonical_id": self.article.canonical_id,
            "tickers": json.dumps(self.tickers),
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "entities": json.dumps(self.entities),
            "processed_at": self.processed_at.isoformat()
        }