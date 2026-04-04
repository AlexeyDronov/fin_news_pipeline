from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class Source(str, Enum):
    # REUTERS = "Reuters"
    CNBC = "Cnbc"

@dataclass
class RawArticle:
    id: str
    source: Source
    headline: str
    summary: str | None
    body: str | None
    url: str
    published_at: datetime | None
    fetched_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["published_at"] = self.published_at.isoformat()
        d["fetched_at"] = self.fetched_at.isoformat()
        d["source"] = self.source.value
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

    def to_dict(self) -> dict:
        base = self.article.to_dict()
        base.update({
            "tickers": self.tickers,
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "entities": self.entities,
            "processed_at": self.processed_at.isoformat()
        })
        return base