from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

class Source(str, Enum):
    REUTERS = "Reuters"

@dataclass
class Article:
    id: str
    source: Source
    headline: str
    summary: str | None
    body: str | None
    url: str
    published_at: datetime
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Filled in by the NLP step
    tickers: list[str] = field(default_factory=list)
    sentiment_score: float | None = None
    sentiment_label: str | None = None
    entities: list[str] = field(default_factory=list)