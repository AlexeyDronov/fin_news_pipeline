import pytest
from datetime import datetime, timezone
from typing import Protocol
from fin_news_pipeline.models import RawArticle, EnrichedArticle, Source

class RawArticleFactory(Protocol):
    def __call__(self, **overrides) -> RawArticle: ...

class EnrichedArticleFactory(Protocol):
    def __call__(self, raw_overrides: dict | None = None, **overrides) -> EnrichedArticle: ...

@pytest.fixture
def raw_article_factory() -> RawArticleFactory:
    def _make(**overrides):
        defaults = dict(
            id="test-001",
            source=Source.YAHOO,
            headline="Test headline",
            summary="Test summary",
            body="Test body",
            url="https://example.com",
            published_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            fetched_at=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
        )
        return RawArticle(**{**defaults, **overrides})
    return _make

@pytest.fixture
def enriched_article_factory(raw_article_factory) -> EnrichedArticleFactory:
    def _make(raw_overrides=None, **overrides):
        raw = raw_article_factory(**(raw_overrides or {}))
        defaults = dict(
            article = raw,
            tickers=["AAPL"],
            sentiment_score=0.4,
            sentiment_label="positive",
            entities=["Apple"],
            processed_at=datetime(2024, 1, 1, 12, 10, 0, tzinfo=timezone.utc)
        )
        return EnrichedArticle(**{**defaults, **overrides})
    return _make