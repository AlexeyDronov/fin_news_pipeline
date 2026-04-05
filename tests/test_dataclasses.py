import pytest
from datetime import datetime, timezone, timedelta
from fin_news_pipeline.models import RawArticle, EnrichedArticle, Source
from conftest import RawArticleFactory, EnrichedArticleFactory


class TestRawArticle:
    def test_to_dict_serialises_all_fields(self, raw_article_factory: RawArticleFactory):
        article = raw_article_factory()
        d = article.to_dict()

        assert d == {
            "id": article.id,
            "source": article.source.value,
            "headline": article.headline,
            "summary": article.summary,
            "body": article.body,
            "url": article.url,
            "published_at": article.published_at.isoformat(),
            "fetched_at": article.fetched_at.isoformat()
        }
 
    @pytest.mark.parametrize("summary,body", [
        ("some summary", "some body"),
        (None, "some body"),
        ("some summary", None),
        (None, None)
    ])
    def test_optional_fields_are_preserved(self, raw_article_factory: RawArticleFactory, summary, body):
        article = raw_article_factory(summary=summary, body=body)
        d = article.to_dict()

        assert article.summary == d["summary"] == summary
        assert article.body == d["body"] == body

    def test_soure_enum_serialises_to_string_value(self, raw_article_factory: RawArticleFactory):
        article = raw_article_factory(source=Source.REUTERS)
        d = article.to_dict()

        assert d["source"] == "Reuters"
        assert not isinstance(d["source"], Source)

    def test_datetime_fields_serialise_to_iso_strings(self, raw_article_factory: RawArticleFactory):
        article = raw_article_factory()
        d = article.to_dict()

        assert isinstance(d["published_at"], str)
        assert isinstance(d["fetched_at"], str)
        assert datetime.fromisoformat(d["published_at"]) == article.published_at
        assert datetime.fromisoformat(d["fetched_at"]) == article.fetched_at

    def test_default_fetched_at_is_set_by_default_factory(self):
        before = datetime.now(timezone.utc) - timedelta(seconds=1)
        article = RawArticle(
            id="test-001",
            source=Source.REUTERS,
            headline="headline",
            summary=None,
            body=None,
            url="https://example.com",
            published_at=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        after = datetime.now(timezone.utc) + timedelta(seconds=1)

        assert isinstance(article.fetched_at, datetime)
        assert article.fetched_at.tzinfo == timezone.utc
        assert before <= article.fetched_at <= after



class TestEnrichedArticleToDict:
    def test_to_dict_merges_raw_and_enriched_fields(self, enriched_article_factory: EnrichedArticleFactory):
        enriched = enriched_article_factory()
        d = enriched.to_dict()

        assert d == {
            "id": enriched.article.id,
            "source": enriched.article.source.value,
            "headline": enriched.article.headline,
            "summary": enriched.article.summary,
            "body": enriched.article.body,
            "url": enriched.article.url,
            "published_at": enriched.article.published_at.isoformat(),
            "fetched_at": enriched.article.fetched_at.isoformat(),
            "tickers": enriched.tickers,
            "sentiment_score": enriched.sentiment_score,
            "sentiment_label": enriched.sentiment_label,
            "entities": enriched.entities,
            "processed_at": enriched.processed_at.isoformat(),
        }

    def test_default_nlp_fields_are_none_or_empty(self, raw_article_factory: RawArticleFactory):
        enriched = EnrichedArticle(article=raw_article_factory())
        d = enriched.to_dict()

        assert enriched.sentiment_label is None
        assert enriched.sentiment_score is None
        assert enriched.tickers == []
        assert enriched.entities == []
        assert d["sentiment_score"] is None
        assert d["sentiment_label"] is None
    
    def test_default_list_fields_are_not_shared(self, raw_article_factory: RawArticleFactory):
        raw_1 = raw_article_factory(id="a1")
        raw_2 = raw_article_factory(id="a2")
        enriched_1 = EnrichedArticle(article=raw_1)
        enriched_2 = EnrichedArticle(article=raw_2)
        enriched_1.tickers.append("APPL")
        enriched_1.entities.append("Apple")

        assert enriched_1.tickers == ["APPL"]
        assert enriched_1.entities == ["Apple"]
        assert enriched_2.tickers == []
        assert enriched_2.entities == []

    def test_processed_at_serialises_to_iso_string(self, enriched_article_factory: EnrichedArticleFactory):
        enriched = enriched_article_factory()
        d = enriched.to_dict()

        assert isinstance(d["processed_at"], str)
        assert datetime.fromisoformat(d["processed_at"]) == enriched.processed_at

    def test_default_processed_at_is_set_by_default_factory(self):
        before = datetime.now(timezone.utc) - timedelta(seconds=1)
        raw = RawArticle(
            id="test-001",
            source=Source.REUTERS,
            headline="headline",
            summary=None,
            body=None,
            url="https://example.com",
            published_at=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        enriched = EnrichedArticle(article=raw)
        after = datetime.now(timezone.utc) + timedelta(seconds=1)
        
        assert isinstance(enriched.processed_at, datetime)
        assert enriched.processed_at.tzinfo == timezone.utc
        assert before <= enriched.processed_at <= after
    
    def test_to_dict_is_idempotent(self, enriched_article_factory: EnrichedArticleFactory):
        """Calling to_dict() twice returns the same result (base.update doesn't mutate shared state)."""
        enriched = enriched_article_factory()

        assert enriched.to_dict() == enriched.to_dict()