from transformers import pipeline

from fin_news_pipeline.models import EnrichedArticle, RawArticle
from fin_news_pipeline.utils.tickers import build_lookup, process_or_load_tickers

class NLPPipeline:
    def __init__(self, spacy_model: str | None = None, bert_model: str | None = None) -> None:
        self.sentiment_pipe = pipeline("text-classification",
                                  bert_model or "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
        self.entity_pipe = ...
        
        # process ticker data if not already
        ticker_df = process_or_load_tickers("sp500.csv") # TODO: download and apply for all tickers.
        self.ticker_dict, self.ticker_processor = build_lookup(ticker_df)

    def extract_scores(self, texts: list[str]) -> list[dict]:
        return self.sentiment_pipe(texts)
    
    def extract_tickers(self, texts: list[str]) -> list[list[str]]:
        results = []

        for text in texts:
            tickers = self.ticker_processor.extract_keywords(text, span_info=False) # TODO: consider spans
            results.append(tickers)

        return results
    
    def extract_named_entities(self,):
        # TODO: Implement
        raise NotImplementedError
    
    def run_pipeline(self, articles: dict[str, list[RawArticle]]):
        downloadable = articles["downloadable"]
        summaries = articles["summary_only"]
        all_texts = [a.body if a.body else "" for a in downloadable] + [a.summary if a.summary else "" for a in summaries]

        scores = self.extract_scores(all_texts)
        tickers = self.extract_tickers(all_texts)

        enriched_articles = []
        for article, scores, tickers in zip(downloadable + summaries, scores, tickers):
            enriched_articles.append(EnrichedArticle(
                article=article,
                tickers=tickers,
                sentiment_score=round(scores["score"], 4),
                sentiment_label=scores["label"],
                entities=["test_entity"]
            ))
        
        return enriched_articles