import logging
import time
from fin_news_pipeline.models import RawArticle
from fin_news_pipeline.processing.nlp import NLPPipeline
from fin_news_pipeline.storage.sqlite_store import DBManager
from fin_news_pipeline.ingestion.polling import poll_all_providers
from fin_news_pipeline.ingestion.dowload import ArticleDownloader

logger = logging.getLogger(__name__)

def get_total_count(d: dict) -> int:
    return len(d["downloadable"]) + len(d["summary_only"])

def main():
    """
    # The pipeline execution order:

    ## Block 1 - Data Collection:
        1. Initiate a database
        2. Poll providers
        3. Push `RawArticle`s to the db
    ## Block 2 - DB fetching:
        1. Fetch unprocessed articles
        2. Route downloadable articles to `ArticleDownloader`
    ## Block 3 - NLP
        1. Run NLP pipeline on body / summary
        2. Push `EnrichedArticle` instances back to the db
    """
    # Block 1
    db = DBManager()
    db.init_db()

    articles = poll_all_providers()
    db.save_articles_metadata(articles)
    
    # Block 2
    batch_size = 40
    downloader = ArticleDownloader()

    # pre-separate downloadable / non-downloadable so NLP doesn't need to filter
    articles_for_nlp: dict[str, list[RawArticle]] = dict(downloadable = [], summary_only = [])
    nlp_pipe = NLPPipeline()

    while True: # simulate constant flow of data and batching
        while (
            get_total_count(articles_for_nlp) <= batch_size 
            and (batch := db.pull_pending_articles(5))
        ):
            articles_for_nlp["summary_only"].extend(batch["summary_only"])
            downloaded = downloader.download_article_batch(batch["downloadable"], db=db)
            articles_for_nlp["downloadable"].extend(downloaded)

        if get_total_count(articles_for_nlp) == 0:
            time.sleep(60)
            continue
        
        enriched = nlp_pipe.run_pipeline(articles_for_nlp)
        db.push_enriched_articles(enriched)

        articles_for_nlp: dict[str, list[RawArticle]] = dict(downloadable = [], summary_only = [])

        time.sleep(60)



if __name__ == "__main__":
    main()