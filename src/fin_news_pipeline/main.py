import logging
import logging.handlers
from pathlib import Path
import sys
import time
import warnings

# Silence NLTK warning before local modules are imported!
warnings.filterwarnings(
    "ignore",
    message=".*nltk is not installed.*",
    category=UserWarning
)

from transformers import logging as transformers_logging

from fin_news_pipeline.ingestion.dowload import ArticleDownloader
from fin_news_pipeline.ingestion.polling import poll_all_providers
from fin_news_pipeline.models import RawArticle
from fin_news_pipeline.processing.nlp import NLPPipeline
from fin_news_pipeline.storage.sqlite_store import DBManager

logger = logging.getLogger(__name__)

def get_total_count(d: dict[str, list[RawArticle]]) -> int:
    return len(d["downloadable"]) + len(d["summary_only"])

def set_logging():
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_filepath = log_dir / "pipeline.log"

    log_format = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(log_format, date_format)
    
    
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_filepath,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not root_logger.handlers:
        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)

    transformers_logging.set_verbosity_error()
    transformers_logger = logging.getLogger("transformers")
    transformers_logger.setLevel(logging.ERROR)

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
    set_logging()
    logger.info("Pipeline execution started.")

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
        logger.debug(f"Trying to accumulate {batch_size} articles for processing...")
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

        articles_for_nlp = dict(downloadable = [], summary_only = [])

        time.sleep(60)



if __name__ == "__main__":
    main()