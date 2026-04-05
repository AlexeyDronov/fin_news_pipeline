CREATE TABLE IF NOT EXISTS raw_articles (
    canonical_id TEXT PRIMARY KEY,
    provider_id TEXT,
    source TEXT,
    headline TEXT,
    summary TEXT,
    body TEXT,
    url TEXT,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP,
    body_status TEXT,
    body_attempts INTEGER
);

CREATE TABLE IF NOT EXISTS article_enrichments (
    canonical_id TEXT PRIMARY KEY,
    tickers TEXT,
    sentiment_score REAL,
    sentiment_label TEXT,
    entities TEXT,
    processed_at TIMESTAMP,
    FOREIGN KEY (canonical_id) REFERENCES raw_articles(canonical_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_raw_articles_body_status ON raw_articles(body_status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_articles_provider_id ON raw_articles(provider_id);