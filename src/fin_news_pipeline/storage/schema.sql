CREATE TABLE IF NOT EXISTS raw_articles (
    canonical_id TEXT PRIMARY KEY,
    provider_id TEXT,
    source TEXT,
    headline TEXT,
    summary TEXT,
    body TEXT,
    url TEXT,
    is_downloadable INTEGER,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP,
    status TEXT,
    body_attempts INTEGER,
    body_last_error TEXT
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

CREATE INDEX IF NOT EXISTS idx_raw_articles_status ON raw_articles(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_articles_provider_id ON raw_articles(provider_id);