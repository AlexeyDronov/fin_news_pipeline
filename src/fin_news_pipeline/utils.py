import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

def utc_now() -> datetime:
    """Returns the current timestamp in UTC to ensure database consistency."""
    return datetime.now(timezone.utc)

def canonicalise_url(url: str) -> str:
    """
    Normalizes a URL by lowercasing the host, removing trailing slashes, 
    stripping fragments, and filtering out common marketing tracking parameters.
    """
    # set of parameters that do not change the content of an article
    TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "tsrc", "ref"}

    parsed = urlparse(url)
    qsl_parsed = parse_qsl(parsed.query)
    query = [(k,v) for k, v in qsl_parsed if k not in TRACKING_PARAMS]
    clean_query = urlencode(query)
    return urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        parsed.path.rstrip("/"),
        parsed.params,
        clean_query,
        ""
    ))

def url_hash(url: str) -> str:
    """
    Generates a deterministic 16-character hexadecimal string from a canonical URL.
    Used for primary identification of the source link.
    """
    canonical = canonicalise_url(url)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]

def content_fingerprint(headline: str) -> str:
    """
    Generates a 16-character hash of the headline. 
    Acts as a fuzzy-match fallback to detect the same story published under different URLs.
    """
    content = headline.strip().lower()
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def build_article_id(url: str, headline: str) -> str:
    """
    Constructs a composite 33-character unique identifier.

    Format: `{url_hash}_{headline_hash}`
    """
    uhash = url_hash(url)
    chash = content_fingerprint(headline)
    return f"{uhash}_{chash}"