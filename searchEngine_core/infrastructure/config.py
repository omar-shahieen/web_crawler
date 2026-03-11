"""
config.py — Central configuration for the crawler.
All tuneable constants live here; nothing else imports raw literals.
"""
from typing import List, Set, Tuple

REQUEST_TIMEOUT: int = 5
MAX_FETCH_RETRIES: int = 3
RETRY_BASE_DELAY: float = 1.5

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/537.36 CriOS/120.0.0.0 Mobile/15E148 Safari/537.36",
]

EXCLUDED_PATHS: List[str] = ["/login", "/admin", "/signup", "/cart", "/checkout"]
EXCLUDED_QUERY_PARAMS: List[str] = ["q=", "search=", "filter=", "sort=", "sessionid", "utm_", "ref", "page"]

IGNORE_EXTENSIONS: Tuple[str, ...] = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".tar",
    ".mp4", ".mp3", ".css", ".js",
)

AUTHORITY_DOMAINS: Set[str] = {
    "wikipedia.org", "github.com",
    "stackoverflow.com", "reddit.com", "google.com",
}

HIGH_VALUE_KEYWORDS: Set[str] = {
    "api", "docs", "documentation", "guide", "tutorial",
    "reference", "research", "paper", "index", "overview",
}

LOW_VALUE_KEYWORDS: Set[str] = {
    "login", "logout", "signup", "cart", "checkout",
    "comment", "reply", "share", "print", "subscribe",
}

LOW_VALUE_PATH_TOKENS: Tuple[str, ...] = (
    ".pdf", ".jpg", ".png", ".zip", "login", "logout",
    "signup", "register", "comment", "tag", "page=",
)

STALE_AFTER_HOURS: float = 24.0
DEFAULT_MAX_PAGES: int = 50
DEFAULT_MAX_WORKERS: int = 5
DEFAULT_DELAY_RANGE: Tuple[float, float] = (1.5, 3.5)
FRONT_QUEUE_BUCKETS: int = 5
ROBOTS_MAX_CONSECUTIVE_BLOCKS: int = 5
MAX_CONSECUTIVE_FETCH_FAILS: int = 3
LOG_FILE: str = "crawl_index.log"
OUTPUT_CSV: str = "crawled_urls.csv"