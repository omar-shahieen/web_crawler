"""
scorer.py — URL priority scoring.

Scores are integers in [0, 9]: lower = higher crawl priority.

Four sub-scorers are combined into one:
  1. default_scorer      — path depth + known low-value patterns
  2. domain_scorer       — authority domains get priority 0
  3. CrawlHistory.score  — stale pages get higher priority
  4. anchor_scorer       — keyword-rich anchors get higher priority
"""
import re
import threading
import time
from typing import Dict
from urllib.parse import urlparse

from infrastructure.config import (
    AUTHORITY_DOMAINS,
    HIGH_VALUE_KEYWORDS,
    LOW_VALUE_KEYWORDS,
    LOW_VALUE_PATH_TOKENS,
    STALE_AFTER_HOURS,
)


# ── 1. Default (path-depth) scorer ───────────────────────────────────────────

def default_scorer(url: str, **_) -> int:
    path = urlparse(url).path.lower()

    if path in ("", "/"):
        return 0  # homepage → highest priority

    if any(token in path for token in LOW_VALUE_PATH_TOKENS):
        return 10

    depth = path.count("/")
    return 1 if depth <= 2 else min(depth, 9)


# ── 2. Domain importance scorer ───────────────────────────────────────────────

def domain_scorer(url: str, **_) -> int:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    if any(host == auth or host.endswith("." + auth) for auth in AUTHORITY_DOMAINS):
        return 0
    return 5


# ── 3. Crawl-history (staleness) scorer ──────────────────────────────────────

class CrawlHistory:
    """Tracks the last crawl timestamp per URL and turns it into a priority score."""

    def __init__(self, stale_after_hours: float = STALE_AFTER_HOURS) -> None:
        self._history: Dict[str, float] = {}
        self._lock = threading.Lock()
        self.stale_after_hours = stale_after_hours

    def record(self, url: str) -> None:
        with self._lock:
            self._history[url] = time.time()

    def score(self, url: str, **_) -> int:
        with self._lock:
            last = self._history.get(url)

        if last is None:
            return 0  # never crawled → highest priority

        hours_since = (time.time() - last) / 3600
        if hours_since >= self.stale_after_hours:
            return 1
        if hours_since >= self.stale_after_hours / 2:
            return 4
        return 9  # recently crawled → deprioritize


# ── 4. Anchor-text keyword scorer ────────────────────────────────────────────

def anchor_scorer(anchor_text: str = "", **_) -> int:
    if not anchor_text:
        return 5
    tokens = set(re.findall(r"\w+", anchor_text.lower()))
    if tokens & HIGH_VALUE_KEYWORDS:
        return 1
    if tokens & LOW_VALUE_KEYWORDS:
        return 9
    return 5


# ── 5. Combined scorer ────────────────────────────────────────────────────────

# Module-level singleton so crawl history is shared across all callers.
crawl_history = CrawlHistory()


def combined_scorer(url: str, anchor_text: str = "", **_) -> int:
    scores = [
        default_scorer(url),
        domain_scorer(url),
        crawl_history.score(url),
        anchor_scorer(anchor_text),
    ]
    return round(sum(scores) / len(scores))