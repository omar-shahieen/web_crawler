"""
parser.py — Parses HTML and extracts filtered, normalised links.
No HTTP, no scoring, no side effects.
"""
import logging
from typing import Set, Tuple
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from config import EXCLUDED_PATHS, EXCLUDED_QUERY_PARAMS, IGNORE_EXTENSIONS
from log import get_logger, log

logger = get_logger(__name__)


# ── Public helpers ────────────────────────────────────────────────────────────

def extract_links(html: str, base_url: str) -> Set[Tuple[str, str]]:
    """
    Return a set of (absolute_url, anchor_text) tuples found in *html*.
    Only same-domain, non-excluded links are returned.
    """
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc
    links: Set[Tuple[str, str]] = set()

    raw_count = 0
    skipped_external = skipped_path = skipped_query = skipped_scheme = 0

    for tag in soup.find_all("a", href=True):
        href = str(tag.get("href", "")).strip()
        if not href:
            continue
        raw_count += 1

        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)

        if not absolute.startswith("http"):
            skipped_scheme += 1
            continue
        if parsed.netloc != base_domain:
            skipped_external += 1
            continue
        if _is_excluded_path(parsed.path):
            skipped_path += 1
            continue
        if _has_excluded_query(parsed.query):
            skipped_query += 1
            continue

        anchor = tag.get_text(strip=True)
        links.add((absolute, anchor))

    accepted = len(links)
    log(logger, logging.DEBUG, "Links extracted",
        base_url=base_url,
        raw=raw_count,
        accepted=accepted,
        skipped_external=skipped_external,
        skipped_path=skipped_path,
        skipped_query=skipped_query,
        skipped_scheme=skipped_scheme,
    )

    if accepted == 0 and raw_count > 0:
        log(logger, logging.WARNING, "No links accepted from page",
            base_url=base_url,
            raw=raw_count,
        )

    return links


def should_skip_url(url: str) -> bool:
    """Return True for URLs that point to binary / non-HTML resources."""
    return url.lower().endswith(IGNORE_EXTENSIONS)


# ── Private helpers ───────────────────────────────────────────────────────────

def _is_excluded_path(path: str) -> bool:
    return any(exc in path for exc in EXCLUDED_PATHS)


def _has_excluded_query(query: str) -> bool:
    return any(param in query for param in EXCLUDED_QUERY_PARAMS)