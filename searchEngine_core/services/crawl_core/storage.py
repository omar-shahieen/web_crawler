"""
storage.py — Persists crawled pages and exports results.
Separates I/O concerns from crawl logic.
"""
import csv
import logging
import time
from typing import Set

from infrastructure.logging_utils import get_logger, log
from .parser import extract_links

logger = get_logger(__name__)

try:
    from .indexer import store_page
    _INDEXER_AVAILABLE = True
except ImportError:
    _INDEXER_AVAILABLE = False
    log(logger, logging.WARNING, "indexer module not found — pages won't be indexed.")


def _normalize_out_links(links) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for link in links or []:
        if isinstance(link, str):
            url = link.strip()
        elif isinstance(link, (list, tuple)) and link:
            url = str(link[0]).strip()
        elif isinstance(link, dict):
            url = str(link.get("url", "")).strip()
        else:
            url = ""

        if not url or url in seen:
            continue

        seen.add(url)
        normalized.append(url)

    return normalized


def save_page(html: str, url: str) -> bool:
    """Persist *html* to MongoDB and update the in-memory inverted index.

    Returns True when the page was stored/indexed, False otherwise.
    """
    if not _INDEXER_AVAILABLE:
        return

    t0 = time.perf_counter()
    try:
        links = extract_links(html, url) or []
        out_links = sorted(_normalize_out_links(links))
        page = store_page(url, html, out_links=out_links)
        if not page:
            log(logger, logging.WARNING, "store_page returned nothing",
                url=url)
            return False

        duration_ms = round((time.perf_counter() - t0) * 1000)
        log(logger, logging.INFO, "Page saved to storage",
            url=url,
            page_id=str(page._id),
            out_links=len(out_links),
            page_bytes=len(html),
            duration_ms=duration_ms,
        )
        return True

    except Exception as exc:
        duration_ms = round((time.perf_counter() - t0) * 1000)
        log(logger, logging.ERROR, "Failed to save page",
            url=url,
            error=str(exc),
            duration_ms=duration_ms,
        )
        return False


def export_visited_csv(visited: Set[str], filepath: str) -> None:
    """Write the set of crawled URLs to a CSV file."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in sorted(visited):
            writer.writerow([url])
    log(logger, logging.INFO, "Exported visited URLs",
        filepath=filepath,
        total_urls=len(visited),
    )