"""
storage.py — Persists crawled pages and exports results.
Separates I/O concerns from crawl logic.
"""
import csv
import logging
import time
from typing import Set

from log import get_logger, log
from parser import extract_links

logger = get_logger(__name__)

try:
    from indexer import store_page, build_postings
    _INDEXER_AVAILABLE = True
except ImportError:
    _INDEXER_AVAILABLE = False
    log(logger, logging.WARNING, "indexer module not found — pages won't be indexed.")


def save_page(html: str, url: str) -> None:
    """Persist *html* to MongoDB and update the in-memory inverted index."""
    if not _INDEXER_AVAILABLE:
        return

    t0 = time.perf_counter()
    try:
        out_links = sorted(extract_links(html, url))
        page = store_page(url, html, out_links=out_links)
        if not page:
            log(logger, logging.WARNING, "store_page returned nothing",
                url=url)
            return

        build_postings(page._id, page.title, page.content or "")
        duration_ms = round((time.perf_counter() - t0) * 1000)
        log(logger, logging.INFO, "Page saved to storage",
            url=url,
            page_id=str(page._id),
            out_links=len(out_links),
            page_bytes=len(html),
            duration_ms=duration_ms,
        )

    except Exception as exc:
        duration_ms = round((time.perf_counter() - t0) * 1000)
        log(logger, logging.ERROR, "Failed to save page",
            url=url,
            error=str(exc),
            duration_ms=duration_ms,
        )


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