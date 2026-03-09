"""
crawler.py — Orchestrates the threaded crawl.
Wires together: FrontQueue → BackQueueRouter → per-host workers.
All business logic (fetching, parsing, scoring, saving) lives in other modules.
"""
import logging
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Set, Tuple

from config import (
    DEFAULT_DELAY_RANGE,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_WORKERS,
    FRONT_QUEUE_BUCKETS,
    OUTPUT_CSV,
)
from fetcher import fetch
from log import get_logger, log
from parser import extract_links, should_skip_url
from queues import BackQueueRouter, FrontQueue
from robots import robots_cache, host_block_tracker, fetch_fail_tracker
from scorer import combined_scorer, crawl_history
from storage import export_visited_csv, save_page

logger = get_logger(__name__)


def threaded_crawl(
    seed_urls: Iterable[str],
    max_pages: int = DEFAULT_MAX_PAGES,
    max_workers: int = DEFAULT_MAX_WORKERS,
    delay_range: Tuple[float, float] = DEFAULT_DELAY_RANGE,
    output_csv: str = OUTPUT_CSV,
) -> Set[str]:
    """
    Crawl up to *max_pages* pages starting from *seed_urls*.
    Returns the set of visited URLs.
    """
    seeds = list(seed_urls)
    crawl_start = time.perf_counter()

    log(logger, logging.INFO, "Crawl started",
        seed_urls=seeds,
        max_pages=max_pages,
        max_workers=max_workers,
        delay_range=delay_range,
        output_csv=output_csv,
    )

    # ── Shared state ──────────────────────────────────────────────────────────
    visited: Set[str] = set()
    visited_lock = threading.Lock()

    pages_crawled = 0
    pages_crawled_lock = threading.Lock()

    active_workers = 0
    active_workers_lock = threading.Lock()
    all_done = threading.Event()

    # ── Worker ────────────────────────────────────────────────────────────────
    def worker(host: str, fifo_queue: queue.Queue) -> None:
        nonlocal pages_crawled, active_workers

        log(logger, logging.INFO, "Worker started", host=host)

        try:
            while True:
                try:
                    url = fifo_queue.get(timeout=5)
                except queue.Empty:
                    log(logger, logging.INFO, "Worker idle — queue empty, exiting", host=host)
                    break

                skip_reason = _skip_reason(url, visited, visited_lock)
                if skip_reason:
                    log(logger, logging.DEBUG, "URL skipped",
                        url=url, host=host, reason=skip_reason)
                    fifo_queue.task_done()
                    continue

                with pages_crawled_lock:
                    if pages_crawled >= max_pages:
                        log(logger, logging.INFO, "Page limit reached — worker stopping",
                            host=host, limit=max_pages)
                        fifo_queue.task_done()
                        break
                    pages_crawled += 1
                    current = pages_crawled

                page_start = time.perf_counter()
                log(logger, logging.INFO, "Crawling page",
                    url=url, host=host, progress=f"{current}/{max_pages}")

                html = fetch(url)
                fetch_ms = round((time.perf_counter() - page_start) * 1000)

                time.sleep(random.uniform(*delay_range))

                if not html:
                    fetch_fail_tracker.record_fail(host)
                    log(logger, logging.WARNING, "Empty response — page skipped",
                        url=url, host=host, fetch_ms=fetch_ms)
                    fifo_queue.task_done()
                    continue

                fetch_fail_tracker.record_success(host)
                crawl_history.record(url)
                host_block_tracker.record_success(host)
                save_page(html, url)

                all_links = extract_links(html, url)
                new_links = 0
                for link, anchor in all_links:
                    with visited_lock:
                        already_seen = link in visited
                    if not already_seen:
                        front_queue.push(link, anchor_text=anchor)
                        new_links += 1

                total_ms = round((time.perf_counter() - page_start) * 1000)
                log(logger, logging.INFO, "Page crawled",
                    url=url,
                    host=host,
                    progress=f"{current}/{max_pages}",
                    fetch_ms=fetch_ms,
                    total_ms=total_ms,
                    page_bytes=len(html),
                    links_found=len(all_links),
                    new_links_queued=new_links,
                )

                fifo_queue.task_done()

        except Exception as exc:
            log(logger, logging.ERROR, "Worker crashed unexpectedly",
                host=host, error=str(exc), exc_info=True)

        finally:
            with active_workers_lock:
                active_workers -= 1
                log(logger, logging.INFO, "Worker finished",
                    host=host, active_workers=active_workers)
                if active_workers == 0:
                    all_done.set()

    # ── Wiring ────────────────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        def spawn_worker(host: str, q: queue.Queue) -> None:
            nonlocal active_workers
            with active_workers_lock:
                active_workers += 1
            futures[host] = executor.submit(worker, host, q)

        back_queue = BackQueueRouter(max_queues=max_workers, spawn_worker_fn=spawn_worker)
        front_queue = FrontQueue(
            num_buckets=FRONT_QUEUE_BUCKETS,
            scorer=combined_scorer,
            router_fn=back_queue.route,
        )

        for seed in seeds:
            front_queue.push(seed)

        log(logger, logging.INFO, "All seeds queued — waiting for workers",
            seed_count=len(seeds))

        all_done.wait()
        front_queue.stop()

    total_s = round(time.perf_counter() - crawl_start, 2)
    pages_per_sec = round(pages_crawled / total_s, 2) if total_s > 0 else 0

    log(logger, logging.INFO, "Crawl complete",
        pages_crawled=pages_crawled,
        total_s=total_s,
        pages_per_sec=pages_per_sec,
        output_csv=output_csv,
    )

    export_visited_csv(visited, output_csv)
    return visited


# ── Private helpers ───────────────────────────────────────────────────────────

def _skip_reason(url: str, visited: Set[str], lock: threading.Lock) -> str:
    """
    Return a non-empty reason string if the URL should be skipped,
    or empty string if it's safe to crawl (and marks it visited).

    Check order matters — cheapest checks first:
      1. Extension filter   (no I/O)
      2. Host abandoned     (no I/O — in-memory set lookup)
      3. Already visited    (no I/O — in-memory set lookup)
      4. robots.txt         (cached after first fetch per host)
    """
    if should_skip_url(url):
        return "ignored_extension"

    host = url.split("/")[2] if "//" in url else ""

    if host_block_tracker.is_abandoned(host):
        return "host_abandoned"

    if fetch_fail_tracker.is_abandoned(host):
        return "host_fetch_failing"

    with lock:
        if url in visited:
            return "already_visited"
        # Mark visited optimistically — prevents other workers racing on same URL.
        visited.add(url)

    if not robots_cache.is_allowed(url):
        host_block_tracker.record_block(host)
        # Un-mark visited: we never actually crawled it.
        with lock:
            visited.discard(url)
        return "robots_disallowed"

    return ""