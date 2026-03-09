"""
fetcher.py — Responsible only for downloading raw HTML.
No parsing, no scoring, no queuing.
"""
import random
import time

import requests
from requests.exceptions import RequestException

from config import USER_AGENTS, REQUEST_TIMEOUT, MAX_FETCH_RETRIES, RETRY_BASE_DELAY
from log import get_logger, log
import logging

logger = get_logger(__name__)


def _build_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }


def fetch(url: str, max_tries: int = MAX_FETCH_RETRIES) -> str:
    """
    Download a URL and return its HTML body.
    Returns an empty string on failure or non-HTML content.
    """
    for attempt in range(max_tries):
        t0 = time.perf_counter()
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers=_build_headers(),
            )
            duration_ms = round((time.perf_counter() - t0) * 1000)

            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    log(logger, logging.DEBUG, "Page fetched",
                        url=url,
                        status=200,
                        duration_ms=duration_ms,
                        page_bytes=len(response.content),
                        content_type=content_type,
                        attempt=attempt + 1,
                    )
                    return response.text
                else:
                    # 200 but not HTML (e.g. JSON, XML, binary)
                    log(logger, logging.WARNING, "Non-HTML response skipped",
                        url=url,
                        status=200,
                        content_type=content_type,
                        duration_ms=duration_ms,
                    )
                    break
            elif response.status_code == 429:  # Too Many Requests
                wait = 2 ** attempt  # 1s, 2s, 4s, 8s, 16s...
                retry_after = response.headers.get("Retry-After")
                wait = int(retry_after) if retry_after else wait
                print(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                log(logger, logging.WARNING, "Non-200 response",
                    url=url,
                    status=response.status_code,
                    duration_ms=duration_ms,
                    attempt=attempt + 1,
                )
                break  # don't retry on HTTP errors (4xx/5xx)

        except RequestException as e:
            duration_ms = round((time.perf_counter() - t0) * 1000)
            backoff = RETRY_BASE_DELAY + 2 ** attempt
            log(logger, logging.WARNING, "Fetch failed — retrying",
                url=url,
                attempt=attempt + 1,
                max_tries=max_tries,
                error=str(e),
                retry_in_s=backoff,
                duration_ms=duration_ms,
            )
            time.sleep(backoff)

    log(logger, logging.ERROR, "Fetch gave up after all retries",
        url=url,
        max_tries=max_tries,
    )
    return ""