"""
robots.py — Thread-safe robots.txt cache + per-host block tracking.
"""
import logging
import threading
from typing import Dict, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from infrastructure.config import ROBOTS_MAX_CONSECUTIVE_BLOCKS, MAX_CONSECUTIVE_FETCH_FAILS
from infrastructure.logging_utils import get_logger, log

logger = get_logger(__name__)


class RobotsCache:
    """
    Fetches and caches robots.txt per host.
    Thread-safe: multiple crawler workers can call is_allowed() concurrently.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Optional[RobotFileParser]] = {}
        self._lock = threading.Lock()

    def is_allowed(self, url: str, user_agent: str = "*", fallback: bool = True) -> bool:
        parsed = urlparse(url)
        host = parsed.netloc
        robot_url = f"{parsed.scheme}://{host}/robots.txt"

        rp = self._get_parser(host, robot_url)
        if rp is None:
            log(logger, logging.DEBUG, "robots.txt unavailable — using fallback",
                url=url, host=host, fallback=fallback)
            return fallback

        allowed = rp.can_fetch(user_agent, url)
        if not allowed:
            log(logger, logging.WARNING, f"URL {robot_url} disallowed by robots.txt",
                url=url, host=host, user_agent=user_agent)
        else:
            log(logger, logging.DEBUG, "URL allowed by robots.txt",
                url=url, host=host)
        return allowed

    def _get_parser(self, host: str, robot_url: str) -> Optional[RobotFileParser]:
        with self._lock:
            if host not in self._cache:
                self._cache[host] = self._fetch_parser(host, robot_url)
            return self._cache[host]

    @staticmethod
    def _fetch_parser(host: str, robot_url: str) -> Optional[RobotFileParser]:
        rp = RobotFileParser()
        rp.set_url(robot_url)
        try:
            rp.read()
            log(logger, logging.INFO, "robots.txt fetched and cached",
                host=host, robot_url=robot_url)
            return rp
        except Exception as exc:
            log(logger, logging.WARNING, "robots.txt fetch failed",
                host=host, robot_url=robot_url, error=str(exc))
            return None


class HostBlockTracker:
    """
    Tracks consecutive robots.txt denials per host.

    After *max_consecutive_blocks* denials in a row with no successful crawl
    in between, the host is marked as abandoned and all its URLs are silently
    dropped — stopping workers from burning cycles on a host that will never
    yield a page.

    A successful crawl on a host resets its counter to zero.
    """

    def __init__(self, max_consecutive_blocks: int = ROBOTS_MAX_CONSECUTIVE_BLOCKS) -> None:
        self._max = max_consecutive_blocks
        self._blocks: Dict[str, int] = {}   # host -> consecutive block count
        self._abandoned: set = set()         # hosts that have hit the limit
        self._lock = threading.Lock()

    def record_block(self, host: str) -> None:
        """Call whenever robots.txt denies a URL for *host*."""
        with self._lock:
            if host in self._abandoned:
                return
            self._blocks[host] = self._blocks.get(host, 0) + 1
            count = self._blocks[host]

        if count >= self._max:
            with self._lock:
                self._abandoned.add(host)
            log(logger, logging.WARNING, "Host abandoned — too many robots.txt blocks",
                host=host, consecutive_blocks=count, limit=self._max)

    def record_success(self, host: str) -> None:
        """Call whenever a page is successfully crawled from *host*."""
        with self._lock:
            self._blocks[host] = 0

    def is_abandoned(self, host: str) -> bool:
        """Return True if this host should be skipped entirely."""
        with self._lock:
            return host in self._abandoned


# Module-level singletons shared across all workers.
robots_cache = RobotsCache()
host_block_tracker = HostBlockTracker()


class FetchFailTracker:
    """
    Tracks consecutive empty/failed fetches per host.

    After *max_consecutive_fails* fetch failures in a row with no success
    in between, the host is abandoned — workers stop wasting time retrying
    a host that is actively blocking scrapers (403/429/empty body).

    A successful fetch resets the counter.
    """

    def __init__(self, max_consecutive_fails: int = MAX_CONSECUTIVE_FETCH_FAILS) -> None:
        self._max = max_consecutive_fails
        self._fails: Dict[str, int] = {}
        self._abandoned: set = set()
        self._lock = threading.Lock()

    def record_fail(self, host: str) -> None:
        """Call whenever a fetch returns empty or a non-200 status for *host*."""
        with self._lock:
            if host in self._abandoned:
                return
            self._fails[host] = self._fails.get(host, 0) + 1
            count = self._fails[host]

        if count >= self._max:
            with self._lock:
                self._abandoned.add(host)
            log(logger, logging.WARNING, "Host abandoned — too many fetch failures",
                host=host, consecutive_fails=count, limit=self._max)

    def record_success(self, host: str) -> None:
        """Call whenever a fetch succeeds for *host*."""
        with self._lock:
            self._fails[host] = 0

    def is_abandoned(self, host: str) -> bool:
        with self._lock:
            return host in self._abandoned


fetch_fail_tracker = FetchFailTracker()