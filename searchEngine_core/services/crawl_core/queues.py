"""
queues.py — Priority front-queue + per-host back-queue routing.
"""
import logging
import queue
import random
import threading
import time
from typing import Callable, Dict, Optional
from urllib.parse import urlparse

from infrastructure.config import FRONT_QUEUE_BUCKETS
from infrastructure.logging_utils import get_logger, log

logger = get_logger(__name__)


class FrontQueue:
    def __init__(
        self,
        num_buckets: int = FRONT_QUEUE_BUCKETS,
        scorer: Callable[..., int] = lambda url, **_: 5,
        router_fn: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._buckets = [queue.Queue() for _ in range(num_buckets)]
        self._scorer = scorer
        self._router_fn = router_fn
        self._stop_event = threading.Event()

        weights = [2 ** (num_buckets - 1 - i) for i in range(num_buckets)]
        self._weighted_indices = [i for i, w in enumerate(weights) for _ in range(w)]

        self._thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._thread.start()

    def push(self, url: str, **scorer_kwargs) -> None:
        score = self._scorer(url, **scorer_kwargs)
        bucket_idx = min(score, len(self._buckets) - 1)
        self._buckets[bucket_idx].put(url)
        log(logger, logging.DEBUG, "URL queued",
            url=url, bucket=bucket_idx, score=score)

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join()

    def _pick_bucket(self) -> Optional[queue.Queue]:
        indices = self._weighted_indices.copy()
        random.shuffle(indices)
        for idx in indices:
            if not self._buckets[idx].empty():
                return self._buckets[idx]
        return None

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            bucket = self._pick_bucket()
            if bucket is None:
                time.sleep(0.1)
                continue
            try:
                url = bucket.get_nowait()
                if self._router_fn:
                    self._router_fn(url)
            except queue.Empty:
                continue


class BackQueueRouter:
    def __init__(
        self,
        max_queues: int,
        spawn_worker_fn: Callable[[str, queue.Queue], None],
    ) -> None:
        self._max_queues = max_queues
        self._spawn_worker_fn = spawn_worker_fn
        self._table: Dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def route(self, url: str) -> None:
        host = urlparse(url).netloc
        if not host:
            return
        q = self._get_or_create_queue(host)
        q.put(url)

    def _get_or_create_queue(self, host: str) -> queue.Queue:
        with self._lock:
            # Host already has a queue (own or inherited via overflow) — fast path
            if host in self._table:
                return self._table[host]

            if len(self._table) >= self._max_queues:
                # Assign host to an existing queue and STORE the mapping so
                # this branch (and its WARNING) only fires once per host.
                target = list(self._table.keys())[hash(host) % self._max_queues]
                self._table[host] = self._table[target]   # <-- key fix
                log(logger, logging.WARNING, "Queue overflow — host hashed to existing queue",
                    host=host,
                    assigned_to=target,
                    total_queues=len(self._table),
                    max_queues=self._max_queues,
                )
                return self._table[host]

            q: queue.Queue = queue.Queue()
            self._table[host] = q
            self._spawn_worker_fn(host, q)
            log(logger, logging.INFO, "New host queue and worker spawned",
                host=host,
                total_queues=len(self._table),
            )
            return q