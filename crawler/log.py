"""
log.py — Logging configuration for the crawler.

All other modules just use: get_logger(__name__)

Design:
  - Each module gets its own named logger (better filtering/tracing).
  - File handler   : DEBUG+,  JSON format  → machine-queryable with jq / Grafana.
  - Console handler: WARNING+, human format → visible problems during a crawl.
  - basicConfig is NOT used — it's a no-op if handlers already exist.
"""
import json
import logging
import sys
import traceback
from logging.handlers import RotatingFileHandler
from typing import Any

from config import LOG_FILE

# ── Constants ─────────────────────────────────────────────────────────────────
HUMAN_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
DATE_FORMAT  = "%Y-%m-%d %H:%M:%S"
MAX_BYTES    = 5 * 1024 * 1024   # 5 MB per file
BACKUP_COUNT = 3                  # keep crawler.log, crawler.log.1, .2, .3


# ── JSON formatter (file handler) ────────────────────────────────────────────
class _JsonFormatter(logging.Formatter):
    """
    Emits one JSON object per line. Structured fields attached via
    logger.info("msg", extra={"data": {...}}) are merged into the top level.

    Example output:
        {"ts": "2024-01-15 14:32:01", "level": "INFO", "module": "fetcher",
         "msg": "Page fetched", "url": "https://...", "status": 200, "duration_ms": 312}
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts":     self.formatTime(record, DATE_FORMAT),
            "level":  record.levelname,
            "module": record.name,
            "msg":    record.getMessage(),
        }

        # Merge any structured fields passed via extra={"data": {...}}
        data = getattr(record, "data", None)
        if isinstance(data, dict):
            payload.update(data)

        # Attach exception traceback when present
        if record.exc_info:
            payload["traceback"] = traceback.format_exception(*record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


# ── Public API ────────────────────────────────────────────────────────────────

def setup_logging(level: int = logging.INFO) -> None:
    """
    Call once at startup (in main.py) before any other import.
    Safe to call multiple times — skips setup if handlers already exist.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured — don't duplicate handlers

    root.setLevel(level)

    # Rotating JSON file — DEBUG and above (full detail for post-mortem)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(_JsonFormatter())

    # Console stderr — WARNING and above (only problems visible during a run)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter(HUMAN_FORMAT, datefmt=DATE_FORMAT))

    root.addHandler(file_handler)
    root.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Use in every module: logger = get_logger(__name__)"""
    return logging.getLogger(name)


def log(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    """
    Emit a structured log record with arbitrary key-value fields.

    Usage:
        log(logger, logging.INFO, "Page fetched",
            url=url, status=200, duration_ms=312, page_bytes=48_000)
    """
    logger.log(level, msg, extra={"data": fields})