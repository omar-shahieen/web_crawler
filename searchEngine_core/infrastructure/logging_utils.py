"""
log.py — Logging configuration for the crawler.

All other modules just use: get_logger(__name__)
"""
import json
import logging
import sys
import traceback
from logging.handlers import RotatingFileHandler
from typing import Any

from infrastructure.config import LOG_FILE


HUMAN_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, DATE_FORMAT),
            "level": record.levelname,
            "module": record.name,
            "msg": record.getMessage(),
        }

        data = getattr(record, "data", None)
        if isinstance(data, dict):
            payload.update(data)

        if record.exc_info:
            payload["traceback"] = traceback.format_exception(*record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(level)

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(_JsonFormatter())

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter(HUMAN_FORMAT, datefmt=DATE_FORMAT))

    root.addHandler(file_handler)
    root.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    logger.log(level, msg, extra={"data": fields})