# Aggregated note copy of Python code from searchEngine_core
# Includes only .py files from the folder tree

# ===== BEGIN FILE: searchEngine_core/cli/__init__.py =====
from cli.commands import *
# ===== END FILE: searchEngine_core/cli/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/cli/commands.py =====
import argparse

from infrastructure.logging_utils import setup_logging
from presentation.api_app import create_app
from services.crawl_service import crawl_and_index, crawl_web, index_content


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search engine operations")
    subparsers = parser.add_subparsers(dest="command")

    crawl_parser = subparsers.add_parser("crawl", help="Run the crawler only")
    crawl_parser.add_argument("--max-pages", type=int, default=100)
    crawl_parser.add_argument("--max-workers", type=int, default=10)
    crawl_parser.add_argument("--delay-min", type=float, default=1.5)
    crawl_parser.add_argument("--delay-max", type=float, default=3.0)

    index_parser = subparsers.add_parser("index", help="Run indexing only")

    crawl_index_parser = subparsers.add_parser("crawl-index", help="Run crawler then indexer")
    crawl_index_parser.add_argument("--max-pages", type=int, default=100)
    crawl_index_parser.add_argument("--max-workers", type=int, default=10)
    crawl_index_parser.add_argument("--delay-min", type=float, default=1.5)
    crawl_index_parser.add_argument("--delay-max", type=float, default=3.0)

    serve_parser = subparsers.add_parser("serve", help="Run the Flask API")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=3001)
    serve_parser.add_argument("--debug", action="store_true")

    return parser


def main() -> None:
    setup_logging()
    parser = _build_parser()
    args = parser.parse_args()
    command = args.command or "index"

    if command == "crawl":
        crawl_web(
            max_pages=args.max_pages,
            max_workers=args.max_workers,
            delay_range=(args.delay_min, args.delay_max),
        )
        return

    if command == "crawl-index":
        crawl_and_index(
            max_pages=args.max_pages,
            max_workers=args.max_workers,
            delay_range=(args.delay_min, args.delay_max),
        )
        return

    if command == "serve":
        create_app().run(host=args.host, port=args.port, debug=args.debug)
        return

    index_content()
# ===== END FILE: searchEngine_core/cli/commands.py =====

# ===== BEGIN FILE: searchEngine_core/domain/__init__.py =====
from domain.fuzzy_matching import *
from domain.query_language import *
from domain.ranking import *
# ===== END FILE: searchEngine_core/domain/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/domain/fuzzy_matching.py =====
from typing import List, Sequence, Tuple


def max_fuzzy_distance(term: str) -> int:
    if len(term) <= 4:
        return 1
    if len(term) <= 8:
        return 2
    return 3


def fuzzy_match_weight(distance: int) -> float:
    return max(0.35, 0.75 - (0.15 * max(distance - 1, 0)))


def bounded_edit_distance(source: str, target: str, max_distance: int) -> int:
    if source == target:
        return 0

    if abs(len(source) - len(target)) > max_distance:
        return max_distance + 1

    if len(source) > len(target):
        source, target = target, source

    previous_previous: List[int] | None = None
    previous = list(range(len(target) + 1))

    for row_index, source_char in enumerate(source, start=1):
        current = [row_index]
        row_min = current[0]

        for column_index, target_char in enumerate(target, start=1):
            insert_cost = current[column_index - 1] + 1
            delete_cost = previous[column_index] + 1
            replace_cost = previous[column_index - 1] + (source_char != target_char)
            best_cost = min(insert_cost, delete_cost, replace_cost)

            if (
                previous_previous is not None
                and row_index > 1
                and column_index > 1
                and source[row_index - 1] == target[column_index - 2]
                and source[row_index - 2] == target[column_index - 1]
            ):
                best_cost = min(best_cost, previous_previous[column_index - 2] + 1)

            current.append(best_cost)
            row_min = min(row_min, best_cost)

        if row_min > max_distance:
            return max_distance + 1

        previous_previous, previous = previous, current

    return previous[-1]


def find_fuzzy_matches(
    term: str,
    candidates: Sequence[str],
    max_distance: int | None = None,
    max_expansions: int = 3,
    min_term_length: int = 3,
) -> List[Tuple[str, float]]:
    normalized_term = term.strip().lower()
    if len(normalized_term) < min_term_length:
        return []

    allowed_distance = max_distance if max_distance is not None else max_fuzzy_distance(normalized_term)
    matches: List[Tuple[str, float, int, int]] = []
    seen: set[str] = set()

    for candidate in candidates:
        if not isinstance(candidate, str):
            continue

        normalized_candidate = candidate.strip().lower()
        if not normalized_candidate or normalized_candidate in seen or normalized_candidate == normalized_term:
            continue

        if abs(len(normalized_candidate) - len(normalized_term)) > allowed_distance:
            continue

        distance = bounded_edit_distance(normalized_term, normalized_candidate, allowed_distance)
        if distance > allowed_distance:
            continue

        seen.add(normalized_candidate)
        matches.append(
            (
                normalized_candidate,
                fuzzy_match_weight(distance),
                distance,
                abs(len(normalized_candidate) - len(normalized_term)),
            )
        )

    matches.sort(key=lambda item: (item[2], item[3], -item[1], item[0]))
    return [(candidate, weight) for candidate, weight, _, _ in matches[:max_expansions]]
# ===== END FILE: searchEngine_core/domain/fuzzy_matching.py =====

# ===== BEGIN FILE: searchEngine_core/domain/query_language.py =====
import re
from typing import Dict, List, Optional


BOOLEAN_OPERATOR_PRECEDENCE = {
    "OR": 1,
    "AND": 2,
    "NOT": 3,
}


def extract_quoted_phrase(query: str) -> Optional[str]:
    match = re.search(r'"([^"]+)"', query)
    if not match:
        return None
    return match.group(1).strip()


def _tokenize_operator_query(query: str) -> Optional[List[str]]:
    tokens: List[str] = []
    buffer: List[str] = []
    in_quotes = False
    index = 0
    patterns = [(" NOT ", "NOT"), (" AND ", "AND"), (" OR ", "OR")]

    while index < len(query):
        char = query[index]
        if char == '"':
            in_quotes = not in_quotes
            buffer.append(char)
            index += 1
            continue

        if not in_quotes:
            matched_operator = False
            for pattern, operator in patterns:
                if query[index:].upper().startswith(pattern):
                    operand = "".join(buffer).strip()
                    if not operand:
                        return None
                    tokens.append(operand)
                    tokens.append(operator)
                    buffer = []
                    index += len(pattern)
                    matched_operator = True
                    break

            if matched_operator:
                continue

        buffer.append(char)
        index += 1

    if in_quotes:
        return None

    trailing_operand = "".join(buffer).strip()
    if not trailing_operand:
        return None

    tokens.append(trailing_operand)
    return tokens


def parse_query_with_operators(query: str) -> Optional[Dict]:
    tokens = _tokenize_operator_query(query)
    if not tokens:
        return None

    operator_count = (len(tokens) - 1) // 2
    if operator_count > 2:
        return None
    if operator_count == 0:
        return None

    operators = tokens[1::2]
    operands = tokens[0::2]
    if not all(operands):
        return None

    return {
        "tokens": tokens,
        "operators": operators,
        "count": operator_count,
    }


def count_boolean_operators(query: str) -> int:
    tokens = _tokenize_operator_query(query)
    if not tokens:
        return 0

    return sum(1 for token in tokens if token in BOOLEAN_OPERATOR_PRECEDENCE)


def extract_query_terms(query: str) -> List[str]:
    quoted_phrases = [match.group(1).strip() for match in re.finditer(r'"([^"]+)"', query) if match.group(1).strip()]
    query_without_phrases = re.sub(r'"[^"]+"', " ", query)
    unquoted_words = re.findall(r"[A-Za-z0-9]+", query_without_phrases)

    raw_terms: List[str] = []
    for phrase in quoted_phrases:
        raw_terms.append(phrase)
        raw_terms.extend(re.findall(r"[A-Za-z0-9]+", phrase))
    raw_terms.extend(unquoted_words)

    terms: List[str] = []
    seen = set()
    for term in raw_terms:
        normalized = term.strip()
        if not normalized:
            continue
        if normalized.upper() in BOOLEAN_OPERATOR_PRECEDENCE:
            continue

        key = normalized.lower()
        if key in seen:
            continue

        seen.add(key)
        terms.append(normalized)

    return terms


def to_postfix(tokens: List[str]) -> List[str]:
    postfix: List[str] = []
    operators: List[str] = []

    for token in tokens:
        if token in BOOLEAN_OPERATOR_PRECEDENCE:
            while operators and BOOLEAN_OPERATOR_PRECEDENCE[operators[-1]] >= BOOLEAN_OPERATOR_PRECEDENCE[token]:
                postfix.append(operators.pop())
            operators.append(token)
        else:
            postfix.append(token)

    while operators:
        postfix.append(operators.pop())

    return postfix
# ===== END FILE: searchEngine_core/domain/query_language.py =====

# ===== BEGIN FILE: searchEngine_core/domain/ranking.py =====
import math
from typing import Dict, List, Set

from bson import ObjectId

from infrastructure.database import Pages, Indeverted_index


_PAGERANK_CACHE: Dict[ObjectId, float] = {}
_PAGERANK_CACHE_DOC_COUNT: int = -1


def get_term_postings(term: str) -> List[dict]:
    row = Indeverted_index.find_one({"term": term}, {"postings": 1})
    if not row:
        return []
    return row.get("postings", [])


def compute_relevance_scores(weighted_terms: Dict[str, float], candidate_docs: Set[ObjectId]) -> Dict[ObjectId, float]:
    total_docs = max(Pages.count_documents({}), 1)
    scores: Dict[ObjectId, float] = {doc_id: 0.0 for doc_id in candidate_docs}

    for term, term_weight in weighted_terms.items():
        postings = get_term_postings(term)
        if not postings:
            continue

        df = max(len(postings), 1)
        idf = math.log(total_docs / df)

        for posting in postings:
            doc_id = posting.get("doc_id")
            if not isinstance(doc_id, ObjectId):
                continue
            if doc_id not in scores:
                continue

            tf_raw = posting.get("tf", 0)
            if not isinstance(tf_raw, (int, float)):
                continue
            page = Pages.find_one({"_id": doc_id}, {"word_count": 1})
            doc_len = max((page or {}).get("word_count", 1), 1)
            tf = tf_raw / doc_len
            scores[doc_id] += tf * idf * term_weight

    return scores


def _build_link_graph() -> tuple[Dict[ObjectId, Set[ObjectId]], Dict[ObjectId, float]]:
    pages = list(Pages.find({}, {"_id": 1, "url": 1, "out_links": 1}))
    if not pages:
        return {}, {}

    url_to_id: Dict[str, ObjectId] = {page["url"]: page["_id"] for page in pages if page.get("url")}
    adjacency: Dict[ObjectId, Set[ObjectId]] = {}

    for page in pages:
        source_id = page["_id"]
        links = page.get("out_links", [])
        targets: Set[ObjectId] = set()

        for link in links:
            normalized_link = ""
            if isinstance(link, str):
                normalized_link = link
            elif isinstance(link, (list, tuple)) and link:
                normalized_link = str(link[0])
            elif isinstance(link, dict):
                normalized_link = str(link.get("url", ""))

            if not normalized_link:
                continue

            target_id = url_to_id.get(normalized_link)
            if target_id and target_id != source_id:
                targets.add(target_id)

        adjacency[source_id] = targets

    size = len(adjacency)
    base_rank = 1.0 / size
    ranks: Dict[ObjectId, float] = {doc_id: base_rank for doc_id in adjacency}
    return adjacency, ranks


def _compute_pagerank_scores(iterations: int = 20, damping: float = 0.85) -> Dict[ObjectId, float]:
    adjacency, ranks = _build_link_graph()
    if not adjacency:
        return {}

    doc_ids = list(adjacency.keys())
    total_docs = len(doc_ids)
    base_jump = (1.0 - damping) / total_docs

    for _ in range(iterations):
        new_ranks: Dict[ObjectId, float] = {doc_id: base_jump for doc_id in doc_ids}
        sink_mass = 0.0

        for source_id, outbound_links in adjacency.items():
            source_rank = ranks[source_id]
            if not outbound_links:
                sink_mass += source_rank
                continue

            share = damping * source_rank / len(outbound_links)
            for destination_id in outbound_links:
                new_ranks[destination_id] += share

        sink_share = damping * sink_mass / total_docs
        for doc_id in doc_ids:
            new_ranks[doc_id] += sink_share

        ranks = new_ranks

    max_rank = max(ranks.values()) if ranks else 1.0
    if max_rank <= 0:
        return {doc_id: 0.0 for doc_id in ranks}

    return {doc_id: score / max_rank for doc_id, score in ranks.items()}


def compute_popularity_scores() -> Dict[ObjectId, float]:
    global _PAGERANK_CACHE
    global _PAGERANK_CACHE_DOC_COUNT

    doc_count = Pages.count_documents({})
    if doc_count == _PAGERANK_CACHE_DOC_COUNT and _PAGERANK_CACHE:
        return _PAGERANK_CACHE

    _PAGERANK_CACHE = _compute_pagerank_scores()
    _PAGERANK_CACHE_DOC_COUNT = doc_count
    return _PAGERANK_CACHE


def combine_scores(
    relevance_scores: Dict[ObjectId, float],
    popularity_scores: Dict[ObjectId, float],
    relevance_weight: float = 0.8,
    popularity_weight: float = 0.2,
) -> Dict[ObjectId, float]:
    if not relevance_scores:
        return {}

    max_rel = max(relevance_scores.values()) if relevance_scores else 1.0
    if max_rel <= 0:
        max_rel = 1.0

    final_scores: Dict[ObjectId, float] = {}
    for doc_id, relevance_score in relevance_scores.items():
        rel_norm = relevance_score / max_rel
        pop = popularity_scores.get(doc_id, 0.0)
        final_scores[doc_id] = (relevance_weight * rel_norm) + (popularity_weight * pop)

    return final_scores


def rank_documents(
    weighted_terms: Dict[str, float],
    candidate_docs: Set[ObjectId],
    relevance_weight: float = 0.8,
    popularity_weight: float = 0.2,
) -> Dict[ObjectId, float]:
    relevance_scores = compute_relevance_scores(weighted_terms, candidate_docs)
    popularity_scores = compute_popularity_scores()
    return combine_scores(
        relevance_scores,
        popularity_scores,
        relevance_weight=relevance_weight,
        popularity_weight=popularity_weight,
    )
# ===== END FILE: searchEngine_core/domain/ranking.py =====

# ===== BEGIN FILE: searchEngine_core/infrastructure/__init__.py =====
from infrastructure.config import *
from infrastructure.database import *
from infrastructure.logging_utils import *
# ===== END FILE: searchEngine_core/infrastructure/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/infrastructure/config.py =====
"""
config.py â€” Central configuration for the crawler.
All tuneable constants live here; nothing else imports raw literals.
"""
from typing import List, Set, Tuple

REQUEST_TIMEOUT: int = 5
MAX_FETCH_RETRIES: int = 3
RETRY_BASE_DELAY: float = 1.5

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/537.36 CriOS/120.0.0.0 Mobile/15E148 Safari/537.36",
]

EXCLUDED_PATHS: List[str] = ["/login", "/admin", "/signup", "/cart", "/checkout"]
EXCLUDED_QUERY_PARAMS: List[str] = ["q=", "search=", "filter=", "sort=", "sessionid", "utm_", "ref", "page"]

IGNORE_EXTENSIONS: Tuple[str, ...] = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".tar",
    ".mp4", ".mp3", ".css", ".js",
)

AUTHORITY_DOMAINS: Set[str] = {
    "wikipedia.org", "github.com",
    "stackoverflow.com", "reddit.com", "google.com",
}

HIGH_VALUE_KEYWORDS: Set[str] = {
    "api", "docs", "documentation", "guide", "tutorial",
    "reference", "research", "paper", "index", "overview",
}

LOW_VALUE_KEYWORDS: Set[str] = {
    "login", "logout", "signup", "cart", "checkout",
    "comment", "reply", "share", "print", "subscribe",
}

LOW_VALUE_PATH_TOKENS: Tuple[str, ...] = (
    ".pdf", ".jpg", ".png", ".zip", "login", "logout",
    "signup", "register", "comment", "tag", "page=",
)

STALE_AFTER_HOURS: float = 24.0
DEFAULT_MAX_PAGES: int = 50
DEFAULT_MAX_WORKERS: int = 5
DEFAULT_DELAY_RANGE: Tuple[float, float] = (1.5, 3.5)
FRONT_QUEUE_BUCKETS: int = 5
ROBOTS_MAX_CONSECUTIVE_BLOCKS: int = 5
MAX_CONSECUTIVE_FETCH_FAILS: int = 3
LOG_FILE: str = "crawl_index.log"
OUTPUT_CSV: str = "crawled_urls.csv"
# ===== END FILE: searchEngine_core/infrastructure/config.py =====

# ===== BEGIN FILE: searchEngine_core/infrastructure/database.py =====
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional, Union

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure


client: MongoClient = MongoClient("mongodb://localhost:27017/")
db: Any = client["search_engine"]
Pages: Collection = db["pages"]
Indeverted_index: Collection = db["inverted_index"]
Metadata = db["metadata"]


try:
    client.admin.command("ping")
    print("Connected to MongoDB successfully!")
except ConnectionFailure as error:
    print(f"Failed to connect to MongoDB: {error}")


class Page:
    def __init__(
        self,
        url: str,
        title: str,
        content: Optional[str],
        content_hash: Optional[str],
        out_links: Optional[List[str]] = None,
    ):
        self.url = url
        self.title = title
        self.content = content
        self._id: ObjectId = ObjectId()
        self.last_crawled: datetime = datetime.now(tz=timezone.utc)
        self.content_hash = content_hash
        self.out_links = out_links if out_links is not None else []

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        url_pattern = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE)
        if not value or not isinstance(value, str) or not url_pattern.match(value):
            raise ValueError(f"Invalid URL format: {value}")
        self._url = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value):
        if not value or len(value.strip()) < 2:
            raise ValueError("Title must be at least 2 characters long.")
        if len(value) > 500:
            raise ValueError("Title is too long (max 500 chars).")
        self._title = value.strip()

    def to_dict(self) -> Dict[str, Any]:
        words: List[str] = self.content.split() if self.content else []
        return {
            "_id": self._id,
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "last_crawled": self.last_crawled,
            "word_count": len(words),
            "content_hash": self.content_hash,
            "out_links": self.out_links,
        }

    def __repr__(self) -> str:
        return (
            f"Page(id={self._id}, "
            f"url='{self.url}', "
            f"title='{self.title}', "
            f"word_count={len(self.content.split()) if self.content else 0})"
        )

    def __str__(self) -> str:
        return f"[Page] {self.title} ({self.url})"


class Posting:
    def __init__(
        self, doc_id: Union[ObjectId, str], tf: int = 1, positions: Optional[List[int]] = None
    ) -> None:
        self.doc_id: Union[ObjectId, str] = doc_id
        self.tf: int = tf
        self.positions: List[int] = positions if positions is not None else []
        self.validate()

    def validate(self):
        if not isinstance(self.doc_id, ObjectId):
            try:
                self.doc_id = ObjectId(self.doc_id)
            except Exception as error:
                raise TypeError("doc_id must be a valid ObjectId or 24-char hex string.") from error

        if not isinstance(self.tf, int) or self.tf < 1:
            raise ValueError(f"Term Frequency (tf) must be a positive integer. Got: {self.tf}")

        if not isinstance(self.positions, list) or not all(isinstance(position, int) for position in self.positions):
            raise TypeError("Positions must be a list of integers.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "tf": self.tf,
            "pos": self.positions,
        }

    def __repr__(self):
        return (
            f"Posting(doc_id={self.doc_id}, "
            f"tf={self.tf}, "
            f"positions={self.positions})"
        )

    def __str__(self):
        return f"[Posting] doc={self.doc_id} | tf={self.tf}"


class InvertedIndex:
    def __init__(self, term: str, postings: Optional[List[Dict[str, Any]]] = None) -> None:
        self.term: str = term.lower().strip()
        self.postings: List[Dict[str, Any]] = postings if postings is not None else []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "postings": self.postings,
        }

    def __repr__(self):
        return (
            f"InvertedIndex(term='{self.term}', "
            f"postings_count={len(self.postings)})"
        )

    def __str__(self):
        return f"[Term] '{self.term}' -> {len(self.postings)} postings"


def get_last_indexed_timestamp():
    doc = Metadata.find_one({"_id": "indexer"})
    return doc["last_run"] if doc else datetime.min


def update_last_indexed_timestamp():
    Metadata.update_one(
        {"_id": "indexer"},
        {"$set": {"last_run": datetime.utcnow()}},
        upsert=True,
    )


Pages.create_index("url", unique=True)
Indeverted_index.create_index("term", unique=True)
Indeverted_index.create_index("postings.doc_id")
# ===== END FILE: searchEngine_core/infrastructure/database.py =====

# ===== BEGIN FILE: searchEngine_core/infrastructure/logging_utils.py =====
"""
log.py â€” Logging configuration for the crawler.

All other modules just use: get_logger(__name__)
"""
import json
import logging
import sys
import traceback
from logging.handlers import RotatingFileHandler
from typing import Any

from infrastructure.config import LOG_FILE


HUMAN_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s â€” %(message)s"
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
# ===== END FILE: searchEngine_core/infrastructure/logging_utils.py =====

# ===== BEGIN FILE: searchEngine_core/main.py =====
from cli.commands import main


if __name__ == "__main__":
    main()
# ===== END FILE: searchEngine_core/main.py =====

# ===== BEGIN FILE: searchEngine_core/presentation/__init__.py =====
from presentation.api_app import *
# ===== END FILE: searchEngine_core/presentation/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/presentation/api_app.py =====
from __future__ import annotations

from typing import Callable, List, Tuple

from flask import Flask, jsonify, request

from domain.query_language import count_boolean_operators, extract_query_terms
from services.search_service import extract_quoted_phrase, make_snippet, parse_query_with_operators, phrase_search, search_query, search_with_operators


DEFAULT_TOP_K = 10
MAX_TOP_K = 50


def _resolve_search_strategy(query_text: str) -> Callable[[str, int], List[Tuple[dict, float]]]:
    if parse_query_with_operators(query_text):
        return search_with_operators

    quoted_phrase = extract_quoted_phrase(query_text)
    if quoted_phrase:
        return lambda _query_text, top_k: phrase_search(quoted_phrase, top_k=top_k)

    return search_query


def _serialize_results(results: List[Tuple[dict, float]], query_text: str) -> List[dict]:
    query_terms = extract_query_terms(query_text)
    serialized: List[dict] = []
    for page, score in results:
        content = page.get("content", "")
        serialized.append(
            {
                "id": str(page.get("_id", "")),
                "title": page.get("title", "No Title"),
                "url": page.get("url", ""),
                "description": make_snippet(content, query_terms),
                "score": round(float(score), 6),
            }
        )
    return serialized


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/api/health")
    def health() -> tuple[dict, int]:
        return {"status": "ok"}, 200

    @app.get("/api/search")
    def search() -> tuple:
        query_text = request.args.get("q", "", type=str).strip()
        if not query_text:
            return jsonify({"error": "Missing required query parameter 'q'."}), 400

        if count_boolean_operators(query_text) > 2:
            return jsonify({"error": "Maximum number of boolean operators per search is 2."}), 400

        top_k = request.args.get("top", DEFAULT_TOP_K, type=int)
        top_k = max(1, min(top_k, MAX_TOP_K))

        search_fn = _resolve_search_strategy(query_text)
        results = search_fn(query_text, top_k=top_k)
        return jsonify(_serialize_results(results, query_text)), 200

    @app.errorhandler(Exception)
    def handle_unexpected_error(error: Exception):
        return jsonify({"error": str(error)}), 500

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3001, debug=False)
# ===== END FILE: searchEngine_core/presentation/api_app.py =====

# ===== BEGIN FILE: searchEngine_core/services/__init__.py =====
from services.crawl_service import *
from services.search_service import *
# ===== END FILE: searchEngine_core/services/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/__init__.py =====
"""Core crawl/index implementation package."""
# ===== END FILE: searchEngine_core/services/crawl_core/__init__.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/crawler.py =====
"""
crawler.py â€” Orchestrates the threaded crawl.
Wires together: FrontQueue â†’ BackQueueRouter â†’ per-host workers.
All business logic (fetching, parsing, scoring, saving) lives in other modules.
"""
import logging
import queue
import random
import threading
import time
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Set, Tuple

from infrastructure.config import (
    DEFAULT_DELAY_RANGE,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_WORKERS,
    FRONT_QUEUE_BUCKETS,
    OUTPUT_CSV,
)
from .fetcher import fetch
from infrastructure.logging_utils import get_logger, log
from .parser import extract_links, should_skip_url
from .queues import BackQueueRouter, FrontQueue
from .robots import robots_cache, host_block_tracker, fetch_fail_tracker
from .scorer import combined_scorer, crawl_history
from .storage import export_visited_csv, save_page

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

    # â”€â”€ Shared state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    visited: Set[str] = set()
    visited_lock = threading.Lock()

    # Detailed per-page crawl info (used to compute precision/recall later)
    crawled_pages_info: list[dict] = []
    crawled_info_lock = threading.Lock()

    skip_reasons: dict = {}
    skip_reasons_lock = threading.Lock()

    pages_crawled = 0
    pages_crawled_lock = threading.Lock()

    active_workers = 0
    active_workers_lock = threading.Lock()
    all_done = threading.Event()

    # â”€â”€ Worker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def worker(host: str, fifo_queue: queue.Queue) -> None:
        nonlocal pages_crawled, active_workers

        log(logger, logging.INFO, "Worker started", host=host)

        try:
            while True:
                try:
                    url = fifo_queue.get(timeout=5)
                except queue.Empty:
                    log(logger, logging.INFO, "Worker idle â€” queue empty, exiting", host=host)
                    break

                skip_reason = _skip_reason(url, visited, visited_lock)
                if skip_reason:
                    log(logger, logging.DEBUG, "URL skipped",
                        url=url, host=host, reason=skip_reason)
                    with skip_reasons_lock:
                        skip_reasons[skip_reason] = skip_reasons.get(skip_reason, 0) + 1
                    # record skipped URL for later analysis
                    with crawled_info_lock:
                        crawled_pages_info.append({
                            "url": url,
                            "predicted_score": combined_scorer(url),
                            "fetched": False,
                            "saved": False,
                            "fetch_ms": None,
                            "total_ms": None,
                            "page_bytes": 0,
                            "links_found": 0,
                            "new_links": 0,
                            "skip_reason": skip_reason,
                            "timestamp": datetime.utcnow().isoformat(),
                            "label": "",
                        })
                    fifo_queue.task_done()
                    continue

                with pages_crawled_lock:
                    if pages_crawled >= max_pages:
                        log(logger, logging.INFO, "Page limit reached â€” worker stopping",
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
                    log(logger, logging.WARNING, "Empty response â€” page skipped",
                        url=url, host=host, fetch_ms=fetch_ms)
                    with skip_reasons_lock:
                        skip_reasons["fetch_failed"] = skip_reasons.get("fetch_failed", 0) + 1
                    with crawled_info_lock:
                        crawled_pages_info.append({
                            "url": url,
                            "predicted_score": combined_scorer(url),
                            "fetched": False,
                            "saved": False,
                            "fetch_ms": fetch_ms,
                            "total_ms": None,
                            "page_bytes": 0,
                            "links_found": 0,
                            "new_links": 0,
                            "skip_reason": "fetch_failed",
                            "timestamp": datetime.utcnow().isoformat(),
                            "label": "",
                        })
                    fifo_queue.task_done()
                    continue

                fetch_fail_tracker.record_success(host)
                crawl_history.record(url)
                host_block_tracker.record_success(host)
                saved = save_page(html, url)

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

                # record per-page detailed info
                with crawled_info_lock:
                    crawled_pages_info.append({
                        "url": url,
                        "predicted_score": combined_scorer(url),
                        "fetched": True,
                        "saved": bool(saved),
                        "fetch_ms": fetch_ms,
                        "total_ms": total_ms,
                        "page_bytes": len(html),
                        "links_found": len(all_links),
                        "new_links": new_links,
                        "skip_reason": "",
                        "timestamp": datetime.utcnow().isoformat(),
                        "label": "",
                    })

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

    # â”€â”€ Wiring â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

        log(logger, logging.INFO, "All seeds queued â€” waiting for workers",
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

    # Write detailed per-page CSV to support precision/recall calculations.
    details_path = output_csv[:-4] + "_details.csv" if output_csv.lower().endswith('.csv') else output_csv + ".details.csv"
    fieldnames = [
        "url", "predicted_score", "label", "fetched", "saved",
        "fetch_ms", "total_ms", "page_bytes", "links_found", "new_links",
        "skip_reason", "timestamp",
    ]
    try:
        with open(details_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            with crawled_info_lock:
                for row in crawled_pages_info:
                    # ensure all keys exist
                    writer.writerow({k: row.get(k, "") for k in fieldnames})
        log(logger, logging.INFO, "Wrote detailed crawl CSV",
            path=details_path, total_rows=len(crawled_pages_info))
    except Exception as exc:
        log(logger, logging.ERROR, "Failed to write detailed CSV",
            path=details_path, error=str(exc), exc_info=True)

    # Log aggregated skip reason counts
    with skip_reasons_lock:
        for reason, cnt in skip_reasons.items():
            log(logger, logging.INFO, "Skip reason summary", reason=reason, count=cnt)

    return visited


# â”€â”€ Private helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _skip_reason(url: str, visited: Set[str], lock: threading.Lock) -> str:
    """
    Return a non-empty reason string if the URL should be skipped,
    or empty string if it's safe to crawl (and marks it visited).

    Check order matters â€” cheapest checks first:
      1. Extension filter   (no I/O)
      2. Host abandoned     (no I/O â€” in-memory set lookup)
      3. Already visited    (no I/O â€” in-memory set lookup)
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
        # Mark visited optimistically â€” prevents other workers racing on same URL.
        visited.add(url)

    if not robots_cache.is_allowed(url):
        host_block_tracker.record_block(host)
        # Un-mark visited: we never actually crawled it.
        with lock:
            visited.discard(url)
        return "robots_disallowed"

    return ""
# ===== END FILE: searchEngine_core/services/crawl_core/crawler.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/fetcher.py =====
"""
fetcher.py â€” Responsible only for downloading raw HTML.
No parsing, no scoring, no queuing.
"""
import random
import time

import requests
from requests.exceptions import RequestException

from infrastructure.config import USER_AGENTS, REQUEST_TIMEOUT, MAX_FETCH_RETRIES, RETRY_BASE_DELAY
from infrastructure.logging_utils import get_logger, log
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
            log(logger, logging.WARNING, "Fetch failed â€” retrying",
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
# ===== END FILE: searchEngine_core/services/crawl_core/fetcher.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/indexer.py =====
import hashlib
from collections import defaultdict
from typing import List, Optional

import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from pymongo import UpdateOne
from datetime import datetime, timezone
from infrastructure.logging_utils import get_logger
from infrastructure.database import Page, Posting, Pages, Indeverted_index, update_last_indexed_timestamp, get_last_indexed_timestamp


nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)
nltk.download("punkt_tab", quiet=True)

porter_stemmer = PorterStemmer()
stop_words = set(stopwords.words("english"))


logger = get_logger(__name__)



# -- Text Processing ----------------------------------------------------------


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _tokenize(text: str) -> list[str]:
    tokens = word_tokenize(text.lower())
    return [w for w in tokens if w.isalpha() and w not in stop_words]


def _stemming(tokens: list[str]) -> list[str]:
    return [porter_stemmer.stem(w) for w in tokens]


def _preprocess(text: str) -> list[str]:
    return _stemming(_tokenize(text))


# -- HTML Parsing -------------------------------------------------------------


def _filter_page(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "html.parser")

    # Remove junk tags.
    for tag in soup(
        ["script", "style", "nav", "footer", "header", "aside", "noscript", "form", "iframe", "svg", "canvas"]
    ):
        tag.decompose()

    # Remove common ad/cookie classes.
    for tag in soup.find_all(True, class_=True):
        # Some pages contain malformed tags where attrs is None.
        raw_attrs = getattr(tag, "attrs", {}) or {}
        class_attr = raw_attrs.get("class", [])
        if isinstance(class_attr, (list, tuple)):
            classes = " ".join(str(c) for c in class_attr if c).lower()
        else:
            classes = str(class_attr).lower()
        if any(x in classes for x in ["cookie", "advert", "promo", "banner", "subscribe"]):
            tag.decompose()

    # Extract title.
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"

    # Extract description.
    desc_tag = soup.find("meta", attrs={"name": "description"})
    description = str(desc_tag.get("content", "")) if desc_tag else ""

    # Extract meaningful tags only.
    content_block = []
    for tag in soup.find_all(["article", "main", "section", "p", "h1", "h2", "h3"]):
        text = tag.get_text(" ", strip=True)
        if len(text.split()) > 5:
            content_block.append(text)

    body_text = " ".join(content_block)

    # Alt text for images.
    alt_text = " ".join(str(img.get("alt", "")) for img in soup.find_all("img", alt=True))

    return body_text + " " + alt_text, title, description


def _is_content_valid(text: str) -> bool:
    words = text.split()
    return (
        len(words) > 100 and  # minimum length
        len(set(words)) > 50 and  # vocabulary diversity
        sum(c.isalpha() for c in text) > 500  # avoid junk pages
    )


def _normalize_out_links(out_links: Optional[List[str]]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()

    for link in out_links or []:
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


def store_page(url: str, html: str, out_links: Optional[List[str]] = None) -> Optional[Page]:
    content, title, _ = _filter_page(html)
    normalized_out_links = _normalize_out_links(out_links)

    if not _is_content_valid(content):
        logger.info("Skipped low-quality page: %s", url)
        return None

    new_hash = _content_hash(content)
    existing = Pages.find_one({"url": url})

    if existing:
        if existing.get("content_hash") == new_hash:
            logger.debug("No changes detected for %s", url)
            return None

        logger.info("Page updated: %s", url)
        Pages.update_one(
            {"url": url},
            {
                "$set": {
                    "title": title,
                    "content": content,
                    "content_hash": new_hash,
                    "out_links": normalized_out_links,
                    "last_crawled": datetime.now(tz=timezone.utc), 

                }
            },
        )
        return Page(
            url=url,
            title=title,
            content=content,
            content_hash=new_hash,
            out_links=normalized_out_links,
        )

    page = Page(
        url=url,
        title=title,
        content=content,
        content_hash=new_hash,
        out_links=normalized_out_links,
    )
    Pages.insert_one(page.to_dict())
    return page



# -- In-Memory Index Building  [ depricated ] -------------------------------------------------

# Structure: { "term": { ObjectId: {"tf": int, "positions": [int]} } }
mapped_inverted_index: dict = defaultdict(dict)

def build_postings(doc_id, title: str, content: str):
    """Update the in-memory index for one document."""
    content_tokens = _preprocess(content)
    title_tokens = _preprocess(title)
    tokens = content_tokens + title_tokens * 3

    for idx, term in enumerate(tokens):
        if doc_id not in mapped_inverted_index[term]:
            mapped_inverted_index[term][doc_id] = {"tf": 0, "positions": []}

        mapped_inverted_index[term][doc_id]["tf"] += 1
        mapped_inverted_index[term][doc_id]["positions"].append(idx)


def index_all_pages():
    """Load every page from MongoDB and build the in-memory inverted index."""
    for raw in Pages.find({}, {"_id": 1, "title": 1, "content": 1}):
        build_postings(raw["_id"], raw.get("title", ""), raw.get("content", ""))
    logger.info("Indexed %d unique terms.", len(mapped_inverted_index))

def remove_doc_from_index(doc_id):
    """
    Pull this doc_id out of every posting list it appears in.
    $pull removes matching elements from an array.
    """
    Indeverted_index.update_many(
        {"postings.doc_id": doc_id},
        {"$pull": {"postings": {"doc_id": doc_id}}}
    )
    
    

# -- Persist Index to MongoDB -------------------------------------------------


def flush_index_to_mongo():
    """Write the in-memory index to MongoDB using bulk upserts."""
    ops = []
    for term, doc_map in mapped_inverted_index.items():
        postings = [Posting(doc_id, data["tf"], data["positions"]).to_dict() for doc_id, data in doc_map.items()]
        ops.append(
            UpdateOne(
                {"term": term},
                {"$set": {"term": term, "postings": postings}},
                upsert=True,
            )
        )

    if ops:
        result = Indeverted_index.bulk_write(ops, ordered=False)
        logger.info(
            "Flushed %d index entries to MongoDB.",
            result.upserted_count + result.modified_count,
        )


# -- build index directly to mongodb 

def build_and_flush_postings(doc_id, title: str, content: str):
    """Process one document and write directly to MongoDB â€” no memory buffer."""
    content_tokens = _preprocess(content)
    title_tokens = _preprocess(title)
    tokens = content_tokens + title_tokens * 3

    # Group positions by term locally (just for this one doc, not all docs)
    term_data: dict = {}
    for idx, term in enumerate(tokens):
        if term not in term_data:
            term_data[term] = {"tf": 0, "positions": []}
        term_data[term]["tf"] += 1
        term_data[term]["positions"].append(idx)

    # Build bulk ops for this single document
    ops = []
    for term, data in term_data.items():
        posting = Posting(doc_id, data["tf"], data["positions"]).to_dict()
        ops.append(
            UpdateOne(
                {"term": term},
                {
                    # Remove any existing posting for this doc first
                    "$pull": {"postings": {"doc_id": doc_id}},
                },
            )
        )
        ops.append(
            UpdateOne(
                {"term": term},
                {
                    "$push": {"postings": posting},
                    "$setOnInsert": {"term": term},
                },
                upsert=True,
            )
        )

    if ops:
        Indeverted_index.bulk_write(ops, ordered=True)  # ordered=True so $pull runs before $push

# -- Entry Point --------------------------------------------------------------

def remove_deleted_pages_from_index():
    # Get all doc_ids currently in the index
    indexed_ids = set(
        p["doc_id"]
        for entry in Indeverted_index.find({}, {"postings.doc_id": 1})
        for p in entry["postings"]
    )
    # Get all doc_ids currently in Pages
    live_ids = set(doc["_id"] for doc in Pages.find({}, {"_id": 1}))
    
    for dead_id in indexed_ids - live_ids:
        remove_doc_from_index(dead_id)
        
        
def run_indexer():
    """streams each page directly to MongoDB."""
    last_indexed = get_last_indexed_timestamp()

    changed_pages = Pages.find(
        {"last_crawled": {"$gt": last_indexed}},
        {"_id": 1, "title": 1, "content": 1}
    )

    count = 0
    for raw in changed_pages:
        build_and_flush_postings(
            raw["_id"],
            raw.get("title", ""),
            raw.get("content", "")
        )
        
        
        print(f"Indexed page {count}: {raw['_id']}" )
        count += 1
        logger.info("Indexed page %d: %s", count, raw["_id"])

    update_last_indexed_timestamp()
    logger.info("Done. Indexed %d pages directly to MongoDB.", count)
# ===== END FILE: searchEngine_core/services/crawl_core/indexer.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/parser.py =====
"""
parser.py â€” Parses HTML and extracts filtered, normalised links.
No HTTP, no scoring, no side effects.
"""
import logging
from typing import Set, Tuple
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from infrastructure.config import EXCLUDED_PATHS, EXCLUDED_QUERY_PARAMS, IGNORE_EXTENSIONS
from infrastructure.logging_utils import get_logger, log

logger = get_logger(__name__)


# â”€â”€ Public helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ Private helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _is_excluded_path(path: str) -> bool:
    return any(exc in path for exc in EXCLUDED_PATHS)


def _has_excluded_query(query: str) -> bool:
    return any(param in query for param in EXCLUDED_QUERY_PARAMS)
# ===== END FILE: searchEngine_core/services/crawl_core/parser.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/queues.py =====
"""
queues.py â€” Priority front-queue + per-host back-queue routing.
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
            # Host already has a queue (own or inherited via overflow) â€” fast path
            if host in self._table:
                return self._table[host]

            if len(self._table) >= self._max_queues:
                # Assign host to an existing queue and STORE the mapping so
                # this branch (and its WARNING) only fires once per host.
                target = list(self._table.keys())[hash(host) % self._max_queues]
                self._table[host] = self._table[target]   # <-- key fix
                log(logger, logging.WARNING, "Queue overflow â€” host hashed to existing queue",
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
# ===== END FILE: searchEngine_core/services/crawl_core/queues.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/robots.py =====
"""
robots.py â€” Thread-safe robots.txt cache + per-host block tracking.
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
            log(logger, logging.DEBUG, "robots.txt unavailable â€” using fallback",
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
    dropped â€” stopping workers from burning cycles on a host that will never
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
            log(logger, logging.WARNING, "Host abandoned â€” too many robots.txt blocks",
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
    in between, the host is abandoned â€” workers stop wasting time retrying
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
            log(logger, logging.WARNING, "Host abandoned â€” too many fetch failures",
                host=host, consecutive_fails=count, limit=self._max)

    def record_success(self, host: str) -> None:
        """Call whenever a fetch succeeds for *host*."""
        with self._lock:
            self._fails[host] = 0

    def is_abandoned(self, host: str) -> bool:
        with self._lock:
            return host in self._abandoned


fetch_fail_tracker = FetchFailTracker()
# ===== END FILE: searchEngine_core/services/crawl_core/robots.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/scorer.py =====
"""
scorer.py â€” URL priority scoring.

Scores are integers in [0, 9]: lower = higher crawl priority.

Four sub-scorers are combined into one:
  1. default_scorer      â€” path depth + known low-value patterns
  2. domain_scorer       â€” authority domains get priority 0
  3. CrawlHistory.score  â€” stale pages get higher priority
  4. anchor_scorer       â€” keyword-rich anchors get higher priority
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


# â”€â”€ 1. Default (path-depth) scorer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def default_scorer(url: str, **_) -> int:
    path = urlparse(url).path.lower()

    if path in ("", "/"):
        return 0  # homepage â†’ highest priority

    if any(token in path for token in LOW_VALUE_PATH_TOKENS):
        return 10

    depth = path.count("/")
    return 1 if depth <= 2 else min(depth, 9)


# â”€â”€ 2. Domain importance scorer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def domain_scorer(url: str, **_) -> int:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    if any(host == auth or host.endswith("." + auth) for auth in AUTHORITY_DOMAINS):
        return 0
    return 5


# â”€â”€ 3. Crawl-history (staleness) scorer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
            return 0  # never crawled â†’ highest priority

        hours_since = (time.time() - last) / 3600
        if hours_since >= self.stale_after_hours:
            return 1
        if hours_since >= self.stale_after_hours / 2:
            return 4
        return 9  # recently crawled â†’ deprioritize


# â”€â”€ 4. Anchor-text keyword scorer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def anchor_scorer(anchor_text: str = "", **_) -> int:
    if not anchor_text:
        return 5
    tokens = set(re.findall(r"\w+", anchor_text.lower()))
    if tokens & HIGH_VALUE_KEYWORDS:
        return 1
    if tokens & LOW_VALUE_KEYWORDS:
        return 9
    return 5


# â”€â”€ 5. Combined scorer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
# ===== END FILE: searchEngine_core/services/crawl_core/scorer.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_core/storage.py =====
"""
storage.py â€” Persists crawled pages and exports results.
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
    log(logger, logging.WARNING, "indexer module not found â€” pages won't be indexed.")


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
# ===== END FILE: searchEngine_core/services/crawl_core/storage.py =====

# ===== BEGIN FILE: searchEngine_core/services/crawl_service.py =====
from typing import List, Optional, Tuple

from services.crawl_core.crawler import threaded_crawl
from services.crawl_core.indexer import run_indexer


SEED_URLS: List[str] = [
    "https://www.wikipedia.org/",
    "https://www.britannica.com/",
    "https://www.bbc.com/news",
    "https://www.nytimes.com/",
    "https://www.theguardian.com/",
    "https://www.reuters.com/",
    "https://github.com/trending",
    "https://news.ycombinator.com/",
    "https://stackoverflow.com/",
    "https://www.khanacademy.org/",
    "https://ocw.mit.edu/",
    "https://www.reddit.com/",
    "https://dev.to/",
    "https://www.cnn.com/",
    "https://www.aljazeera.com/",
]


def crawl_web(
    seed_urls: Optional[List[str]] = None,
    max_pages: int = 100,
    max_workers: int = 10,
    delay_range: Tuple[float, float] = (1.5, 3.0),
):
    return threaded_crawl(
        seed_urls=seed_urls or SEED_URLS,
        max_pages=max_pages,
        max_workers=max_workers,
        delay_range=delay_range,
    )


def index_content() -> None:
    run_indexer()


def crawl_and_index(
    seed_urls: Optional[List[str]] = None,
    max_pages: int = 100,
    max_workers: int = 10,
    delay_range: Tuple[float, float] = (1.5, 3.0),
) -> None:
    crawl_web(
        seed_urls=seed_urls,
        max_pages=max_pages,
        max_workers=max_workers,
        delay_range=delay_range,
    )
    index_content()
# ===== END FILE: searchEngine_core/services/crawl_service.py =====

# ===== BEGIN FILE: searchEngine_core/services/search_service.py =====
import re
from typing import Dict, List, Optional, Set, Tuple

from bson import ObjectId
from nltk.stem import PorterStemmer

from domain.fuzzy_matching import find_fuzzy_matches, max_fuzzy_distance
from domain.query_language import BOOLEAN_OPERATOR_PRECEDENCE, count_boolean_operators, extract_quoted_phrase, parse_query_with_operators, to_postfix
from domain.ranking import rank_documents
from services.crawl_core.indexer import _preprocess as preprocess
from infrastructure.database import Pages, Indeverted_index


porter_stemmer = PorterStemmer()

_SEARCH_CACHE: Dict[str, List[Tuple[dict, float]]] = {}
_TERM_POSTINGS_CACHE: Dict[str, List[dict]] = {}
_FUZZY_MATCH_CACHE: Dict[str, List[Tuple[str, float]]] = {}
_TERM_VOCABULARY_CACHE: List[str] = []
_TERM_VOCABULARY_BY_INITIAL: Dict[str, List[str]] = {}
_TERM_VOCABULARY_COUNT = -1

_CACHE_STATS = {
    "hits": 0,
    "misses": 0,
    "evictions": 0,
}

MAX_CACHE_SIZE = 500
MAX_FUZZY_EXPANSIONS = 3
MIN_FUZZY_TERM_LENGTH = 3


def clear_cache() -> None:
    _SEARCH_CACHE.clear()
    _TERM_POSTINGS_CACHE.clear()
    _FUZZY_MATCH_CACHE.clear()
    _TERM_VOCABULARY_CACHE.clear()
    _TERM_VOCABULARY_BY_INITIAL.clear()
    global _CACHE_STATS, _TERM_VOCABULARY_COUNT
    _CACHE_STATS = {"hits": 0, "misses": 0, "evictions": 0}
    _TERM_VOCABULARY_COUNT = -1
    print("âœ“ All caches cleared")


def get_cache_stats() -> Dict:
    total = _CACHE_STATS["hits"] + _CACHE_STATS["misses"]
    hit_rate = (_CACHE_STATS["hits"] / total * 100) if total > 0 else 0
    return {
        "hits": _CACHE_STATS["hits"],
        "misses": _CACHE_STATS["misses"],
        "evictions": _CACHE_STATS["evictions"],
        "hit_rate": f"{hit_rate:.1f}%",
        "search_cache_size": len(_SEARCH_CACHE),
        "postings_cache_size": len(_TERM_POSTINGS_CACHE),
        "fuzzy_cache_size": len(_FUZZY_MATCH_CACHE),
        "vocabulary_cache_size": len(_TERM_VOCABULARY_CACHE),
    }


def _get_search_result_from_cache(query: str) -> Optional[List[Tuple[dict, float]]]:
    if query in _SEARCH_CACHE:
        _CACHE_STATS["hits"] += 1
        return _SEARCH_CACHE[query]

    _CACHE_STATS["misses"] += 1
    return None


def _cache_search_result(query: str, results: List[Tuple[dict, float]]) -> None:
    if len(_SEARCH_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_SEARCH_CACHE))
        del _SEARCH_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _SEARCH_CACHE[query] = results


def _get_term_postings_from_cache(term: str) -> Optional[List[dict]]:
    return _TERM_POSTINGS_CACHE.get(term)


def _cache_term_postings(term: str, postings: List[dict]) -> None:
    if len(_TERM_POSTINGS_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_TERM_POSTINGS_CACHE))
        del _TERM_POSTINGS_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _TERM_POSTINGS_CACHE[term] = postings


def _cache_fuzzy_matches(term: str, matches: List[Tuple[str, float]]) -> None:
    if len(_FUZZY_MATCH_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_FUZZY_MATCH_CACHE))
        del _FUZZY_MATCH_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _FUZZY_MATCH_CACHE[term] = matches


def _load_term_vocabulary() -> Tuple[List[str], Dict[str, List[str]]]:
    global _TERM_VOCABULARY_CACHE, _TERM_VOCABULARY_BY_INITIAL, _TERM_VOCABULARY_COUNT

    term_count = Indeverted_index.count_documents({})
    if term_count == _TERM_VOCABULARY_COUNT and _TERM_VOCABULARY_CACHE:
        return _TERM_VOCABULARY_CACHE, _TERM_VOCABULARY_BY_INITIAL

    vocabulary: List[str] = []
    buckets: Dict[str, List[str]] = {}

    for row in Indeverted_index.find({}, {"term": 1}):
        term = row.get("term")
        if not isinstance(term, str):
            continue

        normalized_term = term.strip().lower()
        if not normalized_term:
            continue

        vocabulary.append(normalized_term)
        buckets.setdefault(normalized_term[0], []).append(normalized_term)

    _TERM_VOCABULARY_CACHE = vocabulary
    _TERM_VOCABULARY_BY_INITIAL = buckets
    _TERM_VOCABULARY_COUNT = term_count
    _FUZZY_MATCH_CACHE.clear()
    return vocabulary, buckets


def get_fuzzy_term_matches(term: str) -> List[Tuple[str, float]]:
    normalized_term = term.strip().lower()
    if len(normalized_term) < MIN_FUZZY_TERM_LENGTH:
        return []

    if normalized_term in _FUZZY_MATCH_CACHE:
        return _FUZZY_MATCH_CACHE[normalized_term]

    vocabulary, buckets = _load_term_vocabulary()
    max_distance = max_fuzzy_distance(normalized_term)

    matches = find_fuzzy_matches(
        normalized_term,
        buckets.get(normalized_term[0], []),
        max_distance=max_distance,
        max_expansions=MAX_FUZZY_EXPANSIONS,
        min_term_length=MIN_FUZZY_TERM_LENGTH,
    )

    if not matches and vocabulary:
        matches = find_fuzzy_matches(
            normalized_term,
            vocabulary,
            max_distance=max_distance,
            max_expansions=MAX_FUZZY_EXPANSIONS,
            min_term_length=MIN_FUZZY_TERM_LENGTH,
        )

    _cache_fuzzy_matches(normalized_term, matches)
    return matches


def make_snippet(text: str, query_terms: List[str], max_len: int = 180) -> str:
    if not text:
        return ""

    lower = text.lower()
    hit_pos = -1
    for term in query_terms:
        pos = lower.find(term.lower())
        if pos != -1 and (hit_pos == -1 or pos < hit_pos):
            hit_pos = pos

    if hit_pos == -1:
        snippet = text[:max_len]
    else:
        start = max(0, hit_pos - 50)
        snippet = text[start:start + max_len]

    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet + ("..." if len(text) > len(snippet) else "")


def get_term_postings(term: str) -> List[dict]:
    cached_postings = _get_term_postings_from_cache(term)
    if cached_postings is not None:
        return cached_postings

    row = Indeverted_index.find_one({"term": term}, {"postings": 1})
    postings = [] if not row else row.get("postings", [])
    _cache_term_postings(term, postings)
    return postings


def build_weighted_query_terms(query: str) -> Dict[str, float]:
    weighted_terms: Dict[str, float] = {}
    normalized_terms = preprocess(query)

    for term in normalized_terms:
        weighted_terms[term] = max(weighted_terms.get(term, 0.0), 1.0)

    raw_tokens = re.findall(r"[a-zA-Z]+", query.lower())
    for raw in raw_tokens:
        stem = porter_stemmer.stem(raw)
        related_rows = Indeverted_index.find(
            {"term": {"$regex": f"^{re.escape(stem)}"}},
            {"term": 1},
        ).limit(25)

        for row in related_rows:
            term = row.get("term")
            if not term:
                continue
            if term not in weighted_terms and porter_stemmer.stem(term) == stem:
                weighted_terms[term] = 0.6

        if get_term_postings(stem):
            continue

        for fuzzy_term, fuzzy_weight in get_fuzzy_term_matches(stem):
            weighted_terms[fuzzy_term] = max(weighted_terms.get(fuzzy_term, 0.0), fuzzy_weight)

    return weighted_terms


def search_query(query: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    cache_key = f"{query}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    weighted_terms = build_weighted_query_terms(query)
    if not weighted_terms:
        results = []
    else:
        candidate_docs: Set[ObjectId] = set()
        for term in weighted_terms:
            for posting in get_term_postings(term):
                doc_id = posting.get("doc_id")
                if isinstance(doc_id, ObjectId):
                    candidate_docs.add(doc_id)

        if not candidate_docs:
            results = []
        else:
            scores = rank_documents(weighted_terms, candidate_docs)
            ranked_ids = sorted(scores.keys(), key=lambda doc_id: scores[doc_id], reverse=True)[:top_k]

            pages = {
                page["_id"]: page
                for page in Pages.find(
                    {"_id": {"$in": ranked_ids}},
                    {"title": 1, "url": 1, "content": 1},
                )
            }

            results: List[Tuple[dict, float]] = []
            for doc_id in ranked_ids:
                page = pages.get(doc_id)
                if page:
                    results.append((page, scores[doc_id]))

    _cache_search_result(cache_key, results)
    return results


def normalize_text_for_phrase_match(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def phrase_search(phrase: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    cache_key = f"phrase::{phrase}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    terms = preprocess(phrase)
    normalized_phrase = normalize_text_for_phrase_match(phrase)
    if not normalized_phrase:
        return []

    if not terms:
        phrase_hits = list(
            Pages.find(
                {
                    "$or": [
                        {"content": {"$regex": re.escape(phrase), "$options": "i"}},
                        {"title": {"$regex": re.escape(phrase), "$options": "i"}},
                    ]
                },
                {"title": 1, "url": 1, "content": 1},
            ).limit(top_k)
        )
        return [(page, 1.0) for page in phrase_hits]

    postings_by_term: Dict[str, Dict[ObjectId, Set[int]]] = {}
    for term in terms:
        posting_map: Dict[ObjectId, Set[int]] = {}
        for posting in get_term_postings(term):
            posting_map[posting["doc_id"]] = set(posting.get("pos", []))
        postings_by_term[term] = posting_map

    doc_sets = [set(postings_by_term[term].keys()) for term in terms if postings_by_term[term]]
    if not doc_sets:
        return []

    candidate_docs = set.intersection(*doc_sets)
    matched_docs: Set[ObjectId] = set()

    for doc_id in candidate_docs:
        first_positions = postings_by_term[terms[0]][doc_id]
        other_positions = [postings_by_term[term][doc_id] for term in terms[1:]]

        for start_pos in first_positions:
            if all((start_pos + index + 1) in other_positions[index] for index in range(len(other_positions))):
                matched_docs.add(doc_id)
                break

    if not matched_docs:
        return []

    pages_map = {
        page["_id"]: page
        for page in Pages.find(
            {"_id": {"$in": list(matched_docs)}},
            {"title": 1, "url": 1, "content": 1},
        )
    }

    exact_phrase_docs: Set[ObjectId] = set()
    for doc_id, page in pages_map.items():
        full_text = f"{page.get('title', '')} {page.get('content', '')}"
        if normalized_phrase in normalize_text_for_phrase_match(full_text):
            exact_phrase_docs.add(doc_id)

    if not exact_phrase_docs:
        return []

    normal_ranked = search_query(phrase, top_k=max(top_k * 20, 200))
    results: List[Tuple[dict, float]] = []
    for page, score in normal_ranked:
        doc_id = page.get("_id")
        if doc_id in exact_phrase_docs:
            results.append((page, score))
            if len(results) >= top_k:
                break

    if len(results) < top_k:
        missing_docs = exact_phrase_docs - {result[0].get("_id") for result in results}
        if missing_docs:
            weighted_terms = {term: 1.0 for term in terms}
            extra_scores = rank_documents(weighted_terms, missing_docs)
            extra_ids = sorted(missing_docs, key=lambda doc_id: extra_scores.get(doc_id, 0.0), reverse=True)
            for doc_id in extra_ids:
                page = pages_map.get(doc_id)
                if not page:
                    continue
                results.append((page, extra_scores.get(doc_id, 0.0)))
                if len(results) >= top_k:
                    break

    _cache_search_result(cache_key, results)
    return results


def _evaluate_boolean_tokens(tokens: List[str]) -> Set[ObjectId]:
    postfix = to_postfix(tokens)
    stack: List[Set[ObjectId]] = []

    for token in postfix:
        if token not in BOOLEAN_OPERATOR_PRECEDENCE:
            stack.append(get_documents_for_query(token))
            continue

        if len(stack) < 2:
            return set()

        right_docs = stack.pop()
        left_docs = stack.pop()

        if token == "OR":
            stack.append(left_docs | right_docs)
        elif token == "AND":
            stack.append(left_docs & right_docs)
        else:
            stack.append(left_docs - right_docs)

    if len(stack) != 1:
        return set()

    return stack[0]


def get_documents_for_query(query_text: str) -> Set[ObjectId]:
    if not query_text:
        return set()

    if query_text.startswith('"') and query_text.endswith('"'):
        phrase = query_text[1:-1].strip()
        results = phrase_search(phrase, top_k=1000)
    else:
        results = search_query(query_text, top_k=1000)

    return {page.get("_id") for page, _ in results if page.get("_id")}


def search_with_operators(query: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    if count_boolean_operators(query) > 2:
        raise ValueError("Maximum number of boolean operators per search is 2.")

    cache_key = f"operators::{query}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    parsed = parse_query_with_operators(query)

    if not parsed:
        if extract_quoted_phrase(query):
            results = phrase_search(extract_quoted_phrase(query), top_k=top_k)
        else:
            results = search_query(query, top_k=top_k)
    else:
        result_docs = _evaluate_boolean_tokens(parsed["tokens"])

        if not result_docs:
            results = []
        else:
            cleaned_query = re.sub(r"\s+(?:AND|OR|NOT)\s+", " ", query, flags=re.IGNORECASE)
            weighted_terms = build_weighted_query_terms(cleaned_query)
            scores = rank_documents(weighted_terms, result_docs) if weighted_terms else {doc_id: 1.0 for doc_id in result_docs}

            ranked_ids = sorted(scores.keys(), key=lambda doc_id: scores[doc_id], reverse=True)[:top_k]

            pages = {
                page["_id"]: page
                for page in Pages.find(
                    {"_id": {"$in": ranked_ids}},
                    {"title": 1, "url": 1, "content": 1},
                )
            }

            results: List[Tuple[dict, float]] = []
            for doc_id in ranked_ids:
                page = pages.get(doc_id)
                if page:
                    results.append((page, scores[doc_id]))

    _cache_search_result(cache_key, results)
    return results
# ===== END FILE: searchEngine_core/services/search_service.py =====

