from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional, Union

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from dataclasses import  dataclass
from config import MIN_LENGTH_QUERY , MAX_LENGTH_QUERY , BLOCKED_TERMS
import re

client: MongoClient = MongoClient("mongodb://localhost:27017/")
db: Any = client["search_engine"]
Pages: Collection = db["pages"]
Indeverted_index: Collection = db["inverted_index"]
Metadata:Collection = db["metadata"]
QueryLogs:Collection = db["query_logs"]



try:
    client.admin.command("ping")
    print("Connected to MongoDB successfully!")
except ConnectionFailure as error:
    print(f"Failed to connect to MongoDB: {error}")

@dataclass
class QueryLog:
    query: str
    count: int
    
    
    count: int = 0  # default value for new logs

    @staticmethod
    def log_query( query: str):
        query = query.lower().strip()

        QueryLogs.update_one(
            {"query": query},
            {
                "$inc": {"count": 1},
                "$set": {"last_seen": datetime.utcnow()}
            },
            upsert=True
        )
        
    @staticmethod
    def load_queries():
        queries = []

        cursor = QueryLogs.find({}, {"query": 1, "count": 1})

        for doc in cursor:
            queries.append((doc["query"], doc["count"]))

        return queries
    
    @staticmethod
    def _looks_like_spam(self, q: str) -> bool:
        if re.search(r"(https?://|www\.)", q):  return True  # URLs
        if re.search(r"(.)\1{4,}", q):          return True  # "aaaaaaa"
        if len(set(q.replace(" ", ""))) < 2:    return True  # "zzzzz"
        return False
    @staticmethod
    def should_log(query: str) -> bool:
        q = query.strip().lower()

        if len(q) < MIN_LENGTH_QUERY:       return False  # "a", "is"
        if len(q) > MAX_LENGTH_QUERY:       return False  # spam / paste attacks
        if not re.search(r"[a-z]", q):return False  # "123", "!!!"
        if q in BLOCKED_TERMS:        return False  # profanity / slurs
        if QueryLog._looks_like_spam(q):       return False

        return True
    


class Page:
    def __init__(
        self,
        url: str,
        title: str,
        content: Optional[str],
        content_hash: Optional[str],
        description : Optional[str]="",
        out_links: Optional[List[str]] = None,

    ):
        self.url = url
        self.title = title
        self.content = content
        self._id: ObjectId = ObjectId()
        self.description= description
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
            "description" :self.description,
            }

    @staticmethod
    def load_pages():
        pages = []

        cursor = Pages.find({}, {"title": 1, "content": 1})

        for doc in cursor:
            pages.append((doc["title"], doc["content"]))

        return pages
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
QueryLogs.create_index("query", unique=True)
