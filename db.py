from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId
from datetime import datetime, timezone
from pymongo.errors import ConnectionFailure
import re
from typing import Any, Dict, List, Optional, Union


# database conntection
# Local connection
client: MongoClient = MongoClient("mongodb://localhost:27017/")
db: Any = client["search_engine"]
Pages: Collection = db['pages']
Indeverted_index: Collection = db['inverted_index']
try:
    client.admin.command("ping")
    print("✅ Connected to MongoDB successfully!")
except ConnectionFailure as e:
    print(f"❌ Failed to connect to MongoDB: {e}")

# database models 
class Page:
    def __init__(self, url: str, title: str, content: str , content_hash :str, description: str):
        # We call the setters indirectly by assigning to self.url, etc.
        self.url = url
        self.title = title
        self.content = content
        self._id: ObjectId = ObjectId()
        self.last_crawled: datetime = datetime.now(tz=timezone.utc)
        self.content_hash = content_hash
        self.description = description

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        # Regex to check for a valid URL format
        url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$', re.IGNORECASE)
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
            "content_hash" : self.content_hash,
            "description" : self.description
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
        # Ensure doc_id is a valid MongoDB ObjectId
        if not isinstance(self.doc_id, ObjectId):
            try:
                self.doc_id = ObjectId(self.doc_id)
            except:
                raise TypeError("doc_id must be a valid ObjectId or 24-char hex string.")

        # Term Frequency must be at least 1
        if not isinstance(self.tf, int) or self.tf < 1:
            raise ValueError(f"Term Frequency (tf) must be a positive integer. Got: {self.tf}")

        # Positions must be a list of integers
        if not isinstance(self.positions, list) or not all(isinstance(p, int) for p in self.positions):
            raise TypeError("Positions must be a list of integers.")

    def to_dict(self):
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
    
    
    
    
# --- Unique index ---
Pages.create_index("url", unique=True)
Indeverted_index.create_index("term", unique=True)