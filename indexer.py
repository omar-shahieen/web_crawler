import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
from collections import defaultdict
from db import Page, Posting, Pages, Indeverted_index
from pymongo import UpdateOne
from typing import Optional
import hashlib


nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)

porter_stemmer = PorterStemmer()
stop_words = set(stopwords.words("english"))

# Structure: { "term": { ObjectId: {"tf": int, "positions": [int]} } }
mapped_inverted_index: dict = defaultdict(dict)


# ── Text Processing ──────────────────────────────────────────────────────────


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def tokenize(text: str) -> list[str]:
    tokens = word_tokenize(text.lower())
    
    return [w for w in tokens if w.isalpha() and w not in stop_words]

def stemming(tokens: list[str]) -> list[str]:
    return [porter_stemmer.stem(w) for w in tokens]

def preprocess(text: str) -> list[str]:
    return stemming(tokenize(text))


# ── HTML Parsing ─────────────────────────────────────────────────────────────

def filter_page(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "html.parser")
    
    # Remove junk tags
    for tag in soup(["script", "style", "nav", "footer",
        "header", "aside", "noscript", "form",
        "iframe", "svg", "canvas"]):
        tag.decompose()

    # Remove common ad / cookie classes
    for tag in soup.find_all(True , class_=True):
        classes = " ".join(tag['class']).lower()
        if any( x in classes for x in  ["cookie", "advert", "promo", "banner", "subscribe"]):
            tag.decompose()
      
    # extract title  
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    # extract description
    desc_tag = soup.find("meta", attrs={"name": "description"})
    description = desc_tag["content"] if desc_tag else ""

    #extract meaningful tags only
    content_block =[]
    for tag in soup.find_all(["article", "main", "section", "p", "h1", "h2", "h3"]):
        text = tag.get_text(" " , strip=True)
        if len(text.split()) > 5:
            content_block.append(text)
    
    body_text = " ".join(content_block)
    # alt text for images 
    alt_text = " ".join(img["alt"] for img in soup.find_all("img", alt=True))

    return body_text + " " + alt_text, title, description


def is_content_valid(text: str) -> bool :
    words = text.split()
    return(
        len(words) > 100 and # minimum length
        len(set(words)) > 50 and # vocabulary diversity
        sum(c.isalpha() for c in text) > 500 # avoid junk pages
    )
    
    
def store_page(url: str, html: str) -> Optional[Page]:
    content, title, _ = filter_page(html)

    if not is_content_valid(content):
        print(f"Skipped low-quality page: {url}")
        return None

    new_hash = content_hash(content)

    existing = Pages.find_one({"url": url})

    if existing:
        # Same URL exists
        if existing.get("content_hash") == new_hash:
            print("No changes detected.")
            return None

        print("Page updated.")
        Pages.update_one(
            {"url": url},
            {"$set": {
                "title": title,
                "content": content,
                "content_hash": new_hash
            }}
        )
        return Page(url=url, title=title, content=content, content_hash=new_hash)

    else:
        # New page
        page = Page(url=url, title=title, content=content, content_hash=new_hash)
        Pages.insert_one(page.to_dict())
        return page
    

# ── In-Memory Index Building ─────────────────────────────────────────────────

def build_postings(doc_id, title: str, content: str):
    """
    Update the in-memory index for one document.
    Title tokens are appended after content tokens (position reflects that).
    """
    content_tokens = preprocess(content) 
    title_tokens = preprocess(title)
    tokens = content_tokens+ title_tokens * 3

    for idx, term in enumerate(tokens):
        if doc_id not in mapped_inverted_index[term]:
            # First time seeing this (term, doc) pair
            mapped_inverted_index[term][doc_id] = {"tf": 0, "positions": []}

        mapped_inverted_index[term][doc_id]["tf"] += 1
        mapped_inverted_index[term][doc_id]["positions"].append(idx)


def index_all_pages():
    """Load every page from MongoDB and build the in-memory inverted index."""
    for raw in Pages.find({}, {"_id": 1, "title": 1, "content": 1}):
        build_postings(raw["_id"], raw.get("title", ""), raw.get("content", ""))
    print(f" Indexed {len(mapped_inverted_index)} unique terms.")


# ── Persist Index to MongoDB ──────────────────────────────────────────────────

def flush_index_to_mongo():
    """
    Write the in-memory index to MongoDB.
    Uses bulk upserts — safe to call multiple times (idempotent).
    """

    ops = []
    for term, doc_map in mapped_inverted_index.items():
        postings = [
            Posting(doc_id, data["tf"], data["positions"]).to_dict()
            for doc_id, data in doc_map.items()
        ]
        ops.append(UpdateOne(
            {"term": term},
            {"$set": {"term": term, "postings": postings}},
            upsert=True
        ))

    if ops:
        result = Indeverted_index.bulk_write(ops, ordered=False)
        print(f"Flushed {result.upserted_count + result.modified_count} index entries to MongoDB.")


# ── Entry Point ───────────────────────────────────────────────────────────────

def run_indexer():
    mapped_inverted_index.clear()
    index_all_pages()   # build in-memory
    flush_index_to_mongo()  # persist for fault tolerance