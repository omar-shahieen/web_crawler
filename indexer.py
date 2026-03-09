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
from db import Page, Posting, Pages, Indeverted_index , update_last_indexed_timestamp,get_last_indexed_timestamp


nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)
nltk.download("punkt_tab", quiet=True)

porter_stemmer = PorterStemmer()
stop_words = set(stopwords.words("english"))



# -- Text Processing ----------------------------------------------------------


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def tokenize(text: str) -> list[str]:
    tokens = word_tokenize(text.lower())
    return [w for w in tokens if w.isalpha() and w not in stop_words]


def stemming(tokens: list[str]) -> list[str]:
    return [porter_stemmer.stem(w) for w in tokens]


def preprocess(text: str) -> list[str]:
    return stemming(tokenize(text))


# -- HTML Parsing -------------------------------------------------------------


def filter_page(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "html.parser")

    # Remove junk tags.
    for tag in soup(
        ["script", "style", "nav", "footer", "header", "aside", "noscript", "form", "iframe", "svg", "canvas"]
    ):
        tag.decompose()

    # Remove common ad/cookie classes.
    for tag in soup.find_all(True, class_=True):
        classes = " ".join(tag["class"]).lower()
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
    alt_text = " ".join(str(img["alt"]) for img in soup.find_all("img", alt=True))

    return body_text + " " + alt_text, title, description


def is_content_valid(text: str) -> bool:
    words = text.split()
    return (
        len(words) > 100 and  # minimum length
        len(set(words)) > 50 and  # vocabulary diversity
        sum(c.isalpha() for c in text) > 500  # avoid junk pages
    )


def store_page(url: str, html: str, out_links: Optional[List[str]] = None) -> Optional[Page]:
    content, title, _ = filter_page(html)

    if not is_content_valid(content):
        print(f"Skipped low-quality page: {url}")
        return None

    new_hash = content_hash(content)
    existing = Pages.find_one({"url": url})

    if existing:
        if existing.get("content_hash") == new_hash:
            print("No changes detected.")
            return None

        print("Page updated.")
        Pages.update_one(
            {"url": url},
            {
                "$set": {
                    "title": title,
                    "content": content,
                    "content_hash": new_hash,
                    "out_links": out_links if out_links is not None else [],
                    "last_crawled": datetime.now(tz=timezone.utc), 

                }
            },
        )
        return Page(
            url=url,
            title=title,
            content=content,
            content_hash=new_hash,
            out_links=out_links if out_links is not None else [],
        )

    page = Page(
        url=url,
        title=title,
        content=content,
        content_hash=new_hash,
        out_links=out_links if out_links is not None else [],
    )
    Pages.insert_one(page.to_dict())
    return page



# -- In-Memory Index Building  [ depricated ] -------------------------------------------------

# Structure: { "term": { ObjectId: {"tf": int, "positions": [int]} } }
mapped_inverted_index: dict = defaultdict(dict)

def build_postings(doc_id, title: str, content: str):
    """Update the in-memory index for one document."""
    content_tokens = preprocess(content)
    title_tokens = preprocess(title)
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
    print(f"Indexed {len(mapped_inverted_index)} unique terms.")

def remove_doc_from_index(doc_id):
    """
    Pull this doc_id out of every posting list it appears in.
    $pull removes matching elements from an array.
    """
    Indeverted_index.update_many(
        {"postings.doc_id": doc_id},
        {"$pull": {"postings": {"doc_id": doc_id}}}
    )
    
    
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
        print(f"Flushed {result.upserted_count + result.modified_count} index entries to MongoDB.")


# -- build index directly to mongodb 

def build_and_flush_postings(doc_id, title: str, content: str):
    """Process one document and write directly to MongoDB — no memory buffer."""
    content_tokens = preprocess(content)
    title_tokens = preprocess(title)
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

def run_indexer():
    """streams each page directly to MongoDB."""
    last_indexed = get_last_indexed_timestamp()

    changed_pages = Pages.find(
        {"updated_at": {"$gt": last_indexed}},
        {"_id": 1, "title": 1, "content": 1}
    )

    count = 0
    for raw in changed_pages:
        build_and_flush_postings(
            raw["_id"],
            raw.get("title", ""),
            raw.get("content", "")
        )
        count += 1
        print(f"Indexed page {count}: {raw['_id']}")

    update_last_indexed_timestamp()
    print(f"Done. Indexed {count} pages directly to MongoDB.")
