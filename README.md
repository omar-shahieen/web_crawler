# Web Crawler + Search Engine

This project crawls web pages, builds an inverted index in MongoDB, and supports ranked search.

Implemented modules and features:

- `Query Processor` with text preprocessing and stem-aware retrieval.
- `Phrase Searching` with strict word-order matching.
- `Ranker` that combines relevance and popularity.

## Requirements

- Python 3.8+
- MongoDB running on `mongodb://localhost:27017/`
- Python packages used in the project:
- `requests`
- `beautifulsoup4`
- `pymongo`
- `nltk`
- `bson` (provided by `pymongo`)

Recommended setup (Windows PowerShell):

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install requests beautifulsoup4 pymongo nltk
```

Important: use the virtual environment Python when running commands.

```bash
& ".\.venv\Scripts\python.exe" <script>.py ...
```

## Project Modules

- `crawler.py`: threaded crawler, robots handling, link extraction, page persistence.
- `indexer.py`: page cleaning, tokenization, stemming, inverted index build and flush.
- `query.py`: query processing, stem expansion, phrase search, CLI.
- `ranker.py`: relevance scoring (TF-IDF), popularity scoring (PageRank), score combination.
- `db.py`: MongoDB collections and data models.

## Added Features Summary

### 1) Query Processor (Stem-aware retrieval)

Implemented in `query.py`:

- Query terms are preprocessed with the same pipeline used by indexing.
- Stem-related term expansion is supported.
- Exact normalized query terms have higher weight (`1.0`).
- Same-stem expanded variants are included with lower weight (`0.6`).

Example behavior:

- `travel` can match `travel`, `traveler`, `traveling` (with lower degree for variants).

### 2) Phrase Searching

Implemented in `query.py`:

- Phrase mode is available via `--phrase`.
- Phrase search validates strict term order using positional postings.
- Extra text-level validation ensures same sentence order in page text.
- Phrase results are enforced as a subset of normal search results for the same words.

### 3) Ranker

Implemented in `ranker.py` as a separate module.

#### Relevance

- Algorithm: TF-IDF aggregation over query terms.
- For each term and document:
- `tf = term_frequency_in_doc / document_word_count`
- `idf = log(total_docs / df)`
- Relevance term contribution = `tf * idf * term_weight`

#### Popularity

- Algorithm: PageRank over crawled pages graph.
- Graph edges are built from `out_links` stored per page.
- Damping factor: `0.85`
- Iterations: `20`
- Popularity scores are normalized to `[0, 1]`.

#### Final score

- Combined score in `ranker.py`:
- `final = 0.8 * relevance_norm + 0.2 * popularity`

## Data Changes for Popularity

To support PageRank, the crawler/indexer persistence was extended:

- `crawler.py`: extracts page `out_links`.
- `indexer.py`: `store_page(..., out_links=...)` stores links.
- `db.py`: `Page` model includes `out_links` in stored documents.

## How To Run

### Crawl + Index

```bash
& ".\.venv\Scripts\python.exe" main.py
```

Or run crawler/indexer manually from Python snippets as needed.

### Search

Normal search:

```bash
& ".\.venv\Scripts\python.exe" query.py "economy market inflation" --top 10
```

Phrase search:

```bash
& ".\.venv\Scripts\python.exe" query.py "global travel rebounds" --phrase --top 10
```

## Validation Commands

### A) Stemming validation

```bash
& ".\.venv\Scripts\python.exe" query.py "travel" --top 10
& ".\.venv\Scripts\python.exe" query.py "traveling" --top 10
& ".\.venv\Scripts\python.exe" query.py "traveler" --top 10
```

Expected: high overlap in returned results.

### B) Phrase subset validation

```bash
& ".\.venv\Scripts\python.exe" query.py "global travel rebounds" --top 10
& ".\.venv\Scripts\python.exe" query.py "global travel rebounds" --phrase --top 10
```

Expected: phrase results are subset (or equal in special cases) of normal results.

### C) Popularity (PageRank) validation

```bash
& ".\.venv\Scripts\python.exe" -c "from ranker import compute_popularity_scores; from db import Pages; top=sorted(compute_popularity_scores().items(), key=lambda kv: kv[1], reverse=True)[:10]; print('\n'.join([str(i+1)+'. '+(Pages.find_one({'_id':d},{'url':1}) or {}).get('url','?')+' -> '+str(round(s,6)) for i,(d,s) in enumerate(top)]))"
```

Expected: non-uniform popularity scores after having pages with non-empty `out_links`.

## Notes

- If you see `ModuleNotFoundError: No module named 'bson'`, run commands with `.venv` Python.
- If popularity scores are all equal, crawl more pages so `out_links` graph coverage increases.
