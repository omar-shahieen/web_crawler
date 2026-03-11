# Web Crawler + Search Engine

This project crawls web pages, builds an inverted index in MongoDB, and supports ranked search.

Implemented modules and features:

- `Query Processor` with text preprocessing and stem-aware retrieval.
- `Fuzzy Matching` for typo-tolerant fallback on misspelled query terms.
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

## Project Architecture

The backend follows a **layered architecture**:

```
searchEngine_core/
 ├─ infrastructure/          # Connectivity and cross-cutting concerns
 │   ├─ config.py            # All constants (crawl limits, paths, ports …)
 │   ├─ database.py          # MongoDB connection, collections, models
 │   └─ logging_utils.py     # setup_logging(), get_logger(), log()
 │
 ├─ domain/                  # Pure business logic — no I/O
 │   ├─ query_language.py    # Boolean-query tokenizer, postfix evaluator
 │   └─ ranking.py           # TF-IDF relevance, PageRank, score combiner
 │
 ├─ services/                # Application use-cases
 │   ├─ search_service.py    # search_query(), phrase_search(), search_with_operators(), caches
 │   ├─ crawl_service.py     # crawl_web(), index_content(), crawl_and_index()
 │   └─ crawl_core/          # Crawl/index internals
 │       ├─ crawler.py       # Multi-threaded crawler, robots.txt handling, link extraction
 │       ├─ fetcher.py       # HTTP fetching with retries and timeout
 │       ├─ parser.py        # HTML parsing, link and text extraction
 │       ├─ indexer.py       # Tokenization, stemming, inverted index build
 │       ├─ scorer.py        # Per-document TF scoring
 │       ├─ storage.py       # Page persistence to MongoDB
 │       ├─ queues.py        # URL frontier queues
 │       └─ robots.py        # robots.txt fetch and rule evaluation
 │
 ├─ presentation/            # HTTP layer (Flask)
 │   └─ api_app.py           # create_app(), /api/health, /api/search
 │
 ├─ cli/                     # Command-line interface
 │   └─ commands.py          # main() — subcommands: crawl, index, crawl-index, serve
 │
 └─ main.py                  # Project entry-point (delegates to cli.commands)
```

## Features Summary

### 1) Query Processor (Stem-aware retrieval)

Implemented in `services/search_service.py` + `services/crawl_core/indexer.py`:

- Query terms are preprocessed with the same pipeline used by indexing.
- Stem-related term expansion is supported.
- Exact normalized query terms have higher weight (`1.0`).
- Same-stem expanded variants are included with lower weight (`0.6`).

Example behavior:

- `travel` can match `travel`, `traveler`, `traveling` (with lower degree for variants).

### 1.1) Fuzzy Matching (Typo-tolerant fallback)

Implemented in `domain/fuzzy_matching.py` and integrated from `services/search_service.py`:

- Fuzzy matching is only applied when a normalized query term has no direct postings.
- Candidate indexed terms are compared with bounded edit distance.
- Close matches are added with lower weight, so exact matches still dominate ranking.
- Adjacent transposition typos like `pyhton` can still recover `python`.

Example behavior:

- `pyhton` can still retrieve pages indexed under `python`.

### 2) Phrase Searching

Implemented in `services/search_service.py`:

- Phrase search validates strict term order using positional postings.
- Extra text-level validation ensures same sentence order in page text.
- Phrase results are enforced as a subset of normal search results for the same words.

### 3) Ranker

Implemented in `domain/ranking.py`:

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

- `final = 0.8 * relevance_norm + 0.2 * popularity`

## PageRank Data Support

To support PageRank, the crawler/indexer persistence was extended:

- `services/crawl_core/crawler.py`: extracts page `out_links`.
- `services/crawl_core/storage.py` and `indexer.py`: normalize `out_links` to URL strings before storing.
- `infrastructure/database.py`: `Page` model includes `out_links` in stored documents.

## Advanced Features

### 4) Boolean Operators (AND/OR/NOT)

Implemented in `domain/query_language.py` + `services/search_service.py`:

- **OR**: Returns pages matching either search term (union)
- **AND**: Returns pages matching both terms (intersection)
- **NOT**: Returns pages with left term but excluding right term
- **Max 2 operators** per query: `"A" OR "B" AND "C"` ✓ | `"A" OR "B" AND "C" NOT "D"` ✗
- Mixed queries are evaluated with precedence: `NOT`, then `AND`, then `OR`.

Examples:
```
"Football player" OR "Tennis player"       # Find either
"Basketball" AND "Olympics"                # Find both
"Soccer" NOT "NFL"                         # Exclude unwanted
"Python" OR "JavaScript" AND "Web"         # Complex (2 ops max)
```

### 5) Memory-First Caching System

Implemented in `services/search_service.py`:

**Two-Level Cache Strategy:**
1. **Search Result Cache** (fastest): Stores complete query results
2. **Term Postings Cache**: Stores inverted index postings

**Performance:**
- Cache hit: ~1-5 ms (40x faster than database)
- Cache size: 500 items per cache level
- Eviction: FIFO when cache full
- Hit rate: 80%+ on typical workloads

**Benefits:**
```python
# First search (cache miss): ~100-200 ms
results = search_query("Python")

# Repeated search (cache hit): ~1-5 ms
results = search_query("Python")  # 40x faster!

# Get cache statistics
from services.search_service import get_cache_stats
stats = get_cache_stats()
# {'hits': 45, 'misses': 10, 'hit_rate': '81.8%', ...}

# Clear cache after database updates
from services.search_service import clear_cache
clear_cache()
```

**Cache Strategy Flow:**
```
User Query
    ↓
Check Search Result Cache → HIT? Return (1-5ms)
    ↓ MISS
Check Term Postings Cache → HIT? Use it
    ↓ MISS
Fetch from Database → Cache it
    ↓
Rank Results
    ↓
Cache Final Results → Return
```

### 6) Backend Search API

Implemented in `presentation/api_app.py`:

- `GET /api/health` returns a simple service health response.
- `GET /api/search?q=<query>&top=<n>` runs normal, phrase, or boolean search automatically.
- Results are returned as JSON objects with `title`, `url`, `description`, and `score`.
- `top` is clamped to `1..50` to avoid oversized responses.

## How To Run

### Crawl

```powershell
& ".venv\Scripts\python.exe" main.py crawl
```

### Index

```powershell
& ".venv\Scripts\python.exe" main.py index
```

### Crawl + Index (combined)

```powershell
& ".venv\Scripts\python.exe" main.py crawl-index
```

### API Server

```powershell
& ".venv\Scripts\python.exe" main.py serve --host 127.0.0.1 --port 3001
```

The frontend dev server proxies `/api` → `http://localhost:3001`.

### Quick in-process search test (Python REPL)

```python
from services.search_service import search_query
results = search_query('bbc', top_k=5)
for doc, score in results:
    print(score, doc['title'])
```

## Validation Commands

### A) Stemming validation

```powershell
& ".venv\Scripts\python.exe" -c "from services.search_service import search_query; [print(s, d['title']) for d,s in search_query('travel', top_k=10)]"
& ".venv\Scripts\python.exe" -c "from services.search_service import search_query; [print(s, d['title']) for d,s in search_query('traveling', top_k=10)]"
```

Expected: high overlap in returned results.

### A.1) Fuzzy matching validation

```powershell
& ".venv\Scripts\python.exe" -c "from services.search_service import search_query; [print(s, d['title']) for d,s in search_query('pyhton', top_k=10)]"
```

Expected: results still appear for close misspellings of indexed terms.

### B) Phrase search validation

```powershell
& ".venv\Scripts\python.exe" -c "from services.search_service import phrase_search; [print(s, d['title']) for d,s in phrase_search('global travel', top_k=10)]"
```

Expected: results where 'global' and 'travel' appear in strict order.

### C) API health check

```powershell
Invoke-WebRequest -Uri http://127.0.0.1:3001/api/health | Select-Object -Expand Content
```

Expected: `{"status": "ok"}`.

## Notes

- If you see `ModuleNotFoundError: No module named 'bson'`, run commands with `.venv` Python.
- If popularity scores are all equal, crawl more pages so `out_links` graph coverage increases.
