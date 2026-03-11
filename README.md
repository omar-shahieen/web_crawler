# Web Crawler Search Engine

A full-stack search engine that crawls web pages, builds an inverted index in MongoDB, and serves ranked search results through a Google-like React UI.

This monorepo contains:

- `searchEngine_core` — Backend (Python + Flask + Crawler + Indexer)
- `searchEngine_client` — Frontend (React + Vite)

## Features

### Backend
- **Multi-threaded web crawler** with robots.txt respect and visited-URL deduplication
- **Inverted index** built with stemming (NLTK), positional postings, and TF-IDF scoring
- **Fuzzy matching fallback** — typo-tolerant retrieval when a query word is misspelled
- **PageRank** popularity scoring combined with TF-IDF relevance (`0.8 relevance + 0.2 popularity`)
- **Boolean search** — AND / OR / NOT operators, up to 2 operators per query with operator precedence
- **Phrase search** — strict word-order matching using positional postings
- **Two-level memory cache** — result cache → term postings cache (fast repeated-query lookups)
- **REST API** — `GET /api/health`, `GET /api/search?q=<query>&top=<n>`

### Frontend
- **Google-like home page** — centered search bar with proportional layout
- **On-screen virtual keyboard** — toggle panel with full QWERTY layout
- **Voice search** — Web Speech API (no extra packages), mic button with live pulse animation
- **I'm Feeling Lucky** — opens the top result URL directly in a new tab
- **Recent searches** — dropdown suggestions from `localStorage`, auto-persisted (max 8)
- **Keyword highlighting** — matched terms highlighted in result snippets
- **Boolean query UI** — AND / OR / NOT operator support surfaced in the search bar

## Architecture

### Backend — Layered Architecture
```
searchEngine_core/
 ├─ infrastructure/     # DB connection, config constants, logging
 ├─ domain/             # Pure business logic (query language, ranking)
 ├─ services/           # Application use-cases (search, crawl)
 │   └─ crawl_core/     # Crawl/index internals (crawler, fetcher, indexer …)
 ├─ presentation/       # Flask API (create_app, /api/health, /api/search)
 └─ cli/                # CLI entry-point (crawl, index, crawl-index, serve)
```

### Frontend — Feature-based Architecture
```
searchEngine_client/src/
 ├─ app/                # Route definitions (AppRoutes.jsx)
 └─ features/search/    # All search code (api, hooks, components, pages, utils)
```

## Quick Start

### 1) Start Frontend + Backend Together

```powershell
Push-Location "searchEngine_client"
npm install
npm run dev
```

Open: `http://127.0.0.1:5173`

### 2) Start Backend Separately (optional)

```powershell
Push-Location "searchEngine_core"
& "../.venv/Scripts/python.exe" main.py serve --host 127.0.0.1 --port 3001
```

## Notes

- Frontend proxy `/api` → `http://localhost:3001` (see `searchEngine_client/vite.config.js`).
- MongoDB must be running at `mongodb://localhost:27017`.
- Create the virtual environment with `python -m venv .venv` if it does not exist yet.

## Detailed Docs

- Backend details: [searchEngine_core/README.md](searchEngine_core/README.md)
- Frontend details: [searchEngine_client/README.md](searchEngine_client/README.md)
