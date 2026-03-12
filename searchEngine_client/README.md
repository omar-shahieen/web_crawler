# Nexus Search Engine UI

A Google-like search engine frontend built with **React + Vite** and **React Router**.

## Features

- 🔍 Centered search bar on the home page (Google-style proportions)
- 🔗 URL-based query routing: `/?search=<query>` → `/results?search=<query>`
- 🎨 Keyword highlighting in result snippets
- 🌐 Live backend integration through `/api/search`
- 📱 Responsive, minimalist design
- 🔄 **Advanced Boolean Search** — AND / OR / NOT operators (max 2 operators per query)
- ⌨️ **On-screen virtual keyboard** — toggle panel with full QWERTY layout (`react-simple-keyboard`)
- 🎤 **Voice search** — Web Speech API mic button with live red-pulse animation (no extra packages)
- 🍀 **I'm Feeling Lucky** — opens the top result URL directly in a new tab
- 🕑 **Recent searches** — auto-persisted suggestions dropdown from `localStorage` (max 8 items)

## Advanced Search with AND/OR/NOT Operators

The search engine now supports boolean operators for more precise queries:

### Examples:
```
"Football player" OR "Tennis player"       # Find either term
"Basketball" AND "Olympics"                 # Find both terms
"Soccer" NOT "NFL"                          # Exclude unwanted results
"Python" OR "JavaScript" AND "Web"         # Complex queries (up to 2 operators)
```

### Supported Operators:
- **OR** — Union: Returns pages matching either search term
- **AND** — Intersection: Returns pages matching both terms
- **NOT** — Exclusion: Returns pages with left term but without right term

Mixed queries follow precedence: `NOT`, then `AND`, then `OR`.

### Rules:
✅ Use quoted phrases: `"exact phrase"`
✅ Maximum 2 operators per query
✅ Mix operators: `"A" OR "B" AND "C"`

❌ Cannot use 3+ operators: `"A" OR "B" AND "C" NOT "D"`
❌ Parentheses not supported (yet): `("A" OR "B") AND "C"`

## UX Features

### Virtual Keyboard
A toggle button next to the search bar opens a full QWERTY on-screen keyboard panel powered by `react-simple-keyboard`. Typing on the panel updates the search input in real time.

### Voice Search
A microphone button (only shown when the browser supports the Web Speech API) starts voice recognition. While listening, the button pulses red. The recognised transcript is placed into the input and the search is submitted automatically.

### I'm Feeling Lucky
A subtle "Feeling Lucky" button calls `/api/search` and opens the URL of the first result directly in a new tab, bypassing the results page.

### Recent Searches
Up to 8 past queries are persisted in `localStorage` under the key `nexus_recent_searches`. A dropdown of matching suggestions appears when the input is focused or typed in, and clicking a suggestion runs that search immediately.

## Project Structure

The frontend uses a **feature-based architecture**:

```
src/
 ├─ app/
 │   └─ AppRoutes.jsx              # Route definitions (/ and /results)
 │
 ├─ features/
 │   └─ search/                    # All search-related code lives here
 │       ├─ api/
 │       │   └─ searchApi.js       # fetch() wrapper → /api/search?q=...
 │       ├─ hooks/
 │       │   └─ useSearchResults.js  # Data-fetching hook with loading/error state
 │       ├─ utils/
 │       │   └─ highlightText.js   # Text splitter for keyword highlighting
 │       ├─ components/
 │       │   ├─ SearchBar.jsx / .module.css
 │       │   ├─ ResultsList.jsx / .module.css
 │       │   └─ SearchResult.jsx / .module.css
 │       └─ pages/
 │           ├─ Home.jsx / .module.css   # Landing page — centered search bar
 │           └─ Results.jsx / .module.css  # Results page — reads URL param, calls API
 │
 └─ main.jsx                       # React root + BrowserRouter
```

## Quick Start

```bash
npm install
npm run dev
```

Open http://localhost:5173

## Backend API

The app expects a backend at `/api/search?q=<query>` returning:

```json
[
  {
    "title": "Result Title",
    "url": "https://example.com",
    "description": "A short description or snippet.",
    "score": 0.914251
  }
]
```

The Vite dev server proxies `/api` to `http://localhost:3001` (see `vite.config.js`).
Change the target to match your backend port.

## Connecting a Real Backend

1. Start your server on port 3001 (or change `vite.config.js`)
2. Implement `GET /api/health` for quick health checks
3. Implement `GET /api/search?q=<query>` returning the JSON array above
