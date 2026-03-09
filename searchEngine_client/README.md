# Nexus Search Engine UI

A Google-like search engine frontend built with **React + Vite** and **React Router**.

## Features

- 🔍 Centered search bar on the home page (Google-style)
- 🔗 URL-based query routing: `/?search=<query>`  → `/results?search=<query>`
- 🎨 Keyword highlighting in result snippets
- ⚡ Mock data fallback when the API is offline (great for dev)
- 📱 Responsive, minimalist design

## Project Structure

```
src/
 ├─ components/
 │   ├─ SearchBar.jsx         # Controlled input, handles submit & navigation
 │   ├─ SearchResult.jsx      # Single result card with keyword highlighting
 │   └─ ResultsList.jsx       # List of SearchResult cards + empty state
 │
 ├─ pages/
 │   ├─ Home.jsx              # Landing page — centered search bar
 │   └─ Results.jsx           # Results page — reads URL param, calls API
 │
 ├─ utils/
 │   └─ highlightText.js      # Splits text into highlighted / plain segments
 │
 ├─ services/
 │   └─ searchApi.js          # fetch() wrapper for /api/search?q=...
 │
 ├─ App.jsx                   # Route definitions
 └─ main.jsx                  # React root + BrowserRouter
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
    "description": "A short description or snippet."
  }
]
```

The Vite dev server proxies `/api` to `http://localhost:3001` (see `vite.config.js`).
Change the target to match your backend port.

If the API is unreachable, the app automatically falls back to built-in demo data so you can develop without a backend.

## Connecting a Real Backend

1. Start your server on port 3001 (or change `vite.config.js`)
2. Implement `GET /api/search?q=<query>` returning the JSON array above
3. Remove the `MOCK_RESULTS` fallback in `src/pages/Results.jsx` when ready for production
