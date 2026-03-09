import { useState, useEffect } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import SearchBar from '../components/SearchBar.jsx'
import ResultsList from '../components/ResultsList.jsx'
import { fetchSearchResults } from '../services/searchApi.js'
import styles from './Results.module.css'

/* ─────────────────────────────────────────────────────────────
   Mock results used as a fallback when the real API is offline.
   Remove this (and the try/catch mock fallback below) once your
   backend is running.
   ───────────────────────────────────────────────────────────── */
const MOCK_RESULTS = [
  {
    title: 'React Router — Declarative Routing for React',
    url: 'https://reactrouter.com',
    description:
      'React Router is a powerful routing library for React applications. Learn how to use React and Router together to build modern, single-page web apps.',
  },
  {
    title: 'Learn React — The Official React Documentation',
    url: 'https://react.dev',
    description:
      'React is a JavaScript library for building user interfaces. Explore the React documentation to get started with components, hooks, and state management.',
  },
  {
    title: 'Vite — Next Generation Frontend Tooling',
    url: 'https://vitejs.dev',
    description:
      'Vite provides a faster and leaner development experience for modern web projects. Great pairing with React for blazing-fast hot module replacement.',
  },
  {
    title: 'MDN Web Docs — JavaScript Reference',
    url: 'https://developer.mozilla.org',
    description:
      'The MDN Web Docs site provides information about JavaScript, HTML, CSS, and web APIs. Find React and router-related tutorials, examples, and documentation.',
  },
  {
    title: 'Stack Overflow — React Questions',
    url: 'https://stackoverflow.com/questions/tagged/react',
    description:
      'Browse thousands of questions and answers about React, React Router, and JavaScript. Community-driven help for developers of all levels.',
  },
]

/**
 * Results — the search results page.
 *
 * Reads the `search` query param from the URL, calls the API,
 * and renders the ResultsList. Falls back to mock data when the
 * API is unreachable (useful for local dev without a backend).
 */
function Results() {
  const [searchParams] = useSearchParams()
  const query = searchParams.get('search') || ''

  const [results, setResults]   = useState([])
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)
  const [isMock, setIsMock]     = useState(false)

  useEffect(() => {
    if (!query.trim()) {
      setResults([])
      return
    }

    let cancelled = false
    setLoading(true)
    setError(null)
    setIsMock(false)

    fetchSearchResults(query)
      .then(data => {
        if (!cancelled) {
          setResults(data)
          setLoading(false)
        }
      })
      .catch(() => {
        if (!cancelled) {
          // ── Fallback: filter mock results containing any query word ──
          const words = query.toLowerCase().split(/\s+/)
          const filtered = MOCK_RESULTS.filter(r =>
            words.some(w =>
              r.title.toLowerCase().includes(w) ||
              r.description.toLowerCase().includes(w)
            )
          )
          setResults(filtered.length ? filtered : MOCK_RESULTS)
          setIsMock(true)
          setLoading(false)
        }
      })

    return () => { cancelled = true }
  }, [query])

  return (
    <div className={styles.page}>
      {/* ── Top header bar ── */}
      <header className={styles.header}>
        <Link to="/" className={styles.logoLink} aria-label="Back to home">
          <span className={styles.logoIcon} aria-hidden="true">✦</span>
          <span className={styles.logoText}>Nexus</span>
        </Link>

        {/* Compact search bar in the header */}
        <SearchBar initialValue={query} compact />
      </header>

      {/* ── Main content ── */}
      <main className={styles.main}>
        {/* Mock-data notice */}
        {isMock && (
          <div className={styles.notice} role="status">
            ⚠ API unreachable — showing demo results. Connect a backend at{' '}
            <code>/api/search</code> to see live data.
          </div>
        )}

        {loading && (
          <div className={styles.loading} role="status" aria-live="polite">
            <span className={styles.spinner} aria-hidden="true" />
            Searching…
          </div>
        )}

        {error && !loading && (
          <p className={styles.error} role="alert">{error}</p>
        )}

        {!loading && !error && (
          <ResultsList results={results} query={query} />
        )}
      </main>
    </div>
  )
}

export default Results
