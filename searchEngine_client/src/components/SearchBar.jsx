import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import styles from './SearchBar.module.css'

/**
 * SearchBar — controlled input component.
 *
 * Props:
 *   initialValue  {string}  — pre-fills the input (used on Results page)
 *   compact       {boolean} — renders a smaller bar for the results header
 */
function SearchBar({ initialValue = '', compact = false }) {
  const [query, setQuery] = useState(initialValue)
  const navigate = useNavigate()

  // Keep input in sync when the URL changes (e.g. browser back/forward)
  useEffect(() => {
    setQuery(initialValue)
  }, [initialValue])

  function handleSubmit(e) {
    e.preventDefault()
    const trimmed = query.trim()
    if (!trimmed) return

    // Navigate to /results?search=<query>
    // encodeURIComponent handles spaces → %20, but URLSearchParams uses + for spaces
    const params = new URLSearchParams({ search: trimmed })
    navigate(`/results?${params}`)
  }

  return (
    <form
      className={`${styles.form} ${compact ? styles.compact : ''}`}
      onSubmit={handleSubmit}
      role="search"
    >
      {/* Search icon */}
      <span className={styles.icon} aria-hidden="true">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
      </span>

      <input
        className={styles.input}
        type="search"
        value={query}
        onChange={e => setQuery(e.target.value)}
        placeholder="Search anything…"
        aria-label="Search query"
        autoComplete="off"
        autoFocus={!compact}
      />

      <button className={styles.button} type="submit" aria-label="Submit search">
        Search
      </button>
    </form>
  )
}

export default SearchBar
