import SearchResult from './SearchResult.jsx'
import styles from './ResultsList.module.css'

/**
 * ResultsList — renders a list of search result cards.
 *
 * Props:
 *   results  Array<{ title, url, description }>
 *   query    string — forwarded to each SearchResult for keyword highlighting
 */
function ResultsList({ results, query }) {
  if (!results || results.length === 0) {
    return (
      <div className={styles.empty}>
        <p>No results found for <strong>"{query}"</strong></p>
        <p className={styles.emptyHint}>Try different keywords or check your spelling.</p>
      </div>
    )
  }

  return (
    <section className={styles.list} aria-label="Search results">
      {/* Result count summary */}
      <p className={styles.meta}>
        About <strong>{results.length.toLocaleString()}</strong>{' '}
        result{results.length !== 1 ? 's' : ''}
      </p>

      {/* Individual result cards */}
      {results.map((result, index) => (
        <SearchResult
          key={`${result.url}-${index}`}
          result={result}
          query={query}
        />
      ))}
    </section>
  )
}

export default ResultsList
