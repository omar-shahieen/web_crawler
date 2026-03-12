import { Link, useSearchParams } from 'react-router-dom'

import ResultsList from '../components/ResultsList.jsx'
import SearchBar from '../components/SearchBar.jsx'
import useSearchResults from '../hooks/useSearchResults.js'
import styles from './Results.module.css'


function Results() {
  const [searchParams] = useSearchParams()
  const query = searchParams.get('search') || ''
  const { results, loading, error } = useSearchResults(query)

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <Link to="/" className={styles.logoLink} aria-label="Back to home">
          <span className={styles.logoIcon} aria-hidden="true">✦</span>
          <span className={styles.logoText}>Nexus</span>
        </Link>

        <SearchBar initialValue={query} compact />
      </header>

      <main className={styles.main}>
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