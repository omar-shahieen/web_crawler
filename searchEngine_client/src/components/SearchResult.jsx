import { highlightText } from '../utils/highlightText.js'
import styles from './SearchResult.module.css'

/**
 * SearchResult — renders a single search result card.
 *
 * Props:
 *   result  { title, url, description }
 *   query   string — used to highlight matching keywords
 */
function SearchResult({ result, query }) {
  const { title, url, description } = result

  // Parse hostname for display (e.g. "reactrouter.com")
  let displayUrl = url
  try {
    displayUrl = new URL(url).hostname.replace(/^www\./, '')
  } catch {
    // If URL is malformed, fall back to the raw string
  }

  return (
    <article className={styles.card}>
      {/* Source URL (breadcrumb line) */}
      <p className={styles.url}>{displayUrl}</p>

      {/* Clickable title */}
      <a
        className={styles.title}
        href={url}
        target="_blank"
        rel="noopener noreferrer"
      >
        <HighlightedText text={title} query={query} />
      </a>

      {/* Description snippet with keyword highlighting */}
      <p className={styles.description}>
        <HighlightedText text={description} query={query} />
      </p>
    </article>
  )
}

/* ─────────────────────────────────────────────────────
   HighlightedText — renders text with matched keywords
   wrapped in <strong> + a highlight class.
   ───────────────────────────────────────────────────── */
function HighlightedText({ text, query }) {
  const segments = highlightText(text, query)

  return (
    <>
      {segments.map((seg, i) =>
        seg.highlight ? (
          <strong key={i} className={styles.highlight}>
            {seg.text}
          </strong>
        ) : (
          <span key={i}>{seg.text}</span>
        )
      )}
    </>
  )
}

export default SearchResult
