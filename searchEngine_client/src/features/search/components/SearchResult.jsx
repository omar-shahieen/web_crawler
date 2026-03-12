import { highlightText } from '../utils/highlightText.js'

import styles from './SearchResult.module.css'


function SearchResult({ result, query }) {
  const { title, url, description } = result

  let displayUrl = url
  try {
    displayUrl = new URL(url).hostname.replace(/^www\./, '')
  } catch {
    // Fall back to the raw value when the URL is malformed.
  }

  return (
    <article className={styles.card}>
      <p className={styles.url}>{displayUrl}</p>

      <a
        className={styles.title}
        href={url}
        target="_blank"
        rel="noopener noreferrer"
      >
        <HighlightedText text={title} query={query} />
      </a>

      <p className={styles.description}>
        <HighlightedText text={description} query={query} />
      </p>
    </article>
  )
}


function HighlightedText({ text, query }) {
  const segments = highlightText(text, query)

  return (
    <>
      {segments.map((segment, index) =>
        segment.highlight ? (
          <strong key={index} className={styles.highlight}>
            {segment.text}
          </strong>
        ) : (
          <span key={index}>{segment.text}</span>
        ),
      )}
    </>
  )
}


export default SearchResult