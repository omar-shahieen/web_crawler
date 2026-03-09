/**
 * highlightText.js
 *
 * Splits a text string into segments, wrapping any segment that matches
 * a query keyword so the caller can render it highlighted.
 *
 * Returns an array of objects:
 *   { text: string, highlight: boolean }
 *
 * Example:
 *   highlightText("React Router is great", "react router")
 *   → [
 *       { text: "React",  highlight: true  },
 *       { text: " ",      highlight: false },
 *       { text: "Router", highlight: true  },
 *       { text: " is great", highlight: false },
 *     ]
 *
 * @param {string} text     — the full snippet text
 * @param {string} query    — the search query (space-separated keywords)
 * @returns {Array<{text: string, highlight: boolean}>}
 */
export function highlightText(text, query) {
  if (!query || !query.trim()) {
    return [{ text, highlight: false }]
  }

  // Build a regex that matches any individual keyword (case-insensitive)
  const keywords = query
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(k => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')) // escape regex specials

  const pattern = new RegExp(`(${keywords.join('|')})`, 'gi')

  // Split the text while keeping the matched delimiters
  const parts = text.split(pattern)

  return parts
    .filter(part => part !== '') // remove empty strings from split artefacts
    .map(part => ({
      text: part,
      highlight: pattern.test(part),
    }))
}
