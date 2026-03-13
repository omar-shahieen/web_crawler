const BOOLEAN_OPERATORS = new Set(['AND', 'OR', 'NOT'])
const COMMON_QUERY_STOPWORDS = new Set([
  'a',
  'an',
  'and',
  'are',
  'as',
  'at',
  'be',
  'by',
  'for',
  'from',
  'how',
  'in',
  'is',
  'it',
  'of',
  'on',
  'or',
  'that',
  'the',
  'to',
  'was',
  'were',
  'what',
  'when',
  'where',
  'who',
  'why',
  'with',
])


function shouldHighlightTerm(term) {
  if (term.includes(' ')) {
    return true
  }

  const normalized = term.toLowerCase()
  if (normalized.length < 3) {
    return false
  }

  return !COMMON_QUERY_STOPWORDS.has(normalized)
}


function extractHighlightKeywords(query) {
  const phraseMatches = [...query.matchAll(/"([^"]+)"/g)]
  const phrases = phraseMatches
    .map(match => match[1].trim())
    .filter(Boolean)

  const queryWithoutPhrases = query.replace(/"[^"]+"/g, ' ')
  const unquotedWords = queryWithoutPhrases.match(/[A-Za-z0-9]+/g) || []

  const rawTerms = []
  for (const phrase of phrases) {
    rawTerms.push(phrase)
    rawTerms.push(...(phrase.match(/[A-Za-z0-9]+/g) || []))
  }
  rawTerms.push(...unquotedWords)

  const keywords = []
  const seen = new Set()

  for (const term of rawTerms) {
    const normalized = term.trim()
    if (!normalized) {
      continue
    }
    if (BOOLEAN_OPERATORS.has(normalized.toUpperCase())) {
      continue
    }
    if (!shouldHighlightTerm(normalized)) {
      continue
    }

    const key = normalized.toLowerCase()
    if (seen.has(key)) {
      continue
    }

    seen.add(key)
    keywords.push(normalized)
  }

  return keywords
}


function buildPattern(keywords) {
  if (!keywords.length) {
    return null
  }

  const escaped = keywords.map((keyword) => {
    const escapedKeyword = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    if (keyword.includes(' ')) {
      return escapedKeyword
    }

    // Word-start matching keeps operator noise low while allowing prefixes like "git" -> "GitHub".
    return `\\b${escapedKeyword}`
  })

  return new RegExp(`(${escaped.join('|')})`, 'gi')
}


export function highlightText(text, query) {
  if (!query || !query.trim()) {
    return [{ text, highlight: false }]
  }

  const keywords = extractHighlightKeywords(query)
  const pattern = buildPattern(keywords)
  if (!pattern) {
    return [{ text, highlight: false }]
  }

  const highlightedLookup = new Set(keywords.map(keyword => keyword.toLowerCase()))
  const parts = text.split(pattern)

  return parts
    .filter(part => part !== '')
    .map(part => ({
      text: part,
      highlight: highlightedLookup.has(part.toLowerCase()),
    }))
}