export function highlightText(text, query) {
  if (!query || !query.trim()) {
    return [{ text, highlight: false }]
  }

  const keywords = query
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(keyword => keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))

  const pattern = new RegExp(`(${keywords.join('|')})`, 'gi')
  const parts = text.split(pattern)

  return parts
    .filter(part => part !== '')
    .map(part => ({
      text: part,
      highlight: pattern.test(part),
    }))
}