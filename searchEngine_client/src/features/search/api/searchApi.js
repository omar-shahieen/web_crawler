const BASE_URL = '/api'


export async function fetchSearchResults(query) {
  if (!query || !query.trim()) return []

  const params = new URLSearchParams({ q: query.trim() })
  const response = await fetch(`${BASE_URL}/search?${params}`)

  if (!response.ok) {
    let message = `Search API error: ${response.status} ${response.statusText}`
    try {
      const payload = await response.json()
      if (payload?.error) {
        message = payload.error
      }
    } catch {
      // Keep the HTTP status message when the response body is not JSON.
    }
    throw new Error(message)
  }

  return response.json()
}