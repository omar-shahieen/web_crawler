/**
 * searchApi.js
 *
 * Thin wrapper around the backend search endpoint.
 * Replace BASE_URL or the endpoint path to point at your real server.
 *
 * Expected API response shape:
 * [
 *   { title: string, url: string, description: string }
 * ]
 */

const BASE_URL = '/api'

/**
 * Fetch search results for a given query string.
 *
 * @param {string} query  — the raw search query
 * @returns {Promise<Array>} array of result objects
 */
export async function fetchSearchResults(query) {
  if (!query || !query.trim()) return []

  const params = new URLSearchParams({ q: query.trim() })
  const response = await fetch(`${BASE_URL}/search?${params}`)

  if (!response.ok) {
    throw new Error(`Search API error: ${response.status} ${response.statusText}`)
  }

  return response.json()
}
