import { useEffect, useState } from 'react'

import { fetchSearchResults } from '../api/searchApi.js'


function useSearchResults(query) {
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!query.trim()) {
      setResults([])
      setError(null)
      setLoading(false)
      return
    }

    let cancelled = false
    setLoading(true)
    setError(null)

    fetchSearchResults(query)
      .then((data) => {
        if (!cancelled) {
          setResults(data)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setResults([])
          setError(err.message || 'Search request failed.')
          setLoading(false)
        }
      })

    return () => {
      cancelled = true
    }
  }, [query])

  return { results, loading, error }
}


export default useSearchResults