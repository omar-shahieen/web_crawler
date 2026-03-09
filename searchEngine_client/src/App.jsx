import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home.jsx'
import Results from './pages/Results.jsx'

/**
 * App — root component that defines client-side routes.
 *
 * Routes:
 *   /          → Home page (centered search bar)
 *   /          → Results page when ?search= param is present
 *
 * We use a single "/" route and let the Results page decide whether
 * to show the results view based on the query parameter, exactly as
 * Google does (same URL, different layout).
 */
function App() {
  return (
    <Routes>
      {/* Home page — no search param */}
      <Route path="/" element={<Home />} />

      {/* Results page — rendered when ?search=... is in the URL */}
      <Route path="/results" element={<Results />} />
    </Routes>
  )
}

export default App
