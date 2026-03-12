import { Route, Routes } from 'react-router-dom'
import Home from '../features/search/pages/Home.jsx'
import Results from '../features/search/pages/Results.jsx'


function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/results" element={<Results />} />
    </Routes>
  )
}


export default AppRoutes