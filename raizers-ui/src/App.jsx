import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { SessionProvider, useSession } from './context/session'
import Layout from './components/Layout'
import Setup from './pages/Setup'
import Operation from './pages/Operation'
import Financier from './pages/Financier'
import Patrimoine from './pages/Patrimoine'
import Immo from './pages/Immo'
import Scraping from './pages/Scraping'
import Export from './pages/Export'
import Switches from './pages/Switches'

function AppRoutes() {
  const { session } = useSession()

  if (session.pipeline !== 'done') return <Setup />

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/audit/operation" replace />} />
        <Route path="/audit/operation"  element={<Operation />} />
        <Route path="/audit/financier"  element={<Financier />} />
        <Route path="/audit/patrimoine" element={<Patrimoine />} />
        <Route path="/immo"             element={<Immo />} />
        <Route path="/scraping"         element={<Scraping />} />
        <Route path="/export"           element={<Export />} />
        <Route path="*"                 element={<Navigate to="/audit/operation" replace />} />
      </Route>
    </Routes>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/switches" element={<Switches />} />
        <Route path="*" element={
          <SessionProvider>
            <AppRoutes />
          </SessionProvider>
        } />
      </Routes>
    </BrowserRouter>
  )
}
