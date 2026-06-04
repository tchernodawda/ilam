import {
  BrowserRouter, Routes, Route, Navigate
} from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import { AuthProvider, useAuth } from './context/AuthContext'
import Layout    from './components/layout/Layout'
import Login     from './pages/Login'
import Overview  from './pages/Overview'
import Data      from './pages/Data'
import Pipeline  from './pages/Pipeline'
import Spark     from './pages/Spark'
import Quality   from './pages/Quality'
import Monitoring from './pages/Monitoring'
import Intelligence from './pages/Intelligence'
import Admin     from './pages/Admin'

function PrivateRoute({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()
  if (loading) return (
    <div style={{
      display        : 'flex',
      alignItems     : 'center',
      justifyContent : 'center',
      height         : '100vh',
    }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  )
  return user ? <>{children}</> : <Navigate to="/login" />
}

function AdminRoute({ children }: { children: React.ReactNode }) {
  const { user, isAdmin } = useAuth()
  if (!user || !isAdmin()) return <Navigate to="/" />
  return <>{children}</>
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/" element={
        <PrivateRoute><Layout /></PrivateRoute>
      }>
        <Route index        element={<Overview />} />
        <Route path="data"  element={<Data />} />
        <Route path="pipeline" element={<Pipeline />} />
        <Route path="spark" element={<Spark />} />
        <Route path="quality" element={<Quality />} />
        <Route path="monitoring" element={<Monitoring />} />
        <Route path="intelligence" element={<Intelligence />} />
        <Route path="admin" element={
          <AdminRoute><Admin /></AdminRoute>
        } />
      </Route>
    </Routes>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <AppRoutes />
        <Toaster
          position="top-right"
          toastOptions={{
            duration : 3000,
            style    : { fontSize: 13 },
            success  : { iconTheme: {
              primary   : 'var(--success)',
              secondary : 'white',
            }},
            error    : { iconTheme: {
              primary   : 'var(--error)',
              secondary : 'white',
            }},
          }}
        />
      </AuthProvider>
    </BrowserRouter>
  )
}
