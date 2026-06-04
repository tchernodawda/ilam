import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { login as apiLogin } from '../api/services'

interface User {
  username : string
  role     : string
}

interface AuthContextType {
  user     : User | null
  loading  : boolean
  login    : (username: string, password: string) => Promise<void>
  logout   : () => void
  isAdmin  : () => boolean
  isEngineer: () => boolean
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser]       = useState<User | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const stored = localStorage.getItem('ilam_user')
    if (stored) setUser(JSON.parse(stored))
    setLoading(false)
  }, [])

  const login = async (username: string, password: string) => {
    const res = await apiLogin(username, password)
    const { access_token, role } = res.data
    localStorage.setItem('ilam_token', access_token)
    const userData = { username, role }
    localStorage.setItem('ilam_user', JSON.stringify(userData))
    setUser(userData)
  }

  const logout = () => {
    localStorage.removeItem('ilam_token')
    localStorage.removeItem('ilam_user')
    setUser(null)
    window.location.href = '/login'
  }

  const isAdmin    = () => user?.role === 'admin'
  const isEngineer = () =>
    ['admin', 'engineer'].includes(user?.role || '')

  return (
    <AuthContext.Provider value={{
      user, loading, login, logout, isAdmin, isEngineer
    }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
