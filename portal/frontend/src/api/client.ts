import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

// Ajouter le token JWT à chaque requête
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('ilam_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Rediriger vers login si 401
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('ilam_token')
      localStorage.removeItem('ilam_user')
      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export default api
