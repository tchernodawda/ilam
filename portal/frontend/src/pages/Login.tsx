import { useState, FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { Eye, EyeOff, LogIn } from 'lucide-react'

export default function Login() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [showPass, setShowPass] = useState(false)
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState('')
  const { login }               = useAuth()
  const navigate                = useNavigate()

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      await login(username, password)
      navigate('/')
    } catch (err: any) {
      setError('Identifiants incorrects. Veuillez réessayer.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      minHeight       : '100vh',
      background      : 'linear-gradient(135deg, #1A1A2E 0%, #16213E 50%, #0F3460 100%)',
      display         : 'flex',
      alignItems      : 'center',
      justifyContent  : 'center',
      padding         : 20,
    }}>
      <div style={{
        width        : '100%',
        maxWidth     : 400,
      }}>
        {/* Logo */}
        <div style={{ textAlign: 'center', marginBottom: 0 }}>
          <div style={{
            display        : 'inline-flex',
            alignItems     : 'center',
            gap            : 1,
            marginBottom   : 1,
          }}>
            <img
              src="/ilam-logo.png"
              alt="ILAM"
              style={{
                width        : 100,
                height       : 100,
                objectFit    : 'cover',
                objectPosition: 'center',
              }}
            />
            <div style={{ textAlign: 'left' }}>
              <div style={{
                color    : 'rgba(255,255,255,0.6)',
                fontSize : 16,
              }}>Data Lakehouse Platform</div>
            </div>
          </div>
          
        </div>

        {/* Card */}
        <div style={{
          background   : 'white',
          borderRadius : 12,
          padding      : 30,
          boxShadow    : '0 20px 60px rgba(0,0,0,0.3)',
        }}>
          <h3 style={{
            fontSize     : 18,
            fontWeight   : 700,
            color        : 'var(--gray-800)',
            marginBottom : 2,
            textAlign    : 'center',
          }}>Connexion</h3>
          <p style={{
            fontSize     : 14,
            color        : 'var(--gray-500)',
            marginBottom : 16,
            textAlign    : 'center',
          }}>
            accédez à votre espace de gestion dans Ilam
          </p>

          {error && (
            <div style={{
              padding      : '10px 14px',
              background   : '#FEE2E2',
              borderRadius : 6,
              color        : '#991B1B',
              fontSize     : 13,
              marginBottom : 16,
            }}>{error}</div>
          )}

          <form onSubmit={handleSubmit}>
            <div style={{ marginBottom: 16 }}>
              <label style={{
                display      : 'block',
                fontSize     : 13,
                fontWeight   : 500,
                color        : 'var(--gray-700)',
                marginBottom : 2,
              }}>Identifiant</label>
              <input
                type="text"
                value={username}
                onChange={e => setUsername(e.target.value)}
                placeholder="ilam_admin"
                required
                style={{
                  width        : '100%',
                  padding      : '10px 12px',
                  border       : '1px solid var(--gray-300)',
                  borderRadius : 6,
                  fontSize     : 13,
                  outline      : 'none',
                  transition   : 'border-color 0.15s',
                }}
                onFocus={e =>
                  e.target.style.borderColor = 'var(--orange)'}
                onBlur={e =>
                  e.target.style.borderColor = 'var(--gray-300)'}
              />
            </div>

            <div style={{ marginBottom: 20 }}>
              <label style={{
                display      : 'block',
                fontSize     : 13,
                fontWeight   : 500,
                color        : 'var(--gray-700)',
                marginBottom : 2,
              }}>Mot de passe</label>
              <div style={{ position: 'relative' }}>
                <input
                  type={showPass ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="••••••••"
                  required
                  style={{
                    width        : '100%',
                    padding      : '10px 40px 10px 12px',
                    border       : '1px solid var(--gray-300)',
                    borderRadius : 6,
                    fontSize     : 13,
                    outline      : 'none',
                  }}
                  onFocus={e =>
                    e.target.style.borderColor = 'var(--orange)'}
                  onBlur={e =>
                    e.target.style.borderColor = 'var(--gray-300)'}
                />
                <button
                  type="button"
                  onClick={() => setShowPass(!showPass)}
                  style={{
                    position   : 'absolute',
                    right      : 10,
                    top        : '50%',
                    transform  : 'translateY(-50%)',
                    background : 'none',
                    border     : 'none',
                    cursor     : 'pointer',
                    color      : 'var(--gray-400)',
                    display    : 'flex',
                  }}
                >
                  {showPass
                    ? <EyeOff size={16} />
                    : <Eye size={16} />}
                </button>
              </div>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="btn btn-primary"
              style={{ width: '100%', justifyContent: 'center',
                       padding: '11px' }}
            >
              {loading
                ? <><div className="spinner"
                    style={{width:16,height:16}}/> Connexion...</>
                : <><LogIn size={16} /> Se connecter</>}
            </button>
          </form>

          {/* Comptes de test */}
          <div style={{
            marginTop    : 0,
            padding      : '12px',
            background   : 'var(--gray-50)',
            borderRadius : 6,
            fontSize     : 11,
            color        : 'var(--gray-500)',
          }}>
            <strong style={{ color: 'var(--gray-600)' }}>
              Comptes disponibles :
            </strong>
            <div style={{ marginTop: 3, lineHeight: 1.4 }}>
              <div>Admin : ilam_admin / ilam_password_2025</div>
              <div>Engineer : engineer / engineer_2025</div>
              <div>Analyst : analyst / analyst_2025</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
