import { useLocation } from 'react-router-dom'
import { Bell, RefreshCw } from 'lucide-react'

const titles: Record<string, string> = {
  '/'            : 'Vue d\'ensemble',
  '/data'        : 'Exploration des données',
  '/pipeline'    : 'Pipeline',
  '/spark'       : 'Spark Processing',
  '/quality'     : 'Qualité des données',
  '/monitoring'  : 'Monitoring',
  '/intelligence': 'Intelligence',
  '/admin'       : 'Administration',
}

export default function Header() {
  const location = useLocation()
  const title    = titles[location.pathname] || 'ILAM Portal'

  return (
    <header style={{
      height          : 'var(--header-height)',
      background      : 'white',
      borderBottom    : '1px solid var(--gray-200)',
      display         : 'flex',
      alignItems      : 'center',
      justifyContent  : 'space-between',
      padding         : '0 24px',
      position        : 'sticky',
      top             : 0,
      zIndex          : 50,
      boxShadow       : '0 1px 3px rgba(0,0,0,0.05)',
    }}>
      <div>
        <h1 style={{
          fontSize   : 18,
          fontWeight : 600,
          color      : 'var(--orange)',
        }}>{title}</h1>
        <div style={{
          fontSize : 13,
          color    : 'var(--teal)',
        }}>
          Ilam Data Lakehouse
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {/* Status indicator */}
        <div style={{
          display    : 'flex',
          alignItems : 'center',
          gap        : 6,
          padding    : '4px 10px',
          background : '#D1FAE5',
          borderRadius: 20,
          fontSize   : 11,
          color      : '#065F46',
          fontWeight : 500,
        }}>
          <div style={{
            width        : 7,
            height       : 7,
            borderRadius : '50%',
            background   : '#10B981',
            animation    : 'pulse 2s infinite',
          }}/>
          Streaming actif
        </div>

        <button style={{
          background : 'none',
          border     : 'none',
          cursor     : 'pointer',
          color      : 'var(--gray-400)',
          display    : 'flex',
          padding    : 6,
          borderRadius: 6,
        }}>
          <Bell size={22} />
        </button>

        {/* Logo Sonatel */}
        <img
          src="/orange-sonatel-logo.png"
          alt="Orange Sonatel"
          style={{
            height   :    30,
            objectFit: 'cover',
          }}
        />
      </div>
    </header>
  )
}
