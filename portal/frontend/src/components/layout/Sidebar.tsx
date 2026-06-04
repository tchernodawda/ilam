import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { useAuth } from '../../context/AuthContext'
import {
  LayoutDashboard, Database, GitBranch,
  Zap, Shield, Activity, Brain,
  LogOut, Settings
} from 'lucide-react'

const navItems = [
  { path: '/',            icon: LayoutDashboard, label: "Overview", roles: ['*'] },
  { path: '/data',        icon: Database,        label: 'Données',        roles: ['*'] },
  { path: '/pipeline',    icon: GitBranch,       label: 'Pipeline',       roles: ['admin','engineer'] },
  { path: '/spark',       icon: Zap,             label: 'Spark',          roles: ['admin','engineer'] },
  { path: '/quality',     icon: Shield,          label: 'Qualité',        roles: ['*'] },
  { path: '/monitoring',  icon: Activity,        label: 'Monitoring',     roles: ['*'] },
  { path: '/intelligence',icon: Brain,           label: 'Intelligence',   roles: ['*'] },
  { path: '/admin',       icon: Settings,        label: 'Administration', roles: ['admin'] },
]

export default function Sidebar() {
  const { user, logout } = useAuth()
  const [collapsed, setCollapsed] = useState(false)

  const canAccess = (roles: string[]) =>
    roles.includes('*') || roles.includes(user?.role || '')

  const width = collapsed ? 64 : 170

  return (
    <>
      {/* Sidebar */}
      <aside style={{
        width           : width,
        minHeight       : '100vh',
        background      : 'var(--sidebar-bg)',
        display         : 'flex',
        flexDirection   : 'column',
        position        : 'fixed',
        left            : 0,
        top             : 0,
        bottom          : 0,
        zIndex          : 100,
        transition      : 'width 0.25s ease',
        overflow        : 'hidden',
      }}>

        {/* Logo — cliquable pour collapse */}
        <div
          onClick={() => setCollapsed(!collapsed)}
          style={{
            padding        : collapsed ? '20px 0' : '20px 16px',
            borderBottom   : '1px solid rgba(255,255,255,0.1)',
            cursor         : 'pointer',
            display        : 'flex',
            alignItems     : 'center',
            justifyContent : collapsed ? 'center' : 'flex-start',
            transition     : 'padding 0.25s ease',
          }}
        >
          <img
            src="/ilam-logo.png"
            alt="ILAM"
            style={{
              height        : 50,
              width         : collapsed ? 60 : 120,
              objectFit     : 'cover',
              objectPosition: 'left center',
              transition    : 'all 0.25s ease',
              flexShrink    : 0,
            }}
          />
        </div>

        {/* Navigation */}
        <nav style={{
          flex      : 1,
          padding   : '12px 8px',
          overflowY : 'auto',
          overflowX : 'hidden',
        }}>
          {navItems.filter(item => canAccess(item.roles)).map(item => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === '/'}
              title={collapsed ? item.label : undefined}
              style={({ isActive }) => ({
                display        : 'flex',
                alignItems     : 'center',
                justifyContent : collapsed ? 'center' : 'flex-start',
                gap            : 8,
                padding        : collapsed ? '10px 0' : '10px 12px',
                borderRadius   : 6,
                marginBottom   : 0.5,
                color          : isActive ? 'white' : 'rgba(255,255,255,0.6)',
                background     : isActive ? 'var(--orange)' : 'transparent',
                textDecoration : 'none',
                fontSize       : 14,
                fontWeight     : isActive ? 600 : 400,
                transition     : 'all 0.15s',
                whiteSpace     : 'nowrap',
                overflow       : 'hidden',
              })}
            >
              <item.icon size={18} style={{ flexShrink: 0 }} />
              {!collapsed && item.label}
            </NavLink>
          ))}
        </nav>

        {/* User info */}
        <div style={{
          padding    : '12px 8px',
          borderTop  : '0px solid rgba(255,255,255,0.1)',
        }}>
          {!collapsed && (
            <div style={{
              display      : 'flex',
              alignItems   : 'center',
              gap          : 8,
              marginBottom : 8,
              padding      : '0 4px',
            }}>
              <div style={{
                width          : 30,
                height         : 30,
                background     : 'var(--orange)',
                borderRadius   : '50%',
                display        : 'flex',
                alignItems     : 'center',
                justifyContent : 'center',
                color          : 'white',
                fontWeight     : 700,
                fontSize       : 16,
                flexShrink     : 0,
              }}>
                {user?.username?.[0]?.toUpperCase()}
              </div>
              <div style={{ overflow: 'hidden' }}>
                <div style={{
                  color        : 'white',
                  fontSize     : 13,
                  fontWeight   : 500,
                  overflow     : 'hidden',
                  textOverflow : 'ellipsis',
                  whiteSpace   : 'nowrap',
                }}>{user?.username}</div>
                <div style={{
                  color         : 'rgba(255,255,255,0.5)',
                  fontSize      : 13,
                  textTransform : 'capitalize',
                }}>{user?.role}</div>
              </div>
            </div>
          )}

          <button
            onClick={logout}
            title={collapsed ? 'Déconnexion' : undefined}
            style={{
              width          : '100%',
              display        : 'flex',
              alignItems     : 'center',
              justifyContent : collapsed ? 'center' : 'flex-start',
              gap            : 6,
              padding        : collapsed ? '8px 0' : '8px 10px',
              background     : 'rgba(255,255,255,0.05)',
              border         : 'none',
              borderRadius   : 6,
              color          : 'rgba(255,255,255,0.6)',
              cursor         : 'pointer',
              fontSize       : 12,
              transition     : 'all 0.15s',
            }}
          >
            <LogOut size={14} />
            {!collapsed && 'Déconnexion'}
          </button>
        </div>
      </aside>

      {/* Spacer pour pousser le contenu principal */}
      <div style={{ width, flexShrink: 0, transition: 'width 0.25s ease' }} />
    </>
  )
}
