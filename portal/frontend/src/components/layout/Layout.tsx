import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import Header from './Header'

export default function Layout() {
  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <Sidebar />
      <div style={{
        flex          : 1,
        display       : 'flex',
        flexDirection : 'column',
        minHeight     : '100vh',
        minWidth      : 0,
        transition    : 'all 0.25s ease',
      }}>
        <Header />
        <main style={{
          flex      : 1,
          padding   : '24px',
          overflowY : 'auto',
        }}>
          <Outlet />
        </main>
      </div>
    </div>
  )
}
