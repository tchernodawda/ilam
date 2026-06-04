import { useEffect, useState } from 'react'
import { getMetrics, getGrafanaDashboards } from '../api/services'
import { ExternalLink, Activity } from 'lucide-react'

export default function Monitoring() {
  const [metrics,     setMetrics]     = useState<any>(null)
  const [dashboards,  setDashboards]  = useState<any[]>([])
  const [activeBoard, setActiveBoard] = useState<string | null>(null)

  useEffect(() => {
    getMetrics().then(r => setMetrics(r.data))
    getGrafanaDashboards().then(r => {
      const boards = Array.isArray(r.data) ? r.data : []
      setDashboards(boards)
      if (boards.length > 0) setActiveBoard(boards[0].url)
    })
    const interval = setInterval(() =>
      getMetrics().then(r => setMetrics(r.data)), 15000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Métriques temps réel */}
      <div className="card">
        <h3 style={{
          fontSize     : 14,
          fontWeight   : 600,
          color        : 'var(--gray-700)',
          marginBottom : 16,
          display      : 'flex',
          alignItems   : 'center',
          gap          : 6,
        }}>
          <Activity size={16} color="var(--orange)" />
          Métriques temps réel
        </h3>
        <div style={{
          display             : 'grid',
          gridTemplateColumns : 'repeat(4, 1fr)',
          gap                 : 12,
        }}>
          {[
            { key: 'flink_jobs_running',
              label: 'Jobs Flink',     unit: '' },
            { key: 'flink_checkpoints',
              label: 'Checkpoints',    unit: '' },
            { key: 'flink_slots_available',
              label: 'Slots libres',   unit: '' },
            { key: 'trino_up',
              label: 'Trino',
              unit: '', isStatus: true },
          ].map(m => (
            <div key={m.key} style={{
              padding      : '14px',
              background   : 'var(--gray-50)',
              borderRadius : 8,
              textAlign    : 'center',
              border       : '1.5px solid var(--gray-500)',
            }}>
              <div style={{
                fontSize   : 24,
                fontWeight : 700,
                color      : m.isStatus
                  ? (metrics?.[m.key] ? 'var(--success)'
                    : 'var(--error)')
                  : 'var(--orange)',
                marginBottom: 4,
              }}>
                {m.isStatus
                  ? (metrics?.[m.key] ? 'UP' : 'DOWN')
                  : (metrics?.[m.key] || 0)}
                {m.unit}
              </div>
              <div style={{
                fontSize : 11,
                color    : 'var(--gray-400)',
              }}>{m.label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Grafana Dashboards */}
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{
          padding      : '12px 16px',
          borderBottom : '1px solid var(--gray-200)',
          display      : 'flex',
          alignItems   : 'center',
          justifyContent: 'space-between',
        }}>
          <h3 style={{
            fontSize   : 14,
            fontWeight : 600,
            color      : 'var(--gray-700)',
          }}>Grafana Dashboards</h3>
          
          <a
            href="http://172.30.188.23:3000"
            target="_blank"
            rel="noopener noreferrer"
            className="btn btn-ghost"
            style={{ fontSize: 12, padding: '5px 10px' }}
          >
            <ExternalLink size={13} />
            Ouvrir Grafana
          </a>
        </div>

        {/* Dashboard tabs */}
        <div style={{
          display      : 'flex',
          gap          : 0,
          padding      : '0 16px',
          borderBottom : '1px solid var(--gray-200)',
          overflowX    : 'auto',
        }}>
          {dashboards.map((d: any) => (
            <button
              key={d.url}
              onClick={() => setActiveBoard(d.url)}
              style={{
                padding      : '8px 16px',
                border       : 'none',
                borderBottom : activeBoard === d.url
                  ? '2px solid var(--orange)'
                  : '2px solid transparent',
                background   : 'transparent',
                cursor       : 'pointer',
                fontSize     : 12,
                color        : activeBoard === d.url
                  ? 'var(--orange)' : 'var(--gray-500)',
                whiteSpace   : 'nowrap',
                fontWeight   : activeBoard === d.url ? 600 : 400,
              }}
            >{d.title}</button>
          ))}
        </div>

        {/* Grafana iframe */}
        {activeBoard ? (
          <iframe
            src={`http://172.30.188.23:3000${activeBoard}?kiosk=tv&theme=light`}
            style={{
              width  : '100%',
              height : 500,
              border : 'none',
            }}
            title="Grafana Dashboard"
          />
        ) : (
          <div style={{
            height         : 300,
            display        : 'flex',
            alignItems     : 'center',
            justifyContent : 'center',
            color          : 'var(--gray-400)',
          }}>
            Aucun dashboard disponible
          </div>
        )}
      </div>
    </div>
  )
}
