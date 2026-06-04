import { useEffect, useState } from 'react'
import { getPlatformOverview, getTableCounts } from '../api/services'
import {
  Activity, Database, Zap, Shield,
  TrendingUp, Server, CheckCircle, XCircle
} from 'lucide-react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell
} from 'recharts'

export default function Overview() {
  const [overview, setOverview] = useState<any>(null)
  const [counts,   setCounts]   = useState<any>({})
  const [loading,  setLoading]  = useState(true)

  useEffect(() => {
    const fetch = async () => {
      try {
        const [ov, ct] = await Promise.all([
          getPlatformOverview(),
          getTableCounts(),
        ])
        setOverview(ov.data)
        setCounts(ct.data)
      } catch (e) {
        console.error(e)
      } finally {
        setLoading(false)
      }
    }
    fetch()
    const interval = setInterval(fetch, 30000)
    return () => clearInterval(interval)
  }, [])

  if (loading) return (
    <div className="loading-container">
      <div className="spinner" />
      Chargement de la vue d'ensemble...
    </div>
  )

  // Préparer les données du graphique Bronze
  const bronzeData = Object.entries(counts)
    .filter(([k]) => k.startsWith('bronze.'))
    .map(([k, v]) => ({
      name  : k.replace('bronze.', ''),
      rows  : v as number,
    }))
    .sort((a, b) => b.rows - a.rows)

  const COLORS = [
    '#FF6600', '#008080', '#FF8533', '#00A0A0',
    '#FFB366', '#33B3B3', '#FF6633', '#009999',
    '#FF9900'
  ]

  return (
    <div>
      {/* KPI Cards */}
      <div style={{
        display             : 'grid',
        gridTemplateColumns : 'repeat(auto-fit, minmax(200px, 1fr))',
        gap                 : 16,
        marginBottom        : 24,
      }}>
        <KpiCard
          icon={<Server size={20} />}
          label="Services actifs"
          value={`${overview?.services_up}/${overview?.services_total}`}
          color="var(--teal)"
          status={overview?.services_up === overview?.services_total
            ? 'success' : 'warning'}
        />
        <KpiCard
          icon={<Zap size={20} />}
          label="Jobs Flink"
          value={overview?.flink_jobs || 0}
          color="var(--orange)"
          status={overview?.flink_jobs > 0 ? 'success' : 'error'}
          suffix="en cours"
        />
        <KpiCard
          icon={<Activity size={20} />}
          label="Checkpoints"
          value={overview?.checkpoints || 0}
          color="var(--teal)"
          status="success"
          suffix="complétés"
        />
        <KpiCard
          icon={<Database size={20} />}
          label="Lignes Bronze"
          value={(overview?.bronze_rows || 0).toLocaleString('fr-FR')}
          color="var(--orange)"
          status="success"
        />
        <KpiCard
          icon={<Shield size={20} />}
          label="Qualité données"
          value={`${overview?.quality_score || 0}%`}
          color="var(--teal)"
          status="success"
        />
        <KpiCard
          icon={<TrendingUp size={20} />}
          label="Tables Gold"
          value={Object.keys(counts)
            .filter(k => k.startsWith('gold.')).length}
          color="var(--orange)"
          status="success"
          suffix="data marts"
        />
      </div>

      <div style={{
        display             : 'grid',
        gridTemplateColumns : '1fr 1fr',
        gap                 : 20,
        marginBottom        : 20,
      }}>
        {/* Services Health */}
        <div className="card">
          <h3 style={{
            fontSize     : 14,
            fontWeight   : 600,
            marginBottom : 16,
            color        : 'var(--gray-700)',
          }}>État des services</h3>
          <div style={{
            display             : 'grid',
            gridTemplateColumns : '1fr 1fr',
            gap                 : 8,
          }}>
            {overview?.services?.map((s: any) => (
              <div key={s.name} style={{
                display      : 'flex',
                alignItems   : 'center',
                gap          : 8,
                padding      : '8px 10px',
                background   : s.status === 'up'
                  ? '#F0FDF4' : '#FFF1F2',
                borderRadius : 6,
                border       : `1px solid ${s.status === 'up'
                  ? '#BBF7D0' : '#FECDD3'}`,
              }}>
                {s.status === 'up'
                  ? <CheckCircle size={14} color="#10B981" />
                  : <XCircle size={14} color="#EF4444" />}
                <div>
                  <div style={{
                    fontSize   : 12,
                    fontWeight : 500,
                    color      : 'var(--gray-700)',
                  }}>{s.name}</div>
                  <div style={{
                    fontSize : 10,
                    color    : 'var(--gray-400)',
                  }}>{s.url}</div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Bronze Chart */}
        <div className="card">
          <h3 style={{
            fontSize     : 14,
            fontWeight   : 600,
            marginBottom : 16,
            color        : 'var(--gray-700)',
          }}>Données ingérées dans la couche Bronze</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={bronzeData}
              margin={{top: 0, right: 0, left: -10, bottom: 0}}>
              <CartesianGrid strokeDasharray="3 3"
                stroke="var(--gray-100)" />
              <XAxis dataKey="name" tick={{fontSize: 10}}
                angle={-30} textAnchor="end" height={50} />
              <YAxis tick={{fontSize: 10}} />
              <Tooltip
                formatter={(v: any) =>
                  [v.toLocaleString('fr-FR'), 'Lignes']}
                contentStyle={{fontSize: 12}}
              />
              <Bar dataKey="rows" radius={[4,4,0,0]}>
                {bronzeData.map((_, i) => (
                  <Cell key={i}
                    fill={COLORS[i % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Medallion Architecture Summary */}
      <div className="card">
        <h3 style={{
          fontSize     : 14,
          fontWeight   : 600,
          marginBottom : 16,
          color        : 'var(--gray-700)',
        }}>Résumé de l'architecture Medallion</h3>
        <div style={{
          display             : 'grid',
          gridTemplateColumns : '1fr 1fr 1fr',
          gap                 : 16,
        }}>
          {['bronze', 'silver', 'gold'].map(schema => {
            const tables = Object.entries(counts)
              .filter(([k]) => k.startsWith(`${schema}.`))
            const total  = tables.reduce(
              (sum, [, v]) => sum + (v as number), 0)
            const colors: any = {
              bronze: {bg:'#FEF3C7', border:'#FDE68A',
                       color:'#92400E', dot:'#F59E0B'},
              silver: {bg:'#F3F4F6', border:'#E5E7EB',
                       color:'#374151', dot:'#6B7280'},
              gold  : {bg:'#FFF7ED', border:'#FED7AA',
                       color:'#C2410C', dot:'#FF6600'},
            }
            const c = colors[schema]
            return (
              <div key={schema} style={{
                padding      : 16,
                background   : c.bg,
                borderRadius : 8,
                border       : `1px solid ${c.border}`,
              }}>
                <div style={{
                  display      : 'flex',
                  alignItems   : 'center',
                  gap          : 8,
                  marginBottom : 8,
                }}>
                  <div style={{
                    width        : 10,
                    height       : 10,
                    borderRadius : '50%',
                    background   : c.dot,
                  }}/>
                  <span style={{
                    fontWeight     : 600,
                    color          : c.color,
                    textTransform  : 'capitalize',
                    fontSize       : 14,
                  }}>{schema}</span>
                </div>
                <div style={{
                  fontSize   : 24,
                  fontWeight : 700,
                  color      : c.color,
                  marginBottom: 4,
                }}>
                  {total.toLocaleString('fr-FR')}
                </div>
                <div style={{
                  fontSize : 11,
                  color    : c.color,
                  opacity  : 0.8,
                }}>
                  {tables.length} tables
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function KpiCard({ icon, label, value, color, status, suffix }: any) {
  const statusColors: any = {
    success : { bg: '#F0FDF4', border: '#BBF7D0' },
    warning : { bg: '#FFFBEB', border: '#FDE68A' },
    error   : { bg: '#FFF1F2', border: '#FECDD3' },
  }
  const sc = statusColors[status] || statusColors.success

  return (
    <div style={{
      background   : 'white',
      borderRadius : 8,
      padding      : '16px 20px',
      border       : `1px solid ${sc.border}`,
      background   : sc.bg,
    }}>
      <div style={{
        display      : 'flex',
        alignItems   : 'center',
        gap          : 8,
        marginBottom : 8,
        color        : color,
      }}>
        {icon}
        <span style={{
          fontSize : 12,
          color    : 'var(--gray-500)',
        }}>{label}</span>
      </div>
      <div style={{
        fontSize   : 24,
        fontWeight : 700,
        color      : 'var(--gray-800)',
      }}>{value}</div>
      {suffix && (
        <div style={{
          fontSize : 11,
          color    : 'var(--gray-400)',
          marginTop: 2,
        }}>{suffix}</div>
      )}
    </div>
  )
}
