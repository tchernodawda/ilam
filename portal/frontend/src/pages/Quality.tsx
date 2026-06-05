import { useEffect, useState } from 'react'
import { Shield, CheckCircle, XCircle, AlertTriangle, RefreshCw } from 'lucide-react'
import api from '../api/client'

export default function Quality() {
  const [report,  setReport]  = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState<string | null>(null)

  const fetchReport = async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.get('/health/quality/report')
      setReport(r.data)
    } catch (e: any) {
      setError('Impossible de charger le rapport GX')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchReport()
    const interval = setInterval(fetchReport, 60000)
    return () => clearInterval(interval)
  }, [])

  const statusIcon = (status: string) => {
    if (status === 'PASS')
      return <CheckCircle size={16} color="var(--success)" />
    if (status === 'WARN')
      return <AlertTriangle size={16} color="var(--warning)" />
    return <XCircle size={16} color="var(--error)" />
  }

  const scoreColor = (score: number) =>
    score >= 80 ? 'var(--success)'
    : score >= 50 ? 'var(--warning)'
    : 'var(--error)'

  if (loading) return (
    <div className="loading-container">
      <div className="spinner" />
      Chargement du rapport qualité...
    </div>
  )

  if (error) return (
    <div style={{
      padding: 20, background: '#FFF1F2',
      borderRadius: 8, color: 'var(--error)', fontSize: 13,
    }}>
      {error} — Aucun rapport GX disponible.
      Déclenchez le pipeline Airflow pour en générer un.
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Score global */}
      <div className="card" style={{
        background  : 'linear-gradient(135deg, #1A1A2E, #16213E)',
        color       : 'white',
      }}>
        <div style={{
          display: 'flex', alignItems: 'center',
          justifyContent: 'space-between',
        }}>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 20,
          }}>
            <div style={{
              width: 80, height: 80, borderRadius: '50%',
              background: 'rgba(255,255,255,0.1)',
              display: 'flex', alignItems: 'center',
              justifyContent: 'center', fontSize: 22,
              fontWeight: 700,
              color: report?.global_score >= 80
                ? '#10B981' : '#F59E0B',
            }}>
              {report?.global_score || 0}%
            </div>
            <div>
              <div style={{
                fontSize: 18, fontWeight: 600, marginBottom: 4,
              }}>Score de qualité global</div>
              <div style={{
                fontSize: 12, color: 'rgba(255,255,255,0.6)',
              }}>
                Dernière validation :{' '}
                {report?.run_date
                  ? new Date(report.run_date)
                      .toLocaleString('fr-FR')
                  : '—'}
              </div>
              <div style={{
                fontSize: 12, color: 'rgba(255,255,255,0.6)',
                marginTop: 4,
              }}>
                {report?.summary?.total_success || 0} /
                {report?.summary?.total_expectations || 0} expectations réussies
              </div>
            </div>
          </div>
          <div style={{
            display: 'flex', flexDirection: 'column',
            alignItems: 'flex-end', gap: 12,
          }}>
            <span className={`badge ${
              report?.global_status === 'PASS' ? 'badge-success'
              : report?.global_status === 'WARN' ? 'badge-warning'
              : 'badge-error'
            }`} style={{ fontSize: 13, padding: '4px 12px' }}>
              {report?.global_status || '—'}
            </span>
            <button
              onClick={fetchReport}
              style={{
                background: 'rgba(255,255,255,0.1)',
                border: 'none', borderRadius: 6,
                color: 'white', cursor: 'pointer',
                padding: '6px 10px', fontSize: 11,
                display: 'flex', alignItems: 'center', gap: 4,
              }}>
              <RefreshCw size={12} /> Actualiser
            </button>
          </div>
        </div>
      </div>

      {/* Détail par table */}
      <div className="card">
        <h3 style={{
          fontSize: 14, fontWeight: 600,
          color: 'var(--gray-700)', marginBottom: 16,
          display: 'flex', alignItems: 'center', gap: 6,
        }}>
          <Shield size={16} color="var(--teal)" />
          Résultats par table
        </h3>

        <div style={{
          display: 'flex', flexDirection: 'column', gap: 12,
        }}>
          {report?.tables?.map((t: any) => (
            <div key={t.suite} style={{
              padding: 16, border: '1px solid var(--gray-200)',
              borderRadius: 8,
            }}>
              <div style={{
                display: 'flex', alignItems: 'center',
                gap: 16, marginBottom: t.failed_expectations?.length > 0 ? 12 : 0,
              }}>
                {statusIcon(t.status)}
                <div style={{ flex: 1 }}>
                  <div style={{
                    fontWeight: 600, fontSize: 13,
                    color: 'var(--gray-800)', marginBottom: 4,
                  }}>{t.suite}</div>
                  <div style={{
                    fontSize: 11, color: 'var(--gray-400)',
                  }}>
                    {t.success}/{t.total} expectations réussies
                  </div>
                </div>

                {/* Barre de score */}
                <div style={{ width: 200 }}>
                  <div style={{
                    display: 'flex', justifyContent: 'space-between',
                    fontSize: 11, marginBottom: 4,
                  }}>
                    <span style={{ color: 'var(--gray-500)' }}>Score</span>
                    <span style={{
                      fontWeight: 600,
                      color: scoreColor(t.score),
                    }}>{t.score}%</span>
                  </div>
                  <div style={{
                    height: 6, background: 'var(--gray-100)',
                    borderRadius: 3, overflow: 'hidden',
                  }}>
                    <div style={{
                      width: `${t.score}%`, height: '100%',
                      background: scoreColor(t.score),
                      borderRadius: 3, transition: 'width 0.5s',
                    }}/>
                  </div>
                </div>

                <span className={`badge ${
                  t.status === 'PASS' ? 'badge-success'
                  : t.status === 'WARN' ? 'badge-warning'
                  : 'badge-error'
                }`}>{t.status}</span>
              </div>

              {/* Expectations échouées */}
              {t.failed_expectations?.length > 0 && (
                <div style={{
                  padding: '8px 12px',
                  background: '#FFF1F2', borderRadius: 6,
                  borderLeft: '3px solid var(--error)',
                }}>
                  <div style={{
                    fontSize: 11, fontWeight: 600,
                    color: '#991B1B', marginBottom: 4,
                  }}>Expectations échouées :</div>
                  {t.failed_expectations.map((f: any, i: number) => (
                    <div key={i} style={{
                      fontSize: 11, color: '#991B1B',
                      fontFamily: 'monospace',
                    }}>
                      ↳ {f.expectation} — colonne : {f.column}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Seuils */}
      <div className="card">
        <h3 style={{
          fontSize: 14, fontWeight: 600,
          color: 'var(--gray-700)', marginBottom: 12,
        }}>Seuils de qualité</h3>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(3, 1fr)', gap: 12,
        }}>
          {[
            { label: 'PASS', range: '≥ 80%',
              color: 'var(--success)', bg: '#F0FDF4',
              desc: 'Pipeline continue normalement' },
            { label: 'WARN', range: '50% – 79%',
              color: 'var(--warning)', bg: '#FFFBEB',
              desc: 'Alerte Slack, pipeline continue' },
            { label: 'FAIL', range: '< 50%',
              color: 'var(--error)', bg: '#FFF1F2',
              desc: 'Pipeline bloqué, investigation requise' },
          ].map(s => (
            <div key={s.label} style={{
              padding: 14, background: s.bg,
              borderRadius: 8, borderLeft: `3px solid ${s.color}`,
            }}>
              <div style={{
                fontWeight: 700, color: s.color,
                fontSize: 14, marginBottom: 4,
              }}>{s.label} — {s.range}</div>
              <div style={{
                fontSize: 11, color: 'var(--gray-500)',
              }}>{s.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
