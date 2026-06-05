import { useEffect, useState } from 'react'
import { getSparkCluster, getSparkApps, submitSparkJob } from '../api/services'
import { Zap, Play } from 'lucide-react'
import toast from 'react-hot-toast'

export default function Spark() {
  const [cluster,    setCluster]    = useState<any>(null)
  const [apps,       setApps]       = useState<any>(null)
  const [submitting, setSubmitting] = useState<string | null>(null)
  const [loading,    setLoading]    = useState(true)

  const fetchData = async () => {
    try {
      const [c, a] = await Promise.all([
        getSparkCluster(),
        getSparkApps(),
      ])
      setCluster(c.data)
      setApps(a.data)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 15000)
    return () => clearInterval(interval)
  }, [])

  const handleSubmit = async (jobType: string) => {
    setSubmitting(jobType)
    try {
      await submitSparkJob(jobType)
      toast.success(`Job Spark ${jobType} soumis !`)
      setTimeout(fetchData, 3000)
    } catch (e) {
      toast.error('Erreur lors de la soumission')
    } finally {
      setSubmitting(null)
    }
  }

  const jobs = [
    {
      id          : 'bronze',
      label       : 'Bronze Preprocessing',
      description : 'Déduplication + normalisation MSISDN',
      input       : 'bronze.cdr_voice',
      output      : 'bronze.cdr_voice_clean',
      color       : '#F59E0B',
    },
    {
      id          : 'silver',
      label       : 'Silver Enrichissement',
      description : 'Jointure CDR × Abonnés + métriques dérivées',
      input       : 'bronze.cdr_voice_clean',
      output      : 'silver.cdr_voice_enriched',
      color       : '#6B7280',
    },
    {
      id          : 'gold',
      label       : 'Gold Agrégation',
      description : 'Revenus, ARPU, détection fraude OM',
      input       : 'bronze.* + silver.*',
      output      : 'gold.spark_mart_*',
      color       : '#FF6600',
    },
  ]

  if (loading) return (
    <div className="loading-container">
      <div className="spinner" />
      Chargement Spark...
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Cluster Status */}
      <div className="card">
        <h3 style={{
          fontSize     : 14,
          fontWeight   : 600,
          color        : 'var(--gray-700)',
          marginBottom : 14,
          display      : 'flex',
          alignItems   : 'center',
          gap          : 6,
        }}>
          <Zap size={16} color="var(--orange)" />
          Cluster Spark
        </h3>
        <div style={{
          display             : 'grid',
          gridTemplateColumns : 'repeat(6, 1fr)',
          gap                 : 12,
        }}>
          {[
            { label: 'Workers',      value: cluster?.workers || 0 },
            { label: 'Cores total',  value: cluster?.cores_total || 0 },
            { label: 'Cores utilisés',value: cluster?.cores_used || 0 },
            { label: 'Mémoire (MB)', value: cluster?.memory_total || 0 },
            { label: 'Apps actives', value: cluster?.apps_running || 0 },
            { label: 'Apps terminées',value: cluster?.apps_completed || 0 },
          ].map(item => (
            <div key={item.label} style={{
              padding      : '10px',
              background   : 'var(--gray-50)',
              borderRadius : 6,
              border       : '1.5px solid var(--gray-400)',
              textAlign    : 'center',
            }}>
              <div style={{
                fontSize   : 20,
                fontWeight : 700,
                color      : 'var(--orange)',
              }}>{item.value}</div>
              <div style={{
                fontSize : 10,
                color    : 'var(--gray-400)',
              }}>{item.label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Jobs */}
      <div className="card">
        <h3 style={{
          fontSize     : 14,
          fontWeight   : 600,
          color        : 'var(--gray-700)',
          marginBottom : 14,
        }}>Soumettre un job Spark</h3>

        <div style={{
          display             : 'grid',
          gridTemplateColumns : 'repeat(3, 1fr)',
          gap                 : 16,
          marginBottom        : 16,
        }}>
          {jobs.map(job => (
            <div key={job.id} style={{
              borderRadius : 8,
              padding      : 16,
              border   : `2px solid ${job.color}`,
            }}>
              <div style={{
                fontWeight   : 600,
                fontSize     : 13,
                color        : 'var(--gray-800)',
                marginBottom : 6,
              }}>{job.label}</div>
              <div style={{
                fontSize     : 12,
                color        : 'var(--gray-500)',
                marginBottom : 12,
              }}>{job.description}</div>
              <div style={{ fontSize: 12, marginBottom: 11 }}>
                <div style={{
                  color        : 'var(--gray-400)',
                  marginBottom : 2,
                }}>
                  Entrée : <span style={{
                    color      : 'var(--teal)',
                    fontFamily : 'monospace',
                  }}>{job.input}</span>
                </div>
                <div style={{ color: 'var(--gray-400)' }}>
                  Sortie : <span style={{
                    color      : job.color,
                    fontFamily : 'monospace',
                  }}>{job.output}</span>
                </div>
              </div>
              <button
                onClick={() => handleSubmit(job.id)}
                disabled={submitting !== null}
                className="btn btn-primary"
                style={{
                  width      : '100%',
                  justifyContent: 'center',
                  fontSize   : 13,
                  background : job.color,
                }}
              >
                <Play size={12} />
                {submitting === job.id
                  ? 'Soumission...' : 'Lancer'}
              </button>
            </div>
          ))}
        </div>

        <button
          onClick={() => handleSubmit('all')}
          disabled={submitting !== null}
          className="btn btn-secondary"
          style={{ width: '100%', justifyContent: 'center' }}
        >
          <Play size={14} />
          {submitting === 'all'
            ? 'Soumission en cours...'
            : 'Lancer tous les jobs (Bronze → Silver → Gold)'}
        </button>
      </div>

      {/* Apps history */}
      {apps?.completed?.length > 0 && (
        <div className="card">
          <h3 style={{
            fontSize     : 14,
            fontWeight   : 600,
            color        : 'var(--gray-700)',
            marginBottom : 16,
          }}>Historique des jobs</h3>
          <table>
            <thead>
              <tr>
                <th>Nom</th>
                <th>Statut</th>
                <th>Cores</th>
                <th>Mémoire</th>
              </tr>
            </thead>
            <tbody>
              {apps.completed.map((app: any, i: number) => (
                <tr key={i}>
                  <td style={{ fontSize: 12 }}>{app.name}</td>
                  <td>
                    <span className="badge badge-info">
                      Terminé
                    </span>
                  </td>
                  <td>{app.cores}</td>
                  <td>{app.memoryperslave} MB</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
