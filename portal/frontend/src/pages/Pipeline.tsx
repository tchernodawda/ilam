import { useEffect, useState } from 'react'
import {
  getFlinkJobs, getFlinkOverview,
  getAirflowDags, getDagRuns, triggerDag,
  startPyFlinkJob, getTaskStates, getKafkaTopics
} from '../api/services'
import {
  Play, RefreshCw, CheckCircle, XCircle,
  AlertTriangle, Zap, ChevronDown, ChevronUp,
  Radio, Database
} from 'lucide-react'
import toast from 'react-hot-toast'

const DAG_ID = 'ilam_sonatel_pipeline'

export default function Pipeline() {
  const [flinkOverview,  setFlinkOverview]  = useState<any>(null)
  const [flinkJobs,      setFlinkJobs]      = useState<any[]>([])
  const [dagRuns,        setDagRuns]        = useState<any[]>([])
  const [taskStates,     setTaskStates]     = useState<Record<string, any[]>>({})
  const [expandedRun,    setExpandedRun]    = useState<string | null>(null)
  const [kafkaTopics,    setKafkaTopics]    = useState<any>(null)
  const [loading,        setLoading]        = useState(true)
  const [triggering,     setTriggering]     = useState(false)
  const [startingFlink,  setStartingFlink]  = useState(false)

  const fetchData = async () => {
    try {
      const [ov, jobs, runs, kafka] = await Promise.all([
        getFlinkOverview(),
        getFlinkJobs(),
        getDagRuns(DAG_ID),
        getKafkaTopics(),
      ])
      setFlinkOverview(ov.data)
      setFlinkJobs(jobs.data || [])
      setDagRuns(runs.data?.dag_runs || [])
      setKafkaTopics(kafka.data)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 30000)
    return () => clearInterval(interval)
  }, [])

  const handleTrigger = async () => {
    setTriggering(true)
    try {
      await triggerDag(DAG_ID)
      toast.success('Pipeline déclenché !')
      setTimeout(fetchData, 2000)
    } catch {
      toast.error('Erreur lors du déclenchement')
    } finally {
      setTriggering(false)
    }
  }

  const handleStartFlink = async () => {
    setStartingFlink(true)
    try {
      await startPyFlinkJob()
      toast.success('Job PyFlink soumis : démarrage en cours...')
      setTimeout(fetchData, 5000)
    } catch {
      toast.error('Erreur lors du démarrage')
    } finally {
      setStartingFlink(false)
    }
  }

  const handleExpandRun = async (runId: string) => {
    if (expandedRun === runId) {
      setExpandedRun(null)
      return
    }
    setExpandedRun(runId)
    if (!taskStates[runId]) {
      try {
        const r = await getTaskStates(DAG_ID, runId)
        setTaskStates(prev => ({
          ...prev,
          [runId]: r.data?.task_instances || []
        }))
      } catch (e) {
        console.error(e)
      }
    }
  }

  const isStreaming = flinkJobs.some(j => j.status === 'RUNNING')

  const statusBadge = (status: string) => {
    const map: any = {
      RUNNING : { cls: 'badge-success', label: 'En cours' },
      FINISHED: { cls: 'badge-info',    label: 'Terminé'  },
      FAILED  : { cls: 'badge-error',   label: 'Échoué'   },
      CANCELED: { cls: 'badge-warning', label: 'Annulé'   },
      success : { cls: 'badge-success', label: 'Succès'   },
      failed  : { cls: 'badge-error',   label: 'Échoué'   },
      running : { cls: 'badge-orange',  label: 'En cours' },
      queued  : { cls: 'badge-warning', label: 'En file'  },
      upstream_failed: { cls: 'badge-warning', label: 'Non exécuté' },
      skipped : { cls: 'badge-warning', label: 'Ignoré'   },
    }
    const s = map[status] || { cls: 'badge-info', label: status }
    return <span className={`badge ${s.cls}`}>{s.label}</span>
  }

  const taskIcon = (state: string) => {
    if (state === 'success')
      return <CheckCircle size={14} color="var(--success)" />
    if (state === 'failed')
      return <XCircle size={14} color="var(--error)" />
    if (state === 'upstream_failed')
      return <AlertTriangle size={14} color="var(--warning)" />
    return <div style={{
      width: 14, height: 14, borderRadius: '50%',
      background: 'var(--gray-300)'
    }}/>
  }

  if (loading) return (
    <div className="loading-container">
      <div className="spinner" /> Chargement du pipeline...
    </div>
  )

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* ── Flink ───────────────────────────────────────────── */}
      <div className="card">
        <div style={{
          display: 'flex', alignItems: 'center',
          justifyContent: 'space-between', marginBottom: 16,
        }}>
          <h3 style={{
            fontSize: 14, fontWeight: 600,
            color: 'var(--gray-700)',
            display: 'flex', alignItems: 'center', gap: 6,
          }}>
            <Zap size={16} color="var(--orange)" />
            Streaming Apache Flink
          </h3>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={fetchData}
              className="btn btn-ghost"
              style={{ padding: '4px 10px', fontSize: 12, border : '1.5px solid var(--gray-400)' }}>
              <RefreshCw size={13} /> Actualiser
            </button>
            {!isStreaming && (
              <button
                onClick={handleStartFlink}
                disabled={startingFlink}
                className="btn btn-secondary"
                style={{ fontSize: 12 }}>
                <Play size={12} />
                {startingFlink ? 'Démarrage...' : 'Démarrer le streaming'}
              </button>
            )}
          </div>
        </div>

        {/* Statut streaming */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 10,
          padding: '10px 14px', borderRadius: 8, marginBottom: 16,
          background: isStreaming ? '#F0FDF4' : '#FFF1F2',
          border: `1px solid ${isStreaming ? '#BBF7D0' : '#FECDD3'}`,
        }}>
          <div style={{
            width: 10, height: 10, borderRadius: '50%',
            background: isStreaming ? 'var(--success)' : 'var(--error)',
            animation: isStreaming ? 'pulse 2s infinite' : 'none',
          }}/>
          <span style={{
            fontSize: 13, fontWeight: 500,
            color: isStreaming ? '#065F46' : '#991B1B',
          }}>
            {isStreaming
              ? 'Streaming actif : données ingérées en temps réel'
              : 'Streaming arrêté : aucun job PyFlink en cours'}
          </span>
        </div>

        {/* Métriques cluster */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, 1fr)',
          gap: 12, marginBottom: isStreaming ? 16 : 0,
        }}>
          {[
            { label: 'Task Managers',
              value: flinkOverview?.['taskmanagers'] || 0 },
            { label: 'Slots total',
              value: flinkOverview?.['slots-total'] || 0 },
            { label: 'Slots disponibles',
              value: flinkOverview?.['slots-available'] || 0 },
            { label: 'Jobs en cours',
              value: flinkOverview?.['jobs-running'] || 0 },
          ].map(item => (
            <div key={item.label} style={{
              padding: '12px', background: 'var(--gray-50)',
              borderRadius: 6, textAlign: 'center',
            }}>
              <div style={{
                fontSize: 22, fontWeight: 700,
                color: 'var(--orange)',
              }}>{item.value}</div>
              <div style={{
                fontSize: 11, color: 'var(--gray-500)',
              }}>{item.label}</div>
            </div>
          ))}
        </div>

        {/* Jobs actifs */}
        {isStreaming && flinkJobs.length > 0 && (
          <>
            <h4 style={{
              fontSize: 12, fontWeight: 600,
              color: 'var(--gray-500)', marginBottom: 8,
              textTransform: 'uppercase', letterSpacing: '0.5px',
            }}>Jobs actifs</h4>
            <table>
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Nom</th>
                  <th>Statut</th>
                  <th>Checkpoints OK</th>
                  <th>Checkpoints FAIL</th>
                </tr>
              </thead>
              <tbody>
                {flinkJobs.map((job: any) => (
                  <tr key={job.id}>
                    <td style={{
                      fontFamily: 'monospace', fontSize: 11,
                      color: 'var(--teal)',
                    }}>{job.id?.slice(0, 8)}...</td>
                    <td style={{
                      maxWidth: 200, overflow: 'hidden',
                      textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    }}>{job.name || '—'}</td>
                    <td>{statusBadge(job.status)}</td>
                    <td style={{ color: 'var(--success)' }}>
                      {job.checkpoints?.completed || 0}
                    </td>
                    <td style={{
                      color: job.checkpoints?.failed > 0
                        ? 'var(--error)' : 'var(--gray-400)',
                    }}>
                      {job.checkpoints?.failed || 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>

      {/* ── Kafka ───────────────────────────────────────────── */}
      <div className="card">
        <h3 style={{
          fontSize: 14, fontWeight: 600,
          color: 'var(--gray-700)', marginBottom: 16,
          display: 'flex', alignItems: 'center', gap: 6,
        }}>
          <Radio size={16} color="var(--teal)" />
          Topics Kafka
        </h3>
        {kafkaTopics?.error ? (
          <div style={{
            padding: 12, background: '#FFF1F2',
            borderRadius: 6, fontSize: 12,
            color: 'var(--error)',
          }}>
            Kafka inaccessible : {kafkaTopics.error}
          </div>
        ) : (
          <div>
            <div style={{
              fontSize: 13, color: 'var(--gray-500)',
              marginBottom: 12,
            }}>
              <span style={{
                fontWeight: 600,
                color: 'var(--teal)',
                fontSize: 20,
              }}>
                {kafkaTopics?.count || 0}
              </span>
              {' '}topics actifs
            </div>
            <div style={{
              display: 'flex', flexWrap: 'wrap', gap: 8,
            }}>
              {(kafkaTopics?.topics || []).map((t: string) => (
                <span key={t} style={{
                  padding: '4px 10px',
                  background: 'var(--gray-50)',
                  border: '1.5px solid var(--gray-200)',
                  borderRadius: 20, fontSize: 13,
                  color: 'var(--teal)',
                  fontFamily: 'monospace',
                }}>{t}</span>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* ── Airflow ─────────────────────────────────────────── */}
      <div className="card">
        <div style={{
          display: 'flex', alignItems: 'center',
          justifyContent: 'space-between', marginBottom: 16,
        }}>
          <h3 style={{
            fontSize: 14, fontWeight: 600,
            color: 'var(--gray-700)',
          }}>
            DAG Airflow : {DAG_ID}
          </h3>
          <button
            onClick={handleTrigger}
            disabled={triggering}
            className="btn btn-primary"
            style={{ fontSize: 12 }}>
            <Play size={12} />
            {triggering ? 'Déclenchement...' : 'Déclencher'}
          </button>
        </div>

        {dagRuns.length === 0 ? (
          <div style={{
            textAlign: 'center', padding: 20,
            color: 'var(--gray-400)', fontSize: 13,
          }}>Aucun run disponible</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Statut</th>
                <th>Début</th>
                <th>Durée</th>
                <th>Détail</th>
              </tr>
            </thead>
            <tbody>
              {dagRuns.map((run: any) => (
                <>
                  <tr key={run.dag_run_id}>
                    <td style={{
                      fontFamily: 'monospace', fontSize: 11,
                      color: 'var(--teal)',
                    }}>
                      {run.dag_run_id?.slice(0, 35)}...
                    </td>
                    <td>{statusBadge(run.state)}</td>
                    <td style={{ fontSize: 11 }}>
                      {run.start_date
                        ? new Date(run.start_date)
                            .toLocaleString('fr-FR')
                        : '—'}
                    </td>
                    <td style={{ fontSize: 11 }}>
                      {run.start_date && run.end_date
                        ? `${Math.round(
                            (new Date(run.end_date).getTime() -
                             new Date(run.start_date).getTime())
                            / 1000)}s`
                        : '—'}
                    </td>
                    <td>
                      <button
                        onClick={() => handleExpandRun(run.dag_run_id)}
                        style={{
                          background: 'none', border: 'none',
                          cursor: 'pointer',
                          color: 'var(--teal)', fontSize: 11,
                          display: 'flex', alignItems: 'center', gap: 4,
                        }}>
                        {expandedRun === run.dag_run_id
                          ? <><ChevronUp size={12} /> Masquer</>
                          : <><ChevronDown size={12} /> Voir tâches</>}
                      </button>
                    </td>
                  </tr>

                  {/* Détail des tâches */}
                  {expandedRun === run.dag_run_id && (
                    <tr key={`${run.dag_run_id}-detail`}>
                      <td colSpan={5} style={{ padding: '0 0 8px 0' }}>
                        <div style={{
                          background: 'var(--gray-50)',
                          borderRadius: 8,
                          padding: '12px 16px',
                          margin: '0 0 4px 0',
                        }}>
                          {!taskStates[run.dag_run_id] ? (
                            <div style={{
                              display: 'flex', gap: 8,
                              alignItems: 'center',
                              fontSize: 12, color: 'var(--gray-400)',
                            }}>
                              <div className="spinner"
                                style={{ width: 14, height: 14 }}/>
                              Chargement...
                            </div>
                          ) : (
                            <div style={{
                              display: 'flex', flexWrap: 'wrap', gap: 8,
                            }}>
                              {taskStates[run.dag_run_id].map((t: any) => (
                                <div key={t.task_id} style={{
                                  display: 'flex', alignItems: 'center',
                                  gap: 6, padding: '6px 10px',
                                  background: 'white', borderRadius: 6,
                                  border: '1px solid var(--gray-200)',
                                  fontSize: 12,
                                }}>
                                  {taskIcon(t.state)}
                                  <span style={{
                                    color: t.state === 'failed'
                                      ? 'var(--error)'
                                      : t.state === 'success'
                                      ? 'var(--success)'
                                      : 'var(--gray-500)',
                                    fontWeight: t.state === 'failed'
                                      ? 600 : 400,
                                  }}>{t.task_id}</span>
                                </div>
                              ))}
                            </div>
                          )}

                          {/* Message explicatif si run échoué */}
                          {run.state === 'failed' &&
                           taskStates[run.dag_run_id] && (
                            <div style={{
                              marginTop: 10, padding: '8px 12px',
                              background: '#FFF1F2', borderRadius: 6,
                              fontSize: 12, color: '#991B1B',
                              borderLeft: '3px solid var(--error)',
                            }}>
                              <strong>Cause probable :</strong>{' '}
                              {taskStates[run.dag_run_id]
                                .find((t: any) => t.state === 'failed')
                                ?.task_id === 'verify_streaming'
                                ? 'Le job PyFlink n\'est pas en cours d\'exécution. Démarrez le streaming depuis le bouton ci-dessus.'
                                : `La tâche "${
                                    taskStates[run.dag_run_id]
                                      .find((t: any) => t.state === 'failed')
                                      ?.task_id || 'inconnue'
                                  }" a échoué.`
                              }
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
