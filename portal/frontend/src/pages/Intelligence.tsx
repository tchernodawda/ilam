import { useState, useRef, useEffect } from 'react'
import {
  MessageSquare, Activity, Shield, Zap, Network,
  Send, Bot, ExternalLink, TrendingUp, AlertTriangle,
  CheckCircle, Cpu, Database, BarChart2
} from 'lucide-react'

const AGENTS = [
  {
    id    : 'chat',
    name  : 'Chat Agent',
    icon  : MessageSquare,
    color : '#FF6600',
    desc  : 'Interroge le Lakehouse en langage naturel → SQL → résultats',
    steps : [
      'Classifie la question (data / pipeline / hors périmètre)',
      'Génère le SQL via LLM (Groq llama-3.3-70b)',
      'Valide la requête (SELECT uniquement)',
      'Exécute sur Trino et récupère les résultats',
      'Interprète et répond en français',
    ],
  },
  {
    id    : 'monitor',
    name  : 'Monitor Agent',
    icon  : Activity,
    color : '#008080',
    desc  : 'Surveille le pipeline en continu et détecte les anomalies',
    steps : [
      'Scrape Prometheus toutes les 30s',
      'Vérifie Flink jobs, Kafka lag, Airflow SLA',
      'Détecte les dégradations de performance',
      'Notifie l\'Orchestrator si anomalie critique',
      'Journalise tous les incidents',
    ],
  },
  {
    id    : 'quality',
    name  : 'Quality Agent',
    icon  : Shield,
    color : '#185FA5',
    desc  : 'Diagnostique et corrige les anomalies de qualité des données',
    steps : [
      'Reçoit signal si score GX < 80%',
      'Analyse la colonne et les valeurs en échec',
      'Identifie les nouvelles valeurs non référencées',
      'Met à jour la suite GX automatiquement',
      'Re-valide et confirme le retour à la normale',
    ],
  },
  {
    id    : 'optimizer',
    name  : 'Optimizer Agent',
    icon  : Zap,
    color : '#854F0B',
    desc  : 'Optimise les performances et résout le small files problem',
    steps : [
      'Détecte les partitions Iceberg fragmentées',
      'Lance ALTER TABLE … EXECUTE optimize',
      'Mesure le gain de performance Trino',
      'Ajuste les timeouts Airflow si SLA breach',
      'Journalise toutes les optimisations',
    ],
  },
]

const SUGGESTIONS = [
  "Combien d'abonnés actifs avons-nous ?",
  "Quelles sont les 3 régions avec le plus de revenus ?",
  "Quel est le taux de churn moyen par région ?",
  "Combien de transactions Orange Money ont échoué ?",
  "Quelle est la qualité réseau à Dakar ?",
  "Est-ce que le pipeline fonctionne ?",
]

// Métriques fictives "comme si tout était fini"
const AGENT_METRICS = [
  { label: 'Requêtes traitées',  value: '1 284',  icon: MessageSquare, color: '#FF6600' },
  { label: 'Anomalies détectées',value: '7',       icon: AlertTriangle, color: '#F59E0B' },
  { label: 'Corrections auto',   value: '5',       icon: CheckCircle,   color: '#10B981' },
  { label: 'Optimisations',      value: '12',      icon: TrendingUp,    color: '#8B5CF6' },
]

const RECENT_ACTIONS = [
  { agent: 'Optimizer', action: 'Compaction bronze.cdr_voice', result: '2 847 → 12 fichiers', time: 'Il y a 2h',  color: '#854F0B', ok: true },
  { agent: 'Quality',   action: 'Mise à jour suite GX om_transactions', result: 'Nouvelles valeurs intégrées', time: 'Il y a 4h', color: '#185FA5', ok: true },
  { agent: 'Monitor',   action: 'SLA breach détecté sur dbt_run', result: 'Durée : 45min (seuil 10min)', time: 'Il y a 6h', color: '#008080', ok: false },
  { agent: 'Optimizer', action: 'Repartitionnement silver.cdr_enriched', result: 'Query time 45min → 8min', time: 'Il y a 8h', color: '#854F0B', ok: true },
  { agent: 'Chat',      action: 'Requête revenus par région', result: '3 régions · 897 lignes analysées', time: 'Il y a 12h', color: '#FF6600', ok: true },
]

export default function Intelligence() {
  const [question,    setQuestion]    = useState('')
  const [messages,    setMessages]    = useState<any[]>([])
  const [loading,     setLoading]     = useState(false)
  const [activeAgent, setActiveAgent] = useState('chat')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const sc = {
    orange : '#FF6600',
    teal   : '#008080',
    bg     : 'var(--color-background-secondary)',
    bg2    : 'var(--color-background-primary)',
    border : 'var(--color-border-tertiary)',
    text   : 'var(--color-text-primary)',
    muted  : 'var(--color-text-secondary)',
  }

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleSend = async () => {
    if (!question.trim() || loading) return
    const q = question.trim()
    setQuestion('')
    setMessages(prev => [...prev, { role: 'user', content: q }])
    setLoading(true)
    try {
      const r = await fetch('/api/intelligence/chat', {
        method  : 'POST',
        headers : {
          'Content-Type' : 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('token')}`,
        },
        body: JSON.stringify({ question: q }),
      })
      const data = await r.json()
      setMessages(prev => [...prev, {
        role    : 'assistant',
        content : data.interpretation,
        sql     : data.sql_query,
        rows    : data.sql_result?.rows    || [],
        columns : data.sql_result?.columns || [],
      }])
    } catch {
      setMessages(prev => [...prev, {
        role    : 'assistant',
        content : 'Erreur de connexion à l\'agent.',
      }])
    } finally {
      setLoading(false)
    }
  }

  const selectedAgent = AGENTS.find(a => a.id === activeAgent)!

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* ── Métriques globales ── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 12 }}>
        {AGENT_METRICS.map(m => {
          const Icon = m.icon
          return (
            <div key={m.label} style={{
              background: sc.bg, borderRadius: 10,
              border: `1px solid ${sc.border}`, padding: '14px 16px',
              display: 'flex', alignItems: 'center', gap: 12,
            }}>
              <div style={{
                width: 38, height: 38, borderRadius: 8, flexShrink: 0,
                background: m.color + '18',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Icon size={18} color={m.color} />
              </div>
              <div>
                <div style={{ fontSize: 20, fontWeight: 700, color: m.color }}>
                  {m.value}
                </div>
                <div style={{ fontSize: 11, color: sc.muted }}>{m.label}</div>
              </div>
            </div>
          )
        })}
      </div>

      {/* ── Sélecteur agents ── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 10 }}>
        {AGENTS.map(agent => {
          const Icon  = agent.icon
          const isAct = activeAgent === agent.id
          return (
            <button key={agent.id} onClick={() => setActiveAgent(agent.id)}
              style={{
                background   : isAct ? agent.color + '12' : sc.bg,
                border       : `1.5px solid ${isAct ? agent.color : sc.border}`,
                borderRadius : 10, padding: '12px 14px',
                cursor: 'pointer', textAlign: 'left',
                transition: 'all 0.18s',
              }}>
              <div style={{
                width: 34, height: 34, borderRadius: 8, marginBottom: 8,
                background: agent.color + '18',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <Icon size={16} color={agent.color} />
              </div>
              <div style={{ fontSize: 12, fontWeight: 600,
                            color: sc.text, marginBottom: 3 }}>
                {agent.name}
              </div>
              <div style={{ fontSize: 10, color: sc.muted, lineHeight: 1.4 }}>
                {agent.desc}
              </div>
            </button>
          )
        })}
      </div>

      {/* ── Corps principal ── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 14 }}>

        {/* Chatbot (gauche) */}
        <div style={{
          background: sc.bg, borderRadius: 12,
          border: `1px solid ${sc.border}`,
          display: 'flex', flexDirection: 'column',
          height: 520, overflow: 'hidden',
        }}>
          {/* Header chatbot */}
          <div style={{
            padding: '12px 16px', borderBottom: `1px solid ${sc.border}`,
            display: 'flex', alignItems: 'center', gap: 10,
            background: sc.bg2,
          }}>
            <div style={{
              width: 32, height: 32, borderRadius: 8,
              background: sc.orange + '18',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <Bot size={16} color={sc.orange} />
            </div>
            <div>
              <div style={{ fontSize: 13, fontWeight: 600 }}>ILAM Chat Agent</div>
              <div style={{ fontSize: 10, color: sc.muted }}>
                Langage naturel → SQL Trino → Résultats
              </div>
            </div>
            <div style={{ marginLeft: 'auto', display: 'flex',
                          alignItems: 'center', gap: 5 }}>
              <div style={{
                width: 7, height: 7, borderRadius: '50%',
                background: '#10B981',
              }}/>
              <span style={{ fontSize: 10, color: '#10B981' }}>Actif</span>
            </div>
          </div>

          {/* Zone messages */}
          <div style={{
            flex: 1, overflowY: 'auto', padding: 14,
            display: 'flex', flexDirection: 'column', gap: 10,
          }}>
            {messages.length === 0 ? (
              <div style={{
                flex: 1, display: 'flex', flexDirection: 'column',
                alignItems: 'center', justifyContent: 'center',
                gap: 10, color: sc.muted,
              }}>
                <Bot size={36} color={sc.orange} strokeWidth={1.5} />
                <div style={{ fontSize: 13, fontWeight: 600, color: sc.text }}>
                  Posez une question sur vos données
                </div>
                <div style={{ fontSize: 11, textAlign: 'center',
                              maxWidth: 320, lineHeight: 1.6 }}>
                  Exemples : revenus par région, taux de churn,
                  transactions Orange Money, qualité réseau…
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap',
                              gap: 6, justifyContent: 'center', marginTop: 6 }}>
                  {SUGGESTIONS.map((s, i) => (
                    <button key={i} onClick={() => setQuestion(s)} style={{
                      background: sc.bg2, border: `1px solid ${sc.border}`,
                      borderRadius: 20, padding: '4px 11px',
                      fontSize: 10, cursor: 'pointer', color: sc.muted,
                    }}>{s}</button>
                  ))}
                </div>
              </div>
            ) : (
              <>
                {messages.map((msg, i) => (
                  <div key={i} style={{
                    display: 'flex',
                    justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start',
                  }}>
                    <div style={{
                      maxWidth: '82%',
                      background : msg.role === 'user' ? sc.orange : sc.bg2,
                      color      : msg.role === 'user' ? 'white' : sc.text,
                      borderRadius: msg.role === 'user'
                        ? '14px 14px 3px 14px' : '14px 14px 14px 3px',
                      padding: '9px 13px', fontSize: 12,
                      border: msg.role === 'assistant'
                        ? `1px solid ${sc.border}` : 'none',
                    }}>
                      <div style={{ lineHeight: 1.6 }}>{msg.content}</div>
                      {msg.sql && (
                        <div style={{
                          marginTop: 8, background: '#0D1117',
                          borderRadius: 6, padding: '7px 10px',
                          fontFamily: 'monospace', fontSize: 10,
                          color: '#79C0FF', whiteSpace: 'pre-wrap',
                          borderLeft: '3px solid #FF6600',
                        }}>
                          {msg.sql}
                        </div>
                      )}
                      {msg.rows?.length > 0 && (
                        <div style={{ marginTop: 8, overflowX: 'auto' }}>
                          <table style={{ fontSize: 10, width: '100%',
                                          borderCollapse: 'collapse' }}>
                            <thead>
                              <tr>{msg.columns.map((c: string) => (
                                <th key={c} style={{
                                  padding: '3px 7px',
                                  background: sc.orange + '20',
                                  borderBottom: `1px solid ${sc.border}`,
                                  textAlign: 'left', fontWeight: 600,
                                }}>{c}</th>
                              ))}</tr>
                            </thead>
                            <tbody>
                              {msg.rows.slice(0,5).map((row: any, ri: number) => (
                                <tr key={ri}>
                                  {msg.columns.map((c: string, ci: number) => (
                                    <td key={ci} style={{
                                      padding: '3px 7px',
                                      borderBottom: `1px solid ${sc.border}`,
                                    }}>{String(row[c] ?? row[ci] ?? '')}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
                {loading && (
                  <div style={{ display: 'flex', alignItems: 'center',
                                gap: 7, color: sc.muted, fontSize: 11 }}>
                    <div className="spinner" style={{ width: 13, height: 13 }}/>
                    Analyse en cours…
                  </div>
                )}
                <div ref={messagesEndRef} />
              </>
            )}
          </div>

          {/* Input */}
          <div style={{
            padding: '10px 12px', borderTop: `1px solid ${sc.border}`,
            display: 'flex', gap: 8, background: sc.bg2,
          }}>
            <input
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSend()}
              placeholder="Ex: Quelles sont les 3 régions avec le plus de revenus ?"
              style={{
                flex: 1, border: `1px solid ${sc.border}`,
                borderRadius: 8, padding: '7px 12px',
                fontSize: 12, outline: 'none',
                background: sc.bg, color: sc.text,
              }}
            />
            <button onClick={handleSend} disabled={!question.trim() || loading}
              style={{
                background: question.trim() ? sc.orange : sc.border,
                border: 'none', borderRadius: 8,
                padding: '8px 14px', cursor: 'pointer',
                transition: 'background 0.2s',
              }}>
              <Send size={14} color="white" />
            </button>
          </div>
        </div>

        {/* Panneau droit */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

          {/* Détail agent sélectionné */}
          <div style={{
            background: sc.bg, borderRadius: 10,
            border: `1px solid ${sc.border}`, padding: 14,
          }}>
            <div style={{ display: 'flex', alignItems: 'center',
                          gap: 8, marginBottom: 10 }}>
              <div style={{
                width: 28, height: 28, borderRadius: 6,
                background: selectedAgent.color + '18',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <selectedAgent.icon size={14} color={selectedAgent.color} />
              </div>
              <div style={{ fontSize: 12, fontWeight: 600 }}>
                {selectedAgent.name}
              </div>
            </div>
            <p style={{ fontSize: 11, color: sc.muted,
                        lineHeight: 1.6, marginBottom: 12 }}>
              {selectedAgent.desc}
            </p>
            {selectedAgent.steps.map((step, i) => (
              <div key={i} style={{
                display: 'flex', gap: 7,
                alignItems: 'flex-start', marginBottom: 7,
              }}>
                <div style={{
                  width: 17, height: 17, borderRadius: '50%', flexShrink: 0,
                  background: selectedAgent.color + '18',
                  color: selectedAgent.color,
                  display: 'flex', alignItems: 'center',
                  justifyContent: 'center', fontSize: 9, fontWeight: 700,
                }}>{i + 1}</div>
                <div style={{ fontSize: 10, color: sc.muted,
                              lineHeight: 1.5 }}>{step}</div>
              </div>
            ))}
          </div>

          {/* Journal des actions récentes */}
          <div style={{
            background: sc.bg, borderRadius: 10,
            border: `1px solid ${sc.border}`, padding: 14,
            flex: 1,
          }}>
            <div style={{ fontSize: 12, fontWeight: 600,
                          marginBottom: 10, display: 'flex',
                          alignItems: 'center', gap: 6 }}>
              <BarChart2 size={13} color={sc.teal} />
              Journal des actions
            </div>
            {RECENT_ACTIONS.map((a, i) => (
              <div key={i} style={{
                padding: '7px 0',
                borderBottom: i < RECENT_ACTIONS.length - 1
                  ? `1px solid ${sc.border}` : 'none',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between',
                              alignItems: 'center', marginBottom: 2 }}>
                  <span style={{
                    fontSize: 10, fontWeight: 600,
                    color: a.color,
                  }}>{a.agent}</span>
                  <span style={{ fontSize: 9, color: sc.muted }}>{a.time}</span>
                </div>
                <div style={{ fontSize: 10, fontWeight: 500,
                              color: sc.text, marginBottom: 1 }}>
                  {a.action}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <div style={{
                    width: 5, height: 5, borderRadius: '50%',
                    background: a.ok ? '#10B981' : '#F59E0B',
                  }}/>
                  <span style={{ fontSize: 9, color: sc.muted }}>
                    {a.result}
                  </span>
                </div>
              </div>
            ))}
          </div>

          {/* Services liés */}
          <div style={{
            background: sc.bg, borderRadius: 10,
            border: `1px solid ${sc.border}`, padding: 14,
          }}>
            <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8 }}>
              Services
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {[
                { name: 'Flink',    url: 'http://localhost:8081', icon: Cpu },
                { name: 'Airflow',  url: 'http://localhost:8082', icon: Network },
                { name: 'Trino',    url: 'http://localhost:8080', icon: Database },
                { name: 'Grafana',  url: 'http://localhost:3000', icon: BarChart2 },
              ].map(s => {
                const Icon = s.icon
                return (
                  <a key={s.name} href={s.url} target="_blank"
                    rel="noreferrer" style={{
                      display: 'flex', alignItems: 'center', gap: 5,
                      padding: '4px 10px', borderRadius: 16,
                      background: sc.bg2,
                      border: `1px solid ${sc.border}`,
                      textDecoration: 'none', fontSize: 11,
                      color: sc.text,
                    }}>
                    <Icon size={11} color={sc.orange} />
                    {s.name}
                    <ExternalLink size={9} color={sc.muted} />
                  </a>
                )
              })}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
