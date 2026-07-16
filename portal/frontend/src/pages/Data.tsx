import { useEffect, useState } from 'react'
import ServiceLinks from '../components/shared/ServiceLinks'
import { getAllTables, previewTable, runQuery } from '../api/services'
import { Database, Search, Play, Table, ChevronRight, GitBranch } from 'lucide-react'
import Editor from '@monaco-editor/react'

export default function Data() {
  const [tables,      setTables]      = useState<any[]>([])
  const [selected,    setSelected]    = useState<any>(null)
  const [preview,     setPreview]     = useState<any>(null)
  const [sql,         setSql]         = useState('SELECT *\nFROM iceberg.bronze.cdr_voice\nLIMIT 20')
  const [queryResult, setQueryResult] = useState<any>(null)
  const [loading,     setLoading]     = useState(false)
  const [activeTab,   setActiveTab]   = useState<'browse'|'query'|'lineage'>('browse')
  const [search,      setSearch]      = useState('')
  const [schema,      setSchema]      = useState('all')

  useEffect(() => { getAllTables().then(r => setTables(r.data)) }, [])

  const handleSelectTable = async (t: any) => {
    setSelected(t)
    setLoading(true)
    try {
      const r = await previewTable(t.schema_name, t.table_name, 50)
      setPreview(r.data)
    } finally { setLoading(false) }
  }

  const handleRunQuery = async () => {
    setLoading(true)
    setQueryResult(null)
    try {
      const r = await runQuery(sql, 100)
      setQueryResult(r.data)
    } finally { setLoading(false) }
  }

  const filtered = tables.filter(t => {
    const matchSearch = t.table_name.includes(search.toLowerCase()) ||
                        t.schema_name.includes(search.toLowerCase())
    const matchSchema = schema === 'all' || t.schema_name === schema
    return matchSearch && matchSchema
  })

  const schemaColors: Record<string, string> = {
    bronze: '#F59E0B', silver: '#6B7280', gold: '#FF6600',
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
      <ServiceLinks title="Services" services={[
        { name: 'Trino UI',  url: 'http://localhost:8080',  port: '8080' },
        { name: 'MinIO',     url: 'http://localhost:9001',  port: '9001' },
        { name: 'dbt docs',  url: 'http://localhost:8086',  port: '8086' },
        { name: 'Iceberg',   url: 'http://localhost:8181',  port: '8181' },
      ]} />
      <div style={{ display: 'flex', gap: 20, height: 'calc(100vh - 160px)' }}>
      {/* Sidebar tables — masquée en mode lineage */}
      <div style={{
        width: activeTab === 'lineage' ? 0 : 260,
        minWidth: activeTab === 'lineage' ? 0 : 260,
        background: 'white', borderRadius: 8,
        border: activeTab === 'lineage'
          ? 'none' : '1px solid var(--gray-200)',
        display: activeTab === 'lineage' ? 'none' : 'flex',
        flexDirection: 'column', overflow: 'hidden',
        transition: 'all 0.25s ease',
      }}>
        <div style={{ padding: '12px', borderBottom: '1px solid var(--gray-200)' }}>
          <div style={{ position: 'relative', marginBottom: 8 }}>
            <Search size={14} style={{
              position: 'absolute', left: 8,
              top: '50%', transform: 'translateY(-50%)',
              color: 'var(--gray-400)',
            }} />
            <input
              placeholder="Rechercher..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              style={{
                width: '100%', padding: '6px 8px 6px 28px',
                border: '1px solid var(--gray-200)', borderRadius: 6,
                fontSize: 12, outline: 'none',
              }}
            />
          </div>
          <div style={{ display: 'flex', gap: 4 }}>
            {['all','bronze','silver','gold'].map(s => (
              <button key={s} onClick={() => setSchema(s)} style={{
                padding: '3px 8px', borderRadius: 12, border: 'none',
                cursor: 'pointer', fontSize: 10, fontWeight: 500,
                background: schema === s ? 'var(--orange)' : 'var(--gray-100)',
                color: schema === s ? 'white' : 'var(--gray-600)',
                textTransform: 'capitalize',
              }}>{s}</button>
            ))}
          </div>
        </div>
        <div style={{ flex: 1, overflowY: 'auto', padding: '8px 4px' }}>
          {filtered.map(t => (
            <button
              key={`${t.schema_name}.${t.table_name}`}
              onClick={() => handleSelectTable(t)}
              style={{
                width: '100%', display: 'flex', alignItems: 'center',
                gap: 8, padding: '8px 10px', borderRadius: 6,
                border: 'none', cursor: 'pointer', textAlign: 'left',
                background: selected?.table_name === t.table_name &&
                            selected?.schema_name === t.schema_name
                  ? '#FFF7ED' : 'transparent',
              }}
            >
              <div style={{
                width: 6, height: 6, borderRadius: '50%', flexShrink: 0,
                background: schemaColors[t.schema_name] || '#6B7280',
              }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{
                  fontSize: 12, fontWeight: 500, color: 'var(--gray-700)',
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>{t.table_name}</div>
                <div style={{ fontSize: 10, color: 'var(--gray-400)' }}>
                  {t.row_count.toLocaleString('fr-FR')} lignes
                </div>
              </div>
              <ChevronRight size={12} color="var(--gray-300)" />
            </button>
          ))}
        </div>
      </div>

      {/* Main content */}
      <div style={{
        flex: 1, display: 'flex', flexDirection: 'column',
        gap: 0, overflow: 'hidden',
      }}>
        {/* Tabs */}
        <div style={{
          display: 'flex', background: 'white',
          borderRadius: '8px 8px 0 0',
          border: '1px solid var(--gray-200)', borderBottom: 'none',
        }}>
          {[
            { id: 'browse',  label: 'Explorateur',  icon: Table },
            { id: 'query',   label: 'Éditeur SQL',  icon: Play },
            { id: 'lineage', label: 'Lignée dbt',   icon: GitBranch },
          ].map(tab => (
            <button key={tab.id} onClick={() => setActiveTab(tab.id as any)} style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '10px 20px', border: 'none', background: 'transparent',
              cursor: 'pointer', fontSize: 13,
              borderBottom: activeTab === tab.id
                ? '2px solid var(--orange)' : '2px solid transparent',
              color: activeTab === tab.id ? 'var(--orange)' : 'var(--gray-500)',
              fontWeight: activeTab === tab.id ? 600 : 400,
            }}>
              <tab.icon size={14} />
              {tab.label}
            </button>
          ))}
        </div>

        <div style={{
          flex: 1, background: 'white',
          border: '1px solid var(--gray-200)',
          borderRadius: '0 0 8px 8px',
          overflow: 'hidden', display: 'flex', flexDirection: 'column',
        }}>
          {activeTab === 'browse' && (
            <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
              {!selected ? (
                <div style={{
                  display: 'flex', flexDirection: 'column',
                  alignItems: 'center', justifyContent: 'center',
                  height: '100%', color: 'var(--gray-400)', gap: 8,
                }}>
                  <Database size={32} />
                  <div>Sélectionnez une table pour explorer ses données</div>
                </div>
              ) : loading ? (
                <div className="loading-container">
                  <div className="spinner" /> Chargement...
                </div>
              ) : preview ? (
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                    <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--gray-800)' }}>
                      {selected.schema_name}.{selected.table_name}
                    </span>
                    <span className="badge badge-orange">
                      {preview.total_rows.toLocaleString('fr-FR')} lignes
                    </span>
                  </div>
                  <div style={{ overflowX: 'auto' }}>
                    <table>
                      <thead>
                        <tr>{preview.columns.map((col: string) => <th key={col}>{col}</th>)}</tr>
                      </thead>
                      <tbody>
                        {preview.rows.map((row: any[], i: number) => (
                          <tr key={i}>
                            {row.map((cell, j) => (
                              <td key={j} style={{
                                maxWidth: 200, overflow: 'hidden',
                                textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                              }}>
                                {cell ?? <span style={{ color: 'var(--gray-300)', fontStyle: 'italic' }}>null</span>}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
            </div>
          )}

          {activeTab === 'lineage' && (
            <iframe
              src="http://localhost:8086"
              style={{
                width   : '100%',
                height  : 'calc(100vh - 170px)',
                border  : 'none',
                display : 'block',
              }}
              title="dbt Lineage"
            />
          )}

          {activeTab === 'query' && (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
              <div style={{ height: 200, borderBottom: '1px solid var(--gray-200)' }}>
                <Editor
                  height="100%"
                  language="sql"
                  value={sql}
                  onChange={v => setSql(v || '')}
                  theme="vs"
                  options={{
                    minimap: { enabled: false }, fontSize: 13,
                    lineNumbers: 'on', scrollBeyondLastLine: false,
                    padding: { top: 8 },
                  }}
                />
              </div>
              <div style={{
                padding: '8px 12px', borderBottom: '1px solid var(--gray-200)',
                display: 'flex', alignItems: 'center', gap: 8,
              }}>
                <button onClick={handleRunQuery} disabled={loading}
                  className="btn btn-primary" style={{ padding: '6px 14px', fontSize: 12 }}>
                  <Play size={12} />
                  {loading ? 'Exécution...' : 'Exécuter'}
                </button>
                {queryResult && (
                  <span style={{ fontSize: 11, color: 'var(--gray-400)' }}>
                    {queryResult.row_count} lignes en {queryResult.duration_ms}ms
                  </span>
                )}
                {queryResult?.error && (
                  <span style={{ fontSize: 11, color: 'var(--error)' }}>
                    {queryResult.error}
                  </span>
                )}
              </div>
              <div style={{ flex: 1, overflow: 'auto', padding: 12 }}>
                {queryResult && !queryResult.error && (
                  <table>
                    <thead>
                      <tr>{queryResult.columns.map((c: string) => <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                      {queryResult.rows.map((row: any[], i: number) => (
                        <tr key={i}>{row.map((cell, j) => <td key={j}>{cell ?? 'null'}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
    </div>
  )
}
