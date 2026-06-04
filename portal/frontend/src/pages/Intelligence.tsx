import { Brain } from 'lucide-react'

export default function Intelligence() {
  return (
    <div style={{
      display        : 'flex',
      flexDirection  : 'column',
      alignItems     : 'center',
      justifyContent : 'center',
      height         : '60vh',
      gap            : 16,
      color          : 'var(--gray-400)',
    }}>
      <Brain size={48} color="var(--orange)" />
      <h2 style={{ color: 'var(--gray-700)', fontWeight: 600 }}>
        Intelligence — LangGraph
      </h2>
      <p style={{ fontSize: 13, textAlign: 'center', maxWidth: 400 }}>
        L'agent conversationnel LangGraph sera intégré ici.
        Il permettra d'interroger le Lakehouse en langage naturel
        et de générer des requêtes SQL automatiquement.
      </p>
      <span className="badge badge-warning">En développement</span>
    </div>
  )
}
