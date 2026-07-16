import { ExternalLink } from 'lucide-react'

interface Service {
  name : string
  url  : string
  desc?: string
  port?: string
}

interface Props {
  services : Service[]
  title?   : string
}

export default function ServiceLinks({ services, title = 'Services' }: Props) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      flexWrap: 'wrap', marginBottom: 12,
    }}>
      {title && (
        <span style={{ fontSize: 11, color: 'var(--color-text-secondary)',
                       fontWeight: 600, marginRight: 4 }}>
          {title} :
        </span>
      )}
      {services.map(s => (
        <a key={s.name} href={s.url} target="_blank" rel="noreferrer"
          style={{
            display        : 'flex', alignItems: 'center', gap: 5,
            padding        : '4px 11px', borderRadius: 16,
            background     : 'var(--color-background-secondary)',
            border         : '1px solid var(--color-border-tertiary)',
            textDecoration : 'none', fontSize: 11,
            color          : 'var(--color-text-primary)',
          }}>
          <ExternalLink size={10} color="#FF6600" />
          <span style={{ fontWeight: 500 }}>{s.name}</span>
          {s.port && (
            <span style={{ fontSize: 9,
                           color: 'var(--color-text-secondary)' }}>
              :{s.port}
            </span>
          )}
        </a>
      ))}
    </div>
  )
}
