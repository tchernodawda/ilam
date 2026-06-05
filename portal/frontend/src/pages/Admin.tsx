import { Users, Database, Server } from 'lucide-react'

const users = [
  { username: 'ilam_admin', role: 'Admin',    email: 'admin@ilam.io' },
  { username: 'engineer',   role: 'Engineer', email: 'engineer@sonatel.sn' },
  { username: 'analyst',    role: 'Analyst',  email: 'analyst@sonatel.sn' },
  { username: 'viewer',     role: 'Viewer',   email: 'viewer@sonatel.sn' },
]

const roleColors: any = {
  Admin   : 'badge-error',
  Engineer: 'badge-orange',
  Analyst : 'badge-info',
  Viewer  : 'badge-warning',
}

export default function Admin() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

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
          <Users size={16} color="var(--teal)" />
          Gestion des utilisateurs
        </h3>
        <table>
          <thead>
            <tr>
              <th>Utilisateur</th>
              <th>Rôle</th>
              <th>Email</th>
              <th>Permissions</th>
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.username}>
                <td style={{ fontWeight: 500 }}>{u.username}</td>
                <td>
                  <span className={`badge ${roleColors[u.role]}`}>
                    {u.role}
                  </span>
                </td>
                <td style={{
                  color      : 'var(--teal)',
                  fontSize   : 12,
                }}>{u.email}</td>
                <td style={{ fontSize: 11, color: 'var(--gray-400)' }}>
                  {u.role === 'Admin'
                    ? 'Accès complet'
                    : u.role === 'Engineer'
                    ? 'Pipeline + Spark + Données'
                    : u.role === 'Analyst'
                    ? 'Données + Qualité + Monitoring'
                    : 'Lecture seule'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div style={{
          marginTop    : 12,
          padding      : 12,
          background   : '#FFF7ED',
          borderRadius : 6,
          fontSize     : 11,
          color        : '#C2410C',
        }}>
          En production, la gestion des utilisateurs est faite via Keycloak
          (SSO + LDAP Sonatel). Actuellement en mode local avec JWT.
        </div>
      </div>

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
          <Server size={16} color="var(--orange)" />
          Configuration de la plateforme
        </h3>
        <div style={{
          display             : 'grid',
          gridTemplateColumns : 'repeat(2, 1fr)',
          gap                 : 12,
        }}>
          {[
            { label: 'MinIO',        url: 'http://localhost:9001',
              desc: 'Stockage objet S3' },
            { label: 'Flink UI',     url: 'http://localhost:8081',
              desc: 'Monitoring streaming' },
            { label: 'Airflow',      url: 'http://localhost:8082',
              desc: 'Orchestration DAGs' },
            { label: 'Spark UI',     url: 'http://localhost:8090',
              desc: 'Cluster Spark' },
            { label: 'Prometheus',   url: 'http://localhost:9090',
              desc: 'Métriques TSDB' },
            { label: 'Grafana',      url: 'http://localhost:3000',
              desc: 'Dashboards monitoring' },
            { label: 'NiFi',         url: 'https://localhost:8443',
              desc: 'Ingestion données' },
            { label: 'Trino',        url: 'http://localhost:8080',
              desc: 'Moteur SQL MPP' },
          ].map(s => (
            <a
              key={s.label}
              href={s.url}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                display      : 'flex',
                alignItems   : 'center',
                gap          : 10,
                padding      : '10px 14px',
                border       : '1px solid var(--gray-200)',
                borderRadius : 6,
                textDecoration: 'none',
                transition   : 'all 0.15s',
              }}
              onMouseEnter={e =>
                (e.currentTarget.style.borderColor = 'var(--orange)')}
              onMouseLeave={e =>
                (e.currentTarget.style.borderColor = 'var(--gray-200)')}
            >
              <Database size={16} color="var(--teal)" />
              <div>
                <div style={{
                  fontSize   : 13,
                  fontWeight : 500,
                  color      : 'var(--gray-700)',
                }}>{s.label}</div>
                <div style={{
                  fontSize : 11,
                  color    : 'var(--gray-400)',
                }}>{s.desc}</div>
              </div>
              <div style={{
                marginLeft : 'auto',
                fontSize   : 10,
                color      : 'var(--teal)',
              }}>{s.url}</div>
            </a>
          ))}
        </div>
      </div>
    </div>
  )
}
