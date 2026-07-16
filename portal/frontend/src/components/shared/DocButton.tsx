import { useState } from 'react'
import { Info, X } from 'lucide-react'

interface Step {
  label : string
  desc  : string
}

interface Props {
  label        : string
  icon?        : React.ReactNode
  onClick?     : () => void
  disabled?    : boolean
  implemented? : boolean
  steps        : Step[]
  color?       : string
}

export default function DocButton({
  label, icon, onClick, disabled = false,
  implemented = true, steps, color = '#FF6600'
}: Props) {
  const [showDoc, setShowDoc] = useState(false)

  return (
    <div style={{ position: 'relative', display: 'inline-flex',
                  alignItems: 'center', gap: 4 }}>
      <button
        onClick={implemented ? onClick : undefined}
        disabled={disabled}
        style={{
          display    : 'flex', alignItems: 'center', gap: 6,
          padding    : '7px 14px', borderRadius: 8, border: 'none',
          background : implemented ? color : 'var(--color-background-secondary)',
          color      : implemented ? 'white'
                     : 'var(--color-text-secondary)',
          cursor     : implemented && !disabled ? 'pointer' : 'not-allowed',
          fontSize   : 12, fontWeight: 500, opacity: disabled ? 0.6 : 1,
        }}>
        {icon}
        {label}
        {!implemented && (
          <span style={{
            fontSize: 9, background: '#F59E0B', color: 'white',
            padding: '1px 5px', borderRadius: 10, marginLeft: 2,
          }}>PRÉVU</span>
        )}
      </button>

      <button onClick={() => setShowDoc(!showDoc)} style={{
        background: 'transparent', border: 'none',
        cursor: 'pointer', padding: 2,
        color: 'var(--color-text-secondary)',
      }} title="Comment ça fonctionne ?">
        <Info size={13} />
      </button>

      {showDoc && (
        <div style={{
          position: 'absolute', top: 36, left: 0, zIndex: 200,
          background  : 'var(--color-background-primary)',
          border      : '1px solid var(--color-border-tertiary)',
          borderRadius: 10, padding: 14, width: 290,
          boxShadow   : '0 4px 24px rgba(0,0,0,0.13)',
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between',
                        alignItems: 'center', marginBottom: 10 }}>
            <div style={{ fontSize: 12, fontWeight: 600 }}>
              {label} — fonctionnement
            </div>
            <button onClick={() => setShowDoc(false)} style={{
              background: 'none', border: 'none',
              cursor: 'pointer', padding: 0,
            }}>
              <X size={14} />
            </button>
          </div>

          {!implemented && (
            <div style={{
              padding: '6px 8px', background: '#FEF3C7',
              borderRadius: 6, marginBottom: 10, fontSize: 11,
              color: '#92400E', borderLeft: '3px solid #F59E0B',
            }}>
              Bouton non encore implémenté — nécessite des
              ressources RAM supplémentaires.
            </div>
          )}

          {steps.map((step, i) => (
            <div key={i} style={{
              display: 'flex', gap: 8, marginBottom: 8,
              alignItems: 'flex-start',
            }}>
              <div style={{
                width: 18, height: 18, borderRadius: '50%', flexShrink: 0,
                background: color + '20', color,
                display: 'flex', alignItems: 'center',
                justifyContent: 'center', fontSize: 9, fontWeight: 700,
              }}>{i + 1}</div>
              <div>
                <div style={{ fontSize: 11, fontWeight: 600 }}>
                  {step.label}
                </div>
                <div style={{ fontSize: 10, lineHeight: 1.4,
                              color: 'var(--color-text-secondary)' }}>
                  {step.desc}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
