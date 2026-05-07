import { useState } from 'react'

export function Toggle({ label, defaultOn = true, onChange }) {
  const [on, setOn] = useState(defaultOn)
  const handle = () => {
    setOn(current => {
      const next = !current
      onChange?.(next)
      return next
    })
  }

  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={on}
      onClick={handle}
      className={`flex w-full items-center justify-between gap-4 rounded-2xl border p-4 text-left shadow-sm transition-all focus:outline-none focus-visible:ring-2 focus-visible:ring-cyan-400 focus-visible:ring-offset-2 focus-visible:ring-offset-[#091723] ${
        on
          ? 'border-cyan-400/30 bg-cyan-400/[0.08]'
          : 'border-white/10 bg-white/[0.02] hover:bg-white/[0.04]'
      }`}
    >
      <span className="flex items-center gap-3 min-w-0">
        <span
          className={`flex h-10 w-10 items-center justify-center rounded-xl text-base transition-colors ${
            on ? 'bg-cyan-400/14 text-cyan-300' : 'bg-white/[0.06] text-white/45'
          }`}
          aria-hidden="true"
        >
          {on ? '✓' : '○'}
        </span>
        <span className="min-w-0">
          <span className={`block font-semibold transition-colors ${on ? 'text-white' : 'text-white/80'}`}>
            {label}
          </span>
          <span className="block text-sm text-white/40">
            {on ? 'Option activée' : 'Option désactivée'}
          </span>
        </span>
      </span>

      <span
        aria-hidden="true"
        className={`flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-full text-sm font-bold transition-colors ${
          on ? 'bg-cyan-400 text-[#07111A]' : 'bg-white/10 text-transparent'
        }`}
      >
        ✓
      </span>
    </button>
  )
}

export function SelectField({ label, items, hint, value, onChange }) {
  return (
    <div>
      {label && <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">{label}</label>}
      <div className="flex gap-2">
        <select
          value={value}
          onChange={e => onChange?.(e.target.value)}
          className="flex-1 bg-white/5 border border-white/14 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-600/60 focus:bg-white/8 transition-colors cursor-pointer"
        >
          {items.map(i => <option key={i} className="bg-[#0c1e2e]">{i}</option>)}
        </select>
        <button
          type="button"
          className="w-11 flex items-center justify-center rounded-xl border border-white/14 bg-white/5 hover:bg-white/10 text-white/40 hover:text-cyan-500 transition-colors text-base cursor-pointer"
        >↺</button>
      </div>
      {hint && <p className="text-xs text-white/35 mt-1.5">{hint}</p>}
    </div>
  )
}

export function Btn({ children, onClick, variant = 'primary', disabled = false, full = false, className = '' }) {
  const base = `${full ? 'w-full' : ''} py-3 px-5 rounded-xl text-sm font-semibold transition-all cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${className}`
  const variants = {
    primary: 'bg-gradient-to-r from-cyan-700 to-sky-800 text-white hover:from-cyan-600 hover:to-sky-700 hover:-translate-y-px hover:shadow-lg hover:shadow-cyan-900/30 active:translate-y-0',
    secondary: 'bg-white/[0.04] border border-white/[0.18] text-white/80 hover:bg-white/[0.09] hover:border-white/30 hover:text-white',
    action: 'bg-sky-500/[0.08] border border-sky-400/[0.22] text-sky-300/90 hover:bg-sky-500/[0.15] hover:border-sky-400/35 hover:text-sky-200',
    ghost: 'text-white/50 hover:text-white hover:bg-white/5',
    danger: 'bg-red-500/10 border border-red-500/20 text-red-400 hover:bg-red-500/20',
  }
  return (
    <button className={`${base} ${variants[variant]}`} onClick={onClick} disabled={disabled}>
      {children}
    </button>
  )
}

const statusConfig = {
  idle:    { dot: 'bg-white/20',    text: 'text-white/35',  label: 'En attente' },
  running: { dot: 'bg-amber-400 animate-pulse', text: 'text-amber-400', label: 'En cours...' },
  done:    { dot: 'bg-emerald-400', text: 'text-emerald-400', label: 'Terminé' },
  cached:  { dot: 'bg-amber-400/60', text: 'text-amber-300/80', label: 'En cache' },
  error:   { dot: 'bg-red-400',     text: 'text-red-400',   label: 'Erreur' },
}

export function StatusDot({ status = 'idle' }) {
  const c = statusConfig[status] ?? statusConfig.idle
  return <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${c.dot}`} />
}

export function StatusBadge({ status = 'idle' }) {
  const c = statusConfig[status] ?? statusConfig.idle
  return (
    <span className={`flex items-center gap-1.5 text-xs font-medium ${c.text}`}>
      <StatusDot status={status} />
      {c.label}
    </span>
  )
}

export function Card({ children, className = '', accent = false }) {
  return (
    <div className={`rounded-2xl border ${accent ? 'border-cyan-600/28 bg-cyan-700/[0.05]' : 'border-white/12 bg-white/[0.02]'} p-5 ${className}`}>
      {children}
    </div>
  )
}

export function PageHeader({ title, description, status, action }) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <h1 className="text-xl font-semibold text-white tracking-tight">{title}</h1>
        {description && <p className="text-sm text-white/45 mt-1">{description}</p>}
      </div>
      <div className="flex items-center gap-3 flex-shrink-0">
        {status && <StatusBadge status={status} />}
        {action}
      </div>
    </div>
  )
}

export function SectionTitle({ children }) {
  return (
    <div className="flex items-center gap-3 mb-4">
      <div className="h-px flex-1 bg-white/[0.1]" />
      <span className="text-xs text-white/30 uppercase tracking-widest font-semibold">{children}</span>
      <div className="h-px flex-1 bg-white/[0.1]" />
    </div>
  )
}

export function Spinner() {
  return (
    <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
    </svg>
  )
}

export function ResultRow({ icon, label, value, ok = true }) {
  return (
    <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${ok ? 'border-emerald-400/15 bg-emerald-400/[0.04]' : 'border-red-400/15 bg-red-400/[0.04]'}`}>
      <span className="text-base">{icon}</span>
      <span className={`text-sm font-medium ${ok ? 'text-emerald-300' : 'text-red-300'}`}>{label}</span>
      <span className="text-xs text-white/40 ml-auto">{value}</span>
    </div>
  )
}
