import { NavLink, useNavigate, Outlet } from 'react-router-dom'
import { useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Spinner, StatusDot } from './ui'
import raizersLogoUrl from '../../../assets/raizers_logo.png'

const NAV = [
  {
    group: 'Audit',
    items: [
      { to: '/audit/operation',  label: 'Opération',       tab: 'operation'  },
      { to: '/audit/patrimoine', label: 'Pappers',         tab: 'patrimoine' },
      { to: '/audit/financier',  label: 'Financier',       tab: 'financier'  },
    ],
  },
  {
    group: 'Outils',
    items: [
      { to: '/immo',     label: 'Comparateur - DVF', tab: null },
      { to: '/scraping', label: 'Scraping',    tab: null },
    ],
  },
]

const AUDIT_TABS = ['operation', 'financier', 'patrimoine']

export default function Layout() {
  const { session, dispatch } = useSession()
  const navigate = useNavigate()
  const [isExtractingAll, setIsExtractingAll] = useState(false)
  const [isReingesting, setIsReingesting] = useState(false)

  const shortName = session.project
    ? session.project.replace(/^\d+\.\s*/, '').trim()
    : '—'

  async function extractAll() {
    if (isExtractingAll) return

    setIsExtractingAll(true)
    for (const tab of AUDIT_TABS) {
      dispatch({ type: 'TAB_START', tab })
      await new Promise(resolve => setTimeout(resolve, 900))
      dispatch({ type: 'TAB_DONE', tab })
    }
    setIsExtractingAll(false)
  }

  async function reingestPipeline() {
    if (isReingesting) return

    setIsReingesting(true)
    dispatch({ type: 'PIPELINE_START' })
    await new Promise(resolve => setTimeout(resolve, 1400))
    dispatch({ type: 'PIPELINE_DONE', stats: session.pipelineStats })
    setIsReingesting(false)
  }

  return (
    <div className="flex h-screen bg-[#0d1d2a] text-white overflow-hidden">

      {/* Sidebar */}
      <aside className="w-56 flex-shrink-0 flex flex-col border-r border-white/[0.14] bg-[#0b1823]">

        {/* Logo */}
        <div className="px-5 py-5 border-b border-white/[0.14]">
          <img
            src={raizersLogoUrl}
            alt="Raizers"
            className="h-[4.05rem] w-auto object-contain invert"
          />
          <div className="text-xs text-white/35 mt-2 truncate">{shortName}</div>
        </div>

        {/* Nav */}
        <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-5">
          {NAV.map(({ group, items }) => (
            <div key={group}>
              <div className="flex items-center justify-between gap-2 px-2 mb-1.5">
                <div className="text-[10px] text-white/25 uppercase tracking-widest font-semibold">{group}</div>
                {group === 'Audit' && (
                  <Btn
                    variant="primary"
                    onClick={extractAll}
                    disabled={isExtractingAll}
                    className="px-2.5 py-1.5 text-[10px] font-semibold tracking-wide"
                  >
                    {isExtractingAll ? (
                      <span className="flex items-center justify-center gap-1.5">
                        <Spinner />
                        Extraire...
                      </span>
                    ) : (
                      'Tout extraire'
                    )}
                  </Btn>
                )}
              </div>
              <div className="space-y-0.5">
                {items.map(({ to, label, tab }) => (
                  <NavLink
                    key={to}
                    to={to}
                    className={({ isActive }) =>
                      `flex items-center justify-between px-3 py-2.5 rounded-xl text-sm transition-all ${
                        isActive
                          ? 'bg-white/8 text-white font-medium'
                          : 'text-white/50 hover:text-white/80 hover:bg-white/[0.04]'
                      }`
                    }
                  >
                    <span>{label}</span>
                    {tab && <StatusDot status={session.tabs[tab] ?? 'idle'} />}
                  </NavLink>
                ))}
              </div>
            </div>
          ))}
        </nav>

        {/* Export + reset */}
        <div className="px-3 py-4 border-t border-white/[0.14] space-y-1">
          <NavLink
            to="/export"
            className={({ isActive }) =>
              `flex items-center gap-2.5 px-3 py-2.5 rounded-xl text-sm transition-all ${
                isActive ? 'bg-cyan-700/12 text-cyan-400' : 'text-white/50 hover:text-white/80 hover:bg-white/[0.04]'
              }`
            }
          >
            <span className="text-base">↓</span>
            <span>Exporter</span>
            {session.generated.length > 0 && (
              <span className="ml-auto text-xs bg-cyan-700/20 text-cyan-400 rounded-full px-1.5 py-0.5">
                {session.generated.length}
              </span>
            )}
          </NavLink>
          <button
            onClick={() => { dispatch({ type: 'RESET' }); navigate('/') }}
            className="w-full flex items-center gap-2.5 px-3 py-2.5 rounded-xl text-sm text-white/30 hover:text-white/60 hover:bg-white/[0.04] transition-all cursor-pointer"
          >
            <span className="text-base">⟵</span>
            <span>Nouveau dossier</span>
          </button>
        </div>
      </aside>

      {/* Content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="flex-shrink-0 flex items-center justify-between px-8 py-4 border-b border-white/[0.14] bg-white/[0.02]">
          <div className="flex items-center gap-3">
            <span className="text-xs text-white/30 uppercase tracking-widest">Dossier actif</span>
            <span className="text-sm text-white font-medium">{session.project ?? '—'}</span>
            {session.subfolder && (
              <>
                <span className="text-white/20">/</span>
                <span className="text-sm text-white/50">{session.subfolder}</span>
              </>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Btn
              variant="secondary"
              onClick={reingestPipeline}
              disabled={isReingesting}
              className="px-3 py-2 text-xs"
            >
              {isReingesting ? (
                <span className="flex items-center gap-2">
                  <Spinner />
                  Réingestion...
                </span>
              ) : (
                'Réingérer'
              )}
            </Btn>
            <span className={`text-xs font-medium px-2.5 py-1 rounded-full border ${
              session.pipeline === 'done'
                ? 'border-emerald-400/20 bg-emerald-400/10 text-emerald-300'
                : 'border-white/14 bg-white/5 text-white/40'
            }`}>
              Pipeline {session.pipeline === 'done' ? '✓' : '—'}
            </span>
          </div>
        </header>

        {/* Page content */}
        <div className="flex-1 overflow-y-auto px-8 py-8">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
