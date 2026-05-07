import { NavLink, useNavigate, Outlet } from 'react-router-dom'
import { useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Spinner, StatusDot } from './ui'
import { getAuditJob, getFinancialResults, refreshAuditPipeline, startFinancialExtract, startOperationExtract, startPatrimoineExtract } from '../lib/api'
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
  const [isReingesting, setIsReingesting] = useState(false)
  const [refreshError, setRefreshError] = useState('')
  const [isExtractingAll, setIsExtractingAll] = useState(false)
  const [extractAllError, setExtractAllError] = useState('')
const refreshSummary = session.lastRefresh?.manifest_diff?.summary ?? null
  const refreshFingerprints = session.lastRefresh?.fingerprints ?? null
  const refreshTargetLabel = session.subfolder
    ? `Opérateur + ${session.subfolder}`
    : 'Opérateur'

  const shortName = session.project
    ? session.project.replace(/^\d+\.\s*/, '').trim()
    : '—'

  async function reingestPipeline() {
    if (isReingesting || !session.projectId || !session.projectPath) return

    setIsReingesting(true)
    setRefreshError('')
    try {
      const job = await refreshAuditPipeline({
        project_id: session.projectId,
        project_path: session.projectPath,
        audit_folder: session.subfolder || null,
      })

      let done = false
      while (!done) {
        await new Promise(resolve => setTimeout(resolve, 1500))
        const nextJob = await getAuditJob(job.job_id)
        if (nextJob.status === 'done') {
          dispatch({ type: 'PIPELINE_DONE', stats: nextJob.result?.stats || null })
          dispatch({ type: 'SET_PROJECT_CATALOG', projectCatalog: nextJob.result?.catalog || null })
          dispatch({ type: 'SET_LAST_REFRESH', lastRefresh: nextJob.result || null })
          done = true
        } else if (nextJob.status === 'error') {
          setRefreshError(nextJob.error || 'Erreur de refresh Dropbox')
          done = true
        }
      }
    } catch (err) {
      setRefreshError(err.message || 'Erreur de refresh Dropbox')
    } finally {
      setIsReingesting(false)
    }
  }

  async function extractAll() {
    if (isExtractingAll || !session.projectId) return
    setIsExtractingAll(true)
    setExtractAllError('')
    try {
      const financialData = await getFinancialResults(session.projectId)
      const companies = financialData.companies || []
      const selections = companies
        .map(company => {
          const periods = Object.entries(company.filesByPeriod || {})
          return {
            company_id: company.id,
            period: periods[0]?.[0] ?? null,
            file_id: periods[0]?.[1]?.[0]?.id ?? null,
          }
        })
        .filter(s => s.period && s.file_id)

      const people = (session.projectCatalog?.people || []).filter(p => p.selected !== false)

      dispatch({ type: 'TAB_START', tab: 'operation' })
      dispatch({ type: 'TAB_START', tab: 'financier' })
      dispatch({ type: 'TAB_START', tab: 'patrimoine' })

      const [opJob, finJob, patJob] = await Promise.all([
        startOperationExtract({ project_id: session.projectId, include_operateur: true, include_patrimoine: true, include_lots: true }),
        startFinancialExtract({ project_id: session.projectId, selections }),
        startPatrimoineExtract({ project_id: session.projectId, people }),
      ])

      dispatch({ type: 'SET_JOB_ID', tab: 'operation', jobId: opJob.job_id })
      dispatch({ type: 'SET_JOB_ID', tab: 'financier', jobId: finJob.job_id })
      dispatch({ type: 'SET_JOB_ID', tab: 'patrimoine', jobId: patJob.job_id })
    } catch (err) {
      setExtractAllError(err.message || 'Erreur lors du lancement')
      ;['operation', 'financier', 'patrimoine'].forEach(tab => {
        if (session.tabs[tab] === 'running') dispatch({ type: 'TAB_ERROR', tab })
      })
    } finally {
      setIsExtractingAll(false)
    }
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
              <div className="px-2 mb-1.5 flex items-center justify-between">
                <div className="text-[10px] text-white/25 uppercase tracking-widest font-semibold">{group}</div>
                {group === 'Audit' && (
                  <button
                    onClick={extractAll}
                    disabled={isExtractingAll || !session.projectId}
                    title="Lancer toutes les extractions"
                    className="text-[10px] px-2 py-1 rounded-lg bg-sky-500/[0.08] border border-sky-400/[0.22] text-sky-300/80 hover:bg-sky-500/[0.15] hover:border-sky-400/35 hover:text-sky-200 disabled:opacity-30 disabled:cursor-not-allowed transition-all cursor-pointer"
                  >
                    {isExtractingAll ? '...' : 'Tout extraire'}
                  </button>
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

        {/* Extract all error */}
        {extractAllError && (
          <div className="px-4 py-2 text-xs text-red-300 border-t border-red-400/10">{extractAllError}</div>
        )}

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
            {session.projectPath && (
              <>
                <span className="text-white/20">•</span>
                <span className="text-xs text-white/35">
                  Sync ciblée: {refreshTargetLabel}
                </span>
              </>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Btn
              variant="action"
              onClick={reingestPipeline}
              disabled={isReingesting || !session.projectId}
              className="px-3 py-2 text-xs"
              title={session.projectPath ? `Rafraîchir uniquement ${session.project}${session.subfolder ? ` / ${session.subfolder}` : ''}` : 'Sélectionne d’abord un dossier'}
            >
              {isReingesting ? (
                <span className="flex items-center gap-2">
                  <Spinner />
                  Refresh ciblé...
                </span>
              ) : (
                'Refresh ce dossier'
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
        {(refreshError || refreshSummary) && (
          <div className="px-8 py-2.5 border-b border-white/[0.07] bg-white/[0.015] flex items-center gap-4 flex-wrap">
            {refreshError && (
              <span className="text-sm text-red-300">{refreshError}</span>
            )}
            {refreshSummary && (
              <>
                <span className="text-xs font-semibold text-white/35 uppercase tracking-widest">Dernier refresh</span>
                <span className="text-sm text-emerald-300">+{refreshSummary.added ?? 0} ajouté{refreshSummary.added !== 1 ? 's' : ''}</span>
                <span className="text-sm text-amber-300">{refreshSummary.modified ?? 0} modifié{refreshSummary.modified !== 1 ? 's' : ''}</span>
                <span className="text-sm text-white/40">{refreshSummary.removed ?? 0} supprimé{refreshSummary.removed !== 1 ? 's' : ''}</span>
                {session.lastRefresh?.people_recomputed && (
                  <span className="text-sm text-cyan-300">Personnes recalculées</span>
                )}
                {session.lastRefresh?.financial_recomputed && (
                  <span className="text-sm text-cyan-300">Financier recalculé</span>
                )}
              </>
            )}
          </div>
        )}

        {/* Page content */}
        <div className="flex-1 overflow-y-auto px-8 py-8">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
