import { useEffect, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Spinner } from '../components/ui'
import { getAuditJob, getAuditProjects, getAuditSubfolders, getHealth, getProjectCatalog, startAuditPipeline } from '../lib/api'
import backgroundUrl from '../../../assets/background.jpg'
import raizersLogoUrl from '../../../assets/raizers_logo.png'

export default function Setup() {
  const { session, dispatch } = useSession()
  const [projects, setProjects] = useState([])
  const [project, setProject] = useState(null)
  const [subfolders, setSubfolders] = useState([])
  const [subfolder, setSubfolder] = useState('')
  const [phase, setPhase] = useState('select') // select | running | done | error
  const [job, setJob] = useState(null)
  const [stats, setStats] = useState(null)
  const [backendStatus, setBackendStatus] = useState('checking') // checking | up | down
  const [loadingProjects, setLoadingProjects] = useState(false)
  const [loadingSubfolders, setLoadingSubfolders] = useState(false)
  const [loadError, setLoadError] = useState('')

  useEffect(() => {
    let cancelled = false

    async function checkBackend() {
      try {
        await getHealth()
        if (!cancelled) {
          setBackendStatus('up')
        }
      } catch {
        if (!cancelled) setBackendStatus('down')
      }
    }

    checkBackend()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (backendStatus !== 'up') {
      setProjects([])
      setProject(null)
      return
    }

    let cancelled = false

    async function loadProjects() {
      setLoadingProjects(true)
      setLoadError('')
      try {
        const data = await getAuditProjects()
        if (cancelled) return
        const items = data.items || []
        setProjects(items)
        setProject(current => {
          if (current && items.some(item => item.id === current.id)) return current
          if (session.projectPath) {
            const matchedProject = items.find(item => item.path === session.projectPath)
            if (matchedProject) return matchedProject
          }
          return items[0] ?? null
        })
      } catch (err) {
        if (!cancelled) {
          setProjects([])
          setProject(null)
          setLoadError(err.message || 'Erreur de chargement des projets')
        }
      } finally {
        if (!cancelled) setLoadingProjects(false)
      }
    }

    loadProjects()
    return () => {
      cancelled = true
    }
  }, [backendStatus, session.projectPath])

  useEffect(() => {
    if (backendStatus !== 'up' || !project?.path) {
      setSubfolders([])
      setSubfolder('')
      return
    }

    let cancelled = false

    async function loadSubfolders() {
      setLoadingSubfolders(true)
      try {
        const data = await getAuditSubfolders(project.path)
        if (cancelled) return
        const items = data.items || []
        setSubfolders(items)
        setSubfolder(current => {
          if (current && items.includes(current)) return current
          if (session.subfolder && items.includes(session.subfolder)) return session.subfolder
          return items[0] ?? ''
        })
      } catch {
        if (!cancelled) {
          setSubfolders([])
          setSubfolder('')
        }
      } finally {
        if (!cancelled) setLoadingSubfolders(false)
      }
    }

    loadSubfolders()
    return () => {
      cancelled = true
    }
  }, [backendStatus, project, session.subfolder])

  useEffect(() => {
    if (!job?.job_id || phase !== 'running') return undefined

    let cancelled = false
    const intervalId = setInterval(async () => {
      try {
        const nextJob = await getAuditJob(job.job_id)
        if (cancelled) return

        setJob(nextJob)

        if (nextJob.status === 'done') {
          const nextStats = nextJob.result?.stats || null
          const nextCatalog = nextJob.result?.catalog || null
          setStats(nextStats)
          setPhase('done')
          dispatch({
            type: 'SET_PROJECT_CATALOG',
            projectCatalog: nextCatalog,
          })
          dispatch({
            type: 'PIPELINE_DONE',
            stats: nextStats,
          })
        } else if (nextJob.status === 'error') {
          setPhase('error')
          dispatch({ type: 'PIPELINE_ERROR' })
        }
      } catch {
        if (!cancelled) {
          setPhase('error')
          dispatch({ type: 'PIPELINE_ERROR' })
        }
      }
    }, 1500)

    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [dispatch, job?.job_id, phase])

  async function launchPipeline() {
    if (backendStatus !== 'up' || !project?.path) {
      setLoadError("Backend indisponible. Impossible de lancer le pipeline depuis l'UI React.")
      setPhase('error')
      dispatch({ type: 'PIPELINE_ERROR' })
      return
    }

    setLoadError('')
    setPhase('running')
    setStats(null)
    dispatch({
      type: 'SET_PROJECT',
      project: project.name,
      projectId: project.id,
      projectPath: project.path,
      subfolder: subfolder || null,
      projectCatalog: null,
    })
    dispatch({ type: 'PIPELINE_START' })

    try {
      const createdJob = await startAuditPipeline({
        project_path: project.path,
        audit_folder: subfolder || null,
      })
      setJob(createdJob)
    } catch (err) {
      setPhase('error')
      setLoadError(err.message || "Impossible de lancer le pipeline")
      dispatch({ type: 'PIPELINE_ERROR' })
    }
  }

  useEffect(() => {
    if (!project?.id || phase !== 'done') return undefined

    let cancelled = false

    async function loadCatalog() {
      try {
        const catalog = await getProjectCatalog(project.id)
        if (!cancelled) {
          dispatch({
            type: 'SET_PROJECT_CATALOG',
            projectCatalog: catalog,
          })
        }
      } catch {
        return
      }
    }

    loadCatalog()
    return () => {
      cancelled = true
    }
  }, [dispatch, phase, project?.id])

  const pipelineSteps = job?.pipeline_steps || []
  const runningStageLabel = backendStatus === 'up'
    ? (job?.stage_label || 'Préparation du job')
    : 'Backend indisponible'

  const progress = phase === 'done'
    ? 100
    : phase === 'running'
      ? Math.max(8, Math.round((job?.progress_ratio || 0) * 100))
      : 0
  const displayedProjects = projects.map(item => item.name)
  const selectedProjectName = project?.name ?? ''
  const displayedSubfolders = subfolders
  const selectedSubfolderName = subfolder
  const canLaunch = backendStatus === 'up' && Boolean(project?.path) && !loadingProjects
  const statsFiles = stats?.files_processed ?? stats?.files ?? 0
  const statsTokens = stats?.total_tokens ?? stats?.tokens ?? 0

  return (
    <div
      className="min-h-screen flex items-center justify-center p-6 bg-cover bg-center bg-no-repeat"
      style={{
        backgroundImage: `linear-gradient(rgba(7, 17, 26, 0.48), rgba(7, 17, 26, 0.62)), url(${backgroundUrl})`,
      }}
    >
      <div className="w-full max-w-md">

        {/* Logo */}
        <div className="text-center mb-8">
          <img
            src={raizersLogoUrl}
            alt="Raizers"
            className="w-56 sm:w-72 h-auto object-contain mx-auto invert drop-shadow-[0_12px_28px_rgba(0,0,0,0.35)]"
          />
        </div>

        {/* Card */}
        <div className="rounded-3xl border border-white/10 bg-[#08131d] shadow-2xl shadow-black/50 overflow-hidden">

          {/* Header */}
          <div className="px-7 pt-6 pb-5 border-b border-white/[0.06]">
            <div className="flex items-center justify-between mb-4">
              <span className="text-xs text-white/40 uppercase tracking-widest font-semibold">
                {phase === 'select' ? 'Nouveau dossier' : phase === 'running' ? 'Pipeline en cours' : 'Prêt'}
              </span>
              {phase !== 'select' && (
                <span className={`text-xs font-semibold ${phase === 'done' ? 'text-emerald-400' : 'text-amber-400'}`}>
                  {phase === 'done' ? '✓ Terminé' : `${progress}%`}
                </span>
              )}
            </div>
            <div className="h-1 bg-white/[0.06] rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${phase === 'done' ? 'bg-emerald-400' : 'bg-gradient-to-r from-cyan-500 to-cyan-400'}`}
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>

          {/* Body */}
          <div className="px-7 py-6">

            {/* Phase : sélection */}
            {phase === 'select' && (
              <div className="space-y-5">
                <div className={`rounded-xl border px-4 py-3 text-sm ${
                  backendStatus === 'up'
                    ? 'border-emerald-400/15 bg-emerald-400/[0.04] text-emerald-200'
                    : backendStatus === 'down'
                      ? 'border-amber-400/15 bg-amber-400/[0.04] text-amber-100'
                      : 'border-white/8 bg-white/[0.03] text-white/55'
                }`}>
                  {backendStatus === 'up' && "Backend FastAPI connecté. Les dossiers Dropbox et le lancement du pipeline passent maintenant par l'API."}
                  {backendStatus === 'down' && "Backend FastAPI non joignable. Le flux Streamlit reste intact, mais cette UI ne peut pas lancer l'ingestion."}
                  {backendStatus === 'checking' && 'Vérification de la connexion backend...'}
                </div>
                {loadError && (
                  <div className="rounded-xl border border-red-400/15 bg-red-400/[0.04] px-4 py-3 text-sm text-red-200">
                    {loadError}
                  </div>
                )}
                <div>
                  <label className="block text-xs text-white/45 mb-2 uppercase tracking-widest font-medium">Dossier</label>
                  <select
                    value={selectedProjectName}
                    onChange={e => {
                      if (backendStatus === 'up') {
                        const nextProject = projects.find(item => item.name === e.target.value) ?? null
                        setProject(nextProject)
                      }
                    }}
                    disabled={loadingProjects || displayedProjects.length === 0}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer disabled:opacity-50"
                  >
                    {displayedProjects.map(p => <option key={p} className="bg-[#0c1e2e]">{p}</option>)}
                  </select>
                  {loadingProjects && <p className="text-xs text-white/30 mt-1.5">Chargement des projets Dropbox...</p>}
                </div>
                <div>
                  <label className="block text-xs text-white/45 mb-2 uppercase tracking-widest font-medium">Sous-dossier d'audit</label>
                  <select
                    value={selectedSubfolderName}
                    onChange={e => setSubfolder(e.target.value)}
                    disabled={loadingSubfolders || displayedSubfolders.length === 0}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer disabled:opacity-50"
                  >
                    {displayedSubfolders.map(s => <option key={s} className="bg-[#0c1e2e]">{s}</option>)}
                  </select>
                  {loadingSubfolders && <p className="text-xs text-white/30 mt-1.5">Lecture des sous-dossiers d'audit...</p>}
                  <p className="text-xs text-white/30 mt-1.5">Le dossier Opérateur associé sera toujours inclus.</p>
                </div>
              </div>
            )}

            {/* Phase : pipeline en cours */}
            {phase === 'running' && (
              <div className="space-y-4">
                <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-cyan-400/20 bg-cyan-400/[0.04]">
                  <div className="w-5 h-5 flex items-center justify-center flex-shrink-0">
                    <Spinner />
                  </div>
                  <div>
                    <div className="text-sm text-white">{runningStageLabel}</div>
                    {job?.current_step ? (
                      <div className="text-xs text-cyan-100/70 mt-0.5">
                        Étape {job.current_step} / {job.total_steps || pipelineSteps.length || 5}
                      </div>
                    ) : null}
                  </div>
                </div>
                {pipelineSteps.length > 0 && (
                  <div className="space-y-1.5">
                    {pipelineSteps.map(step => (
                      <div
                        key={step.key}
                        className={`rounded-lg border px-3 py-2.5 min-h-[46px] flex items-center gap-3 transition-colors ${
                          step.status === 'done'
                            ? 'border-emerald-400/30 bg-emerald-400/[0.06]'
                            : step.status === 'running'
                              ? 'border-cyan-400/30 bg-cyan-400/[0.06]'
                              : 'border-white/8 bg-white/[0.015]'
                        }`}
                      >
                        <div className={`w-6 h-6 rounded-md border flex items-center justify-center text-[10px] font-bold flex-shrink-0 ${
                          step.status === 'done'
                            ? 'border-emerald-400/60 bg-emerald-400/15 text-emerald-300'
                            : step.status === 'running'
                              ? 'border-cyan-400/60 bg-cyan-400/15 text-cyan-200'
                              : 'border-white/12 bg-white/[0.03] text-white/35'
                        }`}>
                          {step.status === 'done' ? '✓' : step.status === 'running' ? '•' : step.label.split(' ')[0]?.[0]}
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className={`text-[13px] leading-tight font-medium ${
                            step.status === 'pending' ? 'text-white/50' : 'text-white'
                          }`}>
                            {step.label}
                          </div>
                          {step.detail ? (
                            <div className="text-[10px] leading-tight text-white/30 mt-0.5 truncate">
                              {step.detail}
                            </div>
                          ) : null}
                        </div>
                        <div className={`text-[10px] uppercase tracking-[0.18em] font-semibold flex-shrink-0 ${
                          step.status === 'done'
                            ? 'text-emerald-300'
                            : step.status === 'running'
                              ? 'text-cyan-200'
                              : 'text-white/25'
                        }`}>
                          {step.status === 'done' ? 'OK' : step.status === 'running' ? 'RUN' : 'WAIT'}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                <p className="text-xs text-white/35 px-1">
                  {backendStatus === 'up'
                    ? "Le pipeline tourne côté backend Python. Cette page interroge l'état du job et affiche les étapes backend."
                    : 'Backend indisponible.'}
                </p>
              </div>
            )}

            {/* Phase : done */}
            {phase === 'done' && (
              <div className="space-y-4">
                <div className="rounded-xl border border-emerald-400/15 bg-emerald-400/[0.04] px-4 py-4">
                  <div className="text-sm font-semibold text-emerald-300 mb-2">Pipeline terminé</div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <div className="text-xs text-white/35">Fichiers traités</div>
                      <div className="text-lg font-bold text-white">{statsFiles}</div>
                    </div>
                    <div>
                      <div className="text-xs text-white/35">Tokens extraits</div>
                      <div className="text-lg font-bold text-white">{statsTokens?.toLocaleString('fr-FR')}</div>
                    </div>
                  </div>
                </div>
                <p className="text-xs text-white/40 text-center">
                  {project?.name?.replace(/^\d+\.\s*/, '')} — {selectedSubfolderName || 'Opérateur uniquement'}
                </p>
              </div>
            )}

            {phase === 'error' && (
              <div className="rounded-xl border border-red-400/15 bg-red-400/[0.04] px-4 py-4 text-sm text-red-200">
                {job?.error || loadError || 'Le pipeline n’a pas pu être lancé.'}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="px-7 pb-6">
            {phase === 'select' && (
              <Btn full onClick={launchPipeline} disabled={!canLaunch}>
                Lancer le pipeline
              </Btn>
            )}
            {phase === 'running' && (
              <button disabled className="w-full py-3 rounded-xl bg-white/5 border border-white/10 text-white/30 text-sm font-semibold flex items-center justify-center gap-2 cursor-not-allowed">
                <Spinner />
                Traitement en cours...
              </button>
            )}
            {phase === 'done' && (
              <Btn full onClick={() => dispatch({ type: 'PIPELINE_DONE', stats })}>
                Entrer dans l'app →
              </Btn>
            )}
            {phase === 'error' && (
              <Btn full onClick={() => setPhase('select')}>
                Revenir à la sélection
              </Btn>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
