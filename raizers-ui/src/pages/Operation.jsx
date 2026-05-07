import { useEffect, useState } from 'react'
import { useSession } from '../context/session'
import { Toggle, Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'
import { getAuditJob, getOperationResults, startOperationExtract } from '../lib/api'

const OPTIONS = [
  { id: 'patrimoine', label: 'Patrimoine' },
  { id: 'lots', label: 'Lots' },
  { id: 'operateur', label: 'Opérateur' },
]

export default function Operation() {
  const { session, dispatch } = useSession()
  const status = session.tabs.operation
  const [opts, setOpts] = useState({ patrimoine: true, lots: true, operateur: true })
  const [results, setResults] = useState(null)
  const [details, setDetails] = useState([])
  const [jobId, setJobId] = useState(null)
  const [error, setError] = useState('')

  const savedJobId = session.jobIds?.operation
  useEffect(() => {
    if (savedJobId && status === 'running' && !jobId) setJobId(savedJobId)
  }, [savedJobId, status, jobId])

  useEffect(() => {
    if (!session.projectId) return

    let cancelled = false

    async function loadExistingResults() {
      try {
        const data = await getOperationResults(session.projectId)
        if (cancelled) return
        setResults((data.summary_cards || []).map(item => ({
          icon: '•',
          label: item.label,
          value: item.value,
          ok: true,
        })))
        setDetails(data.sections || [])
        if ((data.summary_cards || []).length > 0) {
          dispatch({ type: 'TAB_DONE', tab: 'operation' })
        }
      } catch {
        if (cancelled) return
      }
    }

    loadExistingResults()
    return () => {
      cancelled = true
    }
  }, [dispatch, session.projectId])

  useEffect(() => {
    if (!jobId || status !== 'running') return undefined

    let cancelled = false
    const intervalId = setInterval(async () => {
      try {
        const job = await getAuditJob(jobId)
        if (cancelled) return

        if (job.status === 'done') {
          const data = job.result || {}
          setResults((data.summary_cards || []).map(item => ({
            icon: '•',
            label: item.label,
            value: item.value,
            ok: true,
          })))
          setDetails(data.sections || [])
          setError('')
          dispatch({ type: 'TAB_DONE', tab: 'operation' })
        } else if (job.status === 'error') {
          setError(job.error || "Erreur d'extraction")
          dispatch({ type: 'TAB_ERROR', tab: 'operation' })
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || "Erreur d'extraction")
          dispatch({ type: 'TAB_ERROR', tab: 'operation' })
        }
      }
    }, 1500)

    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [dispatch, jobId, status])

  async function extract() {
    dispatch({ type: 'TAB_START', tab: 'operation' })
    setError('')

    if (!session.projectId) {
      setError("Aucun projet chargé. Lance d'abord le pipeline depuis Setup.")
      dispatch({ type: 'TAB_ERROR', tab: 'operation' })
      return
    }

    try {
      const job = await startOperationExtract({
        project_id: session.projectId,
        include_operateur: opts.operateur,
        include_patrimoine: opts.patrimoine,
        include_lots: opts.lots,
      })
      setJobId(job.job_id)
    } catch (err) {
      setError(err.message || "Impossible de lancer l'extraction")
      dispatch({ type: 'TAB_ERROR', tab: 'operation' })
    }
  }

  const displayedResults = results

  return (
    <div className="w-full">
      <PageHeader
        title="Opération"
        description="Extraction des données opérateur, société et financement. TARIF ~ 0,07 € par extraction."
        status={status}
        action={
          status !== 'running' && (
            <Btn variant="action" onClick={extract} disabled={status === 'running'}>
              {status === 'done' ? 'Ré-extraire' : 'Extraire'}
            </Btn>
          )
        }
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card className="mb-4">
            <SectionTitle>Options d'extraction</SectionTitle>
            <div className="grid grid-cols-2 gap-x-8 gap-y-3">
              {OPTIONS.map(o => (
                <Toggle
                  key={o.id}
                  label={o.label}
                  defaultOn={opts[o.id]}
                  onChange={v => setOpts(p => ({ ...p, [o.id]: v }))}
                />
              ))}
            </div>
          </Card>

          {status === 'running' && (
            <Card className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Extraction en cours...
            </Card>
          )}

          {status === 'error' && (
            <Card className="border-red-400/20 bg-red-400/[0.03]">
              <p className="text-sm text-red-300">{error || "Erreur lors de l'extraction. Vérifiez les logs."}</p>
            </Card>
          )}

          {status === 'done' && details.length > 0 && (
            <Card>
              <SectionTitle>Détails extraits</SectionTitle>
              <div className="space-y-5">
                {details.map(section => (
                  <div key={section.id}>
                    <div className="text-xs text-white/35 uppercase tracking-widest font-semibold mb-2">{section.title}</div>
                    <div className="space-y-2">
                      {section.items.map(item => (
                        <div key={item.field_id} className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                          <div className="text-xs text-white/35 mb-1">{item.label}</div>
                          <div className="text-sm text-white/85">{item.value}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>

        <Card accent className="sticky top-0 xl:col-span-1">
          <SectionTitle>Résultats</SectionTitle>
          {status === 'running' ? (
            <div className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Préparation des résultats...
            </div>
          ) : status === 'done' && displayedResults && displayedResults.length > 0 ? (
            <div className="space-y-2">
              {displayedResults.map((r, i) => <ResultRow key={i} {...r} />)}
            </div>
          ) : (
            <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
              Les résultats s'afficheront ici après l'extraction.
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}
