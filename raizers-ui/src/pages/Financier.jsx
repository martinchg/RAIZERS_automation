import { useEffect, useMemo, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'
import { getAuditJob, getFinancialResults, startFinancialExtract } from '../lib/api'

export default function Financier() {
  const { session, dispatch } = useSession()
  const status = session.tabs.financier
  const [companies, setCompanies] = useState([])
  const [openCompanyId, setOpenCompanyId] = useState(null)
  const [selectedFiles, setSelectedFiles] = useState({})
  const [results, setResults] = useState([])
  const [jobId, setJobId] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const savedJobId = session.jobIds?.financier
  useEffect(() => {
    if (savedJobId && status === 'running' && !jobId) setJobId(savedJobId)
  }, [savedJobId, status, jobId])

  useEffect(() => {
    if (!session.projectId) {
      setCompanies([])
      setResults([])
      return
    }

    let cancelled = false

    async function loadFinancialData() {
      setLoading(true)
      try {
        const data = await getFinancialResults(session.projectId)
        if (cancelled) return
        const nextCompanies = data.companies || []
        setCompanies(nextCompanies)
        setOpenCompanyId(current => current ?? nextCompanies[0]?.id ?? null)
        setSelectedFiles(
          Object.fromEntries(
            nextCompanies.map(company => {
              const periods = Object.entries(company.filesByPeriod || {})
              const firstPeriod = periods[0]?.[0] ?? null
              const firstFile = periods[0]?.[1]?.[0] ?? null
              return [
                company.id,
                firstPeriod && firstFile
                  ? { period: firstPeriod, fileId: firstFile.id }
                  : { period: null, fileId: null },
              ]
            }),
          ),
        )
        const cards = data.summary_cards || []
        if (cards.length > 0) {
          setResults(cards.map(item => ({
            icon: '📊',
            label: item.label,
            value: item.value,
            ok: true,
          })))
          dispatch({ type: 'TAB_DONE', tab: 'financier' })
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || 'Erreur de chargement des bilans')
          setCompanies([])
          setResults([])
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadFinancialData()
    return () => {
      cancelled = true
    }
  }, [session.projectId])

  useEffect(() => {
    if (!jobId || status !== 'running') return undefined

    let cancelled = false
    const intervalId = setInterval(async () => {
      try {
        const job = await getAuditJob(jobId)
        if (cancelled) return

        if (job.status === 'done') {
          const data = job.result || {}
          const nextCompanies = data.companies || []
          setCompanies(nextCompanies)
          setOpenCompanyId(current => current ?? nextCompanies[0]?.id ?? null)
          setResults((data.summary_cards || []).map(item => ({
            icon: '📊',
            label: item.label,
            value: item.value,
            ok: true,
          })))
          setError('')
          dispatch({ type: 'TAB_DONE', tab: 'financier' })
        } else if (job.status === 'error') {
          setError(job.error || "Erreur d'extraction financière")
          dispatch({ type: 'TAB_ERROR', tab: 'financier' })
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || "Erreur d'extraction financière")
          dispatch({ type: 'TAB_ERROR', tab: 'financier' })
        }
      }
    }, 1500)

    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [dispatch, jobId, status])

  const totalDetectedFiles = useMemo(
    () => companies.reduce((count, company) => count + (company.fileCount || 0), 0),
    [companies],
  )

  function selectFile(companyId, fileId) {
    const company = companies.find(item => item.id === companyId)
    const allFiles = Object.entries(company?.filesByPeriod || {}).flatMap(([period, files]) =>
      files.map(f => ({ ...f, period }))
    )
    const found = allFiles.find(f => f.id === fileId)
    setSelectedFiles(previous => ({
      ...previous,
      [companyId]: {
        period: found?.period ?? previous[companyId]?.period ?? null,
        fileId,
      },
    }))
  }

  async function extract() {
    if (!session.projectId) {
      setError("Aucun projet chargé. Lance d'abord le pipeline depuis Setup.")
      dispatch({ type: 'TAB_ERROR', tab: 'financier' })
      return
    }

    dispatch({ type: 'TAB_START', tab: 'financier' })
    setError('')

    try {
      const selections = companies
        .map(company => ({
          company_id: company.id,
          period: selectedFiles[company.id]?.period ?? null,
          file_id: selectedFiles[company.id]?.fileId ?? null,
        }))
        .filter(selection => selection.period && selection.file_id)

      const job = await startFinancialExtract({
        project_id: session.projectId,
        selections,
      })
      setJobId(job.job_id)
    } catch (err) {
      setError(err.message || "Impossible de lancer l'extraction financière")
      dispatch({ type: 'TAB_ERROR', tab: 'financier' })
    }
  }

  return (
    <div className="w-full">
      <PageHeader
        title="Bilans financiers"
        description="Détection des bilans comptables présents dans le projet, puis extraction financière. TARIF ~ 0,04 € par société."
        status={status}
        action={
          status !== 'running' && (
            <Btn variant="action" onClick={extract} disabled={totalDetectedFiles === 0 || loading}>
              {status === 'done' ? 'Ré-extraire' : 'Extraire'}
            </Btn>
          )
        }
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card>
            <SectionTitle>Sociétés détectées</SectionTitle>
            {loading ? (
              <div className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Lecture du cache projet...</div>
            ) : companies.length === 0 ? (
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
                Aucun PDF de bilan détecté pour ce projet.
              </div>
            ) : (
              <div className="space-y-4">
                {companies.map(company => {
                  const isOpen = openCompanyId === company.id
                  const currentSelection = selectedFiles[company.id] || {}
                  const allFiles = Object.entries(company.filesByPeriod || {}).flatMap(([period, files]) =>
                    files.map(f => ({ ...f, period }))
                  )
                  return (
                    <div key={company.id} className="rounded-2xl border border-white/8 bg-white/[0.02] overflow-hidden">
                      <button
                        type="button"
                        onClick={() => setOpenCompanyId(current => current === company.id ? null : company.id)}
                        className="w-full flex items-center justify-between gap-4 p-4 text-left hover:bg-white/[0.03] transition-colors cursor-pointer"
                      >
                        <div className="text-sm font-semibold text-white flex items-center gap-2">
                          <span className="text-base">🏢</span>
                          {company.name}
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-xs px-2 py-1 rounded-full bg-cyan-400/10 text-cyan-300 border border-cyan-400/15">
                            {company.fileCount} fichier{company.fileCount > 1 ? 's' : ''}
                          </span>
                          <span className={`text-white/45 text-sm transition-transform ${isOpen ? 'rotate-180' : ''}`}>⌄</span>
                        </div>
                      </button>

                      {isOpen && (
                        <div className="border-t border-white/8 px-4 py-3 bg-white/[0.01]">
                          <div className="text-xs text-white/35 mb-2 uppercase tracking-widest font-medium">Fichier Dropbox retenu</div>
                          <select
                            value={currentSelection.fileId ?? ''}
                            onChange={e => selectFile(company.id, e.target.value)}
                            className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer"
                          >
                            {allFiles.map(file => (
                              <option key={file.id} value={file.id} className="bg-[#0c1e2e]">
                                {file.name}
                              </option>
                            ))}
                          </select>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </Card>

          {status === 'running' && (
            <Card className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Extraction financière en cours...</Card>
          )}

          {status === 'error' && (
            <Card className="border-red-400/20 bg-red-400/[0.03]">
              <p className="text-sm text-red-300">{error || "Erreur lors de l'extraction financière."}</p>
            </Card>
          )}
        </div>

        <Card accent className="sticky top-0 xl:col-span-1">
          <SectionTitle>Résultats</SectionTitle>
          {status === 'running' ? (
            <div className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Préparation des résultats...
            </div>
          ) : status === 'done' && results.length > 0 ? (
            <div className="space-y-2">
              {results.map((result, index) => <ResultRow key={index} {...result} />)}
            </div>
          ) : (
            <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
              Les résultats financiers s'afficheront ici après l'extraction.
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}
