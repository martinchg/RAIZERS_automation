import { useEffect, useMemo, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'
import { getAuditJob, getPatrimoineResults, startPatrimoineExtract } from '../lib/api'

function normalizeManualPerson(form) {
  const displayName = `${form.firstName.trim()} ${form.lastName.trim()}`.trim()
  return {
    id: `${form.lastName.trim().toLowerCase()}-${form.firstName.trim().toLowerCase()}-${Date.now()}`,
    display_name: displayName,
    nom: form.lastName.trim(),
    prenoms: form.firstName.trim(),
    birth_date: form.birthDate || null,
    folder_name: 'Ajout manuel',
    selected: true,
    source: 'manual',
  }
}

export default function Patrimoine() {
  const { session, dispatch } = useSession()
  const status = session.tabs.patrimoine
  const [people, setPeople] = useState([])
  const [selectedIds, setSelectedIds] = useState(new Set())
  const [form, setForm] = useState({ firstName: '', lastName: '', birthDate: '' })
  const [results, setResults] = useState([])
  const [jobId, setJobId] = useState(null)
  const [error, setError] = useState('')

  const savedJobId = session.jobIds?.patrimoine
  useEffect(() => {
    if (savedJobId && status === 'running' && !jobId) setJobId(savedJobId)
  }, [savedJobId, status, jobId])

  useEffect(() => {
    const catalogPeople = session.projectCatalog?.people || []
    setPeople(catalogPeople)
    setSelectedIds(new Set(catalogPeople.filter(person => person.selected !== false).map(person => person.id)))
  }, [session.projectCatalog])

  useEffect(() => {
    if (!session.projectId) return

    let cancelled = false

    async function loadExistingResults() {
      try {
        const data = await getPatrimoineResults(session.projectId)
        if (cancelled) return
        const cards = data.summary_cards || []
        if (cards.length > 0) {
          setResults(cards.map(item => ({
            icon: '👤',
            label: item.label,
            value: item.value,
            ok: true,
          })))
          dispatch({ type: 'TAB_DONE', tab: 'patrimoine' })
        }
      } catch {
        if (cancelled) return
      }
    }

    loadExistingResults()
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
          setResults((data.summary_cards || []).map(item => ({
            icon: '👤',
            label: item.label,
            value: item.value,
            ok: true,
          })))
          setError('')
          dispatch({ type: 'TAB_DONE', tab: 'patrimoine' })
        } else if (job.status === 'error') {
          setError(job.error || "Erreur d'extraction patrimoine")
          dispatch({ type: 'TAB_ERROR', tab: 'patrimoine' })
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || "Erreur d'extraction patrimoine")
          dispatch({ type: 'TAB_ERROR', tab: 'patrimoine' })
        }
      }
    }, 1500)

    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [dispatch, jobId, status])

  const selectedPeople = useMemo(
    () => people.filter(person => selectedIds.has(person.id)),
    [people, selectedIds],
  )

  function togglePerson(id) {
    setSelectedIds(previous => {
      const next = new Set(previous)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function addPerson() {
    if (!form.firstName.trim() || !form.lastName.trim()) return

    const person = normalizeManualPerson(form)
    setPeople(previous => [...previous, person])
    setSelectedIds(previous => new Set([...previous, person.id]))
    setForm({ firstName: '', lastName: '', birthDate: '' })
  }

  async function extract() {
    if (!session.projectId) {
      setError("Aucun projet chargé. Lance d'abord le pipeline depuis Setup.")
      dispatch({ type: 'TAB_ERROR', tab: 'patrimoine' })
      return
    }

    dispatch({ type: 'TAB_START', tab: 'patrimoine' })
    setError('')

    try {
      const job = await startPatrimoineExtract({
        project_id: session.projectId,
        people: selectedPeople,
      })
      setJobId(job.job_id)
    } catch (err) {
      setError(err.message || "Impossible de lancer l'extraction patrimoine")
      dispatch({ type: 'TAB_ERROR', tab: 'patrimoine' })
    }
  }

  return (
    <div className="w-full">
      <PageHeader
        title="Patrimoine Pappers"
        description="Personnes extraites depuis les casiers, complétables manuellement, puis enrichies via Pappers. TARIF : 24€/mois via abonnement Pappers."
        status={status}
        action={
          status !== 'running' && (
            <Btn variant="action" onClick={extract} disabled={selectedPeople.length === 0}>
              {status === 'done' ? 'Ré-extraire' : 'Extraire'}
            </Btn>
          )
        }
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card>
            <SectionTitle>Personnes détectées</SectionTitle>
            <div className="space-y-2 mb-5">
              {people.length === 0 ? (
                <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
                  Aucune personne détectée pour le moment.
                </div>
              ) : people.map(person => {
                const checked = selectedIds.has(person.id)
                return (
                  <label key={person.id} className="flex items-center gap-3 px-4 py-3 rounded-xl bg-white/[0.03] border border-white/8 text-sm text-white/80 cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => togglePerson(person.id)}
                      className="accent-cyan-400"
                    />
                    <div className="flex-1">
                      <div className="font-medium text-white/90">{person.display_name}</div>
                      <div className="text-xs text-white/35">
                        {person.birth_date ? `Né(e) le ${person.birth_date}` : 'Date de naissance à compléter'}
                      </div>
                    </div>
                  </label>
                )
              })}
            </div>

            <SectionTitle>Ajouter une personne</SectionTitle>
            <div className="grid grid-cols-3 gap-3">
              <input
                value={form.lastName}
                onChange={e => setForm(previous => ({ ...previous, lastName: e.target.value }))}
                placeholder="Nom"
                className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
              />
              <input
                value={form.firstName}
                onChange={e => setForm(previous => ({ ...previous, firstName: e.target.value }))}
                placeholder="Prénom"
                className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
              />
              <input
                type="date"
                value={form.birthDate}
                onChange={e => setForm(previous => ({ ...previous, birthDate: e.target.value }))}
                className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
              />
            </div>
            <div className="mt-4">
              <Btn variant="secondary" onClick={addPerson}>
                Ajouter une personne
              </Btn>
            </div>
          </Card>

          {status === 'running' && (
            <Card className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Enrichissement Pappers en cours...</Card>
          )}

          {status === 'error' && (
            <Card className="border-red-400/20 bg-red-400/[0.03]">
              <p className="text-sm text-red-300">{error || "Erreur lors de l'extraction patrimoine."}</p>
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
              Les résultats s'afficheront ici après l'extraction.
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}
