import { useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'

const DEFAULT_PERSONS = [
  { id: 'dupont-jean', firstName: 'Jean', lastName: 'Dupont', birthDate: '1979-04-12' },
  { id: 'martin-marie', firstName: 'Marie', lastName: 'Martin', birthDate: '1986-11-03' },
  { id: 'leroy-pierre', firstName: 'Pierre', lastName: 'Leroy', birthDate: '' },
]

export default function Patrimoine() {
  const { session, dispatch } = useSession()
  const status = session.tabs.patrimoine
  const [people, setPeople] = useState(DEFAULT_PERSONS)
  const [selectedIds, setSelectedIds] = useState(() => new Set(DEFAULT_PERSONS.map(person => person.id)))
  const [form, setForm] = useState({ firstName: '', lastName: '', birthDate: '' })
  const [results, setResults] = useState(null)

  const selectedPeople = people.filter(person => selectedIds.has(person.id))

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

    const person = {
      id: `${form.lastName.trim().toLowerCase()}-${form.firstName.trim().toLowerCase()}-${Date.now()}`,
      firstName: form.firstName.trim(),
      lastName: form.lastName.trim(),
      birthDate: form.birthDate,
    }

    setPeople(previous => [...previous, person])
    setSelectedIds(previous => new Set([...previous, person.id]))
    setForm({ firstName: '', lastName: '', birthDate: '' })
  }

  async function extract() {
    dispatch({ type: 'TAB_START', tab: 'patrimoine' })
    await new Promise(r => setTimeout(r, 2400))
    setResults(selectedPeople.map(person => ({
      icon: '👤',
      label: `${person.firstName} ${person.lastName}`,
      value: 'Patrimoine net estimé extrait',
      ok: true,
    })))
    dispatch({ type: 'TAB_DONE', tab: 'patrimoine' })
  }

  const displayedResults = results ?? (status === 'done'
    ? selectedPeople.map(person => ({
        icon: '👤',
        label: `${person.firstName} ${person.lastName}`,
        value: 'Patrimoine net estimé extrait',
        ok: true,
      }))
    : null)

  return (
    <div className="w-full">
      <PageHeader
        title="Pappers"
        description="Recherche Pappers sur les dirigeants et associés détectés."
        status={status}
        action={
          status !== 'running' && (
            <Btn onClick={extract} disabled={selectedPeople.length === 0}>
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
              {people.map(person => {
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
                      <div className="font-medium text-white/90">{person.firstName} {person.lastName}</div>
                      <div className="text-xs text-white/35">
                        {person.birthDate ? `Né(e) le ${person.birthDate}` : 'Date de naissance à compléter'}
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
            <Card className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Extraction en cours...</Card>
          )}
        </div>

        <Card accent className="sticky top-0 xl:col-span-1">
          <SectionTitle>Résultats</SectionTitle>
          {status === 'running' ? (
            <div className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Préparation des résultats...
            </div>
          ) : displayedResults && displayedResults.length > 0 ? (
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
