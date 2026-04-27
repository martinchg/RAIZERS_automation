import { useState } from 'react'
import { useSession } from '../context/session'
import { Toggle, Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'

const OPTIONS = [
  { id: 'patrimoine', label: 'Patrimoine' },
  { id: 'lots', label: 'Lots' },
  { id: 'operateur', label: 'Opérateur uniquement' },
]

export default function Operation() {
  const { session, dispatch } = useSession()
  const status = session.tabs.operation
  const [opts, setOpts] = useState({ patrimoine: true, lots: true, operateur: true })
  const [results, setResults] = useState(null)

  async function extract() {
    dispatch({ type: 'TAB_START', tab: 'operation' })
    await new Promise(r => setTimeout(r, 2200))
    setResults([
      { icon: '🏢', label: 'Société', value: 'SCI Rue de la Loge — SIREN 882 341 201', ok: true },
      { icon: '👤', label: 'Dirigeants', value: '3 personnes identifiées', ok: true },
      { icon: '💰', label: 'Financement', value: 'LTV 68% — Durée 24 mois', ok: true },
      { icon: '📍', label: 'Localisation', value: '34000 Montpellier', ok: true },
      { icon: '🏛️', label: 'Mandats Pappers', value: '7 mandats — 2 sociétés', ok: true },
    ])
    dispatch({ type: 'TAB_DONE', tab: 'operation' })
  }

  const displayedResults = results ?? (status === 'done'
    ? [
        { icon: '🏢', label: 'Société', value: 'SCI Rue de la Loge — SIREN 882 341 201', ok: true },
        { icon: '👤', label: 'Dirigeants', value: '3 personnes identifiées', ok: true },
        { icon: '💰', label: 'Financement', value: 'LTV 68% — Durée 24 mois', ok: true },
        { icon: '📍', label: 'Localisation', value: '34000 Montpellier', ok: true },
        { icon: '🏛️', label: 'Mandats Pappers', value: '7 mandats — 2 sociétés', ok: true },
      ]
    : null)

  return (
    <div className="w-full">
      <PageHeader
        title="Opération"
        description="Extraction des données opérateur, société et financement."
        status={status}
        action={
          status !== 'running' && (
            <Btn onClick={extract} disabled={status === 'running'}>
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
              <p className="text-sm text-red-300">Erreur lors de l'extraction. Vérifiez les logs.</p>
            </Card>
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
