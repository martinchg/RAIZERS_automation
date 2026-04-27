import { useState } from 'react'
import { useSession } from '../context/session'
import { Toggle, Btn, Card, PageHeader, SectionTitle, Spinner } from '../components/ui'

const FAKE_LOTS = [
  { ref: 'A01', type: 'T3', surface: 68, prix: 245000, statut: 'Vendu' },
  { ref: 'A02', type: 'T2', surface: 48, prix: 172000, statut: 'Réservé' },
  { ref: 'A03', type: 'T4', surface: 89, prix: 318000, statut: 'Disponible' },
  { ref: 'B01', type: 'T3', surface: 72, prix: 259000, statut: 'Vendu' },
  { ref: 'B02', type: 'T5', surface: 112, prix: 398000, statut: 'Disponible' },
]

const STATUT_STYLE = {
  Vendu:      'bg-emerald-400/10 text-emerald-300 border-emerald-400/20',
  Réservé:    'bg-amber-400/10 text-amber-300 border-amber-400/20',
  Disponible: 'bg-white/5 text-white/50 border-white/10',
}

export default function Lots() {
  const { session, dispatch } = useSession()
  const status = session.tabs.lots
  const [showTable, setShowTable] = useState(false)

  async function extract() {
    dispatch({ type: 'TAB_START', tab: 'lots' })
    await new Promise(r => setTimeout(r, 1800))
    setShowTable(true)
    dispatch({ type: 'TAB_DONE', tab: 'lots' })
  }

  return (
    <div className="max-w-2xl">
      <PageHeader
        title="Lots"
        description="Grille de lots et état de commercialisation."
        status={status}
        action={
          status !== 'running' && (
            <Btn onClick={extract}>{status === 'done' ? 'Ré-extraire' : 'Extraire'}</Btn>
          )
        }
      />

      <Card className="mb-4">
        <SectionTitle>Options</SectionTitle>
        <div className="grid grid-cols-2 gap-x-8 gap-y-3">
          <Toggle label="Références & types" defaultOn />
          <Toggle label="Surfaces Carrez" defaultOn />
          <Toggle label="Prix de vente" defaultOn />
          <Toggle label="État commercial" defaultOn />
          <Toggle label="Parking / cave" defaultOn />
        </div>
      </Card>

      {status === 'running' && (
        <Card className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Extraction en cours...</Card>
      )}

      {showTable && (
        <Card accent>
          <SectionTitle>Grille des lots ({FAKE_LOTS.length})</SectionTitle>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-white/35 uppercase tracking-wider border-b border-white/[0.06]">
                  <th className="pb-2 text-left font-medium">Réf</th>
                  <th className="pb-2 text-left font-medium">Type</th>
                  <th className="pb-2 text-right font-medium">Surface</th>
                  <th className="pb-2 text-right font-medium">Prix</th>
                  <th className="pb-2 text-right font-medium">Statut</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/[0.04]">
                {FAKE_LOTS.map(l => (
                  <tr key={l.ref}>
                    <td className="py-2.5 text-white/80 font-medium">{l.ref}</td>
                    <td className="py-2.5 text-white/60">{l.type}</td>
                    <td className="py-2.5 text-right text-white/60">{l.surface} m²</td>
                    <td className="py-2.5 text-right text-white/80">{l.prix.toLocaleString('fr-FR')} €</td>
                    <td className="py-2.5 text-right">
                      <span className={`text-xs px-2 py-0.5 rounded-full border ${STATUT_STYLE[l.statut]}`}>
                        {l.statut}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  )
}
