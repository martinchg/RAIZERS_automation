import { useState } from 'react'
import { Btn, Card, PageHeader, SectionTitle, Toggle, Spinner } from '../components/ui'

const FAKE_RESULTS = [
  { adresse: '5 Rue des Muriers, 34000 Montpellier', prix_m2: 3650, surface: 74, pieces: 3, piscine: false, parking: true, balcon: true },
  { adresse: '18 Av. de la Mer, 34280 La Grande-Motte', prix_m2: 4200, surface: 55, pieces: 2, piscine: true, parking: true, balcon: true },
  { adresse: '2 Impasse des Oliviers, 34090 Montpellier', prix_m2: 3100, surface: 90, pieces: 4, piscine: false, parking: false, balcon: false },
]

export default function Scraping() {
  const [inputs, setInputs] = useState({
    surface: '',
    terrain: '',
    pieces: '',
    garage: true,
    piscine: false,
    cave: false,
    balcon: true,
    ascenseur: false,
    jardin: false,
  })
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState(null)

  async function scrape() {
    setLoading(true)
    setResults(null)
    await new Promise(r => setTimeout(r, 2200))
    setResults(FAKE_RESULTS)
    setLoading(false)
  }

  const computedAverage = results
    ? Math.round(results.reduce((sum, row) => sum + row.prix_m2, 0) / results.length)
    : null

  const computedLow = results
    ? Math.min(...results.map(row => row.prix_m2))
    : null

  const computedHigh = results
    ? Math.max(...results.map(row => row.prix_m2))
    : null

  const displayedAverage = computedAverage ? computedAverage.toLocaleString('fr-FR') : '—'
  const displayedLow = computedLow ? computedLow.toLocaleString('fr-FR') : '—'
  const displayedHigh = computedHigh ? computedHigh.toLocaleString('fr-FR') : '—'

  return (
    <div className="w-full">
      <PageHeader
        title="Scraping immobilier"
        description="Extraction structurée de données immobilières via DVF et Firecrawl."
        action={<Btn onClick={scrape} disabled={loading}>{loading ? <Spinner /> : 'Lancer'}</Btn>}
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card>
            <SectionTitle>Caractéristiques du bien</SectionTitle>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface du bien (m²)</label>
                <input
                  value={inputs.surface}
                  onChange={e => setInputs(current => ({ ...current, surface: e.target.value }))}
                  placeholder="74"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface terrain (m²)</label>
                <input
                  value={inputs.terrain}
                  onChange={e => setInputs(current => ({ ...current, terrain: e.target.value }))}
                  placeholder="120"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Nombre de pièces</label>
                <input
                  value={inputs.pieces}
                  onChange={e => setInputs(current => ({ ...current, pieces: e.target.value }))}
                  placeholder="3"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
              <Toggle label="Garage / parking" defaultOn={inputs.garage} onChange={v => setInputs(current => ({ ...current, garage: v }))} />
              <Toggle label="Piscine" defaultOn={inputs.piscine} onChange={v => setInputs(current => ({ ...current, piscine: v }))} />
              <Toggle label="Cave" defaultOn={inputs.cave} onChange={v => setInputs(current => ({ ...current, cave: v }))} />
              <Toggle label="Balcon / terrasse" defaultOn={inputs.balcon} onChange={v => setInputs(current => ({ ...current, balcon: v }))} />
              <Toggle label="Ascenseur" defaultOn={inputs.ascenseur} onChange={v => setInputs(current => ({ ...current, ascenseur: v }))} />
              <Toggle label="Jardin" defaultOn={inputs.jardin} onChange={v => setInputs(current => ({ ...current, jardin: v }))} />
            </div>
          </Card>

          {loading && (
            <Card className="flex items-center gap-3 text-white/60 text-sm"><Spinner /> Scraping en cours...</Card>
          )}

          {results && (
            <Card accent>
              <SectionTitle>{results.length} résultats</SectionTitle>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
                {results.map((r, i) => (
                  <div key={i} className="px-4 py-3 rounded-xl bg-white/[0.03] border border-white/8">
                    <div className="text-sm text-white/80 font-medium mb-2">{r.adresse}</div>
                    <div className="flex flex-wrap gap-2 text-xs">
                      <span className="bg-cyan-400/10 text-cyan-300 px-2 py-1 rounded-lg">{r.prix_m2.toLocaleString('fr-FR')} €/m²</span>
                      <span className="bg-white/5 text-white/55 px-2 py-1 rounded-lg">{r.surface} m²</span>
                      <span className="bg-white/5 text-white/55 px-2 py-1 rounded-lg">{r.pieces} pièces</span>
                      {r.piscine && <span className="bg-white/5 text-white/55 px-2 py-1 rounded-lg">Piscine</span>}
                      {r.parking && <span className="bg-white/5 text-white/55 px-2 py-1 rounded-lg">Parking</span>}
                      {r.balcon && <span className="bg-white/5 text-white/55 px-2 py-1 rounded-lg">Balcon</span>}
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>

        <div className="space-y-4 xl:col-span-1 sticky top-0">
          <Card accent>
            <SectionTitle>Synthèse prix</SectionTitle>
            <div className="space-y-3">
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                <div className="text-xs text-white/35 mb-1">Prix/m² moyen</div>
                <div className="text-lg font-semibold text-white">{displayedAverage} €</div>
              </div>
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                <div className="text-xs text-white/35 mb-1">Fourchette basse</div>
                <div className="text-lg font-semibold text-white">{displayedLow} €</div>
              </div>
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                <div className="text-xs text-white/35 mb-1">Fourchette haute</div>
                <div className="text-lg font-semibold text-white">{displayedHigh} €</div>
              </div>
            </div>
          </Card>

          <Card accent>
            <SectionTitle>Sortie attendue</SectionTitle>
            <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/50">
              La sortie reste toujours limitée à trois données: prix/m² moyen, fourchette basse et fourchette haute. Les autres champs servent seulement à mieux filtrer et rapprocher les comparables scrapés.
            </div>
          </Card>
        </div>
      </div>
    </div>
  )
}
