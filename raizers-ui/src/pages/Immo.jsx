import { useEffect, useState } from 'react'
import { Btn, Card, PageHeader, SectionTitle, Spinner } from '../components/ui'

const PROPERTY_TYPES = [
  { value: 'appartement', label: 'Appartement' },
  { value: 'maison', label: 'Maison' },
]

function formatCoord(value) {
  return typeof value === 'number'
    ? value.toLocaleString('fr-FR', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
    : '—'
}

function formatNumber(value, suffix = '') {
  if (value === null || value === undefined || value === '') return '—'
  const text = typeof value === 'number' ? value.toLocaleString('fr-FR') : String(value)
  return suffix ? `${text} ${suffix}` : text
}

export default function Immo() {
  const [addressQuery, setAddressQuery] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [selectedSuggestionId, setSelectedSuggestionId] = useState(null)
  const [propertyType, setPropertyType] = useState('appartement')
  const [livingArea, setLivingArea] = useState('80')
  const [rooms, setRooms] = useState('4')
  const [landArea, setLandArea] = useState('100')
  const [searchRadius, setSearchRadius] = useState('225')
  const [apiMinYear, setApiMinYear] = useState('2024')
  const [loading, setLoading] = useState(false)
  const [loadingSuggestions, setLoadingSuggestions] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(null)

  useEffect(() => {
    const query = addressQuery.trim()
    if (query.length < 3) {
      setSuggestions([])
      return undefined
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(async () => {
      try {
        setLoadingSuggestions(true)
        const response = await fetch(`/api/immo/suggestions?q=${encodeURIComponent(query)}&limit=6`, {
          signal: controller.signal,
        })
        const data = await response.json()
        if (!response.ok) {
          throw new Error(data.detail || 'Erreur de suggestions')
        }
        setSuggestions(data.items || [])
      } catch (err) {
        if (err.name !== 'AbortError') {
          setSuggestions([])
        }
      } finally {
        setLoadingSuggestions(false)
      }
    }, 250)

    return () => {
      controller.abort()
      clearTimeout(timeoutId)
    }
  }, [addressQuery])

  const selectedSuggestion = suggestions.find(item => item.id === selectedSuggestionId) ?? null

  async function search() {
    if (!addressQuery.trim() || !livingArea || !rooms) return
    if (propertyType === 'maison' && !landArea) return

    const payload = {
      address: selectedSuggestion?.label ?? addressQuery.trim(),
      property_type: propertyType,
      living_area_sqm: Number(livingArea),
      rooms: Number(rooms),
      land_area_sqm: propertyType === 'maison' && landArea !== '' ? Number(landArea) : null,
      search_radius_m: Number(searchRadius),
      api_min_year: apiMinYear !== '' ? Number(apiMinYear) : null,
    }

    setLoading(true)
    setError('')
    setResult(null)

    try {
      const response = await fetch('/api/immo/compare', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      })
      const data = await response.json()
      if (!response.ok) {
        throw new Error(
          Array.isArray(data.detail)
            ? data.detail.map(item => item.msg).join(', ')
            : (data.detail || 'Erreur DVF'),
        )
      }
      setResult(data)
    } catch (err) {
      setError(err.message || 'Erreur inconnue')
    } finally {
      setLoading(false)
    }
  }

  const subject = result?.subject ?? null
  const statistics = result?.statistics ?? null
  const comparables = result?.comparables ?? []

  return (
    <div className="w-full">
      <PageHeader
        title="Comparateur - DVF"
        description="Comparatif DVF avec géolocalisation, rayon de recherche et critères issus de l'outil immo."
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-5 xl:col-span-2">
          <Card>
            <SectionTitle>Bien à comparer</SectionTitle>
            <div className="space-y-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Adresse</label>
                <input
                  value={addressQuery}
                  onChange={e => {
                    setAddressQuery(e.target.value)
                    setSelectedSuggestionId(null)
                  }}
                  placeholder="Ex. 13 Rue Victor Hugo"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                />
              </div>

              {addressQuery.trim().length >= 3 && (
                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Suggestions d'adresses</label>
                  {loadingSuggestions ? (
                    <div className="flex items-center gap-2 text-sm text-white/45 px-1 py-2">
                      <Spinner />
                      Chargement des suggestions...
                    </div>
                  ) : suggestions.length > 0 ? (
                    <select
                      value={selectedSuggestionId ?? ''}
                      onChange={e => setSelectedSuggestionId(e.target.value || null)}
                      className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 cursor-pointer"
                    >
                      <option value="" className="bg-[#0c1e2e]">Choisis une suggestion si besoin</option>
                      {suggestions.map(item => (
                        <option key={item.id} value={item.id} className="bg-[#0c1e2e]">
                          {item.label}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <p className="text-xs text-white/35">Aucune suggestion trouvée. Tu peux quand même lancer la recherche avec l'adresse saisie.</p>
                  )}
                </div>
              )}

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Type de bien</label>
                  <select
                    value={propertyType}
                    onChange={e => setPropertyType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 cursor-pointer"
                  >
                    {PROPERTY_TYPES.map(item => (
                      <option key={item.value} value={item.value} className="bg-[#0c1e2e]">
                        {item.label}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface habitable (m²)</label>
                  <input
                    type="number"
                    min="1"
                    value={livingArea}
                    onChange={e => setLivingArea(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
                  />
                </div>

                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Nombre de pièces</label>
                  <input
                    type="number"
                    min="1"
                    max="20"
                    value={rooms}
                    onChange={e => setRooms(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
                  />
                </div>

                {propertyType === 'maison' && (
                  <div>
                    <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface terrain (m²)</label>
                    <input
                      type="number"
                      min="0"
                      value={landArea}
                      onChange={e => setLandArea(e.target.value)}
                      className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
                    />
                  </div>
                )}
              </div>
            </div>
          </Card>

          <Card>
            <SectionTitle>Recherche</SectionTitle>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Rayon de recherche (~m)</label>
                <input
                  type="number"
                  min="50"
                  max="5000"
                  step="25"
                  value={searchRadius}
                  onChange={e => setSearchRadius(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
                />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Année min API</label>
                <input
                  type="number"
                  min="2000"
                  max="2025"
                  step="1"
                  value={apiMinYear}
                  onChange={e => setApiMinYear(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors"
                />
              </div>
            </div>

            <Btn
              onClick={search}
              disabled={!addressQuery.trim() || !livingArea || !rooms || (propertyType === 'maison' && !landArea) || loading}
              full
            >
              {loading ? 'Recherche en cours...' : 'Lancer comparatif'}
            </Btn>
          </Card>

          {loading && (
            <Card className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Interrogation DVF et géolocalisation...
            </Card>
          )}

          {error && (
            <Card className="border-red-400/20 bg-red-400/[0.03]">
              <p className="text-sm text-red-300">{error}</p>
            </Card>
          )}

          {comparables.length > 0 && (
            <Card accent>
              <SectionTitle>Transactions comparables</SectionTitle>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-xs text-white/35 uppercase tracking-wider border-b border-white/[0.06]">
                      <th className="pb-2 text-left font-medium">Retenu</th>
                      <th className="pb-2 text-right font-medium">Score</th>
                      <th className="pb-2 text-left font-medium">Raison</th>
                      <th className="pb-2 text-left font-medium">Adresse</th>
                      <th className="pb-2 text-right font-medium">Surface</th>
                      <th className="pb-2 text-right font-medium">Pièces</th>
                      <th className="pb-2 text-right font-medium">Distance</th>
                      <th className="pb-2 text-right font-medium">Prix</th>
                      <th className="pb-2 text-right font-medium">€/m²</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.04]">
                    {comparables.map((row, index) => (
                      <tr key={index}>
                        <td className="py-2.5 text-left">
                          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${row.Retenu === 'Oui' ? 'bg-emerald-400/10 text-emerald-300' : 'bg-white/5 text-white/45'}`}>
                            {row.Retenu}
                          </span>
                        </td>
                        <td className="py-2.5 text-right text-cyan-300">{row.Score ?? '—'}</td>
                        <td className="py-2.5 text-left text-white/45">{row.Raison ?? '—'}</td>
                        <td className="py-2.5 text-white/75 max-w-[260px] truncate">{row.Adresse ?? '—'}</td>
                        <td className="py-2.5 text-right text-white/60">{formatNumber(row['Surface habitable'], 'm²')}</td>
                        <td className="py-2.5 text-right text-white/60">{formatNumber(row.Pièces)}</td>
                        <td className="py-2.5 text-right text-white/50">{formatNumber(row['Distance (m)'], 'm')}</td>
                        <td className="py-2.5 text-right text-white/80">{formatNumber(row['Prix de vente'], '€')}</td>
                        <td className="py-2.5 text-right text-cyan-300 font-medium">{formatNumber(row['Prix par m²'], '€')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>

        <div className="space-y-4 xl:col-span-1">
          <Card accent className="sticky top-0">
            <SectionTitle>Bien cible</SectionTitle>
            {subject ? (
              <div className="space-y-3 text-sm">
                <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                  <div className="text-xs text-white/35 mb-1">Adresse retenue</div>
                  <div className="text-white/90 font-medium">{subject.normalized_address}</div>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Type</div>
                    <div className="text-white/85">{subject.property_type}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Pièces</div>
                    <div className="text-white/85">{formatNumber(subject.rooms)}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Surface habitable</div>
                    <div className="text-white/85">{formatNumber(subject.living_area_sqm, 'm²')}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Terrain</div>
                    <div className="text-white/85">{formatNumber(subject.land_area_sqm, 'm²')}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Ville</div>
                    <div className="text-white/85">{subject.city ?? '—'}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Code postal</div>
                    <div className="text-white/85">{subject.postcode ?? '—'}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Latitude</div>
                    <div className="text-white/85">{formatCoord(subject.latitude)}</div>
                  </div>
                  <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">Longitude</div>
                    <div className="text-white/85">{formatCoord(subject.longitude)}</div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
                Le bien cible et sa géolocalisation s'afficheront ici après lancement du comparatif.
              </div>
            )}
          </Card>

          <Card accent>
            <SectionTitle>Synthèse DVF</SectionTitle>
            {statistics ? (
              <div className="grid grid-cols-1 gap-3">
                {[
                  { label: 'Comparables trouvés', value: formatNumber(statistics.comparables_found) },
                  { label: 'Comparables retenus', value: formatNumber(statistics.comparables_retained) },
                  { label: 'Comparables exclus', value: formatNumber(statistics.comparables_excluded) },
                  { label: 'Prix/m² moyen', value: formatNumber(statistics.average_price_per_sqm_eur, '€') },
                  { label: 'Prix/m² médian', value: formatNumber(statistics.median_price_per_sqm_eur, '€') },
                  { label: 'Prix/m² minimum', value: formatNumber(statistics.min_price_per_sqm_eur, '€') },
                  { label: 'Prix/m² maximum', value: formatNumber(statistics.max_price_per_sqm_eur, '€') },
                  { label: 'Rayon utilisé', value: formatNumber(statistics.search_radius_m_used, 'm') },
                  { label: 'Année min API', value: formatNumber(statistics.api_min_year_used) },
                ].map(item => (
                  <div key={item.label} className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                    <div className="text-xs text-white/35 mb-1">{item.label}</div>
                    <div className="text-lg font-semibold text-white">{item.value}</div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/45">
                Lance un comparatif pour récupérer la synthèse DVF et les comparables réels.
              </div>
            )}
          </Card>
        </div>
      </div>
    </div>
  )
}
