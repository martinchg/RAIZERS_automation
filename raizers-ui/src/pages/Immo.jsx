import { useEffect, useRef, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, Spinner } from '../components/ui'
import { compareImmo, getImmoDraft, getImmoSuggestions, saveImmoDraft } from '../lib/api'

const PROPERTY_TYPES = [
  { value: 'appartement', label: 'Appartement' },
  { value: 'maison', label: 'Maison' },
]

function formatNumber(value, suffix = '') {
  if (value === null || value === undefined || value === '') return '—'
  const text = typeof value === 'number' ? value.toLocaleString('fr-FR') : String(value)
  return suffix ? `${text} ${suffix}` : text
}

export default function Immo() {
  const { session, dispatch } = useSession()
  const draft = session.immoDraft || {}
  const [addressQuery, setAddressQuery] = useState(draft.addressQuery ?? '')
  const [suggestions, setSuggestions] = useState([])
  const [selectedSuggestion, setSelectedSuggestion] = useState(draft.selectedSuggestion ?? null)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [propertyType, setPropertyType] = useState(draft.propertyType ?? 'appartement')
  const [livingArea, setLivingArea] = useState(draft.livingArea ?? '80')
  const [rooms, setRooms] = useState(draft.rooms ?? '4')
  const [landArea, setLandArea] = useState(draft.landArea ?? '100')
  const [searchRadius, setSearchRadius] = useState(draft.searchRadius ?? '225')
  const [apiMinYear, setApiMinYear] = useState(draft.apiMinYear ?? '2024')
  const [loading, setLoading] = useState(false)
  const [loadingSuggestions, setLoadingSuggestions] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(session.immoResult ?? null)
  const hydratedRef = useRef(false)

  useEffect(() => {
    setResult(session.immoResult ?? null)
  }, [session.immoResult])

  useEffect(() => {
    if (!session.projectId || hydratedRef.current) return
    let cancelled = false
    getImmoDraft(session.projectId).then(saved => {
      if (cancelled || !saved || Object.keys(saved).length === 0) return
      hydratedRef.current = true
      if (saved.addressQuery != null) setAddressQuery(saved.addressQuery)
      if (saved.selectedSuggestion != null) setSelectedSuggestion(saved.selectedSuggestion)
      if (saved.propertyType != null) setPropertyType(saved.propertyType)
      if (saved.livingArea != null) setLivingArea(saved.livingArea)
      if (saved.rooms != null) setRooms(saved.rooms)
      if (saved.landArea != null) setLandArea(saved.landArea)
      if (saved.searchRadius != null) setSearchRadius(saved.searchRadius)
      if (saved.apiMinYear != null) setApiMinYear(saved.apiMinYear)
      dispatch({ type: 'SET_IMMO_DRAFT', immoDraft: saved })
    }).catch(() => {})
    return () => { cancelled = true }
  }, [session.projectId, dispatch])

  useEffect(() => {
    dispatch({
      type: 'SET_IMMO_DRAFT',
      immoDraft: {
        addressQuery,
        selectedSuggestion,
        propertyType,
        livingArea,
        rooms,
        landArea,
        searchRadius,
        apiMinYear,
      },
    })
  }, [addressQuery, apiMinYear, dispatch, landArea, livingArea, propertyType, rooms, searchRadius, selectedSuggestion])

  useEffect(() => {
    const query = addressQuery.trim()
    if (query.length < 1) {
      setSuggestions([])
      setShowSuggestions(false)
      return undefined
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(async () => {
      try {
        setLoadingSuggestions(true)
        const data = await getImmoSuggestions(query, 6, controller.signal)
        setSuggestions(data.items || [])
        setShowSuggestions(true)
      } catch (err) {
        if (err.name !== 'AbortError') {
          setSuggestions([])
        }
      } finally {
        setLoadingSuggestions(false)
      }
    }, 120)

    return () => {
      controller.abort()
      clearTimeout(timeoutId)
    }
  }, [addressQuery])

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
      const data = await compareImmo(payload)
      setResult(data)
      dispatch({ type: 'SET_IMMO_RESULT', immoResult: data })
      if (session.projectId) {
        const draftToSave = { addressQuery, selectedSuggestion, propertyType, livingArea, rooms, landArea, searchRadius, apiMinYear }
        saveImmoDraft(session.projectId, draftToSave).catch(() => {})
      }
    } catch (err) {
      setError(err.message || 'Erreur inconnue')
    } finally {
      setLoading(false)
    }
  }

  const statistics = result?.statistics ?? null
  const comparables = result?.comparables ?? []

  return (
    <div className="w-full">
      <PageHeader
        title="Comparateur - DVF"
        description="Comparatif DVF avec géolocalisation, rayon de recherche et critères issus de l'outil immo."
        status={loading ? 'running' : result ? 'done' : 'idle'}
      />

      <div className="space-y-5">
          <Card>
            <SectionTitle>Bien à comparer</SectionTitle>
            <div className="space-y-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Adresse</label>
                <div className="relative">
                  <input
                    value={addressQuery}
                    onChange={e => {
                      setAddressQuery(e.target.value)
                      setSelectedSuggestion(null)
                    }}
                    onFocus={() => {
                      if (suggestions.length > 0 || loadingSuggestions) {
                        setShowSuggestions(true)
                      }
                    }}
                    onBlur={() => {
                      window.setTimeout(() => setShowSuggestions(false), 120)
                    }}
                    placeholder="Ex. 13 Rue Victor Hugo"
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                  />
                  {showSuggestions && addressQuery.trim().length >= 1 && (
                    <div className="absolute z-20 left-0 right-0 top-[calc(100%+0.4rem)] rounded-2xl border border-white/10 bg-[#0b1823] shadow-2xl shadow-black/40 overflow-hidden">
                      {loadingSuggestions ? (
                        <div className="flex items-center gap-2 px-4 py-3 text-sm text-white/45">
                          <Spinner />
                          Chargement des suggestions...
                        </div>
                      ) : suggestions.length > 0 ? (
                        <div className="max-h-72 overflow-y-auto py-1">
                          {suggestions.map(item => (
                            <button
                              key={item.id}
                              type="button"
                              onMouseDown={() => {
                                setAddressQuery(item.label)
                                setSelectedSuggestion(item)
                                setShowSuggestions(false)
                              }}
                              className={`w-full text-left px-4 py-3 text-sm transition-colors ${
                                selectedSuggestion?.id === item.id
                                  ? 'bg-cyan-400/10 text-cyan-100'
                                  : 'text-white/85 hover:bg-white/[0.05]'
                              }`}
                            >
                              {item.label}
                            </button>
                          ))}
                        </div>
                      ) : (
                        <div className="px-4 py-3 text-xs text-white/35">
                          Aucune suggestion trouvée. Tu peux quand même lancer la recherche avec l'adresse saisie.
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>

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
                <table className="w-full min-w-[1120px] text-sm table-auto">
                  <thead>
                    <tr className="text-xs text-white/35 uppercase tracking-wider border-b border-white/[0.06]">
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Retenu</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Score</th>
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Raison</th>
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap min-w-[320px]">Adresse</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Surface</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Pièces</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Date vente</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Distance</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap min-w-[120px]">Prix</th>
                      <th className="pb-2 text-right font-medium whitespace-nowrap min-w-[110px]">€/m²</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.04]">
                    {comparables.map((row, index) => (
                      <tr key={index}>
                        <td className="py-3 pr-4 text-left whitespace-nowrap align-middle">
                          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${row.Retenu === 'Oui' ? 'bg-emerald-400/10 text-emerald-300' : 'bg-white/5 text-white/45'}`}>
                            {row.Retenu}
                          </span>
                        </td>
                        <td className="py-3 pr-4 text-right text-cyan-300 whitespace-nowrap align-middle">{row.Score ?? '—'}</td>
                        <td className="py-3 pr-4 text-left text-white/45 whitespace-nowrap align-middle">{row.Raison ?? '—'}</td>
                        <td className="py-3 pr-4 text-white/75 align-middle">{row.Adresse ?? '—'}</td>
                        <td className="py-3 pr-4 text-right text-white/60 whitespace-nowrap align-middle">{formatNumber(row['Surface habitable'], 'm²')}</td>
                        <td className="py-3 pr-4 text-right text-white/60 whitespace-nowrap align-middle">{formatNumber(row.Pièces)}</td>
                        <td className="py-3 pr-4 text-right text-white/50 whitespace-nowrap align-middle">{row['Date de vente'] ?? '—'}</td>
                        <td className="py-3 pr-4 text-right text-white/50 whitespace-nowrap align-middle">{formatNumber(row['Distance (m)'], 'm')}</td>
                        <td className="py-3 pr-4 text-right text-white/80 whitespace-nowrap align-middle">{formatNumber(row['Prix de vente'], '€')}</td>
                        <td className="py-3 text-right text-cyan-300 font-medium whitespace-nowrap align-middle">{formatNumber(row['Prix par m²'], '€')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
          <Card accent>
            <SectionTitle>Synthèse DVF</SectionTitle>
            {statistics ? (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                {[
                  { label: 'Prix/m² moyen', value: formatNumber(statistics.average_price_per_sqm_eur, '€') },
                  { label: 'Prix/m² minimum', value: formatNumber(statistics.min_price_per_sqm_eur, '€') },
                  { label: 'Prix/m² maximum', value: formatNumber(statistics.max_price_per_sqm_eur, '€') },
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
  )
}
