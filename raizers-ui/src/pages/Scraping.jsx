import { useEffect, useMemo, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, Spinner } from '../components/ui'
import { exportScraping, getScrapingCache, getScrapingJob, runScraping } from '../lib/api'

const PROPERTY_TYPES = [
  { value: 'appartement', label: 'Appartement' },
  { value: 'maison', label: 'Maison' },
]

const CONDITION_OPTIONS = [
  { value: '', label: 'Non renseigné' },
  { value: 'neuf', label: 'Neuf / refait à neuf' },
  { value: 'bon état', label: 'Bon état / standard' },
  { value: 'rafraîchissement', label: 'Rafraîchissement' },
  { value: 'travaux importants', label: 'Travaux importants' },
]

const SOURCE_LABELS = {
  consortium_immobilier: 'Consortium Immobilier',
  lesclesdumidi: 'Les Clés du Midi',
  terrain_construction: 'Terrain Construction',
  meilleursagents: 'Meilleurs Agents',
}

const DEFAULT_SCRAPERS = [
  'consortium_immobilier',
  'lesclesdumidi',
  'terrain_construction',
  'meilleursagents',
]

function formatNumber(value, suffix = '') {
  if (value === null || value === undefined || value === '') return '—'
  const text = typeof value === 'number' ? value.toLocaleString('fr-FR') : String(value)
  return suffix ? `${text} ${suffix}` : text
}

function statusLabel(status) {
  if (status === 'ok') return 'OK'
  if (status === 'skipped') return 'Ignoré'
  if (status === 'empty') return 'Vide'
  return 'Erreur'
}

function statusClass(status) {
  if (status === 'ok') return 'bg-emerald-400/10 text-emerald-300'
  if (status === 'skipped') return 'bg-amber-400/10 text-amber-300'
  if (status === 'empty') return 'bg-white/5 text-white/45'
  return 'bg-red-400/10 text-red-300'
}

export default function Scraping() {
  const { session } = useSession()
  const draft = session.immoDraft || {}
  const suggestion = draft.selectedSuggestion || {}

  const [address, setAddress] = useState(suggestion.label ?? draft.addressQuery ?? '')
  const [propertyType, setPropertyType] = useState(draft.propertyType ?? 'appartement')
  const [surface, setSurface] = useState(draft.livingArea ?? '')
  const [terrain, setTerrain] = useState(draft.landArea ?? '')
  const [rooms, setRooms] = useState(draft.rooms ?? '')
  const [city, setCity] = useState(suggestion.city ?? '')
  const [postalCode, setPostalCode] = useState(suggestion.postcode ?? '')
  const [departmentCode, setDepartmentCode] = useState('')

  const [bedrooms, setBedrooms] = useState('')
  const [bathrooms, setBathrooms] = useState('1')
  const [levels, setLevels] = useState('1')
  const [floor, setFloor] = useState('1')
  const [buildingFloors, setBuildingFloors] = useState('1')
  const [balcony, setBalcony] = useState(false)
  const [balconyArea, setBalconyArea] = useState('')
  const [terrace, setTerrace] = useState(false)
  const [terraceArea, setTerraceArea] = useState('')
  const [elevator, setElevator] = useState(false)
  const [cellars, setCellars] = useState('0')
  const [parkingSpaces, setParkingSpaces] = useState('0')
  const [serviceRooms, setServiceRooms] = useState('0')
  const [constructionYear, setConstructionYear] = useState('')
  const [propertyCondition, setPropertyCondition] = useState('')
  const [selectedScrapers, setSelectedScrapers] = useState(DEFAULT_SCRAPERS)

  const [apifyToken, setApifyToken] = useState('')
  const [maEmail, setMaEmail] = useState('')
  const [maPassword, setMaPassword] = useState('')

  const [loading, setLoading] = useState(false)
  const [exporting, setExporting] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(null)

  useEffect(() => {
    const nextDraft = session.immoDraft || {}
    const nextSuggestion = nextDraft.selectedSuggestion || {}
    setAddress(current => current || nextSuggestion.label || nextDraft.addressQuery || '')
    setPropertyType(current => current || nextDraft.propertyType || 'appartement')
    setSurface(current => current || nextDraft.livingArea || '')
    setTerrain(current => current || nextDraft.landArea || '')
    setRooms(current => current || nextDraft.rooms || '')
    setCity(current => current || nextSuggestion.city || '')
    setPostalCode(current => current || nextSuggestion.postcode || '')
  }, [session.immoDraft])

  useEffect(() => {
    if (!session.projectId) return
    let cancelled = false

    getScrapingCache(session.projectId, address).then(data => {
      if (cancelled || !data || Object.keys(data).length === 0) return
      setResult(data)
    }).catch(() => {})

    return () => {
      cancelled = true
    }
  }, [session.projectId])

  const rows = result?.results || []
  const statistics = result?.statistics || null
  const canRun = address.trim() && surface && rooms && selectedScrapers.length > 0 && (propertyType === 'appartement' || terrain !== '')

  const summary = useMemo(() => {
    return {
      average: statistics?.average_price_per_sqm_eur ?? null,
      low: Math.min(...rows.filter(row => row.prix_bas_m2 != null).map(row => row.prix_bas_m2), Infinity),
      high: Math.max(...rows.filter(row => row.prix_haut_m2 != null).map(row => row.prix_haut_m2), -Infinity),
    }
  }, [rows, statistics])

  const displayedLow = Number.isFinite(summary.low) ? summary.low.toLocaleString('fr-FR') : '—'
  const displayedHigh = Number.isFinite(summary.high) ? summary.high.toLocaleString('fr-FR') : '—'

  async function handleRun() {
    if (!canRun) return

    setLoading(true)
    setError('')
    setResult(null)

    try {
      const job = await runScraping({
        project_id: session.projectId || null,
        address: address.trim(),
        scrapers: selectedScrapers,
        property_type: propertyType,
        living_area_sqm: Number(surface),
        rooms: Number(rooms),
        land_area_sqm: propertyType === 'maison' && terrain !== '' ? Number(terrain) : null,
        city: city || null,
        postal_code: postalCode || null,
        department_code: departmentCode || null,
        nb_chambres: bedrooms !== '' ? Number(bedrooms) : null,
        nb_salles_bain: bathrooms !== '' ? Number(bathrooms) : null,
        nb_niveaux: propertyType === 'maison' && levels !== '' ? Number(levels) : null,
        etage: propertyType === 'appartement' && floor !== '' ? Number(floor) : null,
        nb_etages_immeuble: propertyType === 'appartement' && buildingFloors !== '' ? Number(buildingFloors) : null,
        ascenseur: elevator,
        balcon: balcony,
        surface_balcon: balcony && balconyArea !== '' ? Number(balconyArea) : null,
        terrasse: terrace,
        surface_terrasse: terrace && terraceArea !== '' ? Number(terraceArea) : null,
        nb_caves: Number(cellars || 0),
        nb_places_parking: Number(parkingSpaces || 0),
        nb_chambres_service: Number(serviceRooms || 0),
        annee_construction: constructionYear !== '' ? Number(constructionYear) : null,
        etat_bien: propertyCondition || null,
        apify_api_token: apifyToken || null,
        meilleursagents_email: maEmail || null,
        meilleursagents_password: maPassword || null,
      })

      while (true) {
        await new Promise(resolve => setTimeout(resolve, 2000))
        const status = await getScrapingJob(job.job_id)
        if (status.status === 'done') {
          setResult(status.result)
          break
        }
        if (status.status === 'error') {
          throw new Error(status.error || 'Erreur scraping')
        }
      }
    } catch (err) {
      setError(err.message || 'Erreur inconnue')
    } finally {
      setLoading(false)
    }
  }

  function toggleScraper(scraperKey) {
    setSelectedScrapers(current => (
      current.includes(scraperKey)
        ? current.filter(item => item !== scraperKey)
        : [...current, scraperKey]
    ))
  }

  async function handleExport() {
    if (!rows.length) return

    setExporting(true)
    setError('')
    try {
      const { blob, filename } = await exportScraping({
        results: rows,
        property_type: propertyType,
      })
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      window.URL.revokeObjectURL(url)
    } catch (err) {
      setError(err.message || 'Erreur export')
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="w-full">
      <PageHeader
        title="Scraping immobilier"
        description="Agrégation multi-sources sur Consortium Immobilier, Les Clés du Midi, Terrain Construction et Meilleurs Agents."
        status={loading ? 'running' : result ? 'done' : 'idle'}
        action={(
          <div className="flex items-center gap-2">
            <Btn variant="secondary" onClick={handleExport} disabled={!rows.length || exporting}>
              {exporting ? <Spinner /> : 'Exporter Excel'}
            </Btn>
            <Btn onClick={handleRun} disabled={!canRun || loading}>
              {loading ? <Spinner /> : 'Lancer'}
            </Btn>
          </div>
        )}
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card>
            <SectionTitle>Bien à analyser</SectionTitle>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="md:col-span-2">
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Adresse</label>
                <input
                  value={address}
                  onChange={e => setAddress(e.target.value)}
                  placeholder="Ex. 12 rue de Rivoli"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors placeholder:text-white/20"
                />
              </div>

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
                  value={surface}
                  onChange={e => setSurface(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50"
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
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50"
                />
              </div>

              {propertyType === 'maison' && (
                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface terrain (m²)</label>
                  <input
                    type="number"
                    min="0"
                    value={terrain}
                    onChange={e => setTerrain(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50"
                  />
                </div>
              )}

              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Ville</label>
                <input
                  value={city}
                  onChange={e => setCity(e.target.value)}
                  placeholder="Auto-déduit si vide"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>

              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Code postal</label>
                <input
                  value={postalCode}
                  onChange={e => setPostalCode(e.target.value)}
                  placeholder="Auto-déduit si vide"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>

              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Département</label>
                <input
                  value={departmentCode}
                  onChange={e => setDepartmentCode(e.target.value.toUpperCase())}
                  placeholder="Auto-déduit si vide"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>
            </div>
          </Card>

          <Card>
            <SectionTitle>Sites à scraper</SectionTitle>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {DEFAULT_SCRAPERS.map(scraperKey => {
                const checked = selectedScrapers.includes(scraperKey)
                return (
                  <label
                    key={scraperKey}
                    className={`flex items-center gap-3 rounded-xl border px-4 py-3 cursor-pointer transition-colors ${
                      checked
                        ? 'border-cyan-400/30 bg-cyan-400/[0.08]'
                        : 'border-white/10 bg-white/[0.02]'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleScraper(scraperKey)}
                    />
                    <span className="text-sm text-white/85">{SOURCE_LABELS[scraperKey]}</span>
                  </label>
                )
              })}
            </div>
            {selectedScrapers.length === 0 && (
              <p className="text-xs text-red-300 mt-3">Sélectionne au moins un site avant de lancer le scraping.</p>
            )}
          </Card>

          <Card>
            <SectionTitle>Paramètres Meilleurs Agents</SectionTitle>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Chambres</label>
                <input type="number" min="0" value={bedrooms} onChange={e => setBedrooms(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Salles de bain</label>
                <input type="number" min="0" value={bathrooms} onChange={e => setBathrooms(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Année construction</label>
                <input type="number" min="1800" max="2100" value={constructionYear} onChange={e => setConstructionYear(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>

              {propertyType === 'appartement' ? (
                <>
                  <div>
                    <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Étage</label>
                    <input type="number" min="0" value={floor} onChange={e => setFloor(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
                  </div>
                  <div>
                    <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Étages immeuble</label>
                    <input type="number" min="0" value={buildingFloors} onChange={e => setBuildingFloors(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
                  </div>
                </>
              ) : (
                <div>
                  <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Niveaux</label>
                  <input type="number" min="0" value={levels} onChange={e => setLevels(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
                </div>
              )}

              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">État du bien</label>
                <select
                  value={propertyCondition}
                  onChange={e => setPropertyCondition(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 cursor-pointer"
                >
                  {CONDITION_OPTIONS.map(item => (
                    <option key={item.value} value={item.value} className="bg-[#0c1e2e]">
                      {item.label}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Caves</label>
                <input type="number" min="0" value={cellars} onChange={e => setCellars(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Places parking</label>
                <input type="number" min="0" value={parkingSpaces} onChange={e => setParkingSpaces(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Chambres de service</label>
                <input type="number" min="0" value={serviceRooms} onChange={e => setServiceRooms(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50" />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
              <label className="flex items-center gap-2 text-sm text-white/75">
                <input type="checkbox" checked={elevator} onChange={e => setElevator(e.target.checked)} />
                Ascenseur
              </label>
              <label className="flex items-center gap-2 text-sm text-white/75">
                <input type="checkbox" checked={balcony} onChange={e => setBalcony(e.target.checked)} />
                Balcon
              </label>
              <label className="flex items-center gap-2 text-sm text-white/75">
                <input type="checkbox" checked={terrace} onChange={e => setTerrace(e.target.checked)} />
                Terrasse
              </label>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface balcon (m²)</label>
                <input type="number" min="0" value={balconyArea} onChange={e => setBalconyArea(e.target.value)} disabled={!balcony} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 disabled:opacity-40" />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Surface terrasse (m²)</label>
                <input type="number" min="0" value={terraceArea} onChange={e => setTerraceArea(e.target.value)} disabled={!terrace} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 disabled:opacity-40" />
              </div>
            </div>
          </Card>

          <Card>
            <SectionTitle>Credentials Meilleurs Agents / Apify</SectionTitle>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Apify API Token</label>
                <input
                  type="password"
                  value={apifyToken}
                  onChange={e => setApifyToken(e.target.value)}
                  placeholder="apify_api_..."
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Email Meilleurs Agents</label>
                <input
                  type="email"
                  value={maEmail}
                  onChange={e => setMaEmail(e.target.value)}
                  placeholder="email@exemple.com"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>
              <div>
                <label className="block text-xs text-white/45 mb-1.5 uppercase tracking-widest font-medium">Mot de passe Meilleurs Agents</label>
                <input
                  type="password"
                  value={maPassword}
                  onChange={e => setMaPassword(e.target.value)}
                  placeholder="••••••••"
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 placeholder:text-white/20"
                />
              </div>
            </div>
          </Card>

          {loading && (
            <Card className="flex items-center gap-3 text-white/60 text-sm">
              <Spinner /> Scraping en cours...
            </Card>
          )}

          {error && (
            <Card className="border-red-400/20 bg-red-400/[0.03]">
              <p className="text-sm text-red-300">{error}</p>
            </Card>
          )}

          {rows.length > 0 && (
            <Card accent>
              <SectionTitle>Résultats par source</SectionTitle>
              <div className="overflow-x-auto">
                <table className="w-full min-w-[980px] text-sm table-auto">
                  <thead>
                    <tr className="text-xs text-white/35 uppercase tracking-wider border-b border-white/[0.06]">
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Source</th>
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Statut</th>
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Localisation</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Bas €/m²</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Moyen €/m²</th>
                      <th className="pb-2 pr-4 text-right font-medium whitespace-nowrap">Haut €/m²</th>
                      <th className="pb-2 pr-4 text-left font-medium whitespace-nowrap">Méthode</th>
                      <th className="pb-2 text-left font-medium whitespace-nowrap">Erreur</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.04]">
                    {rows.map(row => (
                      <tr key={row.source}>
                        <td className="py-3 pr-4 text-white/80 font-medium whitespace-nowrap">{SOURCE_LABELS[row.source] || row.source}</td>
                        <td className="py-3 pr-4 whitespace-nowrap">
                          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${statusClass(row.status)}`}>
                            {statusLabel(row.status)}
                          </span>
                        </td>
                        <td className="py-3 pr-4 text-white/60">{row.localisation || '—'}</td>
                        <td className="py-3 pr-4 text-right text-white/70 whitespace-nowrap">{formatNumber(row.prix_bas_m2, '€')}</td>
                        <td className="py-3 pr-4 text-right text-cyan-300 whitespace-nowrap">{formatNumber(row.prix_moyen_m2, '€')}</td>
                        <td className="py-3 pr-4 text-right text-white/70 whitespace-nowrap">{formatNumber(row.prix_haut_m2, '€')}</td>
                        <td className="py-3 pr-4 text-white/50 whitespace-nowrap">{row.method_used || '—'}</td>
                        <td className="py-3 text-red-300/80">{row.error || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>

        <div className="space-y-4 xl:col-span-1 sticky top-0">
          <Card accent>
            <SectionTitle>Synthèse prix</SectionTitle>
            <div className="space-y-3">
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                <div className="text-xs text-white/35 mb-1">Prix/m² moyen global</div>
                <div className="text-lg font-semibold text-white">{formatNumber(summary.average, '€')}</div>
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
            <SectionTitle>Exécution</SectionTitle>
            <div className="space-y-3 text-sm text-white/55">
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                Sources demandées: {result?.sources_requested?.length ?? 4}
              </div>
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                Sources OK: {statistics?.sources_succeeded ?? 0}
              </div>
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                Sources ignorées: {statistics?.sources_skipped ?? 0}
              </div>
              <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                Sources en erreur: {statistics?.sources_failed ?? 0}
              </div>
            </div>
          </Card>

          <Card accent>
            <SectionTitle>Règles</SectionTitle>
            <div className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-4 text-sm text-white/50">
              `terrain_construction` est lancé uniquement pour les maisons. Les champs de confort servent principalement à enrichir Meilleurs Agents.
            </div>
          </Card>
        </div>
      </div>
    </div>
  )
}
