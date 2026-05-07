import { useEffect, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, StatusBadge } from '../components/ui'
import { generateExportReport, getExportStatus } from '../lib/api'

const TAB_META = {
  operation:  { icon: '🏢', label: 'Opération',        desc: 'Données opérateur & société' },
  financier:  { icon: '📊', label: 'Bilans financiers', desc: 'Bilans comptables par société' },
  patrimoine: { icon: '👤', label: 'Pappers',           desc: 'Recherche Pappers dirigeants' },
  immo:       { icon: '📍', label: 'Comparateur - DVF', desc: 'Comparables immobiliers' },
  scraping:   { icon: '🔍', label: 'Scraping',          desc: 'Prix au m² multi-sources' },
}

export default function Export() {
  const { session } = useSession()
  const [selected, setSelected] = useState(new Set(session.generated))
  const [exportingExcel, setExportingExcel] = useState(false)
  const [exportStatus, setExportStatus] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!session.projectId) return

    let cancelled = false

    async function loadStatus() {
      try {
        const data = await getExportStatus(session.projectId)
        if (cancelled) return
        setExportStatus(data)
        setSelected(prev => {
          const fromStatus = Object.entries(data.tabs || {})
            .filter(([, ready]) => ready)
            .map(([tab]) => tab)
          const immoReady = Boolean(session.immoResult)
          const autoReady = new Set([
            ...fromStatus,
            ...(immoReady ? ['immo'] : []),
          ])
          const merged = new Set([...prev, ...autoReady])
          return merged
        })
      } catch (err) {
        if (!cancelled) setError(err.message || "Impossible de charger l'état export")
      }
    }

    loadStatus()
    return () => {
      cancelled = true
    }
  }, [session.projectId, session.immoResult])

  const toggle = (id) => setSelected(prev => {
    const next = new Set(prev)
    next.has(id) ? next.delete(id) : next.add(id)
    return next
  })

  async function generateExcel() {
    setExportingExcel(true)
    setError('')
    try {
      const data = await generateExportReport({
        project_id: session.projectId,
        tabs: Array.from(selected),
        immo_result: selected.has('immo') ? session.immoResult : null,
      })
      setExportStatus(previous => ({
        ...(previous || {}),
        ...data,
        report_exists: data.report_exists,
      }))
    } catch (err) {
      setError(err.message || "Impossible de générer le rapport Excel")
    } finally {
      setExportingExcel(false)
    }
  }

  const allTabs = Object.keys(TAB_META)
  const sessionDoneTabs = allTabs.filter(t =>
    t === 'immo' ? Boolean(session.immoResult) : session.tabs[t] === 'done'
  )
  const cachedTabs = allTabs.filter(t =>
    !sessionDoneTabs.includes(t) && t !== 'immo' && exportStatus?.tabs?.[t]
  )
  const pendingTabs = allTabs.filter(t => !sessionDoneTabs.includes(t) && !cachedTabs.includes(t))
  const reportDownloadUrl = exportStatus?.report_download_url ?? (session.projectId ? `/api/audit/projects/${session.projectId}/export/report` : null)
  const reportFilename = exportStatus?.report_filename ?? 'rapport.xlsx'

  return (
    <div className="max-w-2xl">
      <PageHeader
        title="Exporter"
        description="Consolider les extractions de la session dans un fichier Excel."
      />

      {/* Stats session */}
      <div className="grid grid-cols-3 gap-3 mb-5">
        {[
          { label: 'Onglets générés',  value: session.generated.length },
          { label: 'Onglets sélectionnés', value: selected.size },
          { label: 'Dossier',          value: session.project?.replace(/^\d+\.\s*/, '') ?? '—' },
        ].map(s => (
          <Card key={s.label} className="text-center">
            <div className="text-xl font-bold text-white truncate">{s.value}</div>
            <div className="text-xs text-white/40 mt-1">{s.label}</div>
          </Card>
        ))}
      </div>

      <Card className="mb-4">
        {sessionDoneTabs.length > 0 && (
          <>
            <SectionTitle>Extraits cette session</SectionTitle>
            <div className="space-y-2 mb-4">
              {sessionDoneTabs.map(t => {
                const meta = TAB_META[t]
                const checked = selected.has(t)
                return (
                  <label key={t} onClick={() => toggle(t)} className="flex items-center gap-3 px-4 py-3 rounded-xl border cursor-pointer transition-all select-none" style={{ borderColor: checked ? 'rgba(77,200,232,0.2)' : 'rgba(255,255,255,0.06)', background: checked ? 'rgba(77,200,232,0.04)' : 'rgba(255,255,255,0.01)' }}>
                    <div className={`w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors ${checked ? 'bg-cyan-500 border-cyan-500' : 'border-white/20'}`}>
                      {checked && <svg className="w-2.5 h-2.5 text-[#07111A]" fill="none" viewBox="0 0 10 8"><path d="M1 4l3 3 5-6" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>}
                    </div>
                    <span className="text-base">{meta.icon}</span>
                    <div className="flex-1">
                      <div className="text-sm font-medium text-white">{meta.label}</div>
                      <div className="text-xs text-white/40">{meta.desc}</div>
                    </div>
                    <StatusBadge status="done" />
                  </label>
                )
              })}
            </div>
          </>
        )}

        {cachedTabs.length > 0 && (
          <>
            <SectionTitle>Disponibles en cache</SectionTitle>
            <div className="space-y-2 mb-4">
              {cachedTabs.map(t => {
                const meta = TAB_META[t]
                const checked = selected.has(t)
                return (
                  <label key={t} onClick={() => toggle(t)} className="flex items-center gap-3 px-4 py-3 rounded-xl border cursor-pointer transition-all select-none" style={{ borderColor: checked ? 'rgba(251,191,36,0.15)' : 'rgba(255,255,255,0.04)', background: checked ? 'rgba(251,191,36,0.04)' : 'transparent' }}>
                    <div className={`w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors ${checked ? 'bg-amber-400 border-amber-400' : 'border-white/20'}`}>
                      {checked && <svg className="w-2.5 h-2.5 text-[#07111A]" fill="none" viewBox="0 0 10 8"><path d="M1 4l3 3 5-6" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>}
                    </div>
                    <span className="text-base">{meta.icon}</span>
                    <div className="flex-1">
                      <div className="text-sm font-medium text-white/80">{meta.label}</div>
                      <div className="text-xs text-white/40">{meta.desc}</div>
                    </div>
                    <StatusBadge status="cached" />
                  </label>
                )
              })}
            </div>
          </>
        )}

        {pendingTabs.length > 0 && (
          <>
            <SectionTitle>Non générés</SectionTitle>
            <div className="space-y-2">
              {pendingTabs.map(t => {
                const meta = TAB_META[t]
                return (
                  <div key={t} className="flex items-center gap-3 px-4 py-3 rounded-xl border border-white/[0.04] opacity-40">
                    <span className="text-base">{meta.icon}</span>
                    <div className="flex-1">
                      <div className="text-sm text-white">{meta.label}</div>
                      <div className="text-xs text-white/40">{meta.desc}</div>
                    </div>
                    <StatusBadge status={session.tabs[t]} />
                  </div>
                )
              })}
            </div>
          </>
        )}
      </Card>

      {error && (
        <Card className="border-red-400/20 bg-red-400/[0.04] text-sm text-red-200 mb-4">
          {error}
        </Card>
      )}

      <div className="space-y-4">
        <Card className={exportStatus?.report_exists ? 'border-emerald-400/20 bg-emerald-400/[0.04]' : ''}>
          <SectionTitle>Rapport Final</SectionTitle>
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-white">
                {exportStatus?.report_exists ? `${reportFilename} disponible` : `${reportFilename} à générer`}
              </div>
              <div className="text-xs text-white/40 mt-1">
                Excel consolidé avec uniquement les onglets sélectionnés.
              </div>
            </div>
            <div className="flex items-center gap-3">
              {exportStatus?.report_exists && reportDownloadUrl && (
                <a
                  href={reportDownloadUrl}
                  className="py-3 px-5 rounded-xl text-sm font-semibold bg-white/5 border border-white/14 text-white hover:bg-white/10 transition-all"
                >
                  Télécharger
                </a>
              )}
              <Btn
                onClick={generateExcel}
                disabled={selected.size === 0 || exportingExcel || !session.projectId}
              >
                {exportingExcel ? 'Génération...' : "Générer l'Excel"}
              </Btn>
            </div>
          </div>
        </Card>

        <Card>
          <SectionTitle>Résumés Par Page</SectionTitle>
          <div className="space-y-2">
            {allTabs.map(tab => (
              <div key={tab} className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm text-white">{TAB_META[tab].label}</div>
                  <StatusBadge status={[...sessionDoneTabs, ...cachedTabs].includes(tab) ? 'done' : 'idle'} />
                </div>
                <div className="text-xs text-white/40 mt-1">
                  {tab === 'operation' && `Champs: ${exportStatus?.summaries?.operation?.answered ?? 0}/${exportStatus?.summaries?.operation?.asked ?? 0}`}
                  {tab === 'patrimoine' && `Personnes: ${exportStatus?.summaries?.patrimoine?.people_detected ?? 0} · Sociétés: ${exportStatus?.summaries?.patrimoine?.companies_found ?? 0}`}
                  {tab === 'financier' && `Sociétés: ${exportStatus?.summaries?.financier?.companies_detected ?? 0} · Bilans prêts: ${exportStatus?.summaries?.financier?.bilan_exports_ready ?? 0}`}
                  {tab === 'immo' && `Comparables: ${session.immoResult?.comparables?.length ?? 0}`}
                  {tab === 'scraping' && `Sources: ${exportStatus?.summaries?.scraping?.sources_total ?? 0} · OK: ${exportStatus?.summaries?.scraping?.sources_ok ?? 0}`}
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  )
}
