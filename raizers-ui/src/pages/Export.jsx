import { useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, StatusBadge } from '../components/ui'

const TAB_META = {
  operation:  { icon: '🏢', label: 'Opération',        desc: 'Données opérateur & société' },
  financier:  { icon: '📊', label: 'Bilans financiers', desc: 'Bilans comptables par société' },
  patrimoine: { icon: '👤', label: 'Pappers',           desc: 'Recherche Pappers dirigeants' },
}

export default function Export() {
  const { session } = useSession()
  const [selected, setSelected] = useState(new Set(session.generated))
  const [exportingExcel, setExportingExcel] = useState(false)
  const [exportingWord, setExportingWord] = useState(false)
  const [doneFormat, setDoneFormat] = useState(null)

  const toggle = (id) => setSelected(prev => {
    const next = new Set(prev)
    next.has(id) ? next.delete(id) : next.add(id)
    return next
  })

  async function generateExcel() {
    setExportingExcel(true)
    await new Promise(r => setTimeout(r, 1800))
    setExportingExcel(false)
    setDoneFormat('excel')
  }

  async function generateWord() {
    setExportingWord(true)
    await new Promise(r => setTimeout(r, 1800))
    setExportingWord(false)
    setDoneFormat('word')
  }

  const allTabs = Object.keys(TAB_META)
  const readyTabs = allTabs.filter(t => session.tabs[t] === 'done')
  const pendingTabs = allTabs.filter(t => session.tabs[t] !== 'done')

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
        {readyTabs.length > 0 && (
          <>
            <SectionTitle>Prêts à exporter</SectionTitle>
            <div className="space-y-2 mb-4">
              {readyTabs.map(t => {
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

      {doneFormat ? (
        <Card className="border-emerald-400/20 bg-emerald-400/[0.04] text-center py-6">
          <div className="text-2xl mb-2">✓</div>
          <div className="text-sm font-semibold text-emerald-300 mb-1">
            {doneFormat === 'excel' ? 'Excel généré' : 'Word généré'}
          </div>
          <div className="text-xs text-white/40 mb-4">
            {doneFormat === 'excel'
              ? `rapport_${session.project?.replace(/^\d+\.\s*/, '').replace(/\s+/g, '_')}.xlsx`
              : `rapport_${session.project?.replace(/^\d+\.\s*/, '').replace(/\s+/g, '_')}.docx`}
          </div>
          <Btn variant="secondary" onClick={() => setDoneFormat(null)}>Générer à nouveau</Btn>
        </Card>
      ) : (
        <div className="grid grid-cols-2 gap-3">
          <Btn
            full
            onClick={generateExcel}
            disabled={selected.size === 0 || exportingExcel || exportingWord}
          >
            {exportingExcel ? 'Génération...' : `Générer l'Excel (${selected.size} onglet${selected.size > 1 ? 's' : ''})`}
          </Btn>
          <Btn
            full
            variant="secondary"
            onClick={generateWord}
            disabled={selected.size === 0 || exportingExcel || exportingWord}
          >
            {exportingWord ? 'Génération...' : `Générer le Word (${selected.size} onglet${selected.size > 1 ? 's' : ''})`}
          </Btn>
        </div>
      )}
    </div>
  )
}
