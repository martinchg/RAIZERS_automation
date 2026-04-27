import { useMemo, useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Card, PageHeader, SectionTitle, ResultRow, Spinner } from '../components/ui'

const FAKE_COMPANIES = [
  {
    id: 'sci-rue-loge',
    name: 'SCI Rue de la Loge',
    filesByPeriod: {
      'Bilan année N': ['Liasse fiscale 2023.pdf', 'Bilan SCI Rue de la Loge.xlsx'],
      'Bilan année N-1': ['Liasse fiscale 2022.pdf', 'Balance SCI Rue de la Loge.xlsx'],
    },
  },
  {
    id: 'promotion-midi',
    name: 'SARL Promotion Midi',
    filesByPeriod: {
      'Bilan année N': ['Compte de resultat Promotion Midi.pdf', 'Bilan Promotion Midi 2023.xlsx'],
      'Bilan année N-1': ['Compte de resultat Promotion Midi 2022.pdf'],
    },
  },
  {
    id: 'fonciere-sud',
    name: 'SAS Foncière Sud',
    filesByPeriod: {
      'Bilan année N': ['Grand livre Fonciere Sud.xlsx', 'Balance generale 2023.xlsx'],
      'Bilan année N-1': ['Balance generale 2022.xlsx', 'Compte de resultat Fonciere Sud.pdf'],
    },
  },
]

export default function Financier() {
  const { session, dispatch } = useSession()
  const status = session.tabs.financier
  const [openCompanyId, setOpenCompanyId] = useState(FAKE_COMPANIES[0].id)
  const [selectedFiles, setSelectedFiles] = useState(() =>
    Object.fromEntries(
      FAKE_COMPANIES.map(company => [
        company.id,
        Object.fromEntries(
          Object.entries(company.filesByPeriod).map(([period, files]) => [period, files[0] ?? '']),
        ),
      ]),
    ),
  )
  const [results, setResults] = useState(null)

  const selectedSummary = useMemo(
    () => FAKE_COMPANIES.map(company => ({
      name: company.name,
      files: Object.entries(company.filesByPeriod)
        .map(([period, files]) => ({
          period,
          file: selectedFiles[company.id]?.[period] || files[0] || '',
        }))
        .filter(item => item.file),
    })),
    [selectedFiles],
  )

  const totalSelectedFiles = selectedSummary.reduce((count, company) => count + company.files.length, 0)

  function selectFile(companyId, period, fileName) {
    setSelectedFiles(previous => ({
      ...previous,
      [companyId]: {
        ...previous[companyId],
        [period]: fileName,
      },
    }))
  }

  async function extract() {
    dispatch({ type: 'TAB_START', tab: 'financier' })
    await new Promise(r => setTimeout(r, 2800))
    setResults(selectedSummary.map(company => ({
      icon: '📊',
      label: company.name,
      value: company.files.length > 0 ? `${company.files.length} fichier${company.files.length > 1 ? 's' : ''} sélectionné${company.files.length > 1 ? 's' : ''}` : 'Aucun fichier retenu',
      ok: company.files.length > 0,
    })).filter(result => result.ok))
    dispatch({ type: 'TAB_DONE', tab: 'financier' })
  }

  const displayedResults = results ?? (status === 'done'
    ? selectedSummary
        .filter(company => company.files.length > 0)
        .map(company => ({
          icon: '📊',
          label: company.name,
          value: `${company.files.length} fichier${company.files.length > 1 ? 's' : ''} sélectionné${company.files.length > 1 ? 's' : ''}`,
          ok: true,
        }))
    : null)

  return (
    <div className="w-full">
      <PageHeader
        title="Bilans financiers"
        description="Sélection des sociétés détectées et des fichiers comptables à extraire automatiquement depuis le dossier source."
        status={status}
        action={
          status !== 'running' && (
            <Btn onClick={extract} disabled={totalSelectedFiles === 0}>
              {status === 'done' ? 'Ré-extraire' : 'Extraire'}
            </Btn>
          )
        }
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 items-start">
        <div className="space-y-4 xl:col-span-2">
          <Card>
            <SectionTitle>Sociétés détectées</SectionTitle>
            <div className="space-y-4">
              {FAKE_COMPANIES.map(company => {
                const selectedCount = Object.values(selectedFiles[company.id] ?? {}).filter(Boolean).length
                const isOpen = openCompanyId === company.id

                return (
                  <div key={company.id} className="rounded-2xl border border-white/8 bg-white/[0.02] overflow-hidden">
                    <button
                      type="button"
                      onClick={() => setOpenCompanyId(current => current === company.id ? null : company.id)}
                      className="w-full flex items-center justify-between gap-4 p-4 text-left hover:bg-white/[0.03] transition-colors cursor-pointer"
                    >
                      <div>
                        <div className="text-sm font-semibold text-white flex items-center gap-2">
                          <span className="text-base">🏢</span>
                          {company.name}
                        </div>
                        <p className="text-xs text-white/35 mt-1">Cliquer pour afficher les fichiers à extraire.</p>
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-xs px-2 py-1 rounded-full bg-cyan-400/10 text-cyan-300 border border-cyan-400/15">
                          {selectedCount} fichier{selectedCount > 1 ? 's' : ''}
                        </span>
                        <span className={`text-white/45 text-sm transition-transform ${isOpen ? 'rotate-180' : ''}`}>⌄</span>
                      </div>
                    </button>

                    {isOpen && (
                      <div className="border-t border-white/8 p-4 space-y-3 bg-white/[0.01]">
                        {Object.entries(company.filesByPeriod).map(([period, files]) => (
                          <div key={period} className="rounded-xl border border-white/8 bg-white/[0.02] px-4 py-3">
                            <div className="flex items-center justify-between gap-3 mb-2">
                              <div className="text-sm font-medium text-white">{period}</div>
                              <span className="text-white/35 text-sm">⌄</span>
                            </div>
                            <select
                              value={selectedFiles[company.id]?.[period] ?? ''}
                              onChange={e => selectFile(company.id, period, e.target.value)}
                              className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer"
                            >
                              {files.map(file => (
                                <option key={file} value={file} className="bg-[#0c1e2e]">
                                  {file}
                                </option>
                              ))}
                            </select>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )
              })}
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
