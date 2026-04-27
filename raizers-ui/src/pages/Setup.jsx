import { useState } from 'react'
import { useSession } from '../context/session'
import { Btn, Spinner } from '../components/ui'
import backgroundUrl from '../../../assets/background.jpg'
import raizersLogoUrl from '../../../assets/raizers_logo.png'

const FAKE_PROJECTS = [
  '3. Opération - Rue de la Loge',
  '12. Résidence Les Acacias',
  '7. Tour Horizon',
  '5. Domaine du Soleil',
]
const FAKE_SUBFOLDERS = ['Signature 2024', 'Due Diligence', 'Financier Q3', 'Bilan 2023']

const PIPELINE_STEPS = [
  { label: 'Connexion Dropbox', duration: 800 },
  { label: 'Synchronisation des fichiers', duration: 1400 },
  { label: 'Extraction du texte', duration: 1200 },
  { label: 'Indexation des documents', duration: 900 },
]

export default function Setup() {
  const { dispatch } = useSession()
  const [project, setProject] = useState(FAKE_PROJECTS[0])
  const [subfolder, setSubfolder] = useState(FAKE_SUBFOLDERS[0])
  const [phase, setPhase] = useState('select') // select | running | done
  const [stepIndex, setStepIndex] = useState(0)
  const [stats, setStats] = useState(null)

  async function launchPipeline() {
    setPhase('running')
    dispatch({ type: 'SET_PROJECT', project, subfolder })
    dispatch({ type: 'PIPELINE_START' })

    for (let i = 0; i < PIPELINE_STEPS.length; i++) {
      setStepIndex(i)
      await new Promise(r => setTimeout(r, PIPELINE_STEPS[i].duration))
    }

    const fakeStats = { files: 42, tokens: 118_430 }
    setStats(fakeStats)
    setPhase('done')
    dispatch({ type: 'PIPELINE_DONE', stats: fakeStats })
  }

  const progress = phase === 'running'
    ? Math.round(((stepIndex + 1) / PIPELINE_STEPS.length) * 100)
    : phase === 'done' ? 100 : 0

  return (
    <div
      className="min-h-screen flex items-center justify-center p-6 bg-cover bg-center bg-no-repeat"
      style={{
        backgroundImage: `linear-gradient(rgba(7, 17, 26, 0.48), rgba(7, 17, 26, 0.62)), url(${backgroundUrl})`,
      }}
    >
      <div className="w-full max-w-md">

        {/* Logo */}
        <div className="text-center mb-8">
          <img
            src={raizersLogoUrl}
            alt="Raizers"
            className="w-56 sm:w-72 h-auto object-contain mx-auto invert drop-shadow-[0_12px_28px_rgba(0,0,0,0.35)]"
          />
        </div>

        {/* Card */}
        <div className="rounded-3xl border border-white/10 bg-[#08131d] shadow-2xl shadow-black/50 overflow-hidden">

          {/* Header */}
          <div className="px-7 pt-6 pb-5 border-b border-white/[0.06]">
            <div className="flex items-center justify-between mb-4">
              <span className="text-xs text-white/40 uppercase tracking-widest font-semibold">
                {phase === 'select' ? 'Nouveau dossier' : phase === 'running' ? 'Pipeline en cours' : 'Prêt'}
              </span>
              {phase !== 'select' && (
                <span className={`text-xs font-semibold ${phase === 'done' ? 'text-emerald-400' : 'text-amber-400'}`}>
                  {phase === 'done' ? '✓ Terminé' : `${progress}%`}
                </span>
              )}
            </div>
            <div className="h-1 bg-white/[0.06] rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${phase === 'done' ? 'bg-emerald-400' : 'bg-gradient-to-r from-cyan-500 to-cyan-400'}`}
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>

          {/* Body */}
          <div className="px-7 py-6">

            {/* Phase : sélection */}
            {phase === 'select' && (
              <div className="space-y-5">
                <div>
                  <label className="block text-xs text-white/45 mb-2 uppercase tracking-widest font-medium">Dossier</label>
                  <select
                    value={project}
                    onChange={e => setProject(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer"
                  >
                    {FAKE_PROJECTS.map(p => <option key={p} className="bg-[#0c1e2e]">{p}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs text-white/45 mb-2 uppercase tracking-widest font-medium">Sous-dossier d'audit</label>
                  <select
                    value={subfolder}
                    onChange={e => setSubfolder(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 text-sm text-white outline-none focus:border-cyan-400/50 transition-colors cursor-pointer"
                  >
                    {FAKE_SUBFOLDERS.map(s => <option key={s} className="bg-[#0c1e2e]">{s}</option>)}
                  </select>
                  <p className="text-xs text-white/30 mt-1.5">Le dossier Opérateur associé sera toujours inclus.</p>
                </div>
              </div>
            )}

            {/* Phase : pipeline en cours */}
            {phase === 'running' && (
              <div className="space-y-3">
                {PIPELINE_STEPS.map((step, i) => {
                  const done = i < stepIndex
                  const active = i === stepIndex
                  return (
                    <div key={i} className={`flex items-center gap-3 px-4 py-3 rounded-xl border transition-all ${
                      done    ? 'border-emerald-400/15 bg-emerald-400/[0.04]' :
                      active  ? 'border-cyan-400/20 bg-cyan-400/[0.04]' :
                                'border-white/5 bg-transparent'
                    }`}>
                      <div className="w-5 h-5 flex items-center justify-center flex-shrink-0">
                        {done   ? <span className="text-emerald-400 text-sm">✓</span> :
                         active ? <Spinner /> :
                                  <span className="w-1.5 h-1.5 rounded-full bg-white/15 block" />}
                      </div>
                      <span className={`text-sm ${done ? 'text-emerald-300' : active ? 'text-white' : 'text-white/30'}`}>
                        {step.label}
                      </span>
                    </div>
                  )
                })}
              </div>
            )}

            {/* Phase : done */}
            {phase === 'done' && (
              <div className="space-y-4">
                <div className="rounded-xl border border-emerald-400/15 bg-emerald-400/[0.04] px-4 py-4">
                  <div className="text-sm font-semibold text-emerald-300 mb-2">Pipeline terminé</div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <div className="text-xs text-white/35">Fichiers traités</div>
                      <div className="text-lg font-bold text-white">{stats?.files}</div>
                    </div>
                    <div>
                      <div className="text-xs text-white/35">Tokens extraits</div>
                      <div className="text-lg font-bold text-white">{stats?.tokens?.toLocaleString('fr-FR')}</div>
                    </div>
                  </div>
                </div>
                <p className="text-xs text-white/40 text-center">
                  {project.replace(/^\d+\.\s*/, '')} — {subfolder}
                </p>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="px-7 pb-6">
            {phase === 'select' && (
              <Btn full onClick={launchPipeline}>Lancer le pipeline</Btn>
            )}
            {phase === 'running' && (
              <button disabled className="w-full py-3 rounded-xl bg-white/5 border border-white/10 text-white/30 text-sm font-semibold flex items-center justify-center gap-2 cursor-not-allowed">
                <Spinner />
                Traitement en cours...
              </button>
            )}
            {phase === 'done' && (
              <Btn full onClick={() => dispatch({ type: 'PIPELINE_DONE', stats })}>
                Entrer dans l'app →
              </Btn>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
