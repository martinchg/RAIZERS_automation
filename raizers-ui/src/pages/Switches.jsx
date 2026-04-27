import React, { useState } from "react";
import { motion } from "framer-motion";
import { Bell, Check, Lock, Moon, Sun, Wifi } from "lucide-react";

function BasicSwitch({ checked, onChange, label }) {
  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-8 w-14 items-center rounded-full transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 ${
        checked ? "bg-blue-600" : "bg-slate-300"
      }`}
    >
      <span
        aria-hidden="true"
        className={`inline-block h-6 w-6 rounded-full bg-white shadow transition-transform ${
          checked ? "translate-x-7" : "translate-x-1"
        }`}
      />
    </button>
  );
}

function IconSwitch({ checked, onChange, label }) {
  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={`relative flex h-10 w-20 items-center rounded-full p-1 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-amber-400 focus-visible:ring-offset-2 ${
        checked ? "bg-amber-400" : "bg-indigo-900"
      }`}
    >
      <motion.span
        aria-hidden="true"
        className="flex h-8 w-8 items-center justify-center rounded-full bg-white shadow"
        animate={{ x: checked ? 40 : 0 }}
        transition={{ type: "spring", stiffness: 420, damping: 26 }}
      >
        {checked ? <Sun size={17} /> : <Moon size={17} />}
      </motion.span>
    </button>
  );
}

function PillSwitch({ checked, onChange, label }) {
  const options = [
    { text: "Mensuel", value: false },
    { text: "Annuel", value: true },
  ];

  return (
    <div role="radiogroup" aria-label={label} className="inline-flex rounded-2xl bg-slate-100 p-1 shadow-inner">
      {options.map((option) => {
        const active = checked === option.value;

        return (
          <button
            type="button"
            role="radio"
            key={option.text}
            aria-checked={active}
            onClick={() => onChange(option.value)}
            className={`relative rounded-xl px-5 py-2 text-sm font-medium transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 focus-visible:ring-offset-2 ${
              active ? "text-slate-950" : "text-slate-500"
            }`}
          >
            {active && (
              <motion.span
                aria-hidden="true"
                layoutId="billing-period-pill-bg"
                className="absolute inset-0 rounded-xl bg-white shadow"
                transition={{ type: "spring", stiffness: 450, damping: 34 }}
              />
            )}
            <span className="relative">{option.text}</span>
          </button>
        );
      })}
    </div>
  );
}

function ToggleCard({ checked, onChange, label }) {
  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={`flex w-full items-center justify-between rounded-2xl border p-4 text-left shadow-sm transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-500 focus-visible:ring-offset-2 ${
        checked ? "border-emerald-300 bg-emerald-50" : "border-slate-200 bg-white"
      }`}
    >
      <span className="flex items-center gap-3">
        <span className={`rounded-xl p-2 ${checked ? "bg-emerald-100" : "bg-slate-100"}`}>
          <Bell size={18} aria-hidden="true" />
        </span>
        <span>
          <span className="block font-semibold text-slate-900">Notifications</span>
          <span className="block text-sm text-slate-500">Switch sous forme de carte</span>
        </span>
      </span>
      <span
        aria-hidden="true"
        className={`flex h-7 w-7 items-center justify-center rounded-full transition-colors ${
          checked ? "bg-emerald-500 text-white" : "bg-slate-200 text-transparent"
        }`}
      >
        <Check size={16} />
      </span>
    </button>
  );
}

function NeumorphicSwitch({ checked, onChange, label }) {
  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className="relative h-12 w-24 rounded-full bg-slate-100 p-1 shadow-[inset_6px_6px_12px_rgba(15,23,42,0.12),inset_-6px_-6px_12px_rgba(255,255,255,0.95)] focus:outline-none focus-visible:ring-2 focus-visible:ring-violet-500 focus-visible:ring-offset-2"
    >
      <motion.span
        aria-hidden="true"
        className={`flex h-10 w-10 items-center justify-center rounded-full shadow-lg ${
          checked ? "bg-violet-600 text-white" : "bg-white text-slate-500"
        }`}
        animate={{ x: checked ? 48 : 0 }}
        transition={{ type: "spring", stiffness: 380, damping: 25 }}
      >
        <Wifi size={18} />
      </motion.span>
    </button>
  );
}

function VerticalSwitch({ checked, onChange, label }) {
  return (
    <button
      type="button"
      role="switch"
      aria-label={label}
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={`relative h-20 w-10 rounded-full p-1 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-rose-500 focus-visible:ring-offset-2 ${
        checked ? "bg-rose-500" : "bg-slate-300"
      }`}
    >
      <motion.span
        aria-hidden="true"
        className="flex h-8 w-8 items-center justify-center rounded-full bg-white shadow"
        animate={{ y: checked ? 40 : 0 }}
        transition={{ type: "spring", stiffness: 420, damping: 28 }}
      >
        <Lock size={15} />
      </motion.span>
    </button>
  );
}

function DemoBlock({ title, description, children }) {
  return (
    <section className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-5">
        <h2 className="text-lg font-semibold text-slate-950">{title}</h2>
        <p className="mt-1 text-sm text-slate-500">{description}</p>
      </div>
      <div className="flex min-h-24 items-center justify-center rounded-2xl bg-slate-50 p-6">{children}</div>
    </section>
  );
}

export default function SwitchVariantsDemo() {
  const [basic, setBasic] = useState(true);
  const [icon, setIcon] = useState(false);
  const [pill, setPill] = useState(false);
  const [card, setCard] = useState(true);
  const [neumorphic, setNeumorphic] = useState(false);
  const [vertical, setVertical] = useState(true);

  return (
    <main className="min-h-screen bg-slate-100 p-6 text-slate-900">
      <div className="mx-auto max-w-6xl">
        <header className="mb-8 rounded-3xl bg-slate-950 p-8 text-white shadow-xl">
          <p className="text-sm uppercase tracking-[0.25em] text-slate-400">React UI</p>
          <h1 className="mt-3 text-3xl font-bold md:text-5xl">Différentes formes de switches</h1>
          <p className="mt-4 max-w-2xl text-slate-300">
            Six variantes visibles, interactives, prêtes à adapter dans un composant React.
          </p>
        </header>

        <div className="grid gap-5 md:grid-cols-2 lg:grid-cols-3">
          <DemoBlock title="Classique" description="Le toggle standard on/off.">
            <BasicSwitch checked={basic} onChange={setBasic} label="Activer le switch classique" />
          </DemoBlock>

          <DemoBlock title="Avec icône" description="Utile pour thème clair/sombre.">
            <IconSwitch checked={icon} onChange={setIcon} label="Changer le thème" />
          </DemoBlock>

          <DemoBlock title="Segmenté" description="Alternative type tabs ou pricing.">
            <PillSwitch checked={pill} onChange={setPill} label="Choisir la périodicité" />
          </DemoBlock>

          <DemoBlock title="Carte sélectionnable" description="Bon pour paramètres ou préférences.">
            <ToggleCard checked={card} onChange={setCard} label="Activer les notifications" />
          </DemoBlock>

          <DemoBlock title="Neumorphism" description="Effet doux avec ombres internes.">
            <NeumorphicSwitch checked={neumorphic} onChange={setNeumorphic} label="Activer le wifi" />
          </DemoBlock>

          <DemoBlock title="Vertical" description="Format compact ou mobile.">
            <VerticalSwitch checked={vertical} onChange={setVertical} label="Verrouiller l'option" />
          </DemoBlock>
        </div>
      </div>
    </main>
  );
}
