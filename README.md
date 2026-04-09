# RAIZERS — Pipeline d'audit automatisé

## Objectif

Automatiser l'analyse de dossiers d'audit Dropbox : extraction documentaire (PDF, DOCX, XLSX…), extraction LLM structurée, enrichissement Pappers (mandats/sociétés), et génération de rapport Excel.

## Commandes

Les 4 étapes sont indépendantes et se lancent dans l'ordre :

### 1. Pipeline — Dropbox → extraction texte → chunking

```bash
python run.py pipeline --project "/RAIZERS - En audit/NOM_DU_PROJET"
```

Synchronise le dossier Dropbox, extrait le texte de tous les documents, et génère les chunks dans `output/<project_id>/`.

### 2. Extract — Extraction structurée via LLM

```bash
python run.py extract --project raizers-en-audit-nom-du-projet
```

Envoie les documents au LLM pour extraire les champs métier. Résultat dans `extraction_results.json`.

### 3. Mandats — Enrichissement Pappers

```bash
python run.py mandats --project raizers-en-audit-nom-du-projet
```

Extrait les personnes depuis les casiers judiciaires, recherche leurs sociétés via Pappers (`/recherche`), puis enrichit chaque société active via `/entreprise` (rôle, détention, commentaires). Résultat dans `mandats_results.json`.

### 4. Fill — Génération du rapport Excel

```bash
python run.py fill --results output/raizers-en-audit-nom-du-projet/extraction_results.json
```

Génère `rapport.xlsx` avec 3 onglets (Opération, Patrimoine, Mandats). Charge automatiquement `mandats_results.json` s'il est présent dans le même dossier.

## Structure de sortie

```bash
output/<project_id>/
├── manifest.json                  # Métadonnées des fichiers
├── documents/<doc_id>.jsonl       # Contenu structuré par document
├── extraction_results.json        # Résultats LLM
├── mandats_results.json           # Résultats Pappers
├── mandats_debug_recherche.json   # Debug Pappers (audit trail)
└── rapport.xlsx                   # Rapport Excel final
```

## Variables d'environnement requises

```bash
DROPBOX_APP_KEY=
DROPBOX_APP_SECRET=
DROPBOX_REFRESH_TOKEN=
OPENAI_API_KEY=
GEMINI_API_KEY=
PAPPERS_API_KEY=
```

Configurer dans un fichier `.env` à la racine (non versionné).
  