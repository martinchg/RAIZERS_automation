# RAIZERS — Pipeline d'audit automatisé

## Objectif

Automatiser l'analyse de dossiers d'audit Dropbox : extraction documentaire (PDF, DOCX, XLSX…), extraction LLM structurée, enrichissement Pappers (mandats/sociétés), et génération de rapport Excel.

## Commandes CLI

Les 4 étapes sont indépendantes et se lancent dans l'ordre :

### 1. Pipeline — Dropbox → extraction texte → chunking

```bash
python run.py pipeline --project "/RAIZERS - En audit/NOM_DU_PROJET"
python run.py pipeline --project "/RAIZERS - En audit/SIGNATURE" --audit-folder "3. Opération - Rue de la Loge"
```

Le pipeline :
- synchronise uniquement le dossier `Opérateur` et le sous-dossier d'audit sélectionné ;
- ignore les chemins archivés (`old`, `.old`, `archive`, etc.) ;
- applique un préfiltrage métier avant extraction/chunking ;
- réutilise le cache des documents déjà chunkés quand le `source_path` et la taille du fichier n'ont pas changé ;
- génère les documents chunkés dans `output/<project_id>/`.

Option :
- `--audit-folder` : nom exact du sous-dossier d'audit à inclure en plus de `1. Opérateur`.

Exclusion manuelle optionnelle :

```bash
PIPELINE_EXCLUDE_PATTERNS="reporting,q&a,constat" python run.py pipeline --project "/RAIZERS - En audit/SIGNATURE" --audit-folder "3. Opération - Rue de la Loge"
```

Les patterns sont comparés de façon souple sur le nom de fichier et le `source_path`.

### 2. Extract — Extraction structurée via LLM

```bash
python run.py extract --project raizers-en-audit-signature
```

Envoie les documents au LLM pour extraire les champs métier. Résultat dans `extraction_results.json`.

### 3. Mandats — Enrichissement Pappers

```bash
python run.py mandats --project raizers-en-audit-signature
```

Extrait les personnes depuis les casiers judiciaires, identifie la bonne personne via Pappers (`/recherche-dirigeants`) en s'appuyant sur le nom/prénom et la date de naissance quand elle est disponible, puis enrichit chaque société via `/recherche` avec le `SIREN`. Le rôle du dirigeant provient de `/recherche-dirigeants`. Résultat dans `mandats_results.json`.

### 4. Fill — Génération du rapport Excel

```bash
python run.py fill --project raizers-en-audit-signature
```

Génère `rapport.xlsx` avec 3 onglets (Opération, Patrimoine, Mandats). Charge automatiquement `mandats_results.json` s'il est présent dans le même dossier.

Option utile :

```bash
python run.py fill --results output/raizers-en-audit-signature/extraction_results.json
```

Options avancées :

```bash
python run.py fill \
  --results output/raizers-en-audit-signature/extraction_results.json \
  --questions config/questions_operateur.json \
  --output-dir output/raizers-en-audit-signature
```

Sans `--questions`, le remplissage charge automatiquement les fichiers split du dossier `config/`
(`questions_operateur.json`, `questions_patrimoine.json`, `questions_finance.json`).

## Lancement Streamlit

```bash
streamlit run app.py
```

L'application Streamlit orchestre le même flux :
- sélection du projet Dropbox ;
- sélection d'un sous-dossier d'audit ;
- pipeline ;
- extraction LLM ;
- mandats Pappers ;
- génération Excel.

## Structure du code

```bash
src/
├── core/               # utilitaires transversaux
│   ├── runtime_config.py
│   ├── normalization.py
│   ├── excel_utils.py
│   ├── chunking.py
│   └── llm_client.py
├── extraction/         # pipeline LLM documents → JSON structuré
│   ├── extract_structured.py
│   ├── extract_structured_documents.py
│   ├── extract_structured_prompts.py
│   ├── extract_structured_runtime.py
│   └── question_config.py
├── financial/          # pipeline PDF bilan
│   ├── financial_pdf_pipeline.py
│   ├── financial_pdf_llm_prep.py
│   ├── financial_pdf_second_pass.py
│   ├── financial_tables_native.py
│   └── financial_mapping.py
├── pappers/            # Pappers API + comptes
│   ├── pappers_enrichment.py
│   ├── pappers_comptes_flatten.py
│   ├── pappers_comptes_revelateur.py
│   └── pappers_fetch_comptes.py
├── sheets/             # écriture Excel par onglet
│   ├── excel_filler.py
│   ├── bilan_sheet.py
│   ├── lots_sheet.py
│   ├── mandats_sheet.py
│   ├── operation_sheet.py
│   └── patrimoine_sheet.py
├── pipeline.py         # orchestrateur principal Dropbox → cache → manifest
├── ingestion.py
├── dropbox_client.py
├── mandats_pipeline.py
├── extract_people_from_casiers.py
├── immo_scoring.py
├── tab_audit.py
├── tab_immo.py
└── patrimoine_tables_native.py
```

Repères :
- `app.py` pilote l'interface Streamlit.
- `run.py` expose les commandes CLI (`pipeline`, `extract`, `mandats`, `fill`).
- Les imports Python sont désormais organisés par domaine (`core.*`, `extraction.*`, `financial.*`, `pappers.*`, `sheets.*`).

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

Champs utiles dans `manifest.json` :
- `selected_audit_folder` : sous-dossier d'audit retenu ;
- `stats.files_found` : fichiers supportés trouvés dans le cache local ;
- `stats.files_in_scope` : fichiers dans le périmètre `Opérateur` + sous-dossier d'audit ;
- `stats.files_processed` : fichiers réellement ré-extraits/re-chunkés sur ce run ;
- `stats.files_reused_from_cache` : fichiers réutilisés depuis le cache ;
- `stats.files_ready` : nombre total de documents disponibles pour les étapes suivantes.

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

En déploiement sur Streamlit Community Cloud, ajoute ces mêmes clés dans `st.secrets` :
le code injecte automatiquement les secrets Streamlit dans `os.environ`, tandis qu'en local
le fichier `.env` continue d'être utilisé.
  
