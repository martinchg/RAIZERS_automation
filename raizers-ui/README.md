# RAIZERS UI

Interface frontend React/Vite du projet RAIZERS Automation.

Cette application a vocation a remplacer progressivement l'interface Streamlit historique avec une experience plus claire, plus modulaire, et plus simple a brancher a un backend Python plus tard.

## Objectif

L'UI couvre le parcours principal d'un audit RAIZERS :

- selection d'un dossier et lancement du pipeline
- extraction des donnees `Operation`
- extraction des `Bilans financiers`
- extraction du `Patrimoine`
- outils complementaires `Comparateur` et `Scraping`
- consolidation finale dans un export Excel

## Etat actuel

L'application est pour l'instant un frontend autonome.

Cela signifie que :

- les ecrans sont fonctionnels visuellement
- la navigation est en place
- l'etat de session est gere localement en memoire
- plusieurs donnees sont encore mockees
- aucun backend API n'est encore branche

En pratique, l'UI simule aujourd'hui une future integration avec le pipeline Python existant.

## Stack

- React 19
- React Router
- Vite
- Tailwind CSS v4

## Demarrage local

Depuis le dossier `raizers-ui` :

```bash
npm install
npm run dev
```

Puis ouvrir l'URL affichee par Vite, en general :

```bash
http://localhost:5173
```

## Build production

```bash
npm run build
```

Le build compile les assets statiques dans `dist/`.

## Structure

```text
raizers-ui/
├── src/
│   ├── components/   # layout, composants UI reutilisables
│   ├── context/      # session locale de l'application
│   ├── pages/        # ecrans metier
│   ├── App.jsx       # routing principal
│   ├── main.jsx      # bootstrap React
│   └── index.css     # styles globaux
├── index.html
├── package.json
└── vite.config.js
```

## Pages principales

- `Setup` : selection du dossier actif et lancement du pipeline
- `Operation` : extraction des donnees operateur / societe
- `Financier` : societes detectees et fichiers comptables a extraire
- `Patrimoine` : selection des personnes et ajout manuel d'une personne
- `Immo` : comparateur immobilier
- `Scraping` : collecte structuree de donnees immo
- `Export` : preparation de l'export final

## Choix UX actuels

- l'onglet `Lots` a ete retire de la navigation
- la logique `Lots` est destinee a etre reintegree plus tard dans `Operation`
- une action globale `Tout extraire` est disponible dans la barre haute
- `Financier` est deja pense pour une detection automatique des fichiers source
- `Patrimoine` permet de choisir les personnes a inclure dans l'extraction

## Limites actuelles

Les points suivants ne sont pas encore branches :

- appel au pipeline Python reel
- recuperation des dossiers Dropbox
- lecture automatique des vrais fichiers comptables
- persistance de session
- generation reelle de l'Excel depuis le frontend
- gestion des jobs longs via API

## Integration cible

L'architecture cible la plus simple est :

- `raizers-ui` pour le frontend
- `FastAPI` pour exposer quelques routes backend
- reutilisation du pipeline Python existant dans le repo principal

Exemples de routes futures :

- `POST /pipeline/start`
- `GET /projects`
- `GET /projects/{id}/status`
- `GET /projects/{id}/results/operation`
- `GET /projects/{id}/results/financier`
- `GET /projects/{id}/results/patrimoine`
- `POST /projects/{id}/export`

## Notes

- cette UI vit dans le meme repo que le backend Python, mais reste pour l'instant separee
- le README racine du projet documente le pipeline backend et l'interface Streamlit
- ce README documente uniquement la partie frontend React
