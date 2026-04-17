"""Construction des prompts pour extract_structured.py."""

from typing import Dict, List

from extract_structured_documents import needs_broad_financial_context


def build_prompt(
    document_text: str,
    questions: List[Dict],
    filename: str,
    source_path: str,
    native_context: str = "",
) -> str:
    fields_desc = []
    for question in questions:
        desc = f'- "{question["field_id"]}": {question["question"]}'
        if question.get("format_hint"):
            desc += f' (format: {question["format_hint"]})'
        if question.get("context_hint"):
            desc += f' — Contexte: {question["context_hint"]}'

        source_doc_name = question.get("source_doc_name")
        source_doc_name_variants = question.get("source_doc_name_variants")
        if source_doc_name:
            desc += f' — SourceDoc attendu: "{source_doc_name}"'
        elif source_doc_name_variants:
            variants = [variant for variant in source_doc_name_variants if isinstance(variant, str)]
            if variants:
                joined = ", ".join(f'"{variant}"' for variant in variants)
                desc += f" — SourceDoc attendu (variantes): {joined}"

        fields_desc.append(desc)

    fields_block = "\n".join(fields_desc)
    field_ids = [question["field_id"] for question in questions]

    extra_instructions = ""
    if needs_broad_financial_context(questions):
        extra_instructions = """
## ATTENTION — Etats financiers

### Colonnes N et N-1
- N = exercice le PLUS RÉCENT. N-1 = exercice précédent.
- Si 4+ colonnes (Brut, Amort, Net N, Net N-1) : utilise TOUJOURS Net.
- Si un seul exercice : N-1 = null.

### Totaux vs sous-lignes
- S'il existe un sous-total affiché, utilise-le.
- Sinon, somme les sous-lignes nettes.
- Ne prends JAMAIS une seule sous-ligne quand il y en a plusieurs.

### INTERDIT — Double comptage
Chaque montant du document ne peut apparaître que dans UN SEUL poste de ta réponse.
Exemple d'erreur fréquente : mettre 501 dans immobilisations_financieres ET dans autres_actif_residuel.

### Vérification obligatoire
Avant de répondre, vérifie : somme de tes postes de détail ≈ total affiché (tolérance 5%).
Si l'écart est trop grand, tu as probablement confondu des colonnes ou double-compté.

### Portée des tableaux
- Pour les champs de type tableau financier, prends UNIQUEMENT les tableaux principaux Actif, Passif et Compte de résultat.
- Privilégie les tableaux présentés au début du document d'états financiers / bilan.
- Ignore les annexes, notes, tableaux détaillés secondaires, SIG et reprises plus loin dans le document.

### Extraction ligne par ligne
- Pour les champs de type tableau financier, retourne en priorité les CLES METIER demandees par la question, pas un recopiage brut du tableau.
- Chaque entrée doit contenir au minimum `key`, `n`, `n1`, et optionnellement `poste_source`.
- N'inclus pas les lignes purement textuelles ou les en-têtes sans montant.
"""

    return f"""Tu es un analyste financier expert. Tu extrais des informations précises depuis des documents de projet immobilier (crowdfunding obligataire).

## Document : {filename}
## Source path : {source_path}

{document_text}
{native_context}

## Instructions

Extrais les informations suivantes de ce document. Pour chaque champ :
- Si l'information est clairement présente, retourne la valeur exacte.
- Si l'information N'EST PAS dans ce document, retourne null.
- Ne devine PAS. Ne fabrique PAS de données. Si tu n'es pas sûr, retourne null.
- Respecte le format demandé.
- Si un champ contient une contrainte "SourceDoc attendu" (ou variantes), vérifie intelligemment que le nom du document actuel correspond bien (tolérance: accents, tirets, underscores, fautes mineures, mots manquants comme "complétée"). Si la correspondance n'est pas solide, retourne null pour ce champ.
- Si un bloc "EXTRACTION NATIVE PRE-NETTOYEE" est fourni, utilise-le comme base prioritaire pour les tableaux financiers, les dates et les intitulés.
- En cas d'ambiguïté, corrige ou complète à partir du document brut, mais n'invente rien qui ne soit pas appuyé par une des deux sources.
{extra_instructions}

## ATTENTION — Distinction importante

Dans ce type de montage, il y a souvent DEUX sociétés distinctes :
- La **société portant l'emprunt** (émettrice des obligations, celle qui emprunte via Raizers).
- La **société portant l'opération** (celle qui réalise le projet immobilier). Elle peut être identique ou différente.

Les champs "*_emprunt" concernent la société émettrice. Les champs "*_operation" ne doivent être remplis QUE si la société opération est DIFFÉRENTE de la société emprunt. Sinon, retourne null pour les champs *_operation.

## Champs à extraire

{fields_block}

## Format de réponse

Retourne UNIQUEMENT un objet JSON avec ces clés : {field_ids}
Chaque valeur est soit une string, soit null, soit un array JSON (pour les champs de type tableau).
"""


def build_multimodal_financial_prompt(
    questions: List[Dict],
    filename: str,
    source_path: str,
    page_labels: Dict[str, int],
) -> str:
    fields_desc = []
    for question in questions:
        desc = f'- "{question["field_id"]}": {question["question"]}'
        if question.get("format_hint"):
            desc += f' (format: {question["format_hint"]})'
        fields_desc.append(desc)

    page_hints = "\n".join(
        f"- {section}: page {page}" for section, page in sorted(page_labels.items(), key=lambda item: item[1])
    ) or "- pages financieres non determinees automatiquement"

    field_ids = [question["field_id"] for question in questions]
    fields_block = "\n".join(fields_desc)

    return f"""Tu es un analyste financier expert. Tu lis directement des images de pages PDF d'etats financiers.

## Document : {filename}
## Source path : {source_path}

## Pages fournies
{page_hints}

## Instructions

- Les images jointes sont la source principale.
- Extrais uniquement ce qui est lisible sur ces pages.
- Si une information n'est pas visible ou reste ambiguë, retourne null.
- Pour les tableaux financiers, retourne une ligne par ligne chiffrée utile.
- N'invente rien et ne complète pas avec des hypothèses.
- N = exercice le plus recent. N-1 = exercice precedent.
- Si plusieurs colonnes existent (Brut / Amort. / Net / N-1), utilise uniquement les valeurs Net.
- Ne double-compte jamais un montant dans 2 postes differents.
- Si un sous-total ou total est affiché clairement, prefere-le aux details.

## Champs a extraire
{fields_block}

## Format de reponse

Retourne UNIQUEMENT un objet JSON avec ces cles : {field_ids}
Chaque valeur est soit une string, soit null, soit un array JSON.
"""
