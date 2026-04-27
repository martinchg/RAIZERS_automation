"""
normalization.py : Normalisation et matching flexibles de noms de fichiers/dossiers.

Utilisé par tout le pipeline pour éviter toute dépendance à un nommage rigide :
- pipeline.py       : trouver dossiers "Audit" / "Opérateur" quel que soit le préfixe
- extract_structured.py : matcher source_dirs / hint_keywords aux source_path
- extract_people_from_casiers.py / excel_filler.py : identifier sous-dossiers personnes

Règles de canonicalisation :
    - minuscules
    - accents retirés (é → e, ô → o, ...)
    - préfixes numériques ("0.", "1. ", "10) ") retirés
    - préfixe "X." retiré (archive / placeholder)
    - suffixes numériques (" 1", " 2") retirés
    - séparateurs (_ / -) normalisés en espace
    - espaces multiples collapsed
    - support des motifs avec ``*`` (ex: ``*Audit/*Opérateur/*RH``)
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import List, Optional


# ``^(digits | 'x') . or )`` — mais on évite de consommer un "10.0" ou "1.2".
_LEADING_PREFIX_RE = re.compile(r"^\s*(?:\d+|x)\s*[\.\)](?!\d)\s*", re.IGNORECASE)
# Suffixe numérique (" 1", " 23") — exige au moins un espace devant pour ne pas
# amputer un nom type "Audit2023" qu'on ne sait pas interpréter.
_TRAILING_NUMBER_RE = re.compile(r"\s+\d+\s*$")


# ---------------------------------------------------------------------------
# Canonicalisation
# ---------------------------------------------------------------------------
def _canonicalize_text(text: str, *, preserve_wildcards: bool = False) -> str:
    if not text:
        return ""
    text = text.strip().lower()
    wildcard_marker = "codexwildcardtoken"
    if preserve_wildcards:
        text = text.replace("*", wildcard_marker)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.replace("_", " ").replace("-", " ")
    text = _LEADING_PREFIX_RE.sub("", text)
    text = _TRAILING_NUMBER_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    if preserve_wildcards:
        text = text.replace(wildcard_marker, "*")
        text = re.sub(r"\s*\*\s*", "*", text)
    return text.strip()


def canonical_name(text: str) -> str:
    """Canonicalise un libellé (dossier, fichier, clé...) pour comparaison.

    >>> canonical_name("0. Opérateur")
    'operateur'
    >>> canonical_name("1. Opérateur")
    'operateur'
    >>> canonical_name("Opérateur")
    'operateur'
    >>> canonical_name("2. Audit")
    'audit'
    >>> canonical_name("Audit 1")
    'audit'
    >>> canonical_name("X. audit")
    'audit'
    """
    return _canonicalize_text(text)


def canonical_pattern(text: str) -> str:
    """Canonicalise un motif en conservant les jokers ``*``.

    Exemples :
        >>> canonical_pattern("*Audit")
        '*audit'
        >>> canonical_pattern("1. *Opérateur")
        '*operateur'
    """
    return _canonicalize_text(text, preserve_wildcards=True)


def canonical_stem(filename: str) -> str:
    """Canonicalise le nom d'un fichier en retirant son extension."""
    return canonical_name(Path(filename).stem)


def canonical_segments(path: str) -> List[str]:
    """Décompose un chemin en segments canonicalisés."""
    if not path:
        return []
    raw = path.replace("\\", "/")
    parts = [p for p in raw.split("/") if p.strip()]
    return [canonical_name(p) for p in parts]


def canonical_pattern_segments(path: str) -> List[str]:
    """Décompose un chemin en segments canonicalisés, avec support des ``*``."""
    if not path:
        return []
    raw = path.replace("\\", "/")
    parts = [p for p in raw.split("/") if p.strip()]
    return [canonical_pattern(p) for p in parts]


# ---------------------------------------------------------------------------
# Matching primitives
# ---------------------------------------------------------------------------
def matches_pattern(name: str, pattern: str) -> bool:
    """True si *name* matche *pattern* avec support des jokers ``*``.

    Le matching reste canonicalisé : casse, accents et préfixes numériques
    sont ignorés avant comparaison.
    """
    name_canon = canonical_name(name)
    pattern_canon = canonical_pattern(pattern)
    if not pattern_canon:
        return not name_canon
    regex = "^" + re.escape(pattern_canon).replace(r"\*", ".*") + "$"
    return re.fullmatch(regex, name_canon) is not None


def path_has_segments(source_path: str, target_dir: str) -> bool:
    """True si *target_dir* apparaît comme sous-séquence de segments dans
    *source_path*, comparaison canonique (accents, casse, préfixes...).

    Exemples :
        >>> path_has_segments("2. Audit /1. Opérateur/3. RH/Pernod/file.pdf", "2. Audit/1. Opérateur/3. RH")
        True
        >>> path_has_segments("Audit/Opérateur/Société Emprunteuse/kbis.pdf", "Société Emprunteuse")
        True
        >>> path_has_segments("Audit/Opérateur/Société Emprunteuse/kbis.pdf", "Société Opération")
        False
    """
    target = [s for s in canonical_pattern_segments(target_dir) if s]
    source = [s for s in canonical_segments(source_path) if s]
    if not target:
        return True
    window = len(target)
    if len(source) < window:
        return False
    for i in range(len(source) - window + 1):
        if all(matches_pattern(source[i + j], target[j]) for j in range(window)):
            return True
    return False


# ---------------------------------------------------------------------------
# Folder discovery
# ---------------------------------------------------------------------------
def iter_direct_subfolders(base: Path) -> List[Path]:
    """Retourne les sous-dossiers directs de *base*, triés alphabétiquement."""
    if not base.exists():
        return []
    try:
        return sorted(
            [c for c in base.iterdir() if c.is_dir()],
            key=lambda p: p.name.lower(),
        )
    except (PermissionError, OSError):
        return []


def find_folder_by_canonical(
    base: Path,
    target: str,
    *,
    recursive: bool = False,
    max_depth: int = 1,
) -> Optional[Path]:
    """Trouve un sous-dossier dont le nom canonicalisé matche *target*.

    - ``recursive=False`` : cherche uniquement les enfants directs de *base*.
    - ``recursive=True``  : descend jusqu'à *max_depth* niveaux.
    - ``target`` peut inclure des jokers ``*``.
    - Retourne le premier match, ou ``None``.
    """
    if not canonical_name(target) or not base.exists():
        return None

    def _walk(directory: Path, depth: int) -> Optional[Path]:
        children = iter_direct_subfolders(directory)
        for child in children:
            if matches_pattern(child.name, target):
                return child
        if recursive and depth < max_depth:
            for child in children:
                found = _walk(child, depth + 1)
                if found:
                    return found
        return None

    return _walk(base, 0)


# ---------------------------------------------------------------------------
# Archive detection
# ---------------------------------------------------------------------------
_OLD_RAW = {"old", ".old", "x - old", "x-old", "x old", "_old", "archive", "archives"}


def is_archived_path(path: str) -> bool:
    """True si le chemin contient un marqueur d'archive (old, .old, 'x - old').

    Comparaison tolérante : casse, accents, préfixes, séparateurs.
    """
    if not path:
        return False
    raw_parts = [p.strip().lower() for p in path.replace("\\", "/").split("/") if p.strip()]
    for raw in raw_parts:
        if raw in _OLD_RAW:
            return True
        canon = canonical_name(raw)
        if canon in {"old", "archive", "archives"}:
            return True
    return False


# ---------------------------------------------------------------------------
# RH / personnes
# ---------------------------------------------------------------------------
_RH_CANON_ALIASES = {"rh", "ressources humaines"}


def extract_person_folder(source_path: str) -> Optional[str]:
    """Retourne le nom du sous-dossier personne sous RH, sinon None.

    Matching tolérant : '3. RH', 'RH', '3. Ressources Humaines', 'ressources
    humaines' (casse/accents/préfixes ignorés).

    Exemples acceptés :
    - 2. Audit/1. Opérateur/3. RH/Pernod/file.pdf       -> "Pernod"
    - Audit/Opérateur/Ressources Humaines/Juliette/file.pdf -> "Juliette"

    Exclut les fichiers directement dans le dossier RH et les chemins archivés.
    """
    if is_archived_path(source_path):
        return None

    raw_parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    canon_parts = [canonical_name(p) for p in raw_parts]

    rh_idx = next(
        (i for i, p in enumerate(canon_parts) if p in _RH_CANON_ALIASES),
        None,
    )
    if rh_idx is None:
        return None

    # Veut STRICTEMENT un sous-dossier de RH (et un fichier en dessous).
    if rh_idx + 1 >= len(raw_parts) - 1:
        return None

    candidate = raw_parts[rh_idx + 1]
    if is_archived_path(candidate):
        return None
    return candidate
