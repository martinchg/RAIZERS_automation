"""
chunking.py : Découpe les éléments extraits en sections (parents).
Parents = unité principale envoyée au LLM.

IDs déterministes :
  document_id = sha256(project_id + source_path)[:12]
  parent_id   = {document_id}_{index:04d}

Tokenizer : tiktoken (cl100k_base) — instantané, local.
"""

import hashlib
import re
import logging
from typing import List, Dict, Tuple

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

logger = logging.getLogger(__name__)

# --- Tokenizer ---
_ENC = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    if not text:
        return 0
    return len(_ENC.encode(text, disallowed_special=()))


def clean_markdown_images(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'!\[.*?\]\(.*?\)', ' ', text)
    return re.sub(r'[ \t]+', ' ', text).strip()


# --- Filtrage bruit ---
_NOISE_PATTERNS = re.compile(
    r'^(#{1,6}\s*)?'                    # header markdown optionnel
    r'[\s\*_#\-|]*$'                    # que du formatting / vide
    r'|^Tous droits de reproduction'    # footer récurrent
    r'|^Page \d+',                      # numéro de page
    re.IGNORECASE
)

MIN_INFORMATIVE_CHARS = 50  # en dessous = non informatif


def is_informative(text: str) -> bool:
    """Retourne True si le texte contient du contenu exploitable."""
    if not text:
        return False
    stripped = text.strip()
    if len(stripped) < MIN_INFORMATIVE_CHARS:
        return False
    # Vérifier que ce n'est pas que du formatting
    clean = re.sub(r'[#\*_\-\|\s]', '', stripped)
    if len(clean) < 10:
        return False
    if _NOISE_PATTERNS.match(stripped):
        return False
    return True


# --- IDs déterministes ---
def make_document_id(project_id: str, source_path: str) -> str:
    """Hash stable : même projet + même fichier = même ID, toujours."""
    raw = f"{project_id}::{source_path}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Split par Headers (markdown + bold titles) + safety split
# ---------------------------------------------------------------------------
_HEADER_RE = re.compile(r'^(#{1,6})\s+(.*)')
_BOLD_RE = re.compile(r'^\*\*(.+?)\*\*\s*(.*)')
_TITLE_KEYWORDS = {"ARTICLE", "SECTION", "CHAPITRE", "PARTIE", "ANNEXE", "TITRE"}


def _is_bold_header(line: str) -> str | None:
    m = _BOLD_RE.match(line.strip())
    if not m:
        return None
    title_part = m.group(1).strip()
    rest = m.group(2).strip()
    if any(kw in title_part.upper() for kw in _TITLE_KEYWORDS) or len(line.strip()) < 100:
        return f"{title_part} {rest}".strip().replace('*', '').replace('_', '')
    return None


def split_markdown_by_headers(markdown_text: str, metadata: Dict,
                              max_parent_tokens: int = 2000) -> List[Dict]:
    lines = markdown_text.split('\n')
    initial_parents = []
    current_buffer = []
    current_title = "Introduction"

    for line in lines:
        md_match = _HEADER_RE.match(line)
        bold_title = None if md_match else _is_bold_header(line)

        if md_match or bold_title:
            if current_buffer:
                clean = clean_markdown_images("\n".join(current_buffer))
                if clean and len(clean.strip()) >= 5:
                    initial_parents.append({
                        "text": clean, "title": current_title, "metadata": metadata
                    })
            current_buffer = [line]
            current_title = (
                bold_title if bold_title
                else md_match.group(2).strip().replace('*', '').replace('_', '')
            )
        else:
            current_buffer.append(line)

    if current_buffer:
        clean = clean_markdown_images("\n".join(current_buffer))
        if clean and len(clean.strip()) >= 5:
            initial_parents.append({
                "text": clean, "title": current_title, "metadata": metadata
            })

    # Safety split
    safety_splitter = RecursiveCharacterTextSplitter(
        chunk_size=max_parent_tokens,
        chunk_overlap=200,
        length_function=count_tokens,
        separators=["\n\n", "\n", ".", " ", ""],
        strip_whitespace=True,
    )

    final_parents = []
    for parent in initial_parents:
        tokens = count_tokens(parent['text'])
        if tokens <= max_parent_tokens:
            parent["section_type"] = "section"
            final_parents.append(parent)
        else:
            logger.info(f"Section '{parent['title'][:50]}' trop grosse ({tokens} tok). Safety split.")
            for i, sub in enumerate(safety_splitter.split_text(parent['text'])):
                final_parents.append({
                    "text": sub,
                    "section_type": "section_split",
                    "title": f"{parent['title']} (Part {i+1})",
                    "metadata": parent['metadata'],
                })

    return final_parents


# ---------------------------------------------------------------------------
# Chunking enfants (conservé pour usage futur : embedding, RAG, etc.)
# ---------------------------------------------------------------------------
def _create_text_children(text: str, chunk_size: int, overlap: int = 50) -> List[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        length_function=count_tokens,
        separators=["\n\n", "\n", ". ", "! ", "? ", " ", ""],
        keep_separator=True,
        strip_whitespace=True,
    )
    return splitter.split_text(text)


def _chunk_table(table_text: str, max_tokens: int) -> List[str]:
    table_text = re.sub(r'<br\s*/?>', ' ', table_text)
    table_text = re.sub(r'•\s*\n\s*', '• ', table_text)
    table_text = re.sub(r'[ \t]+', ' ', table_text)
    if count_tokens(table_text) <= max_tokens:
        return [table_text]

    lines = table_text.strip().split('\n')
    sep_idx = -1
    for i, line in enumerate(lines[:10]):
        if re.search(r'\|[\s-]*:?[\s-]*\|', line):
            sep_idx = i
            break
    if sep_idx == -1:
        return _create_text_children(table_text, max_tokens)

    header = "\n".join(lines[:sep_idx + 1])
    header_tok = count_tokens(header)
    chunks, rows, cur_tok = [], [], header_tok

    for row in lines[sep_idx + 1:]:
        row = row.strip()
        if not row:
            continue
        rtok = count_tokens(row)
        if cur_tok + rtok > (max_tokens - 10) and rows:
            chunks.append(header + "\n" + "\n".join(rows))
            rows, cur_tok = [row], header_tok + rtok
        else:
            rows.append(row)
            cur_tok += rtok + 1
    if rows:
        chunks.append(header + "\n" + "\n".join(rows))
    return chunks


def build_children(parents: List[Dict], child_size_tokens: int = 200) -> List[Dict]:
    """
    Prend une liste de parents (sortie de build_parents), retourne les children.
    Non utilisé par le pipeline principal, conservé pour embedding/RAG futur.
    """
    children_out = []
    for parent in parents:
        txt = parent['text']
        is_table = txt.count('|') > txt.count('\n') and "---" in txt
        titre = parent.get('section_title') or ''
        is_toc = "Contents" in titre or "Table of Contents" in txt

        if is_table and not is_toc:
            child_texts = _chunk_table(txt, child_size_tokens)
        else:
            child_texts = _create_text_children(txt, child_size_tokens)

        for c_idx, child_text in enumerate(child_texts):
            if not is_informative(child_text):
                continue
            children_out.append({
                "chunk_id": f"{parent['parent_id']}_c_{c_idx:03d}",
                "parent_id": parent['parent_id'],
                "document_id": parent['document_id'],
                "project_id": parent['project_id'],
                "text": child_text,
                "filename": parent['filename'],
                "source_path": parent['source_path'],
                "page": parent.get('page_start'),
            })

    return children_out


# ---------------------------------------------------------------------------
# Entrée principale (pipeline utilise build_parents uniquement)
# ---------------------------------------------------------------------------
def build_parents(
    elements: List[Dict],
    project_id: str,
    document_id: str,
    parent_size_tokens: int = 2000,
) -> Tuple[List[Dict], int]:
    """
    Prend les éléments extraits d'UN document, retourne (parents, nb_skipped).
    IDs déterministes basés sur document_id.
    """
    # Phase 1 : split en sections
    raw_parents = []
    for el in elements:
        text = clean_markdown_images(el.get('text', ''))
        if not text or len(text.strip()) < 5:
            continue
        metadata = el.get('metadata', {})
        raw_parents.extend(
            split_markdown_by_headers(text, metadata, max_parent_tokens=parent_size_tokens)
        )

    # Phase 2 : construire les parents avec IDs stables
    parents_out = []
    skipped = 0

    for idx, parent in enumerate(raw_parents):
        text = parent['text']
        meta = parent.get('metadata', {})

        # Filtrage bruit
        if not is_informative(text):
            skipped += 1
            continue

        filename = meta.get('filename', 'unknown')
        source_path = meta.get('source_path', '')
        file_type = meta.get('file_type', 'other')
        page_label = meta.get('page_label')
        sheet_name = meta.get('sheet_name')

        tok = count_tokens(text)

        parents_out.append({
            "parent_id": f"{document_id}_{idx:04d}",
            "document_id": document_id,
            "project_id": project_id,
            "source_path": source_path,
            "filename": filename,
            "file_type": file_type,
            "section_type": parent.get('section_type', 'section'),
            "section_title": parent.get('title', ''),
            "page_start": page_label,
            "page_end": page_label,  # même page (split par page en amont)
            "sheet_name": sheet_name,
            "text": text,
            "char_count": len(text),
            "token_estimate": tok,
        })

    return parents_out, skipped
