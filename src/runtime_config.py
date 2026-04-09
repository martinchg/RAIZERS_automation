"""
runtime_config.py : charge la configuration runtime locale et Streamlit.

Source de vérité pour le reste du code : os.environ
"""

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def configure_environment(root_dir: str | Path) -> None:
    """Charge `.env` en local puis complète depuis `st.secrets` si disponible."""
    root_dir = Path(root_dir).resolve()
    load_dotenv(root_dir / ".env", override=False)
    _load_streamlit_secrets()


def _load_streamlit_secrets() -> None:
    try:
        import streamlit as st

        _inject_into_environ(st.secrets)
    except Exception:
        # Hors Streamlit Cloud/local sans secrets configurés: on garde simplement os.environ.
        return


def _inject_into_environ(values: Mapping[str, Any]) -> None:
    for key, value in values.items():
        if isinstance(value, Mapping):
            _inject_into_environ(value)
            continue
        if value is None:
            continue
        os.environ.setdefault(str(key), str(value))
