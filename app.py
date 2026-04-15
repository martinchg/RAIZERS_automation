"""
RAIZERS — Interface d'audit automatique (Streamlit).

Usage local  : streamlit run app.py
Déploiement  : Streamlit Community Cloud (gratuit)
"""

import json
import contextlib
import importlib
import logging
import os
import re
import smtplib
import sys
import time
import base64
from datetime import datetime
from email.message import EmailMessage
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

# Setup paths
ROOT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT_DIR / "src"))

from runtime_config import configure_environment
from tab_audit import render_audit_tab
from tab_immo import render_real_estate_tab

configure_environment(ROOT_DIR)

from normalization import matches_pattern

OUTPUT_DIR = ROOT_DIR / "output"
LOGO_PATH = ROOT_DIR / "assets" / "raizers_logo.png"
BACKGROUND_PATH = ROOT_DIR / "assets" / "background.jpg"
HISTORY_PATH = OUTPUT_DIR / "audit_history.json"
AUTH_USER_ENV = "APP_AUTH_USER"
EN_AUDIT_PATTERNS = ["en audit", "*en audit*", "audit", "*audit*"]
AUDIT_PATTERNS = ["audit", "*audit", "audit*", "*audit*"]
OPERATEUR_PATTERNS = ["operateur", "*operateur", "operateur*", "*operateur*"]
AUTH_PASS_ENV = "APP_AUTH_PASS"
GEOCODER_URL = "https://data.geopf.fr/geocodage/search"
DVF_API_BASE_URL = "https://apidf-preprod.cerema.fr"
DVF_API_TOKEN = None
DEFAULT_TIMEOUT_SECONDS = 20
USER_AGENT = "comparateur-immo-streamlit/0.1"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="RAIZERS Audit", page_icon="📊", layout="centered")

def _load_background_data_url(path: Path) -> str:
    if not path.exists():
        return ""
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"

def _get_auth_credentials() -> tuple[str, str]:
    username = os.environ.get(AUTH_USER_ENV, "").strip()
    password = os.environ.get(AUTH_PASS_ENV, "").strip()
    return username, password

# --- Custom CSS ---
background_data_url = _load_background_data_url(BACKGROUND_PATH)

st.markdown(f"""
<style>
    /* Global */
    .stApp {{
        background:
            linear-gradient(rgba(11, 58, 103, 0.56), rgba(17, 72, 123, 0.56)),
            linear-gradient(rgba(8, 24, 42, 0.18), rgba(8, 24, 42, 0.18)),
            url("{background_data_url}");
        background-size: cover;
        background-position: center center;
        background-repeat: no-repeat;
        background-attachment: fixed;
    }}

    /* Header bar */
    .main-header {{
        background: linear-gradient(135deg, rgba(29,109,179,0.72) 0%, rgba(27,45,69,0.82) 55%, rgba(77,200,232,0.55) 150%);
        border-bottom: 3px solid #4DC8E8;
        padding: 2.5rem 2rem 2rem;
        margin: -1rem -1rem 2rem -1rem;
        text-align: center;
        backdrop-filter: blur(2px);
    }}
    .main-header h1 {{
        color: #FFFFFF !important;
        font-size: 2.4rem !important;
        font-weight: 700 !important;
        margin: 0 !important;
        letter-spacing: 2px;
    }}
    .main-header .accent {{
        color: #4DC8E8;
    }}
    .main-header p {{
        color: rgba(255,255,255,0.6);
        font-size: 0.95rem;
        margin: 0.5rem 0 0 0;
        letter-spacing: 3px;
        text-transform: uppercase;
    }}

    /* Cards */
    .step-card {{
        background: rgba(27,45,69,0.90);
        border: 1px solid rgba(77,200,232,0.15);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.8rem;
        backdrop-filter: blur(2px);
    }}
    .step-card h3 {{
        font-size: 1rem;
        color: #FFFFFF;
        margin: 0;
        font-weight: 600;
    }}
    .step-number {{
        display: inline-block;
        background: #4DC8E8;
        color: #0D1B2A;
        width: 26px;
        height: 26px;
        border-radius: 50%;
        text-align: center;
        line-height: 26px;
        font-size: 0.8rem;
        font-weight: 700;
        margin-right: 8px;
        vertical-align: middle;
    }}

    /* Result badges */
    .result-ok {{
        background: rgba(39,174,96,0.12);
        border-left: 4px solid #27AE60;
        color: #A8F0C8;
        padding: 0.6rem 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.4rem 0;
    }}
    .result-err {{
        background: rgba(231,76,60,0.12);
        border-left: 4px solid #E74C3C;
        color: #F5B7B1;
        padding: 0.6rem 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.4rem 0;
    }}

    /* Hide default streamlit header/footer */
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    footer {{visibility: hidden;}}

    /* Button styling */
    .stButton > button[kind="primary"] {{
        background: linear-gradient(135deg, #4DC8E8 0%, #2A9FBF 100%);
        color: #0D1B2A !important;
        border: none;
        border-radius: 10px;
        padding: 0.7rem 1.5rem;
        font-weight: 700;
        font-size: 1rem;
        letter-spacing: 0.3px;
        transition: transform 0.15s, box-shadow 0.15s;
    }}
    .stButton > button[kind="primary"]:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(77,200,232,0.35);
    }}

    /* Selectbox & inputs */
    .stSelectbox > div > div {{
        border-radius: 10px;
        background: rgba(27,45,69,0.92);
    }}
    .stTextInput > div > div > input {{
        background: rgba(27,45,69,0.92);
        border-radius: 10px;
    }}

    /* Login page */
    .login-container {{
        max-width: 520px;
        margin: 2rem auto 0 auto;
        background: rgba(27,45,69,0.90);
        border: 1px solid rgba(77,200,232,0.2);
        border-radius: 16px;
        padding: 2.5rem 2rem;
        text-align: center;
        box-shadow: 0 18px 45px rgba(13,27,42,0.28);
        backdrop-filter: blur(3px);
    }}
    .login-container h2 {{
        color: #FFFFFF;
        font-size: 1.6rem;
        margin-bottom: 0.3rem;
    }}
    .login-container .subtitle {{
        color: rgba(255,255,255,0.5);
        font-size: 0.85rem;
        margin-bottom: 1.5rem;
    }}
    .login-error {{
        background: rgba(231,76,60,0.15);
        border: 1px solid rgba(231,76,60,0.3);
        color: #F5B7B1;
        padding: 0.6rem 1rem;
        border-radius: 8px;
        margin-top: 0.5rem;
        font-size: 0.9rem;
    }}
    .top-logo {{
        margin: -0.25rem 0 1.5rem 0;
    }}
    .history-card {{
        background: rgba(27,45,69,0.92);
        border: 1px solid rgba(77,200,232,0.2);
        border-radius: 12px;
        padding: 0.85rem 1rem;
        color: #FFFFFF;
        margin-bottom: 0.7rem;
        backdrop-filter: blur(2px);
    }}
    .history-card span {{
        color: rgba(255,255,255,0.72);
        font-size: 0.88rem;
    }}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    auth_user, auth_pass = _get_auth_credentials()
    logo_left, logo_center, logo_right = st.columns([1, 5, 1])
    with logo_center:
        st.image(str(LOGO_PATH), use_container_width=True)

    col_left, col_center, col_right = st.columns([1.2, 2.6, 1.2])
    with col_center:
        st.markdown('<div class="login-container">', unsafe_allow_html=True)
        st.markdown("#### Connexion")
        if not auth_user or not auth_pass:
            st.error(
                f"Authentification non configurée. Ajoute {AUTH_USER_ENV} et {AUTH_PASS_ENV} "
                "dans `.env` en local ou dans `st.secrets` sur Streamlit Cloud."
            )
            st.stop()

        username = st.text_input("Identifiant", placeholder="Identifiant")
        password = st.text_input("Mot de passe", type="password", placeholder="Mot de passe")

        if st.button("Se connecter", type="primary", use_container_width=True):
            if username == auth_user and password == auth_pass:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.markdown(
                    '<div class="login-error">Identifiant ou mot de passe incorrect.</div>',
                    unsafe_allow_html=True,
                )
        st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# --- Header ---
st.markdown('<div class="top-logo">', unsafe_allow_html=True)
logo_left, logo_center, logo_right = st.columns([1, 5, 1])
with logo_center:
    st.image(str(LOGO_PATH), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)


table_audit, table_immo = st.tabs(["Audit", "Comparateur"])

with table_audit:
    render_audit_tab()

with table_immo:
    render_real_estate_tab()
