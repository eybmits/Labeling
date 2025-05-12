"""
Dataset Labeler – Version 2.0  (2025-05-12)
================================================

Changelog
---------
* **Deterministic randomisation** – URLs are shuffled once with a seed that is
  derived from the labeler-ID, so every reviewer gets a fixed but random order.
* **Locked labeler field** – After the first confirmation the name field is
  frozen and can’t be edited (and can be hidden entirely for surveys).
* **Guideline screen** – A short onboarding page is shown the first time the
  app is opened in a session.  Users must click *Weiter →* before any data is
  loaded.
* **Hidden Sheet link** – `SHOW_SHEET_LINK = False` by default so that the
  underlying spreadsheet remains invisible for non-authors.
* **Category tool-tips** – Each category checkbox gets a *?*-hint (Streamlit
  *help* parameter).  Explanations live in `SUBCATEGORY_DESCRIPTIONS`.

The code below is a full replacement of the previous single-file Streamlit app.
Copy it over your existing `app.py` (or similar) and adjust the *Secrets* block
as before.
"""

from __future__ import annotations

# ── Standard libs ────────────────────────────────────────────────────────────
import hashlib
import os
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Set
from urllib.parse import urlparse

# ── Third-party ──────────────────────────────────────────────────────────────
import gspread  # Google Sheets
import pandas as pd
import pytz  # Zeitzonen
import requests
import streamlit as st
import streamlit.components.v1 as components
from google.oauth2.service_account import Credentials  # Auth

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dataset Labeler 2.0",
    layout="wide",
    page_icon="📊",
)

# ── Constants & config ───────────────────────────────────────────────────────
DEFAULT_CSV_PATH = "input.csv"
SHOW_SHEET_LINK = False  # <–– keep the sheet URL private by default
TIMEZONE = pytz.timezone("Europe/Berlin")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Google Sheet columns (order matters!)
COL_TS = "Timestamp"
COL_LBL = "Labeler_ID"
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_TS, COL_LBL, COL_URL, COL_CATS, COL_COMMENT]

# Categories & colours ----------------------------------------------------------------
CATEGORIES: Dict[str, List[str]] = {
    "Health": [
        "Lifestyle",
        "Mental Health",
        "Physical Health",
        "Healthcare System",
    ],
    "Social": [
        "Education",
        "Family/Relationships",
        "Employment/Economy",
    ],
    "Environment": [
        "Environmental Policies",
        "Energy Sector",
        "Natural/Man-made Disasters",
    ],
}
ALL_CATEGORIES = [sub for subs in CATEGORIES.values() for sub in subs]

CATEGORY_COLORS = {
    "Health": "dodgerblue",
    "Social": "mediumseagreen",
    "Environment": "darkorange",
}

SUBCATEGORY_COLORS = {
    # Health
    "Lifestyle": "skyblue",
    "Mental Health": "lightcoral",
    "Physical Health": "mediumaquamarine",
    "Healthcare System": "steelblue",
    # Social
    "Education": "sandybrown",
    "Family/Relationships": "lightpink",
    "Employment/Economy": "khaki",
    # Environment
    "Environmental Policies": "mediumseagreen",
    "Energy Sector": "gold",
    "Natural/Man-made Disasters": "slategray",
    # Fallback
    "DEFAULT_COLOR": "grey",
}

# Short tool-tip texts (may be localised later)
SUBCATEGORY_DESCRIPTIONS = {
    "Lifestyle": "Ernährung, Sport, Freizeit …",
    "Mental Health": "Depression, Angst, psychische Gesundheit etc.",
    "Physical Health": "Krankheiten, Fitness, körperliche Gesundheit",
    "Healthcare System": "Krankenversicherung, Spitäler, Pflege …",
    "Education": "Schule, Hochschulen, Weiterbildung …",
    "Family/Relationships": "Ehe, Kinder, Soziale Beziehungen",
    "Employment/Economy": "Arbeit, Arbeitsmarkt, Löhne …",
    "Environmental Policies": "Gesetze, Verordnungen, Politik & Umwelt",
    "Energy Sector": "Strom- & Wärmeerzeugung, Infrastruktur",
    "Natural/Man-made Disasters": "Stürme, Brände, Industrieunfälle …",
}

# ── Helpers ---------------------------------------------------------------------------

def connect_gsheet():
    """Return `(worksheet, header_was_written)` or `None` if connection failed."""
    try:
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
    except KeyError as exc:
        st.error(f"Fehlendes Secret: {exc}")
        return None, False

    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        ws = gc.open(sheet_name).sheet1
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Google Sheet '{sheet_name}' nicht gefunden.")
        return None, False

    # Ensure header is correct --------------------------------------------------------
    header_written = False
    current_header = ws.row_values(1)
    if current_header != HEADER:
        ws.insert_row(HEADER, 1)
        header_written = True
    return ws, header_written


def clean_tweet_url(url: str) -> str:
    base = url.split("?")[0]
    return re.sub(r"/(photo|video)/\d+$", "", base)


def get_tweet_embed_html(url: str):
    try:
        p = urlparse(url)
        if p.netloc not in {"twitter.com", "www.twitter.com", "x.com", "www.x.com"}:
            return None
        if "/status/" not in p.path:
            return None
    except Exception:
        return None
    cleaned = clean_tweet_url(url)
    api = f"https://publish.twitter.com/oembed?url={cleaned}&omit_script=true&theme=dark"
    try:
        res = requests.get(api, timeout=15)
        res.raise_for_status()
        return res.json().get("html")
    except requests.RequestException:
        return None


def save_row(ws, labeler_id: str, url: str, cats: List[str], comment: str):
    if not ws:
        st.error("Keine Sheet-Verbindung verfügbar.")
        return False
    stamp = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z%z")
    row = [stamp, labeler_id, url, "; ".join(cats), comment]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception as exc:
        st.error(f"Fehler beim Schreiben: {exc}")
        return False


def deterministic_shuffle(items: List[str], seed: int | str):
    """Shuffle *in place* with a reproducible seed."""
    if isinstance(seed, str):
        seed = int(hashlib.sha256(seed.encode()).hexdigest(), 16) % (2 ** 32)
    rnd = random.Random(seed)
    rnd.shuffle(items)


# ── Session-state init ----------------------------------------------------------------
if "guidelines_done" not in st.session_state:
    st.session_state.guidelines_done = False
if "labeler_id" not in st.session_state:
    st.session_state.labeler_id = ""
if "labeler_locked" not in st.session_state:
    st.session_state.labeler_locked = False
if "urls" not in st.session_state:
    st.session_state.urls: List[str] = []
if "idx" not in st.session_state:
    st.session_state.idx = 0
if "results" not in st.session_state:
    st.session_state.results: Dict[int, List[str]] = {}
if "comments" not in st.session_state:
    st.session_state.comments: Dict[int, str] = {}


# ── 1) Guidelines screen --------------------------------------------------------------
if not st.session_state.guidelines_done:
    st.title("Willkommen 👋")
    st.markdown(
        """
        **Bitte lies diese kurze Anleitung, bevor du mit dem Labeln beginnst.**

        1. Wähle pro Post mindestens **eine** passende Kategorie.
        2. Bleib konsistent – entscheide dich im Zweifel wie beim letzten Mal.
        3. Optionale Kommentare helfen uns bei Rückfragen.
        """
    )
    if st.button("Weiter →", type="primary"):
        st.session_state.guidelines_done = True
        st.experimental_rerun()
    st.stop()

# ── 2) Labeler ID (locked after first submit) -----------------------------------------
labeler_input = st.text_input(
    "👤 Dein Name (wird gespeichert)",
    value=st.session_state.labeler_id,
    disabled=st.session_state.labeler_locked,
    key="labeler_id_input",
)

if labeler_input and not st.session_state.labeler_locked:
    st.session_state.labeler_id = labeler_input.strip()
    st.session_state.labeler_locked = True
    st.experimental_rerun()

if not st.session_state.labeler_id:
    st.warning("Bitte gib deinen Namen ein, um fortzufahren.")
    st.stop()

# ── 3) Connect to sheet and load data -------------------------------------------------
worksheet, header_fixed = connect_gsheet()

if not st.session_state.urls:
    try:
        df = pd.read_csv(DEFAULT_CSV_PATH, header=None, usecols=[0])
    except FileNotFoundError:
        st.error(f"Datei '{DEFAULT_CSV_PATH}' nicht gefunden.")
        st.stop()
    raw_urls = [u.strip() for u in df.iloc[:, 0].dropna().unique()]
    deterministic_shuffle(raw_urls, st.session_state.labeler_id)  # <–– seed!
    st.session_state.urls = raw_urls

urls = st.session_state.urls
if not urls:
    st.success("Nichts mehr zu tun – alle Einträge bearbeitet ✨")
    st.stop()

# ── 4) Main UI ------------------------------------------------------------------------
current_idx = st.session_state.idx
if current_idx >= len(urls):
    st.success("Alle URLs sind erledigt 🎉")
    st.balloons()
    st.stop()

url = urls[current_idx]

cols = st.columns([2, 1])

# Left – preview -----------------------------------------------------------------------
with cols[0]:
    st.subheader("Vorschau")
    html = get_tweet_embed_html(url)
    if html:
        components.html(html, height=650, scrolling=True)
    else:
        st.markdown(f"[Zum Beitrag]({url})", unsafe_allow_html=True)

# Right – categories -------------------------------------------------------------------
with cols[1]:
    st.subheader("Kategorie wählen")

    chosen: List[str] = st.session_state.results.get(current_idx, [])

    tmp_selected: List[str] = []
    for main, subs in CATEGORIES.items():
        st.markdown(
            f"<h6 style='color:{CATEGORY_COLORS.get(main)}; border-bottom:1px solid {CATEGORY_COLORS.get(main)}'>" +
            f"{main}</h6>",
            unsafe_allow_html=True,
        )
        for sub in subs:
            key = f"cb_{current_idx}_{sub}"
            help_txt = SUBCATEGORY_DESCRIPTIONS.get(sub, "Keine Beschreibung vorhanden.")
            default = sub in chosen
            if st.checkbox(sub, value=default, key=key, help=help_txt):
                tmp_selected.append(sub)

    st.markdown("---")
    if tmp_selected:
        st.write("**Ausgewählt:**", ", ".join(tmp_selected))
    else:
        st.info("Bitte mindestens eine Kategorie wählen.")

    comment_key = f"cmt_{current_idx}"
    comment = st.text_area(
        "Kommentar (optional)",
        value=st.session_state.comments.get(current_idx, ""),
        key=comment_key,
    )

# ── Navigation / actions --------------------------------------------------------------
nav = st.columns([1, 1, 4, 1, 1])

# Back
with nav[0]:
    if st.button("⬅️ Zurück", disabled=current_idx == 0):
        st.session_state.idx -= 1
        st.experimental_rerun()

# Skip
with nav[1]:
    if st.button("Überspringen", disabled=current_idx == len(urls) - 1):
        st.session_state.idx += 1
        st.experimental_rerun()

# Spacer (progress bar)
with nav[2]:
    st.progress((current_idx + 1) / len(urls), text=f"{current_idx + 1} / {len(urls)}")

# Save & next
with nav[4]:
    if st.button("Speichern & Weiter ➡️", type="primary"):
        if tmp_selected:
            ok = save_row(worksheet, st.session_state.labeler_id, url, tmp_selected, comment)
            if ok:
                st.session_state.results[current_idx] = tmp_selected
                st.session_state.comments[current_idx] = comment
                st.session_state.idx += 1
                st.experimental_rerun()
        else:
            st.warning("Du musst mindestens eine Kategorie auswählen.")

# ── Sidebar info ----------------------------------------------------------------------
st.sidebar.header("Status")
st.sidebar.metric("Fortschritt", f"{current_idx + 1} / {len(urls)}")

if SHOW_SHEET_LINK and worksheet:
    st.sidebar.page_link(worksheet.spreadsheet.url, label="Sheet öffnen ↗️")
