# Importiere notwendige Bibliotheken
import streamlit as st
import pandas as pd
import os
import random
import requests
from urllib.parse import urlparse
import time
import re
import gspread # Für Google Sheets
from google.oauth2.service_account import Credentials # Für Authentifizierung
from datetime import datetime # Für Zeitstempel
import pytz # Für Zeitzonen

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="URL-Kategorisierer (Multi-Labeler)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfad zur Standard-CSV-Datei ===
DEFAULT_CSV_PATH = "input.csv"

# === Google Sheets Setup ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

# NEUE Spaltennamen im Google Sheet (REIHENFOLGE WICHTIG!)
COL_TS = "Timestamp"
COL_LBL = "Labeler_ID"
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_TS, COL_LBL, COL_URL, COL_CATS, COL_COMMENT] # Neue Header-Reihenfolge

# Zeitzone für Zeitstempel
TIMEZONE = pytz.timezone("Europe/Berlin")

@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her und gibt das Worksheet-Objekt zurück."""
    try:
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1
        header_written = False
        all_vals = worksheet.get_all_values()
        if not all_vals or all_vals[0] != HEADER : # Prüfe ob leer ODER Header falsch ist
             # Lösche alten Inhalt (optional, aber oft sinnvoll bei Schemaänderung)
             # worksheet.clear()
             worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED') # Füge Header in Zeile 1 ein
             # Entferne ggf. leere Standardzeilen danach
             if len(worksheet.get_all_values()) > 1 and all(v == '' for v in worksheet.row_values(2)):
                 worksheet.delete_rows(2)

             header_written = True
             st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")

        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt. Prüfe secrets.toml/Cloud Secrets."); st.stop()
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop()
    except Exception as e: st.error(f"Fehler bei Google Sheets Verbindung: {e}"); st.stop(); return None, False, None

worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===
CATEGORIES = {
    "Personal Well-being": ["Lifestyle", "Mental Health", "Physical Health", "Family/Relationships"],
    "Societal Systems": ["Healthcare System", "Education System", "Employment/Economy", "Energy Sector"],
    "Environment & Events": ["Environmental Policies", "(Natural/Man-made) Disasters"],
    "Other": ["Politics (General)", "Technology", "Miscellaneous"]
}
ALL_CATEGORIES = [cat for sublist in CATEGORIES.values() for cat in sublist]

# === Hilfsfunktionen ===
# `load_processed_urls_gsheet` wird NICHT mehr benötigt, um Input zu filtern!

@st.cache_data
def load_urls_from_input_csv(file_input_object, source_name="hochgeladene Datei"):
    """Lädt alle URLs aus einem Datei-Objekt (Upload oder geöffnet)."""
    urls = []
    if not file_input_object: st.error("Kein Datei-Objekt."); return urls
    try:
        if hasattr(file_input_object, 'seek'): file_input_object.seek(0)
        df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=True)
        url_series = df.iloc[:, 0].dropna().astype(str)
        urls = url_series[url_series.str.startswith(("http://", "https://"))].unique().tolist()
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer/enthält keine URLs.")
    except IndexError: st.warning(f"Input '{source_name}' hat keine Spalten (Format?).")
    except Exception as e: st.error(f"Fehler beim Lesen von '{source_name}': {e}")
    return urls

# ANGEPASSTE Speicherfunktion
def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Hängt eine neue Zeile mit Labeler-ID und Zeitstempel an."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt. Speichern nicht möglich."); return False

    try:
        # Zeitstempel generieren
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        # Daten in der NEUEN Reihenfolge vorbereiten
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        # Cache für URLs nicht mehr relevant zum Leeren
        return True
    except gspread.exceptions.APIError as e: st.error(f"Sheets API Fehler (Speichern): {e}"); return False
    except Exception as e: st.error(f"Unerw. Fehler (Speichern): {e}"); return False

def clean_tweet_url(url):
    cleaned_url = re.sub(r"/(photo|video)/\d+(?=\?|$).*", "", url)
    if "/photo/" in url and cleaned_url == url: cleaned_url = url.split("/photo/")[0]
    elif "/video/" in url and cleaned_url == url: cleaned_url = url.split("/video/")[0]
    return cleaned_url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    # (Funktion bleibt unverändert)
    try:
        parsed_url = urlparse(tweet_url)
        if parsed_url.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"]: return None
    except Exception: return None
    api_url = f"https://publish.twitter.com/oembed?url={tweet_url}&maxwidth=550&omit_script=false&dnt=true"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json().get("html")
    except requests.exceptions.Timeout: print(f"Timeout embed: {tweet_url}"); return None
    except requests.exceptions.RequestException as e: status = e.response.status_code if e.response else "N/A"; print(f"Embed fail {status}: {tweet_url}"); return None
    except Exception as e: st.warning(f"Embed error {tweet_url}: {e}", icon="❓"); return None

# === Streamlit App Hauptteil ===
st.title("📊 URL-Kategorisierer (Multi-Labeler)")

# --- Session State Initialisierung ---
# Füge labeler_id hinzu
if 'labeler_id' not in st.session_state:
    st.session_state.labeler_id = ""
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.input_file_name = None
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set() # Zählt nur für UI-Feedback, nicht für Filterung
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.default_loaded = False

# --- Labeler ID Eingabe ---
labeler_id_input = st.text_input(
    "👤 Bitte gib deine Labeler ID ein (z.B. Name oder Kürzel):",
    value=st.session_state.labeler_id,
    key="labeler_id_widget" # Key, um den Wert wieder zu lesen
)
# Aktualisiere session state, wenn sich Eingabe ändert
st.session_state.labeler_id = labeler_id_input

# --- Nur weitermachen, wenn Labeler ID eingegeben wurde ---
if not st.session_state.labeler_id:
    st.warning("Bitte gib zuerst deine Labeler ID oben ein, um zu starten.")
    st.stop() # Hält die App hier an

st.divider()

# --- Dateiauswahl und Verarbeitung (angepasst) ---
uploaded_file = st.file_uploader(
    "1. Optional: Lade eine andere CSV hoch (überschreibt Standard)",
    type=["csv"]
)

file_input = None
file_source_name = None
trigger_processing = False

if uploaded_file is not None:
    # Hochgeladene Datei hat Priorität
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        file_input = uploaded_file
        file_source_name = uploaded_file.name
        trigger_processing = True
        st.session_state.default_loaded = False
elif not st.session_state.initialized and not st.session_state.default_loaded:
    # Versuche Standard, wenn nicht initialisiert/default geladen
    if os.path.exists(DEFAULT_CSV_PATH):
        file_input = DEFAULT_CSV_PATH
        file_source_name = DEFAULT_CSV_PATH
        trigger_processing = True
        st.session_state.default_loaded = True
    else:
        st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' nicht gefunden. Lade eine CSV hoch.")

if trigger_processing and worksheet:
    # Reset für neue Datei
    st.session_state.initialized = False
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.input_file_name = file_source_name

    with st.spinner(f"Verarbeite '{file_source_name}'..."):
        all_input_urls = []
        if isinstance(file_input, str): # Standarddatei Pfad
            try:
                with open(file_input, 'rb') as f_default:
                    all_input_urls = load_urls_from_input_csv(f_default, source_name=file_source_name)
            except Exception as e: st.error(f"Fehler Lesen Standarddatei '{file_source_name}': {e}")
        elif file_input is not None: # UploadedFile Objekt
            all_input_urls = load_urls_from_input_csv(file_input, source_name=file_source_name)

        if all_input_urls:
            # !!! WICHTIG: NICHT MEHR GEGEN GOOGLE SHEET FILTERN !!!
            st.session_state.urls_to_process = all_input_urls
            # Optional: Mischen, wenn gewünscht, dass jeder eine andere Reihenfolge bekommt
            # random.shuffle(st.session_state.urls_to_process)
            st.session_state.total_items = len(st.session_state.urls_to_process)
            st.session_state.current_index = 0
            st.success(f"{st.session_state.total_items} URLs aus '{file_source_name}' geladen. Bereit zum Labeln für '{st.session_state.labeler_id}'.")
            st.session_state.initialized = True
        else:
             st.error(f"Datei '{file_source_name}' enthält keine gültigen URLs oder konnte nicht gelesen werden.")
             st.session_state.initialized = False
             st.session_state.default_loaded = False
elif trigger_processing and not worksheet:
     st.error("Sheet-Verbindung fehlgeschlagen."); st.session_state.initialized = False; st.session_state.default_loaded = False

# --- Haupt-Labeling-Interface ---
if st.session_state.get('initialized', False) and st.session_state.urls_to_process:
    total_items = st.session_state.total_items
    if st.session_state.current_index >= total_items:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Du hast alle {total_items} URLs aus '{st.session_state.input_file_name}' bearbeitet!")
        st.balloons()
        st.info(f"Deine Ergebnisse wurden im Google Sheet '{connected_sheet_name}' gespeichert.")
        if worksheet:
            try: sheet_url = worksheet.spreadsheet.url; st.link_button("Google Sheet öffnen", sheet_url)
            except Exception: pass
        if st.button("Bearbeitung zurücksetzen / Andere Datei laden"):
             st.session_state.initialized = False
             st.session_state.input_file_name = None
             st.session_state.default_loaded = False
             st.session_state.urls_to_process = [] # Wichtig
             st.session_state.total_items = 0
             st.session_state.processed_urls_in_session = set()
             st.session_state.current_index = 0
             st.session_state.session_results = {}
             st.session_state.session_comments = {}
             # Labeler ID bleibt erhalten für die Sitzung
             st.rerun()
        st.stop()

    current_idx = st.session_state.current_index
    # --- Navigation und Fortschritt ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True): st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    progress_text = f"{st.session_state.labeler_id}: Link {current_idx + 1} von {total_items} (aus '{st.session_state.input_file_name}')"
    nav_cols_top[1].progress((current_idx + 1) / total_items, text=progress_text)
    st.divider()

    # --- URL Anzeige & Einbettung (unverändert) ---
    current_url = st.session_state.urls_to_process[current_idx]
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url)
    embed_html = get_tweet_embed_html(base_tweet_url)
    if embed_html:
        st.components.v1.html(embed_html, height=650, scrolling=True)
        if base_tweet_url != current_url: st.caption(f"Original-URL (bereinigt): [{current_url}]({current_url})")
    else:
        st.markdown(f"**URL:** [{current_url}]({current_url})")
        if "twitter.com" in current_url or "x.com" in current_url: st.caption("Vorschau nicht verfügbar.")
        else: st.caption("Vorschau nur für X/Twitter Links.")
        st.link_button("Link in neuem Tab öffnen", current_url)
    st.divider()

    # --- Kategorieauswahl & Kommentar (unverändert)---
    st.subheader("Kategorisierung")
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("**Wähle passende Kategorien:**")
        selected_categories_in_widgets = []
        default_selection = st.session_state.session_results.get(current_idx, [])
        for main_topic, sub_categories in CATEGORIES.items():
            with st.expander(f"**{main_topic}**", expanded=True):
                expander_key = f"multiselect_{current_idx}_{main_topic.replace(' ', '_').replace('&','_').replace('/','_')}"
                current_selection = st.multiselect(" ",options=sub_categories,default=[cat for cat in default_selection if cat in sub_categories],key=expander_key,label_visibility="collapsed")
                selected_categories_in_widgets.extend(current_selection)
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets)))
        if selected_categories_in_widgets: st.write("**Ausgewählt:**"); st.info(", ".join(selected_categories_in_widgets))
        else: st.write("_Keine Kategorien ausgewählt._")
    with col2:
        default_comment = st.session_state.session_comments.get(current_idx, "")
        comment_key = f"comment_{current_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=200, key=comment_key)
    st.divider()

    # --- Navigationsbuttons (Unten) ---
    nav_cols_bottom = st.columns(7)
    if current_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom", use_container_width=True): st.session_state.session_results[current_idx]=selected_categories_in_widgets; st.session_state.session_comments[current_idx]=comment; st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom_disabled", disabled=True, use_container_width=True)
    # Speichern & Weiter (ruft angepasste Speicherfunktion auf)
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_labeler_id = st.session_state.labeler_id # Hole aktuelle ID
        if not selected_categories_in_widgets: st.warning("Bitte wähle mindestens eine Kategorie aus.")
        elif not worksheet: st.error("Keine Verbindung zum Google Sheet zum Speichern.")
        elif not current_labeler_id: st.error("Labeler ID nicht gesetzt. Bitte oben eingeben.")
        else:
            categories_str = "; ".join(selected_categories_in_widgets)
            # Rufe die NEUE Speicherfunktion mit Labeler ID auf
            if save_categorization_gsheet(worksheet, current_labeler_id, current_url, categories_str, comment):
                st.session_state.session_results[current_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_idx] = comment
                st.session_state.processed_urls_in_session.add(current_idx) # Zähle Index als in Session bearbeitet
                st.session_state.current_index += 1
                st.rerun()
            else: st.error("Speichern in Google Sheet fehlgeschlagen.")

# --- Fallback-Anzeige ---
elif not st.session_state.get('initialized', False) and uploaded_file is None and not st.session_state.get('default_loaded', False):
    if worksheet and st.session_state.labeler_id: # Nur wenn ID da ist und Sheet verbunden
        st.info(f"Versuche, Standarddatei '{DEFAULT_CSV_PATH}' zu laden oder lade eine andere CSV hoch.")

# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    # Header Check Nachricht entfernt, da sie oben steht
    try: sheet_url = worksheet.spreadsheet.url; st.sidebar.page_link(sheet_url, label="Sheet öffnen ↗️")
    except Exception: pass
else: st.sidebar.error("Keine Verbindung zum Google Sheet.")

st.sidebar.markdown(f"**Aktuelle/r Labeler/in:** `{st.session_state.labeler_id or '(Bitte oben eingeben)'}`")

current_input_info = st.session_state.get('input_file_name', None)
if current_input_info: st.sidebar.markdown(f"**Input-Datei:** `{current_input_info}`")
else: st.sidebar.markdown("**Input-Datei:** -")

st.sidebar.markdown(f"**Datenbank:** Google Sheet")
st.sidebar.markdown(f"**Format Sheet:** Spalten `{', '.join(HEADER)}`")
st.sidebar.markdown("**Fortschritt:** Jedes Labeling wird als neue Zeile gespeichert.")
if st.session_state.get('initialized', False):
    # Angepasste Metriken
    total_urls_in_file = st.session_state.total_items
    labeled_in_session = len(st.session_state.processed_urls_in_session)
    st.sidebar.metric("URLs in Datei", total_urls_in_file)
    st.sidebar.metric("Gelabelt (diese Sitzung)", labeled_in_session)
else:
    st.sidebar.metric("URLs in Datei", "-")
    st.sidebar.metric("Gelabelt (diese Sitzung)", "-")