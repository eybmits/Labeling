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
from gspread.exceptions import CellNotFound # Für Fortschritts-Handling

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="URL-Kategorisierer (Buttons)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfade und Namen ===
DEFAULT_CSV_PATH = "input.csv"
DATA_SHEET_NAME = "Sheet1"
PROGRESS_SHEET_NAME = "Progress"

# === Google Sheets Setup ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
DATA_COL_TS = "Timestamp"; DATA_COL_LBL = "Labeler_ID"; DATA_COL_URL = "URL"; DATA_COL_CATS = "Kategorien"; DATA_COL_COMMENT = "Kommentar"
DATA_HEADER = [DATA_COL_TS, DATA_COL_LBL, DATA_COL_URL, DATA_COL_CATS, DATA_COL_COMMENT]
PROG_COL_LBL = "Labeler_ID"; PROG_COL_IDX = "Last_Index"
PROGRESS_HEADER = [PROG_COL_LBL, PROG_COL_IDX]
TIMEZONE = pytz.timezone("Europe/Berlin")

# (connect_gsheets Funktion bleibt unverändert)
@st.cache_resource
def connect_gsheets():
    ws_data = None; ws_progress = None; header_data_written=False; header_progress_written=False
    sheet_name = "Nicht Verbunden"
    try:
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]; sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES); gc = gspread.authorize(creds)
        spreadsheet = gc.open(sheet_name)
        try: # Daten Sheet
            ws_data = spreadsheet.worksheet(DATA_SHEET_NAME); all_vals_data = ws_data.get_all_values()
            if not all_vals_data or all_vals_data[0] != DATA_HEADER:
                 ws_data.insert_row(DATA_HEADER, 1, value_input_option='USER_ENTERED')
                 if len(ws_data.get_all_values()) > 1 and all(v == '' for v in ws_data.row_values(2)): ws_data.delete_rows(2)
                 header_data_written = True
        except gspread.exceptions.WorksheetNotFound: st.error(f"Daten-Sheet '{DATA_SHEET_NAME}' fehlt!"); ws_data = None
        try: # Fortschritt Sheet
            ws_progress = spreadsheet.worksheet(PROGRESS_SHEET_NAME); all_vals_progress = ws_progress.get_all_values()
            if not all_vals_progress or all_vals_progress[0] != PROGRESS_HEADER:
                ws_progress.insert_row(PROGRESS_HEADER, 1, value_input_option='USER_ENTERED')
                if len(ws_progress.get_all_values()) > 1 and all(v == '' for v in ws_progress.row_values(2)): ws_progress.delete_rows(2)
                header_progress_written = True
        except gspread.exceptions.WorksheetNotFound: st.error(f"Fortschritt-Sheet '{PROGRESS_SHEET_NAME}' fehlt!"); ws_progress = None
        if ws_data and ws_progress: st.sidebar.success(f"Verbunden: '{sheet_name}'");
        return ws_data, ws_progress, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt."); st.stop()
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop()
    except Exception as e: st.error(f"GSheets Verbindung Fehler: {e}"); st.stop(); return None, None, "Fehler"

worksheet_data, worksheet_progress, connected_sheet_name = connect_gsheets()

# === Einstellungen & Visuelle Marker ===
CATEGORIES = {
    "Personal Well-being": ["Lifestyle", "Mental Health", "Physical Health", "Family/Relationships"],
    "Societal Systems": ["Healthcare System", "Education System", "Employment/Economy", "Energy Sector"],
    "Environment & Events": ["Environmental Policies", "(Natural/Man-made) Disasters"],
    "Other": ["Politics (General)", "Technology", "Miscellaneous"]
}
ALL_CATEGORIES = [cat for sublist in CATEGORIES.values() for cat in sublist]
CATEGORY_MARKERS = { "Personal Well-being": "❤️", "Societal Systems": "⚙️", "Environment & Events": "🌳", "Other": "⚪" }
DEFAULT_MARKER = "❓"

# === Hilfsfunktionen ===
# (load_urls_from_input_csv, save_categorization_gsheet, get_labeler_progress, save_labeler_progress,
#  clean_tweet_url, get_tweet_embed_html bleiben unverändert)
@st.cache_data
def load_urls_from_input_csv(file_input_object, source_name="hochgeladene Datei"):
    urls = []
    if not file_input_object: st.error("Kein Datei-Objekt."); return urls
    try:
        if hasattr(file_input_object, 'seek'): file_input_object.seek(0)
        df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=True)
        url_series = df.iloc[:, 0].dropna().astype(str)
        urls = url_series[url_series.str.startswith(("http://", "https://"))].unique().tolist()
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' leer/ohne URLs.")
    except IndexError: st.warning(f"Input '{source_name}' ohne Spalten (Format?).")
    except Exception as e: st.error(f"Fehler Lesen '{source_name}': {e}")
    return urls

def save_categorization_gsheet(ws_data_obj, labeler_id, url, categories_str, comment):
    if not ws_data_obj: st.error("Keine Daten-Sheet-Verbindung."); return False
    if not labeler_id: st.error("Labeler ID fehlt."); return False
    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        ws_data_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except Exception as e: st.error(f"Fehler Speichern (Daten): {e}"); return False

@st.cache_data(ttl=60)
def get_labeler_progress(ws_progress_obj, labeler_id):
    if not ws_progress_obj or not labeler_id: return 0
    try:
        cell = ws_progress_obj.find(labeler_id, in_column=1)
        last_index = ws_progress_obj.cell(cell.row, 2).value
        return int(last_index) if last_index and last_index.isdigit() else 0
    except CellNotFound: return 0
    except Exception as e: st.warning(f"Fehler Laden Fortschritt '{labeler_id}': {e}"); return 0

def save_labeler_progress(ws_progress_obj, labeler_id, current_index):
    if not ws_progress_obj or not labeler_id: st.warning("Kann Fortschritt nicht speichern."); return False
    try:
        cell = ws_progress_obj.find(labeler_id, in_column=1)
        ws_progress_obj.update_cell(cell.row, 2, str(current_index))
        get_labeler_progress.clear(); print(f"Fortschritt für {labeler_id} -> {current_index} gespeichert."); return True
    except CellNotFound:
        try: ws_progress_obj.append_row([labeler_id, str(current_index)], value_input_option='USER_ENTERED'); get_labeler_progress.clear(); print(f"Neuer Fortschritt für {labeler_id} -> {current_index} erstellt."); return True
        except Exception as e: st.error(f"Fehler Hinzufügen Fortschritt '{labeler_id}': {e}"); return False
    except Exception as e: st.error(f"Fehler Update Fortschritt '{labeler_id}': {e}"); return False

def clean_tweet_url(url):
    cleaned_url = re.sub(r"/(photo|video)/\d+(?=\?|$).*", "", url)
    if "/photo/" in url and cleaned_url == url: cleaned_url = url.split("/photo/")[0]
    elif "/video/" in url and cleaned_url == url: cleaned_url = url.split("/video/")[0]
    return cleaned_url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    try:
        parsed_url = urlparse(tweet_url)
        if parsed_url.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"]: return None
    except Exception: return None
    api_url = f"https://publish.twitter.com/oembed?url={tweet_url}&maxwidth=550&omit_script=false&dnt=true"
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status(); return response.json().get("html")
    except requests.exceptions.Timeout: print(f"Timeout embed: {tweet_url}"); return None
    except requests.exceptions.RequestException as e: status = e.response.status_code if e.response else "N/A"; print(f"Embed fail {status}: {tweet_url}"); return None
    except Exception as e: st.warning(f"Embed error {tweet_url}: {e}", icon="❓"); return None

# === Streamlit App Hauptteil ===
st.title("📊 URL-Kategorisierer (Button-Auswahl & Fortschritt)")

# --- Session State ---
if 'labeler_id' not in st.session_state: st.session_state.labeler_id = ""
if 'initialized' not in st.session_state: st.session_state.initialized = False
if 'input_file_name' not in st.session_state: st.session_state.input_file_name = None
if 'urls_to_process' not in st.session_state: st.session_state.urls_to_process = []
if 'total_items' not in st.session_state: st.session_state.total_items = 0
if 'processed_urls_in_session' not in st.session_state: st.session_state.processed_urls_in_session = set()
if 'current_index' not in st.session_state: st.session_state.current_index = 0
if 'session_results' not in st.session_state: st.session_state.session_results = {}
if 'session_comments' not in st.session_state: st.session_state.session_comments = {}
if 'default_loaded' not in st.session_state: st.session_state.default_loaded = False
if 'progress_loaded_for_session' not in st.session_state: st.session_state.progress_loaded_for_session = False

# --- Labeler ID Eingabe ---
labeler_id_input = st.text_input(
    "👤 Deine Labeler ID:", value=st.session_state.labeler_id, key="labeler_id_widget",
    on_change=lambda: setattr(st.session_state, 'initialized', False) or setattr(st.session_state, 'progress_loaded_for_session', False)
)
st.session_state.labeler_id = labeler_id_input.strip()

if not st.session_state.labeler_id: st.warning("Bitte Labeler ID eingeben."); st.stop()
if not worksheet_data or not worksheet_progress: st.error("Sheet-Verbindung(en) fehlen."); st.stop()
st.divider()

# --- Dateiauswahl & Verarbeitung ---
uploaded_file = st.file_uploader("1. Optional: Andere CSV hochladen", type=["csv"])
file_input = None; file_source_name = None; trigger_processing = False
if uploaded_file is not None:
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        file_input = uploaded_file; file_source_name = uploaded_file.name; trigger_processing = True; st.session_state.default_loaded = False
elif not st.session_state.initialized and not st.session_state.default_loaded:
    if os.path.exists(DEFAULT_CSV_PATH): file_input = DEFAULT_CSV_PATH; file_source_name = DEFAULT_CSV_PATH; trigger_processing = True; st.session_state.default_loaded = True
    else: st.info(f"Standard '{DEFAULT_CSV_PATH}' fehlt. Bitte hochladen.")

if trigger_processing and worksheet_data and worksheet_progress:
    st.session_state.initialized = False; st.session_state.urls_to_process = []; st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set(); st.session_state.current_index = 0
    st.session_state.session_results = {}; st.session_state.session_comments = {}; st.session_state.progress_loaded_for_session = False
    st.session_state.input_file_name = file_source_name
    with st.spinner(f"Verarbeite '{file_source_name}'..."):
        all_input_urls = []
        if isinstance(file_input, str):
            try:
                with open(file_input, 'rb') as f_default: all_input_urls = load_urls_from_input_csv(f_default, source_name=file_source_name)
            except Exception as e: st.error(f"Fehler Lesen '{file_source_name}': {e}")
        elif file_input is not None:
            all_input_urls = load_urls_from_input_csv(file_input, source_name=file_source_name)
        if all_input_urls:
            st.session_state.urls_to_process = all_input_urls; st.session_state.total_items = len(all_input_urls)
            st.session_state.current_index = 0; st.session_state.initialized = True
            st.info(f"{st.session_state.total_items} URLs aus '{file_source_name}' geladen für '{st.session_state.labeler_id}'. Lade Fortschritt...")
        else: st.error(f"'{file_source_name}' ohne URLs/Fehler."); st.session_state.initialized=False; st.session_state.default_loaded=False

# --- Fortschritt laden ---
if st.session_state.initialized and not st.session_state.progress_loaded_for_session:
    if worksheet_progress and st.session_state.labeler_id:
        with st.spinner("Lade Fortschritt..."):
            saved_index = get_labeler_progress(worksheet_progress, st.session_state.labeler_id)
            st.session_state.current_index = min(saved_index, st.session_state.total_items)
            st.session_state.progress_loaded_for_session = True
            st.success(f"Fortschritt geladen. Starte bei Link {st.session_state.current_index + 1}.")
            st.rerun()
    else: st.session_state.current_index = 0; st.session_state.progress_loaded_for_session = True

# --- Haupt-Labeling-Interface ---
if st.session_state.initialized and st.session_state.progress_loaded_for_session and st.session_state.urls_to_process:
    total_items = st.session_state.total_items
    current_idx = st.session_state.current_index

    # --- Prüfen ob fertig ---
    if current_idx >= total_items:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Alle {total_items} URLs aus '{st.session_state.input_file_name}' bearbeitet!")
        st.balloons(); st.info(f"Ergebnisse in Sheet '{connected_sheet_name}' gespeichert.")
        # *** KORRIGIERTER TEIL START ***
        if worksheet_data:
            try:
                sheet_url = worksheet_data.spreadsheet.url
                st.link_button("Daten-Sheet öffnen", sheet_url)
            except Exception:
                pass # Ignoriere Fehler beim Link holen
        if worksheet_progress:
            try:
                sheet_url = worksheet_progress.spreadsheet.url
                st.link_button("Fortschritt-Sheet öffnen", sheet_url)
            except Exception:
                pass # Ignoriere Fehler beim Link holen
        # *** KORRIGIERTER TEIL ENDE ***
        if st.button("Bearbeitung zurücksetzen / Andere Datei laden"):
             st.session_state.initialized=False; st.session_state.input_file_name=None; st.session_state.default_loaded=False
             st.session_state.urls_to_process=[]; st.session_state.total_items=0; st.session_state.processed_urls_in_session=set()
             st.session_state.current_index=0; st.session_state.session_results={}; st.session_state.session_comments={}
             st.session_state.progress_loaded_for_session=False; st.rerun()
        st.stop()

    # --- Navigation und Fortschritt ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True): st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    progress_text = f"{st.session_state.labeler_id}: Link {current_idx + 1} von {total_items} ('{st.session_state.input_file_name}')"
    nav_cols_top[1].progress((current_idx + 1) / total_items, text=progress_text)
    st.divider()

    # --- URL Anzeige & Einbettung ---
    current_url = st.session_state.urls_to_process[current_idx]
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url); embed_html = get_tweet_embed_html(base_tweet_url)
    if embed_html: st.components.v1.html(embed_html, height=650, scrolling=True);
    if base_tweet_url != current_url: st.caption(f"Original: [{current_url}]({current_url})")
    else: st.markdown(f"**URL:** [{current_url}]({current_url})"); st.link_button("Link in neuem Tab öffnen", current_url)
    st.divider()

    # --- Kategorieauswahl mit BUTTONS ---
    st.subheader("Kategorisierung (Klicke Buttons zum Auswählen)")
    col1_cat, col2_com = st.columns([3, 2])
    current_selection_set = st.session_state.session_results.get(current_idx, set())

    with col1_cat:
        cols_per_row = 4
        for main_topic, sub_categories in CATEGORIES.items():
            marker = CATEGORY_MARKERS.get(main_topic, DEFAULT_MARKER)
            with st.expander(f"**{marker} {main_topic}**", expanded=True):
                button_cols = st.columns(cols_per_row)
                col_idx = 0
                for category in sub_categories:
                    is_selected = category in current_selection_set
                    button_type = "primary" if is_selected else "secondary"
                    button_label = f"{category}"
                    button_key = f"btn_{current_idx}_{main_topic}_{category}".replace(' ', '_').replace('/', '_').replace('&','_')
                    with button_cols[col_idx % cols_per_row]:
                        if st.button(button_label, key=button_key, type=button_type, use_container_width=True):
                            temp_selection_set = current_selection_set.copy()
                            if is_selected: temp_selection_set.discard(category)
                            else: temp_selection_set.add(category)
                            st.session_state.session_results[current_idx] = temp_selection_set
                            st.rerun()
                    col_idx += 1
        st.markdown("---")
        selected_categories_list = sorted(list(current_selection_set))
        if selected_categories_list:
            st.write("**Ausgewählt:**")
            grouped_selection_str = []
            for main_topic, sub_cats in CATEGORIES.items():
                marker = CATEGORY_MARKERS.get(main_topic, DEFAULT_MARKER)
                selected_in_group = [cat for cat in selected_categories_list if cat in sub_cats]
                if selected_in_group: grouped_selection_str.append(f"**{marker} {main_topic}:** {', '.join(selected_in_group)}")
            st.info("\n\n".join(grouped_selection_str))
        else: st.write("_Keine Kategorien ausgewählt._")

    # --- Kommentarfeld ---
    with col2_com:
        default_comment = st.session_state.session_comments.get(current_idx, "")
        comment_key = f"comment_{current_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=350, key=comment_key)
    st.divider()

    # --- Navigationsbuttons (Unten) ---
    nav_cols_bottom = st.columns(7)
    if current_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom", use_container_width=True): st.session_state.session_comments[current_idx] = comment; st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom_disabled", disabled=True, use_container_width=True)
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_labeler_id = st.session_state.labeler_id
        final_selection_set = st.session_state.session_results.get(current_idx, set())
        if not final_selection_set: st.warning("Bitte wähle mindestens eine Kategorie aus.")
        elif not worksheet_data or not worksheet_progress: st.error("Sheet-Verbindung(en) fehlen.")
        elif not current_labeler_id: st.error("Labeler ID fehlt.")
        else:
            categories_str = "; ".join(sorted(list(final_selection_set)))
            final_comment = comment
            if save_categorization_gsheet(worksheet_data, current_labeler_id, current_url, categories_str, final_comment):
                st.session_state.session_comments[current_idx] = final_comment
                st.session_state.processed_urls_in_session.add(current_idx)
                next_index = current_idx + 1
                if save_labeler_progress(worksheet_progress, current_labeler_id, next_index):
                    st.session_state.current_index = next_index
                    st.rerun()
                else: st.error("Daten gespeichert, aber Fortschritt NICHT gespeichert.")
            else: st.error("Speichern der Kategorisierung fehlgeschlagen.")

# --- Fallback-Anzeige ---
elif not st.session_state.initialized and uploaded_file is None and not st.session_state.default_loaded:
    if worksheet_data and worksheet_progress and st.session_state.labeler_id:
        st.info(f"Warte auf Datei-Upload oder Standarddatei '{DEFAULT_CSV_PATH}'.")

# --- Sidebar ---
# (Bleibt gleich wie im vorherigen Skript)
st.sidebar.header("Info & Status")
if worksheet_data and worksheet_progress:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    try: sheet_url=worksheet_data.spreadsheet.url; st.sidebar.page_link(sheet_url, label="Daten-Sheet ↗️")
    except: pass
    try: sheet_url=worksheet_progress.spreadsheet.url; st.sidebar.page_link(sheet_url, label="Fortschritt-Sheet ↗️")
    except: pass
else: st.sidebar.error("Sheet-Verbindung(en) fehlen.")

st.sidebar.markdown(f"**Labeler/in:** `{st.session_state.labeler_id or '(Bitte eingeben)'}`")
current_input_info = st.session_state.get('input_file_name', None)
if current_input_info: st.sidebar.markdown(f"**Input-Datei:** `{current_input_info}`")
else: st.sidebar.markdown("**Input-Datei:** -")

st.sidebar.markdown(f"**Datenbank:** Google Sheet")
st.sidebar.markdown(f"**Format Daten:** `{', '.join(DATA_HEADER)}`")
st.sidebar.markdown(f"**Format Fortschritt:** `{', '.join(PROGRESS_HEADER)}`")
st.sidebar.markdown("**Fortschritt:** Wird gespeichert & geladen.")
if st.session_state.initialized and st.session_state.progress_loaded_for_session:
    total_urls_in_file = st.session_state.total_items
    current_pos = st.session_state.current_index
    st.sidebar.metric("URLs in Datei", total_urls_in_file)
    st.sidebar.metric("Aktuelle Position", f"{current_pos + 1} / {total_urls_in_file}" if total_urls_in_file > 0 else "-")
else:
    st.sidebar.metric("URLs in Datei", "-")
    st.sidebar.metric("Aktuelle Position", "-")