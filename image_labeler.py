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

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="URL-Kategorisierer (Google Sheets)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfad zur Standard-CSV-Datei ===
DEFAULT_CSV_PATH = "input.csv"

# === Google Sheets Setup ===
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_URL, COL_CATS, COL_COMMENT]

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
        if not all_vals:
             worksheet.append_row(HEADER, value_input_option='USER_ENTERED')
             header_written = True
        return worksheet, header_written, sheet_name
    except KeyError as e:
        st.error(f"Fehler: Secret '{e}' nicht in st.secrets gefunden. Überprüfe deine secrets.toml / Cloud Secrets.")
        st.stop()
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Fehler: Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', 'Name nicht konfiguriert')}' nicht gefunden. Prüfe Namen und Freigabe.")
        st.stop()
    except Exception as e:
        st.error(f"Fehler beim Verbinden/Initialisieren von Google Sheets: {e}")
        st.stop()
        return None, False, None

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
@st.cache_data(ttl=300)
def load_processed_urls_gsheet(_worksheet_ref):
    """Lädt bereits bearbeitete URLs aus dem Google Sheet."""
    processed_urls = set()
    if not _worksheet_ref: return processed_urls
    try:
        url_column_values = _worksheet_ref.col_values(1)
        if len(url_column_values) > 1: processed_urls = set(url_column_values[1:])
    except gspread.exceptions.APIError as e: st.error(f"Google Sheets API Fehler (URLs lesen): {e}")
    except Exception as e: st.error(f"Unerw. Fehler (URLs lesen): {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(file_input_object, source_name="hochgeladene Datei"):
    """Lädt alle URLs aus einem Datei-Objekt (Upload oder geöffnet)."""
    urls = []
    if not file_input_object:
        st.error("Kein Datei-Objekt zum Laden übergeben.")
        return urls
    try:
        # Wichtig: Das Objekt zurücksetzen, falls es mehrmals gelesen wird (relevant bei open())
        if hasattr(file_input_object, 'seek'):
            file_input_object.seek(0)
        df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=True)
        url_series = df.iloc[:, 0].dropna().astype(str)
        urls = url_series[url_series.str.startswith(("http://", "https://"))].unique().tolist()
    except pd.errors.EmptyDataError:
         st.warning(f"Input-Datei '{source_name}' ist leer oder enthält keine URLs.")
    except IndexError:
         st.warning(f"Input-Datei '{source_name}' scheint keine Spalten zu enthalten (Format?).")
    except Exception as e:
        st.error(f"Fehler beim Lesen der Input-CSV ('{source_name}'): {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, url, categories_str, comment):
    """Hängt eine neue Zeile mit den Daten an das Google Sheet an."""
    if not worksheet_obj:
        st.error("Keine Verbindung zum Google Sheet zum Speichern.")
        return False
    try:
        worksheet_obj.append_row([url, categories_str, comment], value_input_option='USER_ENTERED')
        load_processed_urls_gsheet.clear() # Cache leeren nach dem Schreiben
        return True
    except gspread.exceptions.APIError as e: st.error(f"Google Sheets API Fehler (Speichern): {e}")
    except Exception as e: st.error(f"Unerw. Fehler (Speichern): {e}")
    return False

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
        response.raise_for_status()
        return response.json().get("html")
    except requests.exceptions.Timeout: print(f"Timeout embed: {tweet_url}"); return None
    except requests.exceptions.RequestException as e: status = e.response.status_code if e.response else "N/A"; print(f"Embed fail {status}: {tweet_url}"); return None
    except Exception as e: st.warning(f"Embed error {tweet_url}: {e}", icon="❓"); return None

# === Streamlit App Hauptteil ===
st.title("📊 URL-Kategorisierer (mit Google Sheets)")

# --- Session State Initialisierung ---
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.input_file_name = None
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.default_loaded = False # Flag, ob Standarddatei geladen wurde

# --- Dateiauswahl und Standarddatei-Logik ---
uploaded_file = st.file_uploader(
    "1. Wähle optional eine andere Input-CSV (überschreibt Standard)",
    type=["csv"]
)

# Zu verarbeitende Datei und Name bestimmen
file_input = None
file_source_name = None
trigger_processing = False

if uploaded_file is not None:
    # Wenn eine Datei hochgeladen wird, hat sie Priorität
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        file_input = uploaded_file
        file_source_name = uploaded_file.name
        trigger_processing = True
        st.session_state.default_loaded = False # Reset flag if upload happens
elif not st.session_state.initialized and not st.session_state.default_loaded:
    # Wenn noch nicht initialisiert und Standard noch nicht geladen: Versuche Standard
    if os.path.exists(DEFAULT_CSV_PATH):
        file_input = DEFAULT_CSV_PATH # Wir übergeben nur den Pfad, öffnen später
        file_source_name = DEFAULT_CSV_PATH
        trigger_processing = True
        st.session_state.default_loaded = True # Mark that default is being loaded
    else:
        # Keine Standarddatei gefunden, warte auf Upload
        st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' nicht gefunden. Bitte lade eine CSV-Datei hoch.")


# --- Verarbeitungslogik ---
if trigger_processing and worksheet:
    # Reset state für neue Datei (Upload ODER erstmaliger Standard-Load)
    st.session_state.initialized = False
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.input_file_name = file_source_name # Namen der Quelle merken

    with st.spinner(f"Verarbeite '{file_source_name}' und prüfe Google Sheet..."):
        all_input_urls = []
        # Lade URLs entweder aus dem Pfad (Standard) oder dem Upload-Objekt
        if isinstance(file_input, str): # Es ist der Pfad zur Standarddatei
            try:
                with open(file_input, 'rb') as f_default: # 'rb' für binäres Lesen
                    all_input_urls = load_urls_from_input_csv(f_default, source_name=file_source_name)
            except FileNotFoundError:
                st.error(f"Standarddatei '{file_source_name}' nicht gefunden.")
            except Exception as e:
                 st.error(f"Fehler beim Lesen der Standarddatei '{file_source_name}': {e}")
        elif file_input is not None: # Es ist ein UploadedFile Objekt
            all_input_urls = load_urls_from_input_csv(file_input, source_name=file_source_name)

        # --- Restliche Verarbeitung nach Laden der URLs ---
        if all_input_urls:
            processed_urls_from_gsheet = load_processed_urls_gsheet(worksheet)
            st.info(f"{len(processed_urls_from_gsheet)} bereits bearbeitete URLs im Google Sheet gefunden.")

            st.session_state.urls_to_process = [
                url for url in all_input_urls if url not in processed_urls_from_gsheet
            ]
            random.shuffle(st.session_state.urls_to_process)
            st.session_state.total_items = len(st.session_state.urls_to_process)
            st.session_state.current_index = 0

            if not st.session_state.urls_to_process:
                st.warning(f"Alle URLs aus '{file_source_name}' wurden bereits im Google Sheet gefunden oder die Datei enthält keine gültigen neuen URLs.")
                # Initialized bleibt False, damit ggf. neue Datei geladen werden kann
            else:
                st.success(f"{st.session_state.total_items} neue URLs aus '{file_source_name}' zum Bearbeiten gefunden und gemischt.")
                st.session_state.initialized = True # Initialisierung erfolgreich
        else:
             st.error(f"Datei '{file_source_name}' konnte nicht verarbeitet werden oder enthält keine URLs.")
             st.session_state.initialized = False # Nicht erfolgreich initialisiert
             st.session_state.default_loaded = False # Erlaube erneuten Versuch, Default zu laden, falls es fehlschlug
elif trigger_processing and not worksheet:
     st.error("Verbindung zum Google Sheet fehlgeschlagen. Datei kann nicht verarbeitet werden.")
     st.session_state.initialized = False
     st.session_state.default_loaded = False


# --- Haupt-Labeling-Interface ---
if st.session_state.get('initialized', False) and st.session_state.urls_to_process:
    # (Dieser Teil bleibt inhaltlich genau gleich wie vorher)
    # --- Prüfen ob fertig ---
    total_items = st.session_state.total_items
    if st.session_state.current_index >= total_items:
        st.success(f"🎉 Alle neuen URLs aus '{st.session_state.input_file_name}' wurden bearbeitet!")
        st.balloons()
        st.info(f"Die Ergebnisse wurden laufend im Google Sheet '{connected_sheet_name}' gespeichert.")
        if worksheet:
            try: sheet_url = worksheet.spreadsheet.url; st.link_button("Google Sheet öffnen", sheet_url)
            except Exception: st.info("Link zum Sheet konnte nicht generiert werden.")
        if st.button("Bearbeitung zurücksetzen / Neue Datei laden"):
             st.session_state.initialized = False
             st.session_state.input_file_name = None
             st.session_state.default_loaded = False # Wichtig, damit Default neu geladen wird
             # ... (restliche Resets für urls_to_process etc.) ...
             st.session_state.urls_to_process = []
             st.session_state.total_items = 0
             st.session_state.processed_urls_in_session = set()
             st.session_state.current_index = 0
             st.session_state.session_results = {}
             st.session_state.session_comments = {}
             st.rerun()
        st.stop()

    # --- Gültigen Index holen ---
    current_idx = st.session_state.current_index

    # --- Navigation und Fortschritt oben ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True): st.session_state.current_index -= 1; st.rerun()
    else:
        nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    progress_text = f"Link {current_idx + 1} von {total_items} (aus '{st.session_state.input_file_name}')"
    nav_cols_top[1].progress((current_idx + 1) / total_items, text=progress_text)
    st.divider()

    # --- Aktuelle URL und Einbettung ---
    current_url = st.session_state.urls_to_process[current_idx]
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url)
    embed_html = get_tweet_embed_html(base_tweet_url)
    if embed_html:
        st.components.v1.html(embed_html, height=650, scrolling=True)
        if base_tweet_url != current_url: st.caption(f"Original-URL (bereinigt): [{current_url}]({current_url})")
    else:
        st.markdown(f"**URL:** [{current_url}]({current_url})")
        if "twitter.com" in current_url or "x.com" in current_url: st.caption("Vorschau nicht verfügbar (Tweet gelöscht/privat, API o.ä.).")
        else: st.caption("Vorschau nur für X/Twitter Links verfügbar.")
        st.link_button("Link in neuem Tab öffnen", current_url)
    st.divider()

    # --- Kategorieauswahl und Kommentar ---
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
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        if not selected_categories_in_widgets: st.warning("Bitte wähle mindestens eine Kategorie aus.")
        elif not worksheet: st.error("Keine Verbindung zum Google Sheet zum Speichern.")
        else:
            categories_str = "; ".join(selected_categories_in_widgets)
            if save_categorization_gsheet(worksheet, current_url, categories_str, comment):
                st.session_state.session_results[current_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_idx] = comment
                st.session_state.processed_urls_in_session.add(current_url)
                st.session_state.current_index += 1
                st.rerun()
            else: st.error("Speichern in Google Sheet fehlgeschlagen.")

# --- Fallback-Anzeige, wenn nichts geladen wurde ---
elif not st.session_state.get('initialized', False) and uploaded_file is None and not st.session_state.get('default_loaded', False):
    # Nur anzeigen, wenn worksheet existiert und kein Upload da ist und Default nicht geladen wurde
    if worksheet:
        st.info(f"Versuche, Standarddatei '{DEFAULT_CSV_PATH}' zu laden oder lade eine andere CSV hoch.")


# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    if header_written_flag: st.sidebar.info(f"Header in '{connected_sheet_name}' geschrieben.")
    try: sheet_url = worksheet.spreadsheet.url; st.sidebar.page_link(sheet_url, label="Sheet öffnen ↗️")
    except Exception: st.sidebar.info("Link zum Sheet nicht verfügbar.")
else: st.sidebar.error("Keine Verbindung zum Google Sheet.")

# Zeige an, welche Datei aktuell verwendet wird
current_input_info = st.session_state.get('input_file_name', None)
if current_input_info:
    st.sidebar.markdown(f"**Aktuelle Input-Datei:** `{current_input_info}`")
    if current_input_info == DEFAULT_CSV_PATH:
        st.sidebar.caption("(Standarddatei)")
else:
     st.sidebar.markdown("**Aktuelle Input-Datei:** -")

st.sidebar.markdown(f"**Datenbank:** Google Sheet")
st.sidebar.markdown(f"**Format Sheet:** Spalten `{', '.join(HEADER)}`")
st.sidebar.markdown("**Fortschritt:** Nach 'Speichern & Weiter' gesichert.")
st.sidebar.markdown("**Einbettung:** Versucht X/Twitter Posts.")
if st.session_state.get('initialized', False):
    remaining = st.session_state.total_items - st.session_state.current_index
    processed_session = len(st.session_state.processed_urls_in_session)
    st.sidebar.metric("Verbleibende Links (aus Datei)", max(0, remaining))
    st.sidebar.metric("Gespeichert (diese Sitzung)", processed_session)
else:
    st.sidebar.metric("Verbleibende Links (aus Datei)", "-")
    st.sidebar.metric("Gespeichert (diese Sitzung)", "-")