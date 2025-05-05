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
import streamlit.components.v1 as components # Für HTML Einbettung

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="URL-Kategorisierer (Multi-Labeler)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfad zur Standard-CSV-Datei ===
DEFAULT_CSV_PATH = "input.csv"

# === Google Sheets Setup ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

# Spaltennamen im Google Sheet (REIHENFOLGE WICHTIG!)
COL_TS = "Timestamp"
COL_LBL = "Labeler_ID"
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_TS, COL_LBL, COL_URL, COL_CATS, COL_COMMENT] # Header-Reihenfolge

# Zeitzone für Zeitstempel
TIMEZONE = pytz.timezone("Europe/Berlin")

# === Google Sheets Verbindung ===
# Cache die Ressource (Verbindung + Worksheet-Objekt)
@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her und gibt das Worksheet-Objekt zurück."""
    try:
        # Hole Credentials und Sheet-Namen aus Streamlit Secrets
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1
        header_written = False # Flag um zu sehen ob Header geschrieben wurde
        all_vals = worksheet.get_all_values()

        # Prüfe und schreibe Header, falls nötig
        if not all_vals or all_vals[0] != HEADER:
            st.sidebar.warning(f"Header in '{sheet_name}' stimmt nicht mit {HEADER} überein oder fehlt. Schreibe korrekten Header...")
            try:
                # Intelligentes Update/Einfügen des Headers
                if not all_vals or len(all_vals[0]) != len(HEADER):
                    worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                else:
                    cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]
                    worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')

                # Entferne leere Zeilen direkt nach dem Header
                all_vals_after = worksheet.get_all_values()
                if len(all_vals_after) > 1 and all(v == '' for v in worksheet.row_values(2)):
                    worksheet.delete_rows(2)

                header_written = True
                st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")
            except Exception as he:
                st.sidebar.error(f"Konnte Header nicht schreiben: {he}")
                # st.stop() # Optional: Stoppen, wenn Header nicht geschrieben werden kann

        return worksheet, header_written, sheet_name
    except KeyError as e:
        st.error(f"Secret '{e}' fehlt in der Streamlit Cloud Konfiguration (secrets.toml lokal). Bitte überprüfen.")
        st.stop()
        return None, False, None
    except gspread.exceptions.SpreadsheetNotFound:
        sheet_name_from_secrets = st.secrets.get("google_sheets", {}).get("sheet_name", "???")
        st.error(f"Google Sheet '{sheet_name_from_secrets}' nicht gefunden oder Zugriff verweigert. Bitte prüfe den Namen und die Freigabe für die Service-Account-Email.")
        st.stop()
        return None, False, None
    except Exception as e:
        st.error(f"Allgemeiner Fehler bei der Google Sheets Verbindung: {e}")
        st.stop()
        return None, False, None

# Stelle Verbindung her beim Start der App
# Das Ergebnis von connect_gsheet() wird dank @st.cache_resource effizient wiederverwendet.
worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===
CATEGORIES = {
    "Personal Well-being": ["Lifestyle", "Mental Health", "Physical Health", "Family/Relationships"],
    "Societal Systems": ["Healthcare System", "Education System", "Employment/Economy", "Energy Sector"],
    "Environment & Events": ["Environmental Policies", "(Natural/Man-made) Disasters"],
    "Other": ["Politics (General)", "Technology", "Miscellaneous"]
}
ALL_CATEGORIES = [cat for sublist in CATEGORIES.values() for cat in sublist]

# Farben für Hauptkategorien
CATEGORY_COLORS = {
    "Personal Well-being": "dodgerblue",
    "Societal Systems": "mediumseagreen",
    "Environment & Events": "darkorange",
    "Other": "grey"
}

# === Hilfsfunktionen ===

# Cache die *Daten* (das Set der URLs), die von der Labeler ID abhängen.
# --- KORRIGIERTE FUNKTION ---
@st.cache_data(ttl=300) # Cache für 5 Minuten
def get_processed_urls_by_labeler(target_labeler_id): # Nur noch Labeler ID als Parameter
    """Holt alle URLs, die ein bestimmter Labeler bereits im Sheet gespeichert hat."""
    processed_urls = set()
    # Hole das gecachte Worksheet-Objekt hier innerhalb der Funktion
    worksheet_obj, _, _ = connect_gsheet()

    # Prüfe, ob die Verbindung erfolgreich war und eine Labeler ID vorhanden ist
    if not worksheet_obj or not target_labeler_id:
        st.warning("Konnte keine Worksheet-Verbindung herstellen oder Labeler ID fehlt beim Abrufen des Fortschritts.")
        return processed_urls

    print(f"DEBUG: Rufe verarbeitete URLs für Labeler '{target_labeler_id}' aus GSheet ab...")
    try:
        all_data = worksheet_obj.get_all_values()
        if not all_data or len(all_data) < 1:
            print("DEBUG: GSheet ist leer oder Header fehlt.")
            return processed_urls

        header_row = all_data[0]
        # Finde Spaltenindizes dynamisch
        try:
            labeler_col_index = header_row.index(COL_LBL)
            url_col_index = header_row.index(COL_URL)
        except ValueError as e:
            # Wenn Spalten fehlen, können wir den Fortschritt nicht prüfen
            st.error(f"Fehler: Spalte '{e}' nicht im Google Sheet Header gefunden! Header ist: {header_row}. Fortschritt kann nicht geladen werden.")
            return processed_urls # Gib leeres Set zurück

        # Iteriere durch Datenzeilen (überspringe Header)
        for row in all_data[1:]:
            # Zugriff absichern: Prüfen ob Zeile lang genug ist und relevante Zellen Inhalt haben
            if len(row) > max(labeler_col_index, url_col_index) and row[labeler_col_index] and row[url_col_index]:
                labeler_in_row = row[labeler_col_index]
                url_in_row = row[url_col_index]
                # Vergleiche Labeler ID
                if labeler_in_row == target_labeler_id:
                    processed_urls.add(url_in_row.strip()) # Füge bereinigte URL hinzu

        print(f"DEBUG: {len(processed_urls)} bereits verarbeitete URLs für '{target_labeler_id}' im GSheet gefunden.")
    except gspread.exceptions.APIError as e:
        st.warning(f"GSheet API Fehler beim Abrufen verarbeiteter URLs: {e}. Fortschritt wird möglicherweise nicht korrekt fortgesetzt.")
    except Exception as e:
        st.warning(f"Unerwarteter Fehler beim Abrufen verarbeiteter URLs: {e}")

    return processed_urls
# --- ENDE KORRIGIERTE FUNKTION ---

# Cache die Daten (Liste der URLs), die aus der Datei gelesen werden.
@st.cache_data
def load_urls_from_input_csv(file_input_object, source_name="hochgeladene Datei"):
    """Lädt alle URLs aus einem Datei-Objekt (Upload oder geöffnet) und bereinigt sie."""
    urls = []
    if not file_input_object: st.error("Kein Datei-Objekt übergeben."); return urls
    try:
        if hasattr(file_input_object, 'seek'): file_input_object.seek(0)
        try:
            df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
        except UnicodeDecodeError:
            st.warning(f"Konnte '{source_name}' nicht als UTF-8 lesen, versuche latin-1...")
            if hasattr(file_input_object, 'seek'): file_input_object.seek(0)
            df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)

        print(f"DEBUG: CSV gelesen ({source_name}), {len(df)} Zeilen insgesamt in Spalte 0.")
        url_series_raw = df.iloc[:, 0]
        url_series_str = url_series_raw.astype(str)
        url_series_nonan = url_series_str.replace('nan', pd.NA).dropna()
        url_series_stripped = url_series_nonan.str.strip()
        url_series_noempty = url_series_stripped[url_series_stripped != '']
        print(f"DEBUG: Nach Bereinigung (strip, NaN, leer), {len(url_series_noempty)} Zeilen übrig.")
        url_series_filtered = url_series_noempty[url_series_noempty.str.match(r'^https?://\S+$')]
        print(f"DEBUG: Nach Regex-Filter (http/https), {len(url_series_filtered)} Zeilen übrig.")
        urls = url_series_filtered.unique().tolist()
        print(f"DEBUG: Nach unique(), {len(urls)} URLs werden zurückgegeben.")
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer oder enthält keine Daten in der ersten Spalte.")
    except IndexError: st.warning(f"Input '{source_name}' scheint keine Spalten zu haben (Format?). Stelle sicher, dass es eine CSV mit URLs in der ersten Spalte ist.")
    except Exception as e: st.error(f"Fehler beim Lesen/Verarbeiten von '{source_name}': {e}")
    return urls


def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Hängt eine neue Zeile mit Labeler-ID und Zeitstempel an das Google Sheet an."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt. Speichern nicht möglich."); return False
    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        # Nutze das übergebene worksheet_obj
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e: st.error(f"Sheets API Fehler (Speichern): {e}"); return False
    except Exception as e: st.error(f"Unerwarteter Fehler beim Speichern in GSheet: {e}"); return False

def clean_tweet_url(url):
    """Bereinigt Twitter/X URLs."""
    try:
        cleaned_url = url.split('?')[0]
        cleaned_url = re.sub(r"/(photo|video)/\d+$", "", cleaned_url)
        return cleaned_url
    except Exception: return url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    """Holt den oEmbed HTML-Code für einen Tweet."""
    if not isinstance(tweet_url, str): return None
    try:
        parsed_url = urlparse(tweet_url)
        if parsed_url.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"] or "/status/" not in parsed_url.path:
            return None
    except Exception as e:
        print(f"URL-Parsing-Fehler für Embed: {tweet_url}, Fehler: {e}")
        return None

    cleaned_tweet_url = clean_tweet_url(tweet_url)
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("html")
    except requests.exceptions.Timeout:
        print(f"Timeout beim Abrufen des Embeddings für: {cleaned_tweet_url}")
        return f"<p style='color:orange; font-family: sans-serif;'>Timeout beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        print(f"HTTP Fehler {status_code} beim Abrufen des Embeddings für {cleaned_tweet_url}.")
        msg = f"Fehler ({status_code}) beim Laden der Tweet-Vorschau."
        if status_code == 404: msg = "Tweet nicht gefunden (404). Gelöscht oder Link fehlerhaft?"
        elif status_code == 403: msg = "Zugriff auf Tweet verweigert (403). Privat/geschützt?"
        return f"<p style='color:orange; font-family: sans-serif; border: 1px solid orange; padding: 10px; border-radius: 5px;'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.RequestException as e:
        print(f"Netzwerkfehler beim Abrufen des Embeddings für {cleaned_tweet_url}: {e}")
        return f"<p style='color:red; font-family: sans-serif;'>Netzwerkfehler beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except Exception as e:
        st.warning(f"Generischer Fehler beim Abrufen des Embeddings für {cleaned_tweet_url}: {e}", icon="❓")
        return None


# === Streamlit App Hauptteil ===
st.title("📊 URL-Kategorisierer (Multi-Labeler)")

# --- Session State Initialisierung ---
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
if 'original_total_items' not in st.session_state: st.session_state.original_total_items = 0
if 'already_processed_count' not in st.session_state: st.session_state.already_processed_count = 0

# --- Labeler ID Eingabe ---
labeler_id_input = st.text_input(
    "👤 Bitte gib deine Labeler ID ein (z.B. Name oder Kürzel):",
    value=st.session_state.labeler_id,
    key="labeler_id_widget",
    help="Diese ID wird verwendet, um deinen Fortschritt zu speichern."
)
st.session_state.labeler_id = labeler_id_input.strip()

if not st.session_state.labeler_id:
    st.warning("Bitte gib zuerst deine Labeler ID oben ein, um zu starten.")
    st.stop()

st.divider()

# --- Dateiauswahl und Verarbeitung ---
uploaded_file = st.file_uploader(
    "1. Optional: Lade eine andere CSV hoch (eine Spalte mit URLs, kein Header)",
    type=["csv"],
    key="file_uploader"
)

file_input = None
file_source_name = None
trigger_processing = False

# Logik zur Dateiauswahl
if uploaded_file is not None:
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        file_input = uploaded_file
        file_source_name = uploaded_file.name
        trigger_processing = True
        st.session_state.default_loaded = False
        print(f"Verwende hochgeladene Datei: {file_source_name}")
elif not st.session_state.initialized and not st.session_state.default_loaded:
    if os.path.exists(DEFAULT_CSV_PATH):
        try:
            if os.path.getsize(DEFAULT_CSV_PATH) > 0:
                 file_input = DEFAULT_CSV_PATH
                 file_source_name = DEFAULT_CSV_PATH
                 trigger_processing = True
                 st.session_state.default_loaded = True
                 print(f"Verwende Standarddatei: {file_source_name}")
            else:
                 st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' ist leer. Lade eine CSV hoch.")
                 st.session_state.default_loaded = False
        except OSError as e:
             st.warning(f"Konnte Standarddatei '{DEFAULT_CSV_PATH}' nicht lesen: {e}. Lade eine CSV hoch.")
             st.session_state.default_loaded = False
    else:
        st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' nicht gefunden. Lade eine CSV hoch.")
        st.session_state.default_loaded = False

# Verarbeitung auslösen
# Wichtig: Hier wird 'worksheet' aus dem globalen Scope verwendet (gecached durch connect_gsheet)
if trigger_processing and worksheet:
    print(f"Trigger Processing für: {file_source_name} mit Labeler: {st.session_state.labeler_id}")
    # Reset für neue Datei/Verarbeitung
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.input_file_name = file_source_name
    st.session_state.original_total_items = 0
    st.session_state.already_processed_count = 0

    with st.spinner(f"Verarbeite '{file_source_name}' und prüfe Fortschritt für '{st.session_state.labeler_id}'..."):
        all_input_urls_cleaned = []
        file_obj_for_loading = None

        # Öffne Datei sicher
        try:
            if isinstance(file_input, str):
                file_obj_for_loading = open(file_input, 'rb')
            elif file_input is not None:
                file_obj_for_loading = file_input

            if file_obj_for_loading:
                # Lade URLs aus Datei (gecached)
                all_input_urls_cleaned = load_urls_from_input_csv(file_obj_for_loading, source_name=file_source_name)
        except Exception as e:
            st.error(f"Fehler beim Öffnen/Lesen der Input-Datei '{file_source_name}': {e}")
            all_input_urls_cleaned = []
        finally:
            if isinstance(file_input, str) and file_obj_for_loading and not file_obj_for_loading.closed:
                file_obj_for_loading.close()

        if all_input_urls_cleaned:
            st.session_state.original_total_items = len(all_input_urls_cleaned)
            print(f"DEBUG: {st.session_state.original_total_items} eindeutige, gültige URLs aus '{file_source_name}' geladen.")

            # --- Filterung basierend auf Labeler ID ---
            current_labeler_id = st.session_state.labeler_id
            # Cache leeren, um frische Daten aus GSheet zu holen
            get_processed_urls_by_labeler.clear()
            # --- KORRIGIERTER AUFRUF ---
            processed_by_this_labeler = get_processed_urls_by_labeler(current_labeler_id)
            # --- ENDE KORRIGIERTER AUFRUF ---

            # Filtere URLs
            remaining_urls = [url for url in all_input_urls_cleaned if url.strip() not in processed_by_this_labeler]

            st.session_state.urls_to_process = remaining_urls
            st.session_state.total_items = len(remaining_urls)
            st.session_state.already_processed_count = st.session_state.original_total_items - st.session_state.total_items
            st.session_state.current_index = 0

            if st.session_state.total_items > 0:
                st.success(f"{st.session_state.original_total_items} URLs in '{file_source_name}' gefunden. Davon hast du ({current_labeler_id}) bereits {st.session_state.already_processed_count} bearbeitet. Es bleiben {st.session_state.total_items} übrig. Starte bei Item {st.session_state.already_processed_count + 1}.")
                st.session_state.initialized = True
                st.rerun()
            else:
                 st.success(f"Super, {current_labeler_id}! Du hast bereits alle {st.session_state.original_total_items} URLs aus '{file_source_name}' bearbeitet.")
                 st.session_state.initialized = True
                 # Kein Rerun nötig, "Alle erledigt"-Logik greift

        else:
             st.error(f"Datei '{file_source_name}' enthält keine gültigen URLs (auch nach Bereinigung). Prüfe die Datei oder DEBUG-Ausgaben.")
             st.session_state.initialized = False
             st.session_state.default_loaded = False
             st.session_state.input_file_name = None
elif trigger_processing and not worksheet:
     st.error("Sheet-Verbindung fehlgeschlagen. Verarbeitung nicht möglich."); st.session_state.initialized = False; st.session_state.default_loaded = False


# --- Haupt-Labeling-Interface ---
if st.session_state.get('initialized', False):

    remaining_items = st.session_state.total_items
    original_total = st.session_state.original_total_items
    processed_count = st.session_state.already_processed_count
    current_local_idx = st.session_state.current_index

    # Zustand: Alle URLs bearbeitet
    if remaining_items <= 0 or current_local_idx >= remaining_items:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Du hast alle {original_total} URLs aus '{st.session_state.input_file_name}' bearbeitet!")
        st.balloons()
        st.info(f"Deine Ergebnisse wurden im Google Sheet '{connected_sheet_name}' gespeichert.")
        if worksheet:
            try: sheet_url = worksheet.spreadsheet.url; st.link_button("Google Sheet öffnen", sheet_url)
            except Exception: pass
        if st.button("Bearbeitung zurücksetzen / Andere Datei laden"):
             st.session_state.initialized = False
             st.session_state.input_file_name = None
             st.session_state.default_loaded = False
             st.session_state.urls_to_process = []
             st.session_state.total_items = 0
             st.session_state.processed_urls_in_session = set()
             st.session_state.current_index = 0
             st.session_state.session_results = {}
             st.session_state.session_comments = {}
             st.session_state.original_total_items = 0
             st.session_state.already_processed_count = 0
             st.cache_data.clear()
             get_processed_urls_by_labeler.clear()
             st.rerun()
        st.stop()

    # --- Wenn noch Items zu bearbeiten sind ---
    current_url = st.session_state.urls_to_process[current_local_idx]

    # --- Navigation und Fortschritt (Oben) ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_local_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True):
            st.session_state.current_index -= 1
            st.rerun()
    else:
        nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)

    if original_total > 0:
        progress_value = (processed_count + current_local_idx) / original_total
        current_global_item_number = processed_count + current_local_idx + 1
        progress_text = f"{st.session_state.labeler_id}: Item {current_global_item_number} von {original_total} (aus '{st.session_state.input_file_name}')"
        nav_cols_top[1].progress(progress_value, text=progress_text)
    else:
        nav_cols_top[1].progress(0, text="Keine Items in Datei")

    can_go_forward = (current_local_idx + 1) < remaining_items
    next_local_idx_has_data = (current_local_idx + 1) in st.session_state.session_results
    skip_disabled = not can_go_forward or next_local_idx_has_data

    if nav_cols_top[2].button("Überspringen & Weiter ➡️" if can_go_forward else "Letztes Item", key="skip_next_top", use_container_width=True, disabled=skip_disabled, help="Nur aktiv, wenn für das nächste Item noch keine Daten gespeichert wurden. Zum Speichern & Weiter den unteren Button nutzen."):
         if can_go_forward and not next_local_idx_has_data:
            st.session_state.session_results[current_local_idx] = []
            st.session_state.session_comments[current_local_idx] = "[Übersprungen]"
            st.session_state.current_index += 1
            st.rerun()
         elif next_local_idx_has_data:
             st.toast("Nächstes Item hat bereits Daten (aus dieser Sitzung). Bitte 'Speichern & Weiter' unten verwenden.", icon="⚠️")

    st.divider()

    # --- URL Anzeige & Einbettung ---
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url)
    embed_html = get_tweet_embed_html(base_tweet_url)
    display_url = current_url

    if embed_html:
        components.html(embed_html, height=650, scrolling=True)
        if base_tweet_url != current_url:
            st.caption(f"Original-URL (bereinigt für Vorschau):")
            st.markdown(f"[{current_url}]({current_url})")
            display_url = current_url
    else:
        st.markdown(f"**URL:** [{display_url}]({display_url})")
        if "twitter.com" in display_url or "x.com" in display_url:
            st.caption("Vorschau nicht verfügbar (Link evtl. fehlerhaft, Tweet gelöscht/privat oder API-Problem).")
        else:
            st.caption("Vorschau nur für X/Twitter Links verfügbar.")
        st.link_button("Link in neuem Tab öffnen", display_url)
    st.divider()

    # --- Kategorieauswahl & Kommentar ---
    st.subheader("Kategorisierung")
    col1, col2 = st.columns([3, 2])

    saved_selection = st.session_state.session_results.get(current_local_idx, [])
    selected_categories_in_widgets = []

    with col1:
        st.markdown("**Wähle passende Kategorien:**")
        for main_topic, sub_categories in CATEGORIES.items():
            color = CATEGORY_COLORS.get(main_topic, "black")
            st.markdown(f'<h5 style="color:{color}; border-bottom: 2px solid {color}; margin-top: 15px; margin-bottom: 10px;">{main_topic}</h5>', unsafe_allow_html=True)
            num_columns = 2
            checkbox_cols = st.columns(num_columns)
            col_index = 0
            for sub_cat in sub_categories:
                clean_sub_cat_key = re.sub(r'\W+', '', sub_cat)
                checkbox_key = f"cb_{current_local_idx}_{main_topic.replace(' ', '_')}_{clean_sub_cat_key}"
                is_checked_default = sub_cat in saved_selection
                current_col = checkbox_cols[col_index % num_columns]
                with current_col:
                    is_checked_now = st.checkbox(sub_cat, value=is_checked_default, key=checkbox_key)
                    if is_checked_now: selected_categories_in_widgets.append(sub_cat)
                col_index += 1
        st.markdown("---")
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets)))
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**")
            display_tags = []
            for cat in selected_categories_in_widgets:
                 main_cat_found, cat_color = None, "grey"
                 for m_cat, s_cats in CATEGORIES.items():
                     if cat in s_cats: main_cat_found, cat_color = m_cat, CATEGORY_COLORS.get(m_cat, "grey"); break
                 display_tags.append(f'<span style="display: inline-block; color: {cat_color}; border: 1px solid {cat_color}; border-radius: 5px; padding: 2px 6px; margin: 2px; font-size: 0.9em;">{cat}</span>')
            st.markdown(" ".join(display_tags), unsafe_allow_html=True)
        else: st.write("_Keine Kategorien ausgewählt._")

    with col2:
        default_comment = st.session_state.session_comments.get(current_local_idx, "")
        comment_key = f"comment_{current_local_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=250, key=comment_key, placeholder="Füge hier Notizen oder Begründungen hinzu...")

    st.divider()

    # --- Navigationsbuttons (Unten) ---
    nav_cols_bottom = st.columns(7)
    if current_local_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True):
            st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
            st.session_state.session_comments[current_local_idx] = comment
            st.session_state.current_index -= 1
            st.rerun()
    else:
        nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)

    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_labeler_id = st.session_state.labeler_id
        if not selected_categories_in_widgets:
            st.warning("Bitte wähle mindestens eine Kategorie aus.")
        elif not worksheet:
            st.error("Keine Verbindung zum Google Sheet zum Speichern.")
        elif not current_labeler_id:
            st.error("Labeler ID nicht gesetzt.")
        else:
            categories_str = "; ".join(selected_categories_in_widgets)
            # Verwende das globale 'worksheet'-Objekt zum Speichern
            if save_categorization_gsheet(worksheet, current_labeler_id, display_url, categories_str, comment):
                st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_local_idx] = comment
                st.session_state.processed_urls_in_session.add(current_local_idx)
                # get_processed_urls_by_labeler.clear() # Optional: Cache hier leeren? Eher nicht.
                st.session_state.current_index += 1
                st.rerun()
            else:
                st.error("Speichern in Google Sheet fehlgeschlagen.")

# --- Fallback-Anzeige, wenn nicht initialisiert ---
elif not st.session_state.get('initialized', False) and st.session_state.labeler_id:
    if not worksheet:
         st.error("Verbindung zu Google Sheets fehlgeschlagen.")
    else:
        if not st.session_state.input_file_name:
             st.info(f"Lade eine CSV-Datei hoch oder stelle sicher, dass '{DEFAULT_CSV_PATH}' vorhanden ist, um mit dem Labeling zu beginnen.")

# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    try:
        sheet_url = worksheet.spreadsheet.url
        st.sidebar.page_link(sheet_url, label="Google Sheet öffnen ↗️")
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
    original_total = st.session_state.original_total_items
    processed_count = st.session_state.already_processed_count
    remaining_count = st.session_state.total_items
    current_local_idx = st.session_state.current_index
    current_global_item_number = processed_count + current_local_idx + 1
    if remaining_count == 0 and original_total > 0: current_global_item_number = original_total
    if original_total == 0: current_global_item_number = 0

    st.sidebar.metric("URLs in Datei (Gesamt)", original_total)
    st.sidebar.metric("Aktuelles Item / Gesamt", f"{current_global_item_number} / {original_total}")
    st.sidebar.metric("Bereits gespeichert (von dir)", processed_count)
    st.sidebar.metric("Noch offen (für dich)", remaining_count)
else:
    st.sidebar.metric("URLs in Datei (Gesamt)", "-")
    st.sidebar.metric("Aktuelles Item / Gesamt", "-")
    st.sidebar.metric("Bereits gespeichert (von dir)", "-")
    st.sidebar.metric("Noch offen (für dich)", "-")

st.sidebar.caption(f"Letzter Check GSheet Header: {'OK' if not header_written_flag else 'Aktualisiert/Geschrieben'}")
st.sidebar.caption("Tweet-Vorschauen werden gecached.")
st.sidebar.caption("Fortschritt wird beim Laden der Datei aus GSheet abgerufen.")