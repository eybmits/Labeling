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
import hashlib # Für Seed-Generierung

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="Dataset Labeler)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfad zur Standard-CSV-Datei ===
DEFAULT_CSV_PATH = "input.csv"

# === Google Sheets Setup ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
COL_TS = "Timestamp"
COL_LBL = "Labeler_ID"
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_TS, COL_LBL, COL_URL, COL_CATS, COL_COMMENT]
TIMEZONE = pytz.timezone("Europe/Berlin")

# === Google Sheets Verbindung ===
@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her und gibt das Worksheet-Objekt zurück."""
    try:
        if "google_sheets" not in st.secrets or "credentials_dict" not in st.secrets["google_sheets"] or "sheet_name" not in st.secrets["google_sheets"]:
            st.error("Google Sheets Secrets ('google_sheets.credentials_dict', 'google_sheets.sheet_name') fehlen oder sind unvollständig. Bitte in Streamlit Cloud konfigurieren.")
            st.stop(); return None, False, None
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1
        header_written = False
        all_vals = worksheet.get_all_values()
        if not all_vals or all_vals[0] != HEADER:
            st.sidebar.warning(f"Header in '{sheet_name}' stimmt nicht mit {HEADER} überein oder fehlt. Schreibe korrekten Header...")
            try:
                if not all_vals or len(all_vals[0]) != len(HEADER) or all(c == '' for c in all_vals[0]):
                    worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                else:
                    cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]
                    worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')
                try:
                    if len(worksheet.get_all_values()) > 1 and all(v == '' for v in worksheet.row_values(2)):
                        worksheet.delete_rows(2)
                except IndexError: pass
                header_written = True
                st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")
            except Exception as he: st.sidebar.error(f"Konnte Header nicht schreiben: {he}")
        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt."); st.stop(); return None, False, None
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop(); return None, False, None
    except gspread.exceptions.APIError as e: st.error(f"Google API Fehler (Verbindung): {e}. Berechtigungen prüfen?"); st.stop(); return None, False, None
    except Exception as e: st.error(f"Fehler bei GSheets Verbindung: {e}"); st.stop(); return None, False, None

worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===
# (Kategorien, Farben etc. bleiben unverändert)
CATEGORIES = {
    "Health": {
        "desc": "Themen rund um Gesundheit, Wohlbefinden und das Gesundheitssystem.",
        "sub": {
            "Lifestyle": "Ernährung, Bewegung, Gewohnheiten, Prävention.",
            "Mental Health": "Psychische Gesundheit, Stress, Therapie, Störungen.",
            "Physical Health": "Krankheiten, Verletzungen, körperliche Fitness, Altern.",
            "Healthcare System": "Zugang zu Versorgung, Kosten, Politik, Ärzte, Krankenhäuser.",
        }
    },
    "Social": {
        "desc": "Gesellschaftliche Themen wie Bildung, Arbeit und Zusammenleben.",
        "sub": {
            "Education": "Schule, Universität, Ausbildung, lebenslanges Lernen, Bildungspolitik.",
            "Family/Relationships": "Partnerschaft, Kinder, Generationen, soziale Bindungen.",
            "Employment/Economy": "Arbeitsmarkt, Arbeitslosigkeit, Gehälter, Wirtschaftslage, Unternehmen.",
        }
    },
    "Environment": {
        "desc": "Themen bezüglich Umwelt, Klima, Energie und Katastrophen.",
        "sub": {
            "Environmental Policies": "Klimaschutz, Gesetze, Vorschriften, internationale Abkommen.",
            "Energy Sector": "Erneuerbare Energien, fossile Brennstoffe, Energiepreise, Versorgungssicherheit.",
            "Natural/Man-made Disasters": "Naturkatastrophen (Hochwasser, Stürme), menschengemachte Katastrophen (Unfälle, Verschmutzung).",
        }
    }
}
ALL_CATEGORIES = [sub_cat for main_cat_data in CATEGORIES.values() for sub_cat in main_cat_data["sub"]]
CATEGORY_COLORS = { "Health": "dodgerblue", "Social": "mediumseagreen", "Environment": "darkorange" }
SUBCATEGORY_COLORS = {
    "Lifestyle": "skyblue", "Mental Health": "lightcoral", "Physical Health": "mediumaquamarine", "Healthcare System": "steelblue",
    "Education": "sandybrown", "Family/Relationships": "lightpink", "Employment/Economy": "khaki",
    "Environmental Policies": "mediumseagreen", "Energy Sector": "gold", "Natural/Man-made Disasters": "slategray",
    "DEFAULT_COLOR": "grey"
}

# === Hilfsfunktionen ===
# (get_processed_urls_by_labeler, load_urls_from_input_csv, save_categorization_gsheet, clean_tweet_url, get_tweet_embed_html bleiben unverändert)
@st.cache_data(ttl=300)
def get_processed_urls_by_labeler(target_labeler_id):
    processed_urls = set()
    worksheet_obj, _, sheet_name_local = connect_gsheet()
    if not worksheet_obj: st.warning(f"Keine GSheet-Verbindung zum Abrufen des Fortschritts für '{target_labeler_id}'."); return processed_urls
    if not target_labeler_id: st.warning("Leere Labeler ID beim Abrufen des Fortschritts."); return processed_urls
    print(f"DEBUG: Rufe verarbeitete URLs für Labeler '{target_labeler_id}' aus '{sheet_name_local}' ab...")
    try:
        all_data = worksheet_obj.get_all_values()
        if not all_data or len(all_data) < 1: print(f"DEBUG: Sheet '{sheet_name_local}' ist leer oder nur Header."); return processed_urls
        header_row = all_data[0]
        try:
            labeler_col_index = header_row.index(COL_LBL)
            url_col_index = header_row.index(COL_URL)
        except ValueError as e: st.error(f"Fehler: Spalte '{e}' fehlt im Header des Sheets '{sheet_name_local}': {header_row}."); return processed_urls
        for i, row in enumerate(all_data[1:], start=2):
            if len(row) > max(labeler_col_index, url_col_index) and row[labeler_col_index] and row[url_col_index]:
                if row[labeler_col_index].strip() == target_labeler_id:
                    processed_urls.add(row[url_col_index].strip())
        print(f"DEBUG: {len(processed_urls)} verarbeitete URLs für '{target_labeler_id}' in '{sheet_name_local}' gefunden.")
    except gspread.exceptions.APIError as e: st.warning(f"Google API Fehler beim Laden des Fortschritts: {e}")
    except Exception as e: st.warning(f"Allgemeiner Fehler beim Laden des Fortschritts: {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(file_path, source_name="Standarddatei"):
    urls = []
    if not file_path or not isinstance(file_path, str): st.error("Kein gültiger Dateipfad übergeben."); return urls
    if not os.path.exists(file_path): st.error(f"Fehler: Die Datei '{file_path}' wurde nicht gefunden."); return urls
    try:
        try: df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
        except UnicodeDecodeError: st.warning(f"UTF-8 Fehler bei '{source_name}', versuche latin-1..."); df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)
        print(f"DEBUG: CSV gelesen ({source_name}), {len(df)} Zeilen.")
        if df.empty: st.warning(f"Input '{source_name}' ist leer."); return urls
        url_series_raw = df.iloc[:, 0]; url_series_str = url_series_raw.astype(str)
        url_series_nonan = url_series_str.replace('nan', pd.NA).dropna(); url_series_stripped = url_series_nonan.str.strip()
        url_series_noempty = url_series_stripped[url_series_stripped != '']; print(f"DEBUG: Nach Bereinigung, {len(url_series_noempty)} Zeilen übrig.")
        url_pattern = r'^https?://\S+$'; url_series_filtered = url_series_noempty[url_series_noempty.str.match(url_pattern)]
        print(f"DEBUG: Nach Regex-Filter ({url_pattern}), {len(url_series_filtered)} Zeilen übrig.")
        urls = url_series_filtered.unique().tolist(); print(f"DEBUG: Nach unique(), {len(urls)} URLs zurückgegeben.")
    except FileNotFoundError: st.error(f"Fehler: Datei '{file_path}' nicht gefunden.")
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer.")
    except IndexError: st.warning(f"Input '{source_name}' hat keine Spalten.")
    except Exception as e: st.error(f"Fehler beim Lesen/Verarbeiten von '{source_name}': {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt beim Speichern."); return False
    if not url: st.error("URL fehlt beim Speichern."); return False
    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e: st.error(f"Google API Fehler beim Speichern: {e}."); return False
    except Exception as e: st.error(f"Allgemeiner Fehler beim Speichern in GSheet: {e}"); return False

def clean_tweet_url(url):
    if not isinstance(url, str): return url
    try: cleaned_url = url.split('?')[0]; cleaned_url = re.sub(r"/(photo|video)/\d+$", "", cleaned_url); return cleaned_url
    except Exception: return url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    if not isinstance(tweet_url, str): return None
    try:
        parsed_url = urlparse(tweet_url)
        if not (parsed_url.netloc in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"] and "/status/" in parsed_url.path): return None
    except Exception as parse_err: return None
    cleaned_tweet_url = clean_tweet_url(tweet_url)
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try:
        response = requests.get(api_url, timeout=10); response.raise_for_status(); data = response.json()
        html_content = data.get("html")
        if not html_content: print(f"DEBUG: Kein 'html' Feld in oEmbed Antwort für {cleaned_tweet_url}."); return f"<p style='color:orange;'>Fehler: Vorschau unvollständig.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
        return html_content
    except requests.exceptions.Timeout: return f"<p style='color:orange; border:1px solid orange; padding:10px;'>Timeout Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code; msg = f"Fehler ({status_code}) Vorschau."
        if status_code == 404: msg = "Tweet nicht gefunden (404)."
        elif status_code == 403: msg = "Zugriff verweigert (403)."
        elif status_code >= 500: msg = f"Serverfehler Twitter ({status_code})."
        return f"<p style='color:orange; border:1px solid orange; padding:10px;'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.RequestException as e: return f"<p style='color:orange; border:1px solid orange; padding:10px;'>Netzwerkfehler Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except Exception as e: st.warning(f"Embed Fehler {cleaned_tweet_url}: {e}"); return f"<p style='color:orange;'>Unbekannter Fehler Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"

def show_guidelines():
    """Zeigt die Anleitungsseite an."""
    st.header("📊 Anleitung zum Dataset Labeler")
    st.markdown("""
        Willkommen beim Labeling-Tool! Deine Aufgabe ist es, Social Media Posts (aktuell X/Twitter) einer oder mehreren vordefinierten Kategorien zuzuordnen.

        **Ziel:** Wir möchten verstehen, welche Themenbereiche (Gesundheit, Soziales, Umwelt) in den Posts diskutiert werden.

        **Ablauf:**
        1.  **Post ansehen:** Links wird eine Vorschau des Posts angezeigt (falls verfügbar) oder der direkte Link. Öffne den Link bei Bedarf in einem neuen Tab.
        2.  **Kategorien wählen:** Rechts findest du die Hauptkategorien (Health, Social, Environment). Wähle **mindestens eine** passende Subkategorie aus, die den **Hauptinhalt** des Posts am besten beschreibt. Mehrfachauswahl ist möglich, wenn der Post klar mehrere Themen abdeckt.
        3.  **Tooltip nutzen:** Fahre mit der Maus über die Hauptkategorien (`❓`) oder die einzelnen Subkategorien (Checkboxen) für eine kurze Beschreibung.
        4.  **(Optional) Kommentar:** Füge bei Bedarf einen Kommentar hinzu (z.B. bei Unklarheiten, Mehrdeutigkeiten, technischen Problemen mit dem Post).
        5.  **Speichern & Weiter:** Klicke auf "Speichern & Weiter", um deine Auswahl zu speichern und zum nächsten Post zu gelangen.
        6.  **Navigation:** Mit "Zurück" kannst du vorherige (in dieser Sitzung bearbeitete) Posts korrigieren. Mit "Überspringen" (oben rechts) kannst du einen Post markieren, ohne ihn zu speichern (wird als "[Übersprungen]" im Kommentarfeld vermerkt, wenn du dann zum nächsten gehst).

        **Wichtige Hinweise:**
        *   Fokus auf den **Inhalt des Posts**, nicht auf Kommentare darunter.
        *   Sei konsistent.
        *   Wenn ein Post **gar nicht** passt oder **nicht zugänglich** ist, wähle keine Kategorie und hinterlasse optional einen Kommentar. Klicke dann trotzdem auf "Speichern & Weiter".
        *   Dein Fortschritt wird pro URL gespeichert. Die Reihenfolge der Posts ist für dich **zufällig**.

        Danke für deine Hilfe! 🙏
    """)
    st.divider()

# === Streamlit App Hauptteil ===
st.title("📊 Dataset Labeler")

# --- Session State Initialisierung ---
if 'labeler_id' not in st.session_state: st.session_state.labeler_id = ""
# NEU: Status, ob der Name bestätigt wurde
if 'name_confirmed' not in st.session_state: st.session_state.name_confirmed = False
if 'guidelines_shown' not in st.session_state: st.session_state.guidelines_shown = False
if 'initialized' not in st.session_state: st.session_state.initialized = False
if 'input_file_name' not in st.session_state: st.session_state.input_file_name = DEFAULT_CSV_PATH
if 'urls_to_process' not in st.session_state: st.session_state.urls_to_process = []
if 'total_items_in_session' not in st.session_state: st.session_state.total_items_in_session = 0
if 'processed_urls_from_sheet' not in st.session_state: st.session_state.processed_urls_from_sheet = set()
if 'current_index_in_session' not in st.session_state: st.session_state.current_index_in_session = 0
if 'session_results' not in st.session_state: st.session_state.session_results = {}
if 'session_comments' not in st.session_state: st.session_state.session_comments = {}
if 'original_total_items_from_file' not in st.session_state: st.session_state.original_total_items_from_file = 0
if 'already_processed_count_on_start' not in st.session_state: st.session_state.already_processed_count_on_start = 0


# --- Schritt 1: Labeler ID Eingabe ---
# Das Namensfeld wird jetzt durch 'name_confirmed' gesteuert.
labeler_id_input = st.text_input(
    "👤 Bitte gib deinen Vornamen ein:",
    value=st.session_state.labeler_id,
    key="labeler_id_widget",
    help="Dein Name wird zum Speichern des Fortschritts verwendet. Er wird gesperrt, nachdem du die Anleitung bestätigt hast.",
    disabled=st.session_state.name_confirmed # Wird gesperrt, wenn Name bestätigt wurde
)

# Aktualisiere die Labeler ID im Session State nur, wenn das Feld nicht gesperrt ist.
if not st.session_state.name_confirmed:
    st.session_state.labeler_id = labeler_id_input.strip()

# Zeige eine Meldung, wenn der Name gesperrt ist.
if st.session_state.name_confirmed:
    st.caption(f"Labeler ID '{st.session_state.labeler_id}' ist für diese Sitzung festgelegt.")

# Stoppt, wenn *initial* keine Labeler ID eingegeben wurde.
# Nach Bestätigung ist die ID ja vorhanden und gesperrt.
if not st.session_state.labeler_id and not st.session_state.name_confirmed:
    st.warning("Bitte eine Labeler ID (Vorname) eingeben, um fortzufahren.")
    st.stop()

st.divider()


# --- Schritt 2: Guidelines anzeigen (wenn ID da, Name noch NICHT bestätigt) ---
# Zeige Guidelines nur an, wenn ein Name eingegeben wurde, dieser aber noch nicht bestätigt ist.
if st.session_state.labeler_id and not st.session_state.name_confirmed:
    show_guidelines()
    if st.button("✅ Verstanden, starte das Labeling!"):
        # WICHTIG: Hier wird der Name jetzt bestätigt und gesperrt!
        st.session_state.name_confirmed = True
        st.session_state.guidelines_shown = True
        st.session_state.initialized = False # Erzwingt Neuinitialisierung nach Bestätigung
        # Kurze Anzeige, dass es losgeht
        st.success(f"Danke, {st.session_state.labeler_id}! Lade jetzt deine Daten...")
        time.sleep(1) # Kurze Pause, damit die Nachricht sichtbar ist
        st.rerun()
    st.stop() # Anhalten, bis der Button geklickt wird


# --- Schritt 3: Daten initialisieren (wenn Name bestätigt, aber noch nicht initialisiert) ---
# Diese Logik läuft jetzt nur, nachdem der Name bestätigt wurde.
needs_initialization = (st.session_state.name_confirmed and
                        not st.session_state.get('initialized', False))

if needs_initialization and worksheet:
    print(f"Starte Initialisierung für bestätigten Labeler: {st.session_state.labeler_id}")
    # Reset session states related to data processing
    st.session_state.urls_to_process = []
    st.session_state.total_items_in_session = 0
    st.session_state.processed_urls_from_sheet = set()
    st.session_state.current_index_in_session = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.original_total_items_from_file = 0
    st.session_state.already_processed_count_on_start = 0
    st.session_state.input_file_name = DEFAULT_CSV_PATH

    with st.spinner(f"Lade URLs aus '{DEFAULT_CSV_PATH}' und prüfe Fortschritt für '{st.session_state.labeler_id}'..."):
        all_input_urls_cleaned = load_urls_from_input_csv(DEFAULT_CSV_PATH, source_name=DEFAULT_CSV_PATH)
        st.session_state.original_total_items_from_file = len(all_input_urls_cleaned)

        if not all_input_urls_cleaned:
            st.error(f"Konnte keine gültigen URLs in '{DEFAULT_CSV_PATH}' finden."); st.session_state.initialized = False; st.stop()

        current_labeler_id = st.session_state.labeler_id
        get_processed_urls_by_labeler.clear()
        processed_by_this_labeler = get_processed_urls_by_labeler(current_labeler_id)
        st.session_state.processed_urls_from_sheet = processed_by_this_labeler
        st.session_state.already_processed_count_on_start = len(processed_by_this_labeler)

        remaining_urls = [url for url in all_input_urls_cleaned if url.strip() not in processed_by_this_labeler]

        if remaining_urls:
            hasher = hashlib.sha256(current_labeler_id.encode('utf-8'))
            seed_value = int(hasher.hexdigest(), 16)
            random.seed(seed_value)
            random.shuffle(remaining_urls)
            print(f"DEBUG: {len(remaining_urls)} URLs für '{current_labeler_id}' gemischt (Seed: {seed_value}).")

        st.session_state.urls_to_process = remaining_urls
        st.session_state.total_items_in_session = len(remaining_urls)
        st.session_state.current_index_in_session = 0
        st.session_state.initialized = True # Markiere als initialisiert

        total_original = st.session_state.original_total_items_from_file
        processed_on_start = st.session_state.already_processed_count_on_start
        remaining_now = st.session_state.total_items_in_session

        # Keine explizite Erfolgsmeldung hier mehr, da sie direkt nach dem Button kam.
        # Der Spinner endet automatisch und die UI wird durch rerun aktualisiert.
        # if remaining_now > 0: st.success(...) else: st.success(...) # Entfernt

        time.sleep(0.5) # Kurze Pause für UI-Fluss
        st.rerun()

elif needs_initialization and not worksheet:
    st.error("Google Sheet Verbindung fehlgeschlagen. Initialisierung kann nicht abgeschlossen werden.")
    st.session_state.initialized = False
    st.stop()


# --- Schritt 4: Haupt-Labeling-Interface (wenn Name bestätigt UND initialisiert) ---
if st.session_state.get('name_confirmed', False) and st.session_state.get('initialized', False):

    # Hole aktuelle Werte aus dem Session State
    labeler_id = st.session_state.labeler_id
    urls_for_session = st.session_state.urls_to_process
    total_in_session = st.session_state.total_items_in_session
    original_total = st.session_state.original_total_items_from_file
    processed_on_start = st.session_state.already_processed_count_on_start
    current_local_idx = st.session_state.current_index_in_session

    # --- Fall: Alle URLs dieser Sitzung bearbeitet ---
    if total_in_session <= 0 or current_local_idx >= total_in_session:
        st.success(f"🎉 Super, {labeler_id}! Alle {original_total} URLs wurden bearbeitet!")
        st.balloons()
        # Button zum Neuladen
        if st.button("App neu laden (startet von vorn)"):
             # Reset für einen kompletten Neustart
             st.session_state.labeler_id = "" # Name muss neu eingegeben werden
             st.session_state.name_confirmed = False
             st.session_state.guidelines_shown = False
             st.session_state.initialized = False
             # Caches leeren
             st.cache_data.clear()
             st.cache_resource.clear()
             get_processed_urls_by_labeler.clear()
             st.rerun()
        st.stop()

    # --- Fall: Es gibt noch URLs zu bearbeiten ---
    current_url = urls_for_session[current_local_idx]
    current_global_item_number = processed_on_start + current_local_idx + 1

    # --- Navigation Oben ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_local_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True, help="Zum vorherigen Eintrag dieser Sitzung."):
            st.session_state.current_index_in_session -= 1; st.rerun()
    else: nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    if original_total > 0:
        progress_percentage = (processed_on_start + current_local_idx) / original_total
        progress_text = f"{labeler_id}: Item {current_global_item_number} / {original_total} (noch {total_in_session - current_local_idx} in Sitzung)"
        nav_cols_top[1].progress(progress_percentage, text=progress_text)
    else: nav_cols_top[1].progress(0, text="Keine Items")
    can_go_forward = (current_local_idx + 1) < total_in_session
    if nav_cols_top[2].button("Überspringen ➡️" if can_go_forward else "Letztes Item", key="skip_next_top", use_container_width=True, help="Markiert dieses Item als übersprungen und geht zum nächsten."):
        if can_go_forward:
            st.session_state.session_results[current_local_idx] = []
            st.session_state.session_comments[current_local_idx] = "[Übersprungen]"
            st.session_state.current_index_in_session += 1; st.rerun()
        else: st.toast("Dies ist bereits das letzte Item.", icon="ℹ️")
    st.divider()

    # --- Zweispaltiges Layout ---
    left_column, right_column = st.columns([2, 1])

    # --- Linke Spalte: URL Anzeige & Einbettung ---
    with left_column:
        st.subheader("Post Vorschau / Link")
        base_tweet_url = clean_tweet_url(current_url)
        embed_html = get_tweet_embed_html(base_tweet_url)
        display_url = current_url
        st.markdown(f"**URL:** [{display_url}]({display_url})")
        if embed_html: components.html(embed_html, height=650, scrolling=True)
        else:
            if "twitter.com" in display_url or "x.com" in display_url:
                 error_msg = get_tweet_embed_html(base_tweet_url)
                 if error_msg and error_msg.startswith("<p style='color:orange"): st.markdown(error_msg, unsafe_allow_html=True)
                 else: st.caption("Vorschau konnte nicht geladen werden.")
            else: st.caption("Vorschau nur für X/Twitter Posts.")
            st.link_button("Link in neuem Tab öffnen", display_url)

    # --- Rechte Spalte: Kategorieauswahl & Kommentar ---
    with right_column:
        st.subheader("Kategorisierung")
        saved_selection = st.session_state.session_results.get(current_local_idx, [])
        selected_categories_in_widgets = []
        st.markdown("**Wähle passende Subkategorie(n):**")
        for main_topic, main_data in CATEGORIES.items():
            main_color = CATEGORY_COLORS.get(main_topic, "black"); main_desc = main_data["desc"]
            st.markdown(f'''<h6 style="color:{main_color}; border-bottom: 1px solid {main_color}; margin-top: 10px; margin-bottom: 5px;"> {main_topic} <span title="{main_desc}" style="cursor:help; font-weight:normal; color:grey;"> ❓</span></h6>''', unsafe_allow_html=True)
            sub_categories = main_data["sub"]
            for sub_cat, sub_desc in sub_categories.items():
                clean_sub_cat_key = re.sub(r'\W+', '', sub_cat)
                checkbox_key = f"cb_{current_local_idx}_{main_topic.replace(' ', '_')}_{clean_sub_cat_key}"
                is_checked_default = sub_cat in saved_selection
                is_checked_now = st.checkbox(sub_cat, value=is_checked_default, key=checkbox_key, help=sub_desc)
                if is_checked_now: selected_categories_in_widgets.append(sub_cat)
        st.markdown("---")
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets)))
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**")
            display_tags = []
            for cat in selected_categories_in_widgets:
                 cat_color = SUBCATEGORY_COLORS.get(cat, SUBCATEGORY_COLORS.get("DEFAULT_COLOR", "grey"))
                 display_tags.append(f'<span style="display: inline-block; background-color: {cat_color}; color: white; border-radius: 4px; padding: 1px 6px; margin: 2px; font-size: 0.85em;">{cat}</span>')
            st.markdown(" ".join(display_tags), unsafe_allow_html=True)
        else: st.caption("_Keine Kategorien ausgewählt._")
        st.markdown("---")
        default_comment = st.session_state.session_comments.get(current_local_idx, "")
        comment_key = f"comment_{current_local_idx}"
        comment_input = st.text_area("Optionaler Kommentar:", value=default_comment, height=120, key=comment_key, placeholder="Notizen, Link defekt?")

    # --- Navigation Unten ---
    st.divider()
    nav_cols_bottom = st.columns(7)
    if current_local_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True):
            st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
            st.session_state.session_comments[current_local_idx] = comment_input
            st.session_state.current_index_in_session -= 1; st.rerun()
    else: nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)

    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_selection = selected_categories_in_widgets
        current_comment = comment_input
        if not worksheet: st.error("Speichern fehlgeschlagen: Keine GSheet Verbindung.")
        elif not labeler_id: st.error("Speichern fehlgeschlagen: Labeler ID fehlt.")
        else:
            categories_str = "; ".join(current_selection) if current_selection else ""
            save_success = save_categorization_gsheet(worksheet, labeler_id, display_url, categories_str, current_comment)
            if save_success:
                st.toast("Gespeichert!", icon="✅")
                st.session_state.session_results[current_local_idx] = current_selection
                st.session_state.session_comments[current_local_idx] = current_comment
                st.session_state.processed_urls_from_sheet.add(display_url.strip())
                # Anpassung: Nicht `already_processed_count_on_start` erhöhen, da dies nur den *Startzustand* repräsentiert.
                # Die Sidebar-Logik wird angepasst, um den aktuellen Fortschritt korrekt zu berechnen.
                st.session_state.current_index_in_session += 1; st.rerun()
            else: st.error("Speichern in GSheet fehlgeschlagen.")


# --- Fallback-Anzeige, wenn Initialisierung noch aussteht ---
elif st.session_state.name_confirmed and not st.session_state.get('initialized', False):
    # Zeigt nur Nachrichten an, wenn die Initialisierung nach Bestätigung fehlschlägt
    st.warning("Warte auf Initialisierung oder prüfe Fehlermeldungen...")


# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet: st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
else: st.sidebar.error("Keine GSheet Verbindung.")

st.sidebar.markdown(f"**Labeler/in:** {st.session_state.labeler_id or '(Bitte eingeben)'}") # Angepasste Meldung
st.sidebar.markdown(f"**Input-Datei:** {st.session_state.get('input_file_name', DEFAULT_CSV_PATH)}")

if st.session_state.get('initialized', False):
    original_total = st.session_state.original_total_items_from_file
    processed_on_start = st.session_state.already_processed_count_on_start
    # Korrekte Berechnung "Gespeichert": Startwert + Anzahl der in dieser Session durchlaufenen Items
    # Wichtig: `current_local_idx` ist der Index des *aktuellen* Items, d.h. `current_local_idx` Items wurden *vor* diesem abgeschlossen.
    processed_count_total = processed_on_start + st.session_state.current_index_in_session
    remaining_in_session = st.session_state.total_items_in_session - st.session_state.current_index_in_session
    current_global_item_number = processed_count_total + 1

    # Korrekturen für Randfälle (alles fertig / Session leer)
    if st.session_state.total_items_in_session == 0: # Nichts mehr zu tun von Anfang an
         current_global_item_number = original_total
         remaining_in_session = 0
         processed_count_total = original_total # Alle sind gespeichert
    elif st.session_state.current_index_in_session >= st.session_state.total_items_in_session: # Letztes Item gerade abgeschlossen
         current_global_item_number = original_total
         remaining_in_session = 0
         processed_count_total = original_total # Alle sind gespeichert

    st.sidebar.metric("Gesamt aus Datei", original_total)
    # Sicherstellen, dass Zähler nicht über das Total hinausgeht
    st.sidebar.metric("Aktuell / Gesamt", f"{min(current_global_item_number, original_total)} / {original_total}")
    st.sidebar.metric("Von dir gespeichert", processed_count_total)
    st.sidebar.metric("Noch offen (in Session)", remaining_in_session)
else:
    st.sidebar.metric("Gesamt aus Datei", "-"); st.sidebar.metric("Aktuell / Gesamt", "-")
    st.sidebar.metric("Von dir gespeichert", "-"); st.sidebar.metric("Noch offen (in Session)", "-")

st.sidebar.caption(f"GSheet Header: {'OK' if not header_written_flag else 'Geschrieben/Aktualisiert'}")
st.sidebar.caption("Tweet-Vorschauen gecached.")
st.sidebar.caption("Fortschritt wird beim Start abgerufen.")
if st.session_state.get('name_confirmed', False):
    st.sidebar.caption(f"Randomisierung: Aktiv (Seed: {st.session_state.labeler_id})")