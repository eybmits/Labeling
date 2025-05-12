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
@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her und gibt das Worksheet-Objekt zurück."""
    try:
        # Stelle sicher, dass Secrets korrekt konfiguriert sind
        if "google_sheets" not in st.secrets or "credentials_dict" not in st.secrets["google_sheets"] or "sheet_name" not in st.secrets["google_sheets"]:
            st.error("Google Sheets Secrets ('google_sheets.credentials_dict', 'google_sheets.sheet_name') fehlen oder sind unvollständig. Bitte in Streamlit Cloud konfigurieren.")
            st.stop()
            return None, False, None # Hält die App an

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
                # Prüfen, ob die erste Zeile komplett leer oder falsch ist
                if not all_vals or len(all_vals[0]) != len(HEADER) or all(c == '' for c in all_vals[0]):
                    worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                else: # Nur aktualisieren, wenn die Zeile existiert, aber falsch ist
                    cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]
                    worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')

                # Lösche ggf. eine leere zweite Zeile, die durch insert_row entstehen kann
                try:
                    if len(worksheet.get_all_values()) > 1 and all(v == '' for v in worksheet.row_values(2)):
                        worksheet.delete_rows(2)
                except IndexError:
                    pass # Passiert, wenn das Sheet nur den Header hat

                header_written = True
                st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")
            except Exception as he:
                st.sidebar.error(f"Konnte Header nicht schreiben: {he}")
                # Bei Header-Fehler nicht stoppen, aber anzeigen
        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt. Bitte überprüfen."); st.stop(); return None, False, None
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop(); return None, False, None
    except gspread.exceptions.APIError as e: st.error(f"Google API Fehler (Verbindung): {e}. Berechtigungen prüfen?"); st.stop(); return None, False, None
    except Exception as e: st.error(f"Fehler bei GSheets Verbindung: {e}"); st.stop(); return None, False, None

worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===
# Kategorien und ihre Beschreibungen (für Tooltips)
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
# Flache Liste aller Subkategorien
ALL_CATEGORIES = [sub_cat for main_cat_data in CATEGORIES.values() for sub_cat in main_cat_data["sub"]]

# Farben für die Darstellung
CATEGORY_COLORS = {
    "Health": "dodgerblue",
    "Social": "mediumseagreen",
    "Environment": "darkorange",
}
SUBCATEGORY_COLORS = {
    "Lifestyle": "skyblue", "Mental Health": "lightcoral", "Physical Health": "mediumaquamarine", "Healthcare System": "steelblue",
    "Education": "sandybrown", "Family/Relationships": "lightpink", "Employment/Economy": "khaki",
    "Environmental Policies": "mediumseagreen", "Energy Sector": "gold", "Natural/Man-made Disasters": "slategray",
    "DEFAULT_COLOR": "grey"
}

# === Hilfsfunktionen ===
@st.cache_data(ttl=300) # Cache für 5 Minuten
def get_processed_urls_by_labeler(target_labeler_id):
    """Holt die bereits von einem Labeler bearbeiteten URLs aus dem Google Sheet."""
    processed_urls = set()
    # Erneute Verbindung holen, da @st.cache_data keine _resource Objekte cachen kann
    worksheet_obj, _, sheet_name_local = connect_gsheet()
    if not worksheet_obj:
        st.warning(f"Keine GSheet-Verbindung zum Abrufen des Fortschritts für '{target_labeler_id}'.")
        return processed_urls
    if not target_labeler_id:
        st.warning("Leere Labeler ID beim Abrufen des Fortschritts.")
        return processed_urls

    print(f"DEBUG: Rufe verarbeitete URLs für Labeler '{target_labeler_id}' aus '{sheet_name_local}' ab...")
    try:
        all_data = worksheet_obj.get_all_values()
        if not all_data or len(all_data) < 1:
            print(f"DEBUG: Sheet '{sheet_name_local}' ist leer oder nur Header.")
            return processed_urls

        header_row = all_data[0]
        try:
            labeler_col_index = header_row.index(COL_LBL)
            url_col_index = header_row.index(COL_URL)
        except ValueError as e:
            st.error(f"Fehler: Spalte '{e}' fehlt im Header des Sheets '{sheet_name_local}': {header_row}.")
            return processed_urls # Ohne korrekte Spalten kann nichts gefunden werden

        for i, row in enumerate(all_data[1:], start=2): # Start bei Zeile 2
            # Sicherstellen, dass die Zeile lang genug ist und die relevanten Zellen nicht leer sind
            if len(row) > max(labeler_col_index, url_col_index) and row[labeler_col_index] and row[url_col_index]:
                if row[labeler_col_index].strip() == target_labeler_id:
                    processed_urls.add(row[url_col_index].strip())
            # Optional: Warnung bei kurzen oder leeren Zeilen (kann bei vielen Einträgen störend sein)
            # elif len(row) <= max(labeler_col_index, url_col_index):
            #     print(f"DEBUG: Zeile {i} in '{sheet_name_local}' ist zu kurz.")
            # elif not row[labeler_col_index] or not row[url_col_index]:
            #     print(f"DEBUG: Labeler ID oder URL fehlt in Zeile {i} von '{sheet_name_local}'.")


        print(f"DEBUG: {len(processed_urls)} verarbeitete URLs für '{target_labeler_id}' in '{sheet_name_local}' gefunden.")
    except gspread.exceptions.APIError as e:
        st.warning(f"Google API Fehler beim Laden des Fortschritts: {e}")
    except Exception as e:
        st.warning(f"Allgemeiner Fehler beim Laden des Fortschritts: {e}")
    return processed_urls

# Cache pro Datei und Inhalt (implizit durch file_path als Argument)
@st.cache_data
def load_urls_from_input_csv(file_path, source_name="Standarddatei"):
    """Lädt alle URLs aus einem Dateipfad und bereinigt sie."""
    urls = []
    if not file_path or not isinstance(file_path, str):
        st.error("Kein gültiger Dateipfad übergeben."); return urls
    if not os.path.exists(file_path):
         st.error(f"Fehler: Die Datei '{file_path}' wurde nicht gefunden. Bitte stelle sicher, dass sie im Root-Verzeichnis des Repositories liegt.")
         return urls

    try:
        # Versuche mit UTF-8, dann mit Latin-1
        try:
            df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
        except UnicodeDecodeError:
            st.warning(f"UTF-8 Fehler bei '{source_name}', versuche latin-1...")
            df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)

        print(f"DEBUG: CSV gelesen ({source_name}), {len(df)} Zeilen.")
        if df.empty:
            st.warning(f"Input '{source_name}' ist leer oder enthält keine gültigen Daten in der ersten Spalte.")
            return urls

        url_series_raw = df.iloc[:, 0]
        url_series_str = url_series_raw.astype(str) # Sicherstellen, dass alles String ist
        url_series_nonan = url_series_str.replace('nan', pd.NA).dropna() # 'nan' Strings und echte NaNs entfernen
        url_series_stripped = url_series_nonan.str.strip() # Leerzeichen entfernen
        url_series_noempty = url_series_stripped[url_series_stripped != ''] # Leere Strings entfernen
        print(f"DEBUG: Nach Bereinigung, {len(url_series_noempty)} Zeilen übrig.")

        # Filtern auf gültige HTTP/HTTPS URLs
        url_pattern = r'^https?://\S+$'
        url_series_filtered = url_series_noempty[url_series_noempty.str.match(url_pattern)]
        print(f"DEBUG: Nach Regex-Filter ({url_pattern}), {len(url_series_filtered)} Zeilen übrig.")

        urls = url_series_filtered.unique().tolist() # Duplikate entfernen
        print(f"DEBUG: Nach unique(), {len(urls)} URLs zurückgegeben.")

    except FileNotFoundError: st.error(f"Fehler: Datei '{file_path}' nicht gefunden.") # Sollte durch os.path.exists abgedeckt sein, aber sicher ist sicher
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer.")
    except IndexError: st.warning(f"Input '{source_name}' hat keine Spalten oder die erste Spalte konnte nicht gelesen werden.")
    except Exception as e: st.error(f"Fehler beim Lesen/Verarbeiten von '{source_name}': {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Speichert eine einzelne Kategorisierung im Google Sheet."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt beim Speichern."); return False
    if not url: st.error("URL fehlt beim Speichern."); return False # URL sollte nie leer sein hier

    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        # Sicherstellen, dass die Reihenfolge mit HEADER übereinstimmt
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e:
        st.error(f"Google API Fehler beim Speichern: {e}. Warte und versuche erneut?")
        return False
    except Exception as e:
        st.error(f"Allgemeiner Fehler beim Speichern in GSheet: {e}")
        return False

def clean_tweet_url(url):
    """Bereinigt eine Tweet-URL (entfernt Query-Parameter und /photo/...)."""
    if not isinstance(url, str): return url
    try:
        # Basis-URL extrahieren (vor dem '?')
        cleaned_url = url.split('?')[0]
        # /photo/1 oder /video/1 am Ende entfernen
        cleaned_url = re.sub(r"/(photo|video)/\d+$", "", cleaned_url)
        return cleaned_url
    except Exception:
        return url # Im Fehlerfall Original zurückgeben

@st.cache_data(ttl=3600) # Cache für 1 Stunde
def get_tweet_embed_html(tweet_url):
    """Holt den oEmbed HTML-Code für eine Tweet-URL."""
    if not isinstance(tweet_url, str): return None

    # Prüfen, ob es eine plausible Twitter/X URL ist
    try:
        parsed_url = urlparse(tweet_url)
        # Akzeptiere twitter.com, x.com (mit/ohne www) und prüfe auf /status/ Pfadteil
        if not (parsed_url.netloc in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"] and "/status/" in parsed_url.path):
            # print(f"DEBUG: URL '{tweet_url}' ist keine gültige Tweet Status URL.")
            return None
    except Exception as parse_err:
        # print(f"DEBUG: Fehler beim Parsen der URL '{tweet_url}': {parse_err}")
        return None # Ungültige URL

    cleaned_tweet_url = clean_tweet_url(tweet_url)
    # Verwende die offizielle oEmbed API von Twitter/X Publish
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"

    try:
        response = requests.get(api_url, timeout=10) # Timeout von 10 Sekunden
        response.raise_for_status() # Löst HTTPError für 4xx/5xx aus
        data = response.json()
        html_content = data.get("html")
        if not html_content:
             print(f"DEBUG: Kein 'html' Feld in oEmbed Antwort für {cleaned_tweet_url}. Antwort: {data}")
             return f"<p style='color:orange; font-family:sans-serif;'>Fehler: Vorschau-Daten unvollständig.</p><p><a href='{tweet_url}' target='_blank'>Originallink prüfen</a></p>"
        return html_content
    except requests.exceptions.Timeout:
        print(f"Timeout beim Laden der Vorschau für {cleaned_tweet_url}")
        return f"<p style='color:orange; font-family:sans-serif; border:1px solid orange; padding:10px; border-radius:5px;'>Timeout beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link manuell prüfen</a></p>"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        print(f"HTTP Fehler {status_code} für Embed {cleaned_tweet_url}: {e}")
        msg = f"Fehler ({status_code}) beim Laden der Vorschau."
        if status_code == 404: msg = "Tweet nicht gefunden (404) oder gelöscht."
        elif status_code == 403: msg = "Zugriff auf Tweet verweigert (403, privat?)."
        elif status_code == 400: msg = "Fehlerhafte Anfrage an Twitter API (400)."
        elif status_code == 429: msg = "Zu viele Anfragen an Twitter API (429). Bitte später erneut versuchen."
        elif status_code >= 500: msg = f"Serverfehler bei Twitter ({status_code})."
        return f"<p style='color:orange; font-family:sans-serif; border:1px solid orange; padding:10px; border-radius:5px;'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link manuell prüfen</a></p>"
    except requests.exceptions.RequestException as e:
        print(f"Netzwerk Fehler für Embed {cleaned_tweet_url}: {e}")
        return f"<p style='color:orange; font-family:sans-serif; border:1px solid orange; padding:10px; border-radius:5px;'>Netzwerkfehler beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link manuell prüfen</a></p>"
    except Exception as e:
        st.warning(f"Generischer Embed Fehler für {cleaned_tweet_url}: {e}")
        return f"<p style='color:orange; font-family:sans-serif;'>Unbekannter Fehler beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link manuell prüfen</a></p>"


# === Guideline Funktion ===
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
        *   Fokus auf den **Inhalt des Posts**, nicht auf Kommentare darunter (es sei denn, der Post bezieht sich explizit darauf).
        *   Sei konsistent in deiner Bewertung.
        *   Wenn ein Post **gar nicht** zu den Kategorien passt oder **nicht zugänglich/gelöscht** ist, wähle keine Kategorie und hinterlasse optional einen Kommentar. Klicke dann trotzdem auf "Speichern & Weiter".
        *   Dein Fortschritt wird pro URL gespeichert. Du kannst die Arbeit unterbrechen und später fortsetzen. Die Reihenfolge der Posts ist für dich **zufällig**, aber bei Wiederaufnahme beginnst du bei den noch nicht bearbeiteten.

        Danke für deine Hilfe! 🙏
    """)
    st.divider()


# === Streamlit App Hauptteil ===
st.title("📊 Dataset Labeler")

# --- Session State Initialisierung ---
# Grundlegende Zustände
if 'labeler_id' not in st.session_state: st.session_state.labeler_id = ""
if 'guidelines_shown' not in st.session_state: st.session_state.guidelines_shown = False # NEU: Guideline Status
if 'initialized' not in st.session_state: st.session_state.initialized = False # Status der Dateninitialisierung

# Zustände für Daten und Fortschritt (werden bei Initialisierung zurückgesetzt)
if 'input_file_name' not in st.session_state: st.session_state.input_file_name = DEFAULT_CSV_PATH
if 'urls_to_process' not in st.session_state: st.session_state.urls_to_process = [] # Randomisierte Liste für die Session
if 'total_items_in_session' not in st.session_state: st.session_state.total_items_in_session = 0 # Anzahl in urls_to_process
if 'processed_urls_from_sheet' not in st.session_state: st.session_state.processed_urls_from_sheet = set() # Beim Start geladen
if 'current_index_in_session' not in st.session_state: st.session_state.current_index_in_session = 0 # Index in urls_to_process

# Temporäre Speicher für die aktuelle Sitzung (Zurück-Button)
if 'session_results' not in st.session_state: st.session_state.session_results = {} # key: index_in_session, value: [categories]
if 'session_comments' not in st.session_state: st.session_state.session_comments = {} # key: index_in_session, value: comment

# Statistik-Zahlen
if 'original_total_items_from_file' not in st.session_state: st.session_state.original_total_items_from_file = 0 # Gesamtanzahl aus CSV
if 'already_processed_count_on_start' not in st.session_state: st.session_state.already_processed_count_on_start = 0 # Aus Sheet geladen


# --- Schritt 1: Labeler ID Eingabe ---
# Das Namensfeld wird deaktiviert, sobald die Initialisierung abgeschlossen ist.
labeler_id_input = st.text_input(
    "👤 Bitte gib deinen Vornamen ein:",
    value=st.session_state.labeler_id,
    key="labeler_id_widget",
    help="Wird zum Speichern des Fortschritts verwendet. Kann nach dem Start nicht mehr geändert werden.",
    disabled=st.session_state.get('initialized', False) # Deaktivieren, wenn initialisiert
)

# Aktualisiere Labeler ID nur, wenn das Feld nicht deaktiviert ist
if not st.session_state.get('initialized', False):
    st.session_state.labeler_id = labeler_id_input.strip()

# Stoppt, wenn keine Labeler ID eingegeben wurde
if not st.session_state.labeler_id:
    st.warning("Bitte eine Labeler ID (Vorname) eingeben, um fortzufahren.")
    st.stop()

# Zeige die festgelegte ID an, wenn initialisiert
if st.session_state.get('initialized', False):
    st.caption(f"Labeler ID '{st.session_state.labeler_id}' ist für diese Sitzung festgelegt.")

st.divider()


# --- Schritt 2: Guidelines anzeigen (wenn ID da, aber Guidelines noch nicht gezeigt) ---
if st.session_state.labeler_id and not st.session_state.get('guidelines_shown', False):
    show_guidelines()
    if st.button("✅ Verstanden, starte das Labeling!"):
        st.session_state.guidelines_shown = True
        st.session_state.initialized = False # Erzwingt Neuinitialisierung nach Guidelines
        st.rerun()
    st.stop() # Anhalten, bis der Button geklickt wird


# --- Schritt 3: Daten initialisieren (wenn ID da, Guidelines gezeigt, aber noch nicht initialisiert) ---
# Diese Logik läuft nur einmal pro Sitzung (oder wenn explizit zurückgesetzt)
needs_initialization = (st.session_state.labeler_id and
                        st.session_state.guidelines_shown and
                        not st.session_state.get('initialized', False))

if needs_initialization and worksheet:
    print(f"Starte Initialisierung für Labeler: {st.session_state.labeler_id}")
    # Reset session states related to data processing
    st.session_state.urls_to_process = []
    st.session_state.total_items_in_session = 0
    st.session_state.processed_urls_from_sheet = set()
    st.session_state.current_index_in_session = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.original_total_items_from_file = 0
    st.session_state.already_processed_count_on_start = 0
    st.session_state.input_file_name = DEFAULT_CSV_PATH # Sicherstellen, dass der Dateiname gesetzt ist

    with st.spinner(f"Lade URLs aus '{DEFAULT_CSV_PATH}' und prüfe deinen Fortschritt..."):
        # 1. Lade alle URLs aus der CSV
        all_input_urls_cleaned = load_urls_from_input_csv(DEFAULT_CSV_PATH, source_name=DEFAULT_CSV_PATH)
        st.session_state.original_total_items_from_file = len(all_input_urls_cleaned)

        if not all_input_urls_cleaned:
            st.error(f"Konnte keine gültigen URLs in '{DEFAULT_CSV_PATH}' finden oder Datei fehlt/ist leer.")
            st.session_state.initialized = False # Bleibt nicht initialisiert
            st.stop() # Anhalten, da keine Daten zum Labeln da sind

        # 2. Lade bereits verarbeitete URLs für diesen Labeler
        current_labeler_id = st.session_state.labeler_id
        # Wichtig: Cache leeren, um frische Daten zu bekommen
        get_processed_urls_by_labeler.clear()
        processed_by_this_labeler = get_processed_urls_by_labeler(current_labeler_id)
        st.session_state.processed_urls_from_sheet = processed_by_this_labeler
        st.session_state.already_processed_count_on_start = len(processed_by_this_labeler)

        # 3. Finde die noch zu bearbeitenden URLs
        remaining_urls = [url for url in all_input_urls_cleaned if url.strip() not in processed_by_this_labeler]

        # 4. Randomisierung der verbleibenden URLs mit Seed basierend auf Labeler ID
        if remaining_urls:
            # Erzeuge einen deterministischen Seed aus der Labeler ID
            # Verwende Hash, um sicherzustellen, dass es numerisch ist
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

        if remaining_now > 0:
            st.success(f"{total_original} URLs insgesamt gefunden. {processed_on_start} bereits von dir bearbeitet. {remaining_now} verbleibend in dieser Sitzung (zufällige Reihenfolge).")
        else:
             st.success(f"Super! Alle {total_original} URLs wurden bereits von dir bearbeitet.")

        # Kurze Pause und Rerun, um die UI sauber nach der Initialisierung zu laden
        time.sleep(0.5)
        st.rerun()

elif needs_initialization and not worksheet:
    st.error("Google Sheet Verbindung fehlgeschlagen. Initialisierung kann nicht abgeschlossen werden.")
    st.session_state.initialized = False
    st.stop()


# --- Schritt 4: Haupt-Labeling-Interface (wenn ID da, Guidelines gezeigt UND initialisiert) ---
if st.session_state.get('initialized', False):

    # Hole aktuelle Werte aus dem Session State
    labeler_id = st.session_state.labeler_id
    urls_for_session = st.session_state.urls_to_process
    total_in_session = st.session_state.total_items_in_session
    original_total = st.session_state.original_total_items_from_file
    processed_on_start = st.session_state.already_processed_count_on_start
    current_local_idx = st.session_state.current_index_in_session # Index innerhalb der urls_for_session Liste

    # --- Fall: Alle URLs dieser Sitzung bearbeitet ---
    if total_in_session <= 0 or current_local_idx >= total_in_session:
        st.success(f"🎉 Super, {labeler_id}! Alle {original_total} URLs wurden bearbeitet!")
        st.balloons()
        # Kein Link zum Sheet mehr anzeigen
        # if worksheet:
        #     try: st.link_button("Google Sheet öffnen", worksheet.spreadsheet.url)
        #     except Exception: pass

        # Button zum Neuladen (setzt Initialisierung zurück, behält Labeler ID)
        if st.button("App neu laden (Fortschritt wird erneut geprüft)"):
             st.session_state.initialized = False
             st.session_state.guidelines_shown = False # Zeige Guidelines wieder an
             # Caches leeren, um sicherzustellen, dass Fortschritt neu geladen wird
             st.cache_data.clear()
             st.cache_resource.clear() # Sheet Verbindung neu aufbauen
             get_processed_urls_by_labeler.clear()
             st.rerun()
        st.stop()

    # --- Fall: Es gibt noch URLs zu bearbeiten ---
    current_url = urls_for_session[current_local_idx]
    current_global_item_number = processed_on_start + current_local_idx + 1

    # --- Navigation Oben ---
    nav_cols_top = st.columns([1, 3, 1])
    # Zurück Button (nur wenn nicht das erste Item der *Session*)
    if current_local_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True, help="Zum vorherigen Eintrag dieser Sitzung (Änderungen werden zwischengespeichert)."):
            # Speichere aktuellen Stand zwischen, bevor zurückgegangen wird
            # (Wird durch Widgets geholt, wenn 'Zurück' geklickt wird, siehe unten im Code)
            st.session_state.current_index_in_session -= 1
            st.rerun()
    else:
        nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)

    # Fortschrittsanzeige (basiert auf Gesamtanzahl aus Datei)
    if original_total > 0:
        progress_percentage = (processed_on_start + current_local_idx) / original_total
        progress_text = f"{labeler_id}: Item {current_global_item_number} / {original_total} (noch {total_in_session - current_local_idx} in Sitzung)"
        nav_cols_top[1].progress(progress_percentage, text=progress_text)
    else:
        nav_cols_top[1].progress(0, text="Keine Items")

    # Überspringen Button (geht zum nächsten, speichert aktuellen als "[Übersprungen]")
    can_go_forward = (current_local_idx + 1) < total_in_session
    if nav_cols_top[2].button("Überspringen ➡️" if can_go_forward else "Letztes Item", key="skip_next_top", use_container_width=True, help="Markiert dieses Item als übersprungen und geht zum nächsten (falls vorhanden)."):
        if can_go_forward:
            # Markiere als übersprungen im Session State (wird nicht sofort gespeichert!)
            st.session_state.session_results[current_local_idx] = []
            st.session_state.session_comments[current_local_idx] = "[Übersprungen]"
            st.session_state.current_index_in_session += 1
            st.rerun()
        else:
            st.toast("Dies ist bereits das letzte Item.", icon="ℹ️")

    st.divider() # Trenner nach Navigation oben

    # --- Zweispaltiges Layout ---
    left_column, right_column = st.columns([2, 1]) # Links breiter

    # --- Linke Spalte: URL Anzeige & Einbettung ---
    with left_column:
        st.subheader("Post Vorschau / Link")
        base_tweet_url = clean_tweet_url(current_url)
        embed_html = get_tweet_embed_html(base_tweet_url) # Nutzt Cache
        display_url = current_url # Zeige immer die Original-URL an

        st.markdown(f"**URL:** [{display_url}]({display_url})") # Link immer anzeigen

        if embed_html:
            # Stelle sicher, dass Twitter Widgets geladen werden (falls im HTML enthalten)
            # components.html('<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>', height=0)
            components.html(embed_html, height=650, scrolling=True)
            # Wichtiger Hinweis: Das Twitter-Widget-Skript muss möglicherweise global geladen werden,
            # wenn `omit_script=true` in der oEmbed-URL verwendet wird. Hier ist es `false`,
            # also sollte das Skript im `embed_html` enthalten sein.
        else:
            # Fallback, wenn kein Embed verfügbar oder keine Twitter-URL
            if "twitter.com" in display_url or "x.com" in display_url:
                 # Zeigt die Fehlermeldung aus get_tweet_embed_html an, falls vorhanden
                 error_msg = get_tweet_embed_html(base_tweet_url) # Erneuter Aufruf holt aus Cache
                 if error_msg and error_msg.startswith("<p style='color:orange"):
                     st.markdown(error_msg, unsafe_allow_html=True)
                 else:
                     st.caption("Vorschau konnte nicht geladen werden oder Tweet ist nicht verfügbar.")
            else:
                st.caption("Vorschau ist nur für X/Twitter Posts verfügbar.")
            st.link_button("Link in neuem Tab öffnen", display_url)

    # --- Rechte Spalte: Kategorieauswahl & Kommentar ---
    with right_column:
        st.subheader("Kategorisierung")

        # Hole gespeicherte Auswahl für dieses Item aus der Session (falls vorhanden, z.B. durch Zurück-Button)
        saved_selection = st.session_state.session_results.get(current_local_idx, [])
        selected_categories_in_widgets = [] # Sammelt die aktuell ausgewählten Checkboxen

        # Kategorienauswahl (Checkboxen)
        st.markdown("**Wähle passende Subkategorie(n):**")
        for main_topic, main_data in CATEGORIES.items():
            main_color = CATEGORY_COLORS.get(main_topic, "black")
            main_desc = main_data["desc"]
            # Hauptkategorie mit Tooltip (Fragezeichen)
            st.markdown(f'''
                <h6 style="color:{main_color}; border-bottom: 1px solid {main_color}; margin-top: 10px; margin-bottom: 5px;">
                    {main_topic}
                    <span title="{main_desc}" style="cursor:help; font-weight:normal; color:grey;"> ❓</span>
                </h6>
            ''', unsafe_allow_html=True)

            # Subkategorien als Checkboxen mit Tooltips
            sub_categories = main_data["sub"]
            for sub_cat, sub_desc in sub_categories.items():
                # Eindeutiger Key für jedes Widget pro Item
                clean_sub_cat_key = re.sub(r'\W+', '', sub_cat)
                checkbox_key = f"cb_{current_local_idx}_{main_topic.replace(' ', '_')}_{clean_sub_cat_key}"
                # Standardwert ist der gespeicherte Wert aus der Session
                is_checked_default = sub_cat in saved_selection
                # Checkbox mit `help` Parameter für Tooltip
                is_checked_now = st.checkbox(
                    sub_cat,
                    value=is_checked_default,
                    key=checkbox_key,
                    help=sub_desc # Tooltip aus CATEGORIES Definition
                )
                if is_checked_now:
                    selected_categories_in_widgets.append(sub_cat)

        st.markdown("---") # Trenner

        # Anzeige der aktuell ausgewählten Tags (dynamisch)
        # Wichtig: Muss nach den Widgets gelesen werden, um deren aktuellen Status zu erfassen
        # selected_categories_in_widgets wurde oben in der Schleife gefüllt
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets))) # Sortieren und Deduplizieren
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**")
            display_tags = []
            for cat in selected_categories_in_widgets:
                 cat_color = SUBCATEGORY_COLORS.get(cat, SUBCATEGORY_COLORS.get("DEFAULT_COLOR", "grey"))
                 # Kleinere Tags
                 display_tags.append(f'<span style="display: inline-block; background-color: {cat_color}; color: white; border-radius: 4px; padding: 1px 6px; margin: 2px; font-size: 0.85em;">{cat}</span>')
            st.markdown(" ".join(display_tags), unsafe_allow_html=True)
        else:
             st.caption("_Keine Kategorien ausgewählt._")

        st.markdown("---") # Trenner vor Kommentar

        # Kommentarfeld
        # Hole gespeicherten Kommentar aus der Session
        default_comment = st.session_state.session_comments.get(current_local_idx, "")
        comment_key = f"comment_{current_local_idx}"
        comment_input = st.text_area(
            "Optionaler Kommentar:",
            value=default_comment,
            height=120, # Etwas kleiner
            key=comment_key,
            placeholder="Notizen, Unklarheiten, Link defekt?"
        )

    # --- Navigation Unten (unterhalb der beiden Spalten) ---
    st.divider() # Trenner vor Navigation unten
    nav_cols_bottom = st.columns(7) # Layout beibehalten

    # Zurück Button (unten)
    if current_local_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True, help="Zum vorherigen Eintrag dieser Sitzung."):
            # Speichere aktuellen Stand (Kategorien & Kommentar) im Session State, BEVOR der Index geändert wird
            st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
            st.session_state.session_comments[current_local_idx] = comment_input
            # Index ändern und neu laden
            st.session_state.current_index_in_session -= 1
            st.rerun()
    else:
        nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)

    # Speichern & Weiter Button (Hauptaktion)
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        # Lese aktuelle Werte aus Widgets
        current_selection = selected_categories_in_widgets # Bereits oben gesammelt
        current_comment = comment_input # Aus text_area gelesen

        # Validierung (optional, aber gut): Mindestens eine Kategorie oder ein Kommentar?
        # Hier: Erlaube leere Kategorie, wenn Kommentar da ist (z.B. für "Link defekt")
        # Oder erfordere immer eine Kategorie? -> Aktuell: Keine harte Anforderung, speichern geht immer.
        # if not current_selection and not current_comment:
        #     st.warning("Bitte wähle mindestens eine Kategorie oder hinterlasse einen Kommentar.")
        # else: # Fortfahren mit Speichern

        if not worksheet: st.error("Speichern fehlgeschlagen: Keine Google Sheet Verbindung.")
        elif not labeler_id: st.error("Speichern fehlgeschlagen: Labeler ID fehlt.")
        else:
            # Speichere im Google Sheet
            categories_str = "; ".join(current_selection) if current_selection else ""
            save_success = save_categorization_gsheet(worksheet, labeler_id, display_url, categories_str, current_comment)

            if save_success:
                st.toast("Gespeichert!", icon="✅")
                # Speichere auch im Session State (für Zurück-Button Konsistenz)
                st.session_state.session_results[current_local_idx] = current_selection
                st.session_state.session_comments[current_local_idx] = current_comment
                # Füge die URL zur Menge der verarbeiteten URLs hinzu (für den Fall, dass Caching aktiv ist)
                # Dies ist wichtig, damit bei einem Reload (z.B. durch Browser Refresh) der Fortschritt korrekt ist
                st.session_state.processed_urls_from_sheet.add(display_url.strip())
                 # Erhöhe Zähler für "bereits gespeichert" für die Sidebar-Anzeige in dieser Session
                st.session_state.already_processed_count_on_start += 1 # Zählt hoch was gespeichert wurde
                # Gehe zum nächsten Index
                st.session_state.current_index_in_session += 1
                st.rerun()
            else:
                st.error("Speichern in Google Sheet fehlgeschlagen. Bitte prüfe die Fehlermeldung oben oder versuche es erneut.")
                # Nicht zum nächsten Item gehen bei Speicherfehler

# --- Fallback-Anzeige, wenn nicht initialisiert, aber ID und Guidelines vorhanden ---
elif st.session_state.labeler_id and st.session_state.guidelines_shown and not st.session_state.get('initialized', False):
    # Zeigt nur Nachrichten an, wenn die Initialisierung fehlschlagen sollte (z.B. Sheet nicht erreichbar)
    # Die eigentlichen Fehlermeldungen sollten von den Initialisierungsfunktionen kommen.
    st.warning("Warte auf Initialisierung oder prüfe Fehlermeldungen...")


# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    # Kein direkter Link zum Sheet mehr
    # try:
    #     st.sidebar.page_link(worksheet.spreadsheet.url, label="Sheet öffnen ↗️")
    # except Exception: pass
else: st.sidebar.error("Keine GSheet Verbindung.")

st.sidebar.markdown(f"**Labeler/in:** {st.session_state.labeler_id or '(fehlt)'}")
st.sidebar.markdown(f"**Input-Datei:** {st.session_state.get('input_file_name', DEFAULT_CSV_PATH)}")
# Zeigt das erwartete Header-Format an
# st.sidebar.markdown(f"**DB Format:** {', '.join(HEADER)}")

if st.session_state.get('initialized', False):
    # Verwende die Statistik-Werte aus dem Session State
    original_total = st.session_state.original_total_items_from_file
    # Berechne "gespeichert" als Summe aus beim Start geladenen und in dieser Session via Button hinzugefügten
    # Da wir `already_processed_count_on_start` bei jedem Speichern erhöhen:
    processed_count = st.session_state.already_processed_count_on_start
    # Verbleibende in der Session-Liste
    remaining_in_session = st.session_state.total_items_in_session - st.session_state.current_index_in_session
    # Aktuelle globale Nummer
    current_global_item_number = processed_count + st.session_state.current_index_in_session + 1
    # Korrektur, wenn Session leer ist oder fertig
    if st.session_state.total_items_in_session == 0:
        current_global_item_number = original_total
        remaining_in_session = 0
    elif st.session_state.current_index_in_session >= st.session_state.total_items_in_session:
         current_global_item_number = original_total # Am Ende zeigen wir Total/Total an
         remaining_in_session = 0


    st.sidebar.metric("Gesamt aus Datei", original_total)
    st.sidebar.metric("Aktuell / Gesamt", f"{min(current_global_item_number, original_total)} / {original_total}") # Sicherstellen, dass nicht > Total angezeigt wird
    st.sidebar.metric("Von dir gespeichert", processed_count)
    st.sidebar.metric("Noch offen (in Session)", remaining_in_session)
else:
    # Zeige Platzhalter, wenn nicht initialisiert
    st.sidebar.metric("Gesamt aus Datei", "-")
    st.sidebar.metric("Aktuell / Gesamt", "-")
    st.sidebar.metric("Von dir gespeichert", "-")
    st.sidebar.metric("Noch offen (in Session)", "-")

st.sidebar.caption(f"GSheet Header: {'OK' if not header_written_flag else 'Geschrieben/Aktualisiert'}")
st.sidebar.caption("Tweet-Vorschauen gecached.")
st.sidebar.caption("Fortschritt wird beim Start abgerufen.")
st.sidebar.caption(f"Randomisierung: Aktiv (Seed: {st.session_state.labeler_id})")