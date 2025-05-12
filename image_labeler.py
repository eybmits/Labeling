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
            st.error("Google Sheets Secrets ('google_sheets.credentials_dict', 'google_sheets.sheet_name') fehlen oder sind unvollständig.")
            st.stop(); return None, False, None
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1
        header_written = False
        all_vals = worksheet.get_all_values()
        if not all_vals or all_vals[0] != HEADER:
            st.sidebar.warning(f"Header in '{sheet_name}' wird korrigiert...")
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
                st.sidebar.success(f"Header in '{sheet_name}' aktualisiert.")
            except Exception as he: st.sidebar.error(f"Konnte Header nicht schreiben: {he}")
        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt."); st.stop(); return None, False, None
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop(); return None, False, None
    except gspread.exceptions.APIError as e: st.error(f"Google API Fehler (Verbindung): {e}."); st.stop(); return None, False, None
    except Exception as e: st.error(f"Fehler bei GSheets Verbindung: {e}"); st.stop(); return None, False, None

worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===

# Detailliertes Codebook für Kategorien und Tooltips
# Die 'desc' Felder bei den Hauptkategorien werden nicht mehr für Tooltips benötigt, können aber bleiben.
CATEGORIES = {
    "Health": {
        "title": "1. Health",
        "desc": "Posts related to health, well-being, and the healthcare system.",
        "sub": {
            "Lifestyle": { "title": "1.1 Lifestyle", "definition": "...", "include": "...", "exclude": "..." }, # Inhalte gekürzt für Lesbarkeit
            "Mental Health": { "title": "1.2 Mental Health", "definition": "...", "include": "...", "exclude": "..." },
            "Physical Health": { "title": "1.3 Physical Health", "definition": "...", "include": "...", "exclude": "..." },
            "Healthcare System": { "title": "1.4 Healthcare System", "definition": "...", "include": "...", "exclude": "..." }
        }
    },
    "Social": {
         "title": "2. Social",
         "desc": "Posts related to societal issues like education, family, relationships, and employment.",
         "sub": {
            "Education": { "title": "2.1 Education", "definition": "...", "include": "...", "exclude": "..." },
            "Family/Relationships": { "title": "2.2 Family & Relationships", "definition": "...", "include": "...", "exclude": "..." },
            "Employment": { "title": "2.3 Employment", "definition": "...", "include": "...", "exclude": "..." }
        }
    },
    "Environment": {
        "title": "3. Environment",
        "desc": "Posts related to the environment, climate, energy sector, and disasters.",
        "sub": {
            "Environmental Policies": { "title": "3.1 Environmental Policies", "definition": "...", "include": "...", "exclude": "..." },
            "Energy Sector": { "title": "3.2 Energy Sector", "definition": "...", "include": "...", "exclude": "..." },
            "Natural/Man-made Disasters": { "title": "3.3 Natural/Man-made Disasters", "definition": "...", "include": "...", "exclude": "..." }
        }
    }
}

# Fülle die gekürzten Definitionen wieder auf (aus der vorherigen Version kopiert)
# Health
CATEGORIES["Health"]["sub"]["Lifestyle"]["definition"] = "Includes content providing information on maintaining a healthy lifestyle. This includes content related to nutrition, physical activity, wellness routines, and preventive behaviors aimed at general health improvement."
CATEGORIES["Health"]["sub"]["Lifestyle"]["include"] = "workout challenges, meal prep ideas, healthy eating advice."
CATEGORIES["Health"]["sub"]["Lifestyle"]["exclude"] = "content primarily focused on diagnosed medical conditions or mental health disorders."
CATEGORIES["Health"]["sub"]["Mental Health"]["definition"] = "Includes content related to mental health issues and psychological well-being in general. Covers content about awareness, coping strategies, therapy, and prevention."
CATEGORIES["Health"]["sub"]["Mental Health"]["include"] = "stress-reducing techniques, therapy experiences, posts explaining burnout."
CATEGORIES["Health"]["sub"]["Mental Health"]["exclude"] = "content where mental health is incidental or implied but not explicitly discussed."
CATEGORIES["Health"]["sub"]["Physical Health"]["definition"] = "Covers content about physical illnesses, medical conditions, treatment, and disease prevention."
CATEGORIES["Health"]["sub"]["Physical Health"]["include"] = "COVID-19 updates, flu vaccine info, cancer awareness."
CATEGORIES["Health"]["sub"]["Physical Health"]["exclude"] = "health policy, issues related to mental health."
CATEGORIES["Health"]["sub"]["Healthcare System"]["definition"] = "Refers to content focused on the structure, accessibility, funding, or reform of healthcare services. Includes criticisms, suggestions and policy discussions."
CATEGORIES["Health"]["sub"]["Healthcare System"]["include"] = "waiting time issues, insurance access, critiques of public/private healthcare."
CATEGORIES["Health"]["sub"]["Healthcare System"]["exclude"] = "employment-focused healthcare issues (see 2.3)."
# Social
CATEGORIES["Social"]["sub"]["Education"]["definition"] = "Includes content related to educational systems, school curricula, education policy, higher and additional education, teachers, schoolkids, university students as well as buildings like schools and universities. Covers topics such as school reform, special education, and access to education."
CATEGORIES["Social"]["sub"]["Education"]["include"] = "photos of the learning process, debates on university tuition, special needs programs."
CATEGORIES["Social"]["sub"]["Education"]["exclude"] = "posts only about family or children without an educational component."
CATEGORIES["Social"]["sub"]["Family/Relationships"]["definition"] = "Covers posts discussing romantic, familial, and parenting relationships, including expressions of love, support, or conflict. Focus is on interpersonal dynamics."
CATEGORIES["Social"]["sub"]["Family/Relationships"]["include"] = "anniversary posts, family arguments, dating experiences."
CATEGORIES["Social"]["sub"]["Family/Relationships"]["exclude"] = "content focused on mental health issues in relationships."
CATEGORIES["Social"]["sub"]["Employment"]["definition"] = "Refers to content related to labor markets, job conditions, pensions, and workplace policies. Includes discussions about employment policy, job loss and depictions of work processes and working equipment."
CATEGORIES["Social"]["sub"]["Employment"]["include"] = "posts about minimum wage, hiring processes, job training programs."
CATEGORIES["Social"]["sub"]["Employment"]["exclude"] = "healthcare workforce-specific content, issues overlapping with lifestyle/mental health (e.g. work-life balance)."
# Environment
CATEGORIES["Environment"]["sub"]["Environmental Policies"]["definition"] = "Includes content about environmental regulation, governmental decisions, and political discourse related to environmental protection. Can reference specific political actors or parties associated with environment issues."
CATEGORIES["Environment"]["sub"]["Environmental Policies"]["include"] = "climate legislation, carbon tax debates, political campaigns on green policy."
CATEGORIES["Environment"]["sub"]["Environmental Policies"]["exclude"] = "posts about energy or disasters unless policy is the primary focus."
CATEGORIES["Environment"]["sub"]["Energy Sector"]["definition"] = "Covers content on natural and renewable energy (e.g., solar, wind, fossil fuels), including innovation, infrastructure, and research, without explicit political discussion."
CATEGORIES["Environment"]["sub"]["Energy Sector"]["include"] = "solar panel technology, energy storage solutions."
CATEGORIES["Environment"]["sub"]["Energy Sector"]["exclude"] = "political critique, disasters."
CATEGORIES["Environment"]["sub"]["Natural/Man-made Disasters"]["definition"] = "Includes content about environmental hazards and disaster events, such as floods, wildfires, pollution, or industrial accidents. May reference causes or consequences of climate change."
CATEGORIES["Environment"]["sub"]["Natural/Man-made Disasters"]["include"] = "coverage of wildfires, oil spills, climate-induced droughts."
CATEGORIES["Environment"]["sub"]["Natural/Man-made Disasters"]["exclude"] = "content not referencing any kind of disaster."


# Flache Liste aller internen Subkategorien-Keys
ALL_CATEGORIES_KEYS = [sub_cat_key for main_data in CATEGORIES.values() for sub_cat_key in main_data["sub"]]

# Farben
CATEGORY_COLORS = { "Health": "dodgerblue", "Social": "mediumseagreen", "Environment": "darkorange" }
SUBCATEGORY_COLORS = {
    "Lifestyle": "skyblue", "Mental Health": "lightcoral", "Physical Health": "mediumaquamarine", "Healthcare System": "steelblue",
    "Education": "sandybrown", "Family/Relationships": "lightpink", "Employment": "khaki",
    "Environmental Policies": "mediumseagreen", "Energy Sector": "gold", "Natural/Man-made Disasters": "slategray",
    "DEFAULT_COLOR": "grey"
}


# === Hilfsfunktionen ===
# (Datenbank, URL-Handling, etc. bleiben gleich)
@st.cache_data(ttl=300)
def get_processed_urls_by_labeler(target_labeler_id):
    processed_urls = set(); worksheet_obj, _, sheet_name_local = connect_gsheet()
    if not worksheet_obj: st.warning(f"Keine GSheet-Verbindung f. Fortschritt '{target_labeler_id}'."); return processed_urls
    if not target_labeler_id: st.warning("Leere Labeler ID f. Fortschritt."); return processed_urls
    print(f"DEBUG: Rufe verarbeitete URLs f. '{target_labeler_id}' aus '{sheet_name_local}' ab...")
    try:
        all_data = worksheet_obj.get_all_values()
        if not all_data or len(all_data) < 1: return processed_urls
        header_row = all_data[0]
        try: labeler_col_index, url_col_index = header_row.index(COL_LBL), header_row.index(COL_URL)
        except ValueError as e: st.error(f"Fehler: Spalte '{e}' fehlt im Header '{sheet_name_local}': {header_row}."); return processed_urls
        for row in all_data[1:]:
            if len(row) > max(labeler_col_index, url_col_index) and row[labeler_col_index] and row[url_col_index]:
                if row[labeler_col_index].strip() == target_labeler_id: processed_urls.add(row[url_col_index].strip())
        print(f"DEBUG: {len(processed_urls)} verarbeitete URLs f. '{target_labeler_id}' gefunden.")
    except gspread.exceptions.APIError as e: st.warning(f"GSheet API Fehler (Fortschritt): {e}")
    except Exception as e: st.warning(f"Fehler (Fortschritt): {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(file_path, source_name="Standarddatei"):
    urls = [];
    if not file_path or not isinstance(file_path, str): st.error("Kein gültiger Pfad."); return urls
    if not os.path.exists(file_path): st.error(f"Datei '{file_path}' nicht gefunden."); return urls
    try:
        try: df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
        except UnicodeDecodeError: st.warning(f"UTF-8 Fehler bei '{source_name}', versuche latin-1..."); df = pd.read_csv(file_path, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)
        if df.empty: st.warning(f"Input '{source_name}' leer."); return urls
        url_series_raw = df.iloc[:, 0]; url_series_str = url_series_raw.astype(str)
        url_series_nonan = url_series_str.replace('nan', pd.NA).dropna(); url_series_stripped = url_series_nonan.str.strip()
        url_series_noempty = url_series_stripped[url_series_stripped != '']
        url_pattern = r'^https?://\S+$'; url_series_filtered = url_series_noempty[url_series_noempty.str.match(url_pattern)]
        urls = url_series_filtered.unique().tolist(); print(f"DEBUG: {len(urls)} unique URLs geladen.")
    except Exception as e: st.error(f"Fehler Lesen/Verarbeiten '{source_name}': {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_keys_str, comment):
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt beim Speichern."); return False
    if not url: st.error("URL fehlt beim Speichern."); return False
    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        data_row = [now_ts, labeler_id, url, categories_keys_str, comment]
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED'); return True
    except gspread.exceptions.APIError as e: st.error(f"Google API Fehler beim Speichern: {e}."); return False
    except Exception as e: st.error(f"Fehler beim Speichern in GSheet: {e}"); return False

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
    except Exception: return None
    cleaned_tweet_url = clean_tweet_url(tweet_url)
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try:
        response = requests.get(api_url, timeout=10); response.raise_for_status(); data = response.json()
        html_content = data.get("html")
        if not html_content: return f"<p style='color:orange;'>Fehler: Vorschau unvollständig.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
        return html_content
    except requests.exceptions.Timeout: return f"<p style='color:orange; border:1px solid orange; padding:10px;'>Timeout Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code; msg = f"Fehler ({status_code}) Vorschau."
        if status_code == 404: msg = "Tweet nicht gefunden (404)."
        elif status_code == 403: msg = "Zugriff verweigert (403)."
        elif status_code >= 500: msg = f"Serverfehler Twitter ({status_code})."
        return f"<p style='color:orange; border:1px solid orange; padding:10px;'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.RequestException: return f"<p style='color:orange; border:1px solid orange; padding:10px;'>Netzwerkfehler Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except Exception as e: st.warning(f"Embed Fehler {cleaned_tweet_url}: {e}"); return f"<p style='color:orange;'>Unbekannter Fehler Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"


# === ANGEPASST: Kombinierte Intro-Seite ===
def show_intro_page():
    """Zeigt die kombinierte Anleitung und Codebook-Einführung."""
    st.header("📊 Anleitung & Codebook Einführung")
    st.markdown("""
        Willkommen beim Labeling-Tool! Deine Aufgabe ist es, Social Media Posts (aktuell X/Twitter) einer oder mehreren vordefinierten Kategorien zuzuordnen.
    """)
    st.subheader("Ziel")
    st.markdown("""
        Dieses Kodierhandbuch dient der Klassifikation des thematischen Schwerpunkts von Social-Media-Beiträgen. Beiträge können – je nach inhaltlicher Aussage – einer oder mehreren Kategorien zugeordnet werden, unabhängig vom Format (Text, Bild, Hashtag, Emoji usw.). Jede Kategorie enthält eine Definition sowie Anmerkungen zum Geltungsbereich. Die Kodierung bezieht sich jeweils auf den gesamten Beitrag (Text + Bild(er)).

        *Das Kodierhandbuch basiert auf dem Material des Comparative Agendas Project sowie auf einer induktiven Analyse manuell gesammelter Daten von X.*
    """)
    st.subheader("Ablauf")
    # --- HIER IST DIE ÄNDERUNG ---
    st.markdown("""
        1.  **Post ansehen:** Links wird eine Vorschau des Posts angezeigt (falls verfügbar) oder der direkte Link. Öffne den Link bei Bedarf in einem neuen Tab.
        2.  **Kategorien wählen:** Rechts findest du die Hauptkategorien (Health, Social, Environment). Wähle **mindestens eine** passende Subkategorie aus, die den **Hauptinhalt** des Posts am besten beschreibt. Mehrfachauswahl ist möglich, wenn der Post klar mehrere Themen abdeckt.
        3.  **Tooltip nutzen (**Wichtig!**): Fahre mit der Maus über die einzelnen Subkategorien (Checkboxen). Dort findest du die **ausführliche und wichtige Beschreibung** der jeweiligen Kategorie (Definition, Include/Exclude Beispiele) für die korrekte Zuordnung.
        4.  **(Optional) Kommentar:** Füge bei Bedarf einen Kommentar hinzu (z.B. bei Unklarheiten, Mehrdeutigkeiten, technischen Problemen mit dem Post).
        5.  **Speichern & Weiter:** Klicke auf "Speichern & Weiter", um deine Auswahl zu speichern und zum nächsten Post zu gelangen.
        6.  **Navigation:** Mit "Zurück" kannst du vorherige (in dieser Sitzung bearbeitete) Posts korrigieren. Mit "Überspringen" (oben rechts) kannst du einen Post markieren, ohne ihn zu speichern (wird als "[Übersprungen]" im Kommentarfeld vermerkt, wenn du dann zum nächsten gehst).
    """)
    # --- ENDE DER ÄNDERUNG ---
    st.subheader("Wichtige Hinweise")
    st.markdown("""
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
# Nur noch eine Bestätigung nötig
if 'intro_confirmed' not in st.session_state: st.session_state.intro_confirmed = False # Wird True nach Klick auf Intro-Button
if 'initialized' not in st.session_state: st.session_state.initialized = False
# Restliche States bleiben gleich
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
labeler_id_input = st.text_input(
    "👤 Bitte gib deinen Vornamen ein:",
    value=st.session_state.labeler_id,
    key="labeler_id_widget",
    help="Dein Name wird zum Speichern des Fortschritts verwendet. Er wird gesperrt, nachdem du die Anleitung bestätigt hast.",
    # Name sperren, wenn Intro bestätigt wurde
    disabled=st.session_state.intro_confirmed
)
if not st.session_state.intro_confirmed:
    st.session_state.labeler_id = labeler_id_input.strip()
if st.session_state.intro_confirmed:
    st.caption(f"Labeler ID '{st.session_state.labeler_id}' ist für diese Sitzung festgelegt.")
if not st.session_state.labeler_id and not st.session_state.intro_confirmed:
    st.warning("Bitte eine Labeler ID (Vorname) eingeben, um fortzufahren.")
    st.stop()
st.divider()


# --- Schritt 2: Intro-Seite anzeigen (wenn ID da, aber Intro noch nicht bestätigt) ---
if st.session_state.labeler_id and not st.session_state.intro_confirmed:
    show_intro_page() # Zeige die kombinierte Seite
    if st.button("✅ Verstanden, starte das Labeling!"):
        st.session_state.intro_confirmed = True # Bestätigung setzt diesen State
        st.session_state.initialized = False # Trigger für Dateninitialisierung
        st.success(f"Danke, {st.session_state.labeler_id}! Lade jetzt deine Daten...")
        time.sleep(1)
        st.rerun() # Neu laden, um Initialisierung zu starten
    st.stop()


# --- Schritt 3: Daten initialisieren (wenn Intro bestätigt, aber noch nicht initialisiert) ---
needs_initialization = (st.session_state.intro_confirmed and
                        not st.session_state.get('initialized', False))

if needs_initialization and worksheet:
    print(f"Starte Initialisierung für bestätigten Labeler: {st.session_state.labeler_id}")
    # Reset States
    st.session_state.urls_to_process = []
    st.session_state.total_items_in_session = 0
    st.session_state.processed_urls_from_sheet = set()
    st.session_state.current_index_in_session = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.original_total_items_from_file = 0
    st.session_state.already_processed_count_on_start = 0
    st.session_state.input_file_name = DEFAULT_CSV_PATH

    with st.spinner(f"Lade URLs & prüfe Fortschritt für '{st.session_state.labeler_id}'..."):
        all_input_urls_cleaned = load_urls_from_input_csv(DEFAULT_CSV_PATH, source_name=DEFAULT_CSV_PATH)
        st.session_state.original_total_items_from_file = len(all_input_urls_cleaned)
        if not all_input_urls_cleaned: st.error(f"Keine gültigen URLs in '{DEFAULT_CSV_PATH}'."); st.session_state.initialized = False; st.stop()

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
            print(f"DEBUG: {len(remaining_urls)} URLs für '{current_labeler_id}' gemischt.")

        st.session_state.urls_to_process = remaining_urls
        st.session_state.total_items_in_session = len(remaining_urls)
        st.session_state.current_index_in_session = 0
        st.session_state.initialized = True # Initialisierung abgeschlossen

        time.sleep(0.5)
        st.rerun() # UI neu laden für Labeling-Interface

elif needs_initialization and not worksheet:
    st.error("Google Sheet Verbindung fehlgeschlagen. Initialisierung kann nicht abgeschlossen werden.")
    st.session_state.initialized = False; st.stop()


# --- Schritt 4: Haupt-Labeling-Interface (wenn Intro bestätigt UND initialisiert) ---
if st.session_state.intro_confirmed and st.session_state.get('initialized', False):

    # Aktuelle Werte holen
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
        if st.button("App neu laden (startet von vorn)"):
             st.session_state.labeler_id = "" # Reset für kompletten Neustart
             st.session_state.intro_confirmed = False # Zurücksetzen
             st.session_state.initialized = False
             st.cache_data.clear(); st.cache_resource.clear(); get_processed_urls_by_labeler.clear()
             st.rerun()
        st.stop()

    # --- Fall: Es gibt noch URLs zu bearbeiten ---
    current_url = urls_for_session[current_local_idx]
    processed_count_total = processed_on_start + current_local_idx
    current_global_item_number = processed_count_total + 1

    # --- Navigation Oben ---
    nav_cols_top = st.columns([1, 3, 1])
    # Zurück
    if current_local_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True, help="Zum vorherigen Eintrag dieser Sitzung."):
            st.session_state.current_index_in_session -= 1; st.rerun()
    else: nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    # Fortschritt
    if original_total > 0:
        progress_percentage = processed_count_total / original_total
        progress_text = f"{labeler_id}: Item {current_global_item_number} / {original_total} (noch {total_in_session - current_local_idx} in Sitzung)"
        nav_cols_top[1].progress(progress_percentage, text=progress_text)
    else: nav_cols_top[1].progress(0, text="Keine Items")
    # Überspringen
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
        saved_selection_keys = st.session_state.session_results.get(current_local_idx, [])
        selected_category_keys_in_widgets = []

        st.markdown("**Wähle passende Subkategorie(n):**")
        for main_cat_key, main_data in CATEGORIES.items():
            main_color = CATEGORY_COLORS.get(main_cat_key, "black")
            main_title = main_data["title"]
            # Hauptkategorie OHNE Fragezeichen/Tooltip
            st.markdown(f'''<h6 style="color:{main_color}; border-bottom: 1px solid {main_color}; margin-top: 10px; margin-bottom: 5px;">
                           {main_title}
                         </h6>''', unsafe_allow_html=True)

            # Subkategorien als Checkboxen
            for sub_cat_key, sub_data in main_data["sub"].items():
                sub_cat_title = sub_data["title"]
                widget_key = f"cb_{current_local_idx}_{main_cat_key}_{sub_cat_key}"
                is_checked_default = sub_cat_key in saved_selection_keys
                # Tooltip Text formatieren
                tooltip_text = f"""Definition: {sub_data['definition']}\nInclude: {sub_data['include']}\nExclude: {sub_data['exclude']}"""
                # Checkbox mit detailliertem Tooltip
                is_checked_now = st.checkbox(
                    sub_cat_title,
                    value=is_checked_default,
                    key=widget_key,
                    help=tooltip_text
                )
                if is_checked_now:
                    selected_category_keys_in_widgets.append(sub_cat_key)

        st.markdown("---")

        # Anzeige der ausgewählten Tags
        selected_category_keys_in_widgets = sorted(list(set(selected_category_keys_in_widgets)))
        if selected_category_keys_in_widgets:
            st.write("**Ausgewählt:**")
            display_tags = []
            for key in selected_category_keys_in_widgets:
                display_title = key # Fallback
                cat_color = SUBCATEGORY_COLORS.get(key, SUBCATEGORY_COLORS["DEFAULT_COLOR"])
                for main_data in CATEGORIES.values():
                    if key in main_data["sub"]: display_title = main_data["sub"][key]["title"]; break
                display_tags.append(f'<span style="display: inline-block; background-color: {cat_color}; color: white; border-radius: 4px; padding: 1px 6px; margin: 2px; font-size: 0.85em;">{display_title}</span>')
            st.markdown(" ".join(display_tags), unsafe_allow_html=True)
        else: st.caption("_Keine Kategorien ausgewählt._")

        st.markdown("---")

        # Kommentarfeld
        default_comment = st.session_state.session_comments.get(current_local_idx, "")
        comment_key = f"comment_{current_local_idx}"
        comment_input = st.text_area("Optionaler Kommentar:", value=default_comment, height=120, key=comment_key, placeholder="Notizen, Link defekt?")

    # --- Navigation Unten ---
    st.divider()
    nav_cols_bottom = st.columns(7)
    # Zurück
    if current_local_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True):
            st.session_state.session_results[current_local_idx] = selected_category_keys_in_widgets
            st.session_state.session_comments[current_local_idx] = comment_input
            st.session_state.current_index_in_session -= 1; st.rerun()
    else: nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)

    # Speichern & Weiter
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_selection_keys = selected_category_keys_in_widgets
        current_comment = comment_input
        if not worksheet: st.error("Speichern fehlgeschlagen: Keine GSheet Verbindung.")
        elif not labeler_id: st.error("Speichern fehlgeschlagen: Labeler ID fehlt.")
        else:
            categories_keys_str = "; ".join(current_selection_keys) if current_selection_keys else ""
            save_success = save_categorization_gsheet(worksheet, labeler_id, display_url, categories_keys_str, current_comment)
            if save_success:
                st.toast("Gespeichert!", icon="✅")
                st.session_state.session_results[current_local_idx] = current_selection_keys
                st.session_state.session_comments[current_local_idx] = current_comment
                st.session_state.processed_urls_from_sheet.add(display_url.strip())
                st.session_state.current_index_in_session += 1
                st.rerun()
            else: st.error("Speichern in Google Sheet fehlgeschlagen.")


# --- Fallback-Anzeige, wenn Initialisierung noch aussteht ---
elif st.session_state.intro_confirmed and not st.session_state.get('initialized', False):
    st.warning("Warte auf Initialisierung oder prüfe Fehlermeldungen...")


# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet: st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
else: st.sidebar.error("Keine GSheet Verbindung.")

st.sidebar.markdown(f"**Labeler/in:** {st.session_state.labeler_id or '(Bitte eingeben)'}")
st.sidebar.markdown(f"**Input-Datei:** {st.session_state.get('input_file_name', DEFAULT_CSV_PATH)}")

if st.session_state.get('initialized', False):
    original_total = st.session_state.original_total_items_from_file
    processed_on_start = st.session_state.already_processed_count_on_start
    processed_count_total = processed_on_start + st.session_state.current_index_in_session
    remaining_in_session = st.session_state.total_items_in_session - st.session_state.current_index_in_session
    current_global_item_number = processed_count_total + 1

    if st.session_state.total_items_in_session == 0:
         current_global_item_number = original_total; remaining_in_session = 0; processed_count_total = original_total
    elif st.session_state.current_index_in_session >= st.session_state.total_items_in_session:
         current_global_item_number = original_total; remaining_in_session = 0; processed_count_total = original_total

    st.sidebar.metric("Gesamt aus Datei", original_total)
    st.sidebar.metric("Aktuell / Gesamt", f"{min(current_global_item_number, original_total)} / {original_total}")
    st.sidebar.metric("Von dir gespeichert", processed_count_total)
    st.sidebar.metric("Noch offen (in Session)", remaining_in_session)
else:
    st.sidebar.metric("Gesamt aus Datei", "-"); st.sidebar.metric("Aktuell / Gesamt", "-")
    st.sidebar.metric("Von dir gespeichert", "-"); st.sidebar.metric("Noch offen (in Session)", "-")

st.sidebar.caption(f"GSheet Header: {'OK' if not header_written_flag else 'Geschrieben/Aktualisiert'}")
st.sidebar.caption("Tweet-Vorschauen gecached.")
st.sidebar.caption("Fortschritt wird beim Start abgerufen.")
# Randomisierung aktiv sobald Intro bestätigt
if st.session_state.get('intro_confirmed', False):
    st.sidebar.caption(f"Randomisierung: Aktiv (Seed: {st.session_state.labeler_id})")