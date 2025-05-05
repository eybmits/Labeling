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
        # Strenge Prüfung: Header muss exakt übereinstimmen
        if not all_vals or all_vals[0] != HEADER :
             st.sidebar.warning(f"Header in '{sheet_name}' stimmt nicht mit {HEADER} überein oder fehlt. Schreibe korrekten Header...")
             # worksheet.clear() # Optional: Alten Inhalt löschen
             # Versuche intelligent zu aktualisieren oder neu zu schreiben
             try:
                 # Prüfe, ob die erste Zeile leer oder ganz anders ist
                 if not all_vals or len(all_vals[0]) != len(HEADER):
                     worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                 else: # Wenn die Anzahl der Spalten übereinstimmt, nur überschreiben
                     # Erstelle eine Liste von Cell-Objekten für die erste Reihe
                     cell_list = [gspread.Cell(1, i+1, value) for i, value in enumerate(HEADER)]
                     worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')

                 # Entferne ggf. leere Standardzeilen danach
                 all_vals_after = worksheet.get_all_values() # Neu laden
                 if len(all_vals_after) > 1 and all(v == '' for v in worksheet.row_values(2)):
                     worksheet.delete_rows(2)

                 header_written = True
                 st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")
             except Exception as he:
                 st.sidebar.error(f"Konnte Header nicht schreiben: {he}")


        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt. Prüfe secrets.toml/Cloud Secrets."); st.stop(); return None, False, None
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop(); return None, False, None
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

# === NEU: Farben für Hauptkategorien (CSS-kompatibel) ===
CATEGORY_COLORS = {
    "Personal Well-being": "dodgerblue",
    "Societal Systems": "mediumseagreen",
    "Environment & Events": "darkorange",
    "Other": "grey"
}

# === Hilfsfunktionen ===
@st.cache_data
def load_urls_from_input_csv(file_input_object, source_name="hochgeladene Datei"):
    """Lädt alle URLs aus einem Datei-Objekt (Upload oder geöffnet)."""
    urls = []
    if not file_input_object: st.error("Kein Datei-Objekt."); return urls
    try:
        if hasattr(file_input_object, 'seek'): file_input_object.seek(0)
        df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=True)
        url_series = df.iloc[:, 0].dropna().astype(str)
        # Striktere URL-Validierung (optional, kann angepasst werden)
        urls = url_series[url_series.str.match(r'^https?://\S+$')].unique().tolist()
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer/enthält keine URLs.")
    except IndexError: st.warning(f"Input '{source_name}' hat keine Spalten (Format?).")
    except Exception as e: st.error(f"Fehler beim Lesen von '{source_name}': {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Hängt eine neue Zeile mit Labeler-ID und Zeitstempel an."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt. Speichern nicht möglich."); return False

    try:
        now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')
        data_row = [now_ts, labeler_id, url, categories_str, comment]
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e: st.error(f"Sheets API Fehler (Speichern): {e}"); return False
    except Exception as e: st.error(f"Unerw. Fehler (Speichern): {e}"); return False

def clean_tweet_url(url):
    """Bereinigt Twitter/X URLs von Tracking-Parametern und Media-Anhängen."""
    # Entferne Query-Parameter (?s=..., ?t=...)
    cleaned_url = url.split('?')[0]
    # Entferne /photo/1, /video/1 etc. am Ende
    cleaned_url = re.sub(r"/(photo|video)/\d+$", "", cleaned_url)
    return cleaned_url

@st.cache_data(ttl=3600) # Cache Embed HTML für 1 Stunde
def get_tweet_embed_html(tweet_url):
    """Holt den oEmbed HTML-Code für einen Tweet."""
    try:
        # Überprüfe Domain
        parsed_url = urlparse(tweet_url)
        if parsed_url.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"]:
            # print(f"Nicht-Twitter/X-Domain: {parsed_url.netloc}")
            return None
        # Stelle sicher, dass es wie eine Tweet-URL aussieht (enthält /status/)
        if "/status/" not in parsed_url.path:
            # print(f"Keine Status-ID in Pfad: {parsed_url.path}")
            return None
    except Exception as e:
        print(f"URL-Parsing-Fehler für Embed: {tweet_url}, Fehler: {e}")
        return None

    # Verwende die bereinigte URL für oEmbed
    cleaned_tweet_url = clean_tweet_url(tweet_url)
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark" # dark theme hinzugefügt

    try:
        # print(f"Versuche Embed für: {cleaned_tweet_url} via {api_url}") # Debugging
        response = requests.get(api_url, timeout=15) # Timeout erhöht
        response.raise_for_status() # Löst HTTPError für 4xx/5xx aus
        data = response.json()
        # print(f"Embed API Antwort für {cleaned_tweet_url}: {data.get('html')[:100]}...") # Debugging
        return data.get("html")
    except requests.exceptions.Timeout:
        print(f"Timeout beim Abrufen des Embeddings für: {cleaned_tweet_url}")
        return None
    except requests.exceptions.HTTPError as e:
        # Speziell 404 (Not Found) oder 403 (Forbidden) behandeln
        status_code = e.response.status_code
        print(f"HTTP Fehler {status_code} beim Abrufen des Embeddings für {cleaned_tweet_url}. Tweet evtl. gelöscht oder privat?")
        if status_code == 404:
             return f"<p style='color:orange;'>Tweet nicht gefunden (404). Wurde er gelöscht?</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
        elif status_code == 403:
             return f"<p style='color:orange;'>Zugriff auf Tweet verweigert (403). Ist er privat oder geschützt?</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
        else:
            return f"<p style='color:red;'>Fehler ({status_code}) beim Laden der Tweet-Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except requests.exceptions.RequestException as e:
        print(f"Netzwerkfehler beim Abrufen des Embeddings für {cleaned_tweet_url}: {e}")
        return f"<p style='color:red;'>Netzwerkfehler beim Laden der Vorschau.</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
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
if 'session_results' not in st.session_state: st.session_state.session_results = {} # Speichert {index: [cat1, cat2]}
if 'session_comments' not in st.session_state: st.session_state.session_comments = {} # Speichert {index: "comment"}
if 'default_loaded' not in st.session_state: st.session_state.default_loaded = False

# --- Labeler ID Eingabe ---
labeler_id_input = st.text_input(
    "👤 Bitte gib deine Labeler ID ein (z.B. Name oder Kürzel):",
    value=st.session_state.labeler_id,
    key="labeler_id_widget"
)
st.session_state.labeler_id = labeler_id_input # Immer aktualisieren

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

# Logik zum Bestimmen, welche Datei verwendet wird
if uploaded_file is not None:
    # Wenn eine neue Datei hochgeladen wurde oder der Name sich geändert hat
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        file_input = uploaded_file
        file_source_name = uploaded_file.name
        trigger_processing = True
        st.session_state.default_loaded = False # Standarddatei ist nicht mehr relevant
        print(f"Verwende hochgeladene Datei: {file_source_name}")
elif not st.session_state.initialized and not st.session_state.default_loaded:
    # Wenn noch nicht initialisiert und keine Datei hochgeladen wurde, versuche Standarddatei
    if os.path.exists(DEFAULT_CSV_PATH):
        try:
            # Prüfe, ob die Datei nicht leer ist
            if os.path.getsize(DEFAULT_CSV_PATH) > 0:
                 file_input = DEFAULT_CSV_PATH
                 file_source_name = DEFAULT_CSV_PATH
                 trigger_processing = True
                 st.session_state.default_loaded = True
                 print(f"Verwende Standarddatei: {file_source_name}")
            else:
                 st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' ist leer. Lade eine CSV hoch.")
                 st.session_state.default_loaded = False # Markieren, dass Versuch fehlgeschlagen
        except OSError as e:
             st.warning(f"Konnte Standarddatei '{DEFAULT_CSV_PATH}' nicht lesen: {e}. Lade eine CSV hoch.")
             st.session_state.default_loaded = False
    else:
        st.info(f"Standarddatei '{DEFAULT_CSV_PATH}' nicht gefunden. Lade eine CSV hoch.")
        st.session_state.default_loaded = False # Markieren, dass Versuch fehlgeschlagen

# Verarbeitung auslösen, wenn nötig und GSheet verbunden ist
if trigger_processing and worksheet:
    print(f"Trigger Processing für: {file_source_name}")
    # Reset für neue Datei
    st.session_state.initialized = False
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}
    st.session_state.input_file_name = file_source_name # Neuen Dateinamen speichern

    with st.spinner(f"Verarbeite '{file_source_name}'..."):
        all_input_urls = []
        if isinstance(file_input, str): # Standarddatei Pfad
            try:
                with open(file_input, 'r', encoding='utf-8') as f_default: # Lese als Text
                    all_input_urls = load_urls_from_input_csv(f_default, source_name=file_source_name)
            except Exception as e: st.error(f"Fehler Lesen Standarddatei '{file_source_name}': {e}")
        elif file_input is not None: # UploadedFile Objekt
            # Streamlit's UploadedFile ist standardmäßig im Bytes-Modus,
            # load_urls_from_input_csv erwartet ein Text-IO-ähnliches Objekt.
            # Wir können es direkt übergeben, da pandas es handhaben kann.
             all_input_urls = load_urls_from_input_csv(file_input, source_name=file_source_name)

        if all_input_urls:
            # KEINE Filterung gegen Google Sheet mehr
            st.session_state.urls_to_process = all_input_urls
            # random.shuffle(st.session_state.urls_to_process) # URLs mischen?
            st.session_state.total_items = len(st.session_state.urls_to_process)
            st.session_state.current_index = 0
            st.success(f"{st.session_state.total_items} URLs aus '{file_source_name}' geladen. Bereit zum Labeln für '{st.session_state.labeler_id}'.")
            st.session_state.initialized = True
            # Force rerun, um das Interface zu laden
            st.rerun()
        else:
             st.error(f"Datei '{file_source_name}' enthält keine gültigen URLs oder konnte nicht gelesen werden.")
             st.session_state.initialized = False
             st.session_state.default_loaded = False # Reset, falls Standarddatei fehlschlug
             st.session_state.input_file_name = None # Reset Dateiname
elif trigger_processing and not worksheet:
     st.error("Sheet-Verbindung fehlgeschlagen. Verarbeitung nicht möglich."); st.session_state.initialized = False; st.session_state.default_loaded = False

# --- Haupt-Labeling-Interface ---
if st.session_state.get('initialized', False) and st.session_state.urls_to_process:
    total_items = st.session_state.total_items

    # Zustand: Alle URLs bearbeitet
    if st.session_state.current_index >= total_items:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Du hast alle {total_items} URLs aus '{st.session_state.input_file_name}' bearbeitet!")
        st.balloons()
        st.info(f"Deine Ergebnisse wurden im Google Sheet '{connected_sheet_name}' gespeichert.")
        if worksheet:
            try: sheet_url = worksheet.spreadsheet.url; st.link_button("Google Sheet öffnen", sheet_url)
            except Exception: pass
        # Knopf zum Neustart/andere Datei
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
             # Labeler ID bleibt erhalten
             # Cache leeren für File Uploader und Rerun
             st.cache_data.clear()
             st.cache_resource.clear()
             st.rerun()
        st.stop()

    # Aktuellen Index und URL holen
    current_idx = st.session_state.current_index
    current_url = st.session_state.urls_to_process[current_idx]

    # --- Navigation und Fortschritt (Oben) ---
    nav_cols_top = st.columns([1, 3, 1])
    # Zurück-Button (Oben)
    if current_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True):
            # WICHTIG: aktuellen Stand speichern, BEVOR Index geändert wird
            # (Da die Widgets beim Rerun neu gezeichnet werden, holen wir den Wert direkt)
            # Wir lesen die Checkbox-Werte *nicht* direkt hier, da sie im unteren Teil definiert sind.
            # Der untere Zurück-Button muss die Werte in den session_state schreiben!
            # Daher kann der obere Zurück-Button nur navigieren.
            st.session_state.current_index -= 1
            st.rerun()
    else:
        nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)

    # Fortschrittsanzeige
    progress_text = f"{st.session_state.labeler_id}: Link {current_idx + 1} von {total_items} (aus '{st.session_state.input_file_name}')"
    nav_cols_top[1].progress((current_idx + 1) / total_items, text=progress_text)
    # Weiter-Button (Oben) - Navigiert nur, speichert nicht aktiv (macht der untere Button)
    can_go_forward = (current_idx + 1) < total_items
    if nav_cols_top[2].button("Überspringen & Weiter ➡️" if can_go_forward else "Letztes Item", key="skip_next_top", use_container_width=True, disabled=not can_go_forward):
         if can_go_forward:
            # Nur Index erhöhen, nicht speichern
            st.session_state.current_index += 1
            st.rerun()


    st.divider()

    # --- URL Anzeige & Einbettung ---
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url) # Bereinigte URL für Embed
    embed_html = get_tweet_embed_html(base_tweet_url) # Verwende bereinigte URL für API

    display_url = current_url # URL, die angezeigt wird

    if embed_html:
        components.html(embed_html, height=650, scrolling=True)
        # Zeige Original-URL, wenn sie bereinigt wurde
        if base_tweet_url != current_url:
            st.caption(f"Original-URL (bereinigt für Vorschau):")
            st.markdown(f"[{current_url}]({current_url})")
            display_url = current_url # Stelle sicher, dass Original-URL gespeichert wird
    else:
        # Fallback: Nur Link anzeigen
        st.markdown(f"**URL:** [{display_url}]({display_url})")
        if "twitter.com" in current_url or "x.com" in current_url:
            st.caption("Vorschau nicht verfügbar oder Tweet gelöscht/privat.")
        else:
            st.caption("Vorschau nur für X/Twitter Links verfügbar.")
        st.link_button("Link in neuem Tab öffnen", display_url)
    st.divider()

    # --- NEUE Kategorieauswahl & Kommentar ---
    st.subheader("Kategorisierung")
    col1, col2 = st.columns([3, 2]) # Verhältnis anpassen bei Bedarf

    # Aktuell ausgewählte Kategorien für diesen Index holen (oder leere Liste)
    # WICHTIG: Dies sind die *gespeicherten* Werte für diesen Index
    saved_selection = st.session_state.session_results.get(current_idx, [])

    # Liste zum Sammeln der *aktuell* in den Widgets ausgewählten Kategorien
    selected_categories_in_widgets = []

    with col1:
        st.markdown("**Wähle passende Kategorien:**")

        # Iteriere durch Hauptkategorien für Gruppierung und Farbe
        for main_topic, sub_categories in CATEGORIES.items():
            color = CATEGORY_COLORS.get(main_topic, "black") # Hole Farbe, default schwarz
            # Zeige Hauptkategorie als farbige Überschrift
            st.markdown(f'<h5 style="color:{color}; border-bottom: 2px solid {color}; margin-top: 15px; margin-bottom: 10px;">{main_topic}</h5>', unsafe_allow_html=True)

            # Zeige Checkboxen für jede Subkategorie an
            # Optional: Mehrspaltiges Layout für Checkboxen innerhalb einer Gruppe
            num_columns = 2 # Oder 3, je nach Breite und Anzahl Subkategorien
            checkbox_cols = st.columns(num_columns)
            col_index = 0

            for sub_cat in sub_categories:
                # Eindeutiger Key für jede Checkbox: index + Kategorie
                checkbox_key = f"cb_{current_idx}_{main_topic.replace(' ', '_')}_{sub_cat.replace(' ', '_').replace('/','_')}"
                # Standardwert der Checkbox = ist sie in der *gespeicherten* Auswahl?
                is_checked_default = sub_cat in saved_selection

                # Platziere Checkbox in der nächsten Spalte
                current_col = checkbox_cols[col_index % num_columns]
                with current_col:
                    # Erstelle die Checkbox. Wenn sie geklickt wird (True zurückgibt), füge sie zur Liste hinzu
                    is_checked_now = st.checkbox(
                        sub_cat,
                        value=is_checked_default,
                        key=checkbox_key
                    )
                    if is_checked_now:
                        selected_categories_in_widgets.append(sub_cat)

                col_index += 1 # Nächste Spalte

        st.markdown("---") # Trenner nach allen Kategorien
        # Anzeige der aktuell ausgewählten Kategorien
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets))) # Eindeutig und sortiert
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**")
            # Zeige ausgewählte Kategorien mit ihren Farben
            display_tags = []
            for cat in selected_categories_in_widgets:
                 # Finde die Hauptkategorie und Farbe
                 main_cat_found = None
                 cat_color = "grey" # Default
                 for m_cat, s_cats in CATEGORIES.items():
                     if cat in s_cats:
                         main_cat_found = m_cat
                         cat_color = CATEGORY_COLORS.get(m_cat, "grey")
                         break
                 # Erstelle einen farbigen Tag (Markdown oder HTML)
                 display_tags.append(f'<span style="color: {cat_color}; border: 1px solid {cat_color}; border-radius: 5px; padding: 2px 6px; margin: 2px;">{cat}</span>')

            st.markdown(" ".join(display_tags), unsafe_allow_html=True)

        else:
            st.write("_Keine Kategorien ausgewählt._")

    with col2:
        # Kommentarfeld bleibt gleich
        default_comment = st.session_state.session_comments.get(current_idx, "")
        comment_key = f"comment_{current_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=250, key=comment_key) # Etwas höher gemacht

    st.divider()

    # --- Navigationsbuttons (Unten) ---
    nav_cols_bottom = st.columns(7) # Behalte Layout bei

    # Zurück-Button (Unten) - SPEICHERT aktuelle Auswahl!
    if current_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True): # Leerzeichen hinzugefügt für Unterscheidung von oben
            # Speichere die *aktuell ausgewählten* Kategorien UND Kommentar für den *aktuellen* Index
            st.session_state.session_results[current_idx] = selected_categories_in_widgets
            st.session_state.session_comments[current_idx] = comment
            # Gehe dann zum vorherigen Index
            st.session_state.current_index -= 1
            st.rerun()
    else:
        nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)

    # Speichern & Weiter Button
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_labeler_id = st.session_state.labeler_id
        # Überprüfe, ob mindestens eine Kategorie ausgewählt wurde
        if not selected_categories_in_widgets:
            st.warning("Bitte wähle mindestens eine Kategorie aus, bevor du speicherst.")
        elif not worksheet:
            st.error("Keine Verbindung zum Google Sheet zum Speichern.")
        elif not current_labeler_id:
            st.error("Labeler ID nicht gesetzt. Bitte oben eingeben.")
        else:
            # Kategorien als String formatieren
            categories_str = "; ".join(selected_categories_in_widgets)
            # Speichere in Google Sheet
            if save_categorization_gsheet(worksheet, current_labeler_id, display_url, categories_str, comment):
                # Speichere Auswahl und Kommentar im Session State für diesen Index
                st.session_state.session_results[current_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_idx] = comment
                st.session_state.processed_urls_in_session.add(current_idx) # Markiere als bearbeitet in dieser Session
                # Gehe zum nächsten Index
                st.session_state.current_index += 1
                # Scroll to top (optional, kann manchmal nützlich sein)
                # js = '''<script>window.scrollTo({ top: 0, behavior: 'smooth' });</script>'''
                # st.components.v1.html(js)
                st.rerun()
            else:
                st.error("Speichern in Google Sheet fehlgeschlagen. Bitte prüfe die Verbindung oder versuche es erneut.")

# --- Fallback-Anzeige, wenn nichts geladen wurde ---
elif not st.session_state.get('initialized', False) and not uploaded_file and not st.session_state.get('default_loaded', False) and st.session_state.labeler_id:
    if worksheet: # Nur wenn Sheet verbunden ist und ID da ist
        # Diese Nachricht wird jetzt durch die Logik in der Dateiauswahl abgedeckt.
        # st.info(f"Versuche, Standarddatei '{DEFAULT_CSV_PATH}' zu laden oder lade eine andere CSV hoch.")
        pass # Die Logik oben zeigt bereits passende Infos/Warnungen an.
    elif not worksheet and st.session_state.labeler_id:
         st.error("Verbindung zu Google Sheets fehlgeschlagen. Kann keine Daten laden oder speichern.")


# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    try:
        sheet_url = worksheet.spreadsheet.url
        st.sidebar.page_link(sheet_url, label="Google Sheet öffnen ↗️")
    except Exception: pass
else:
    st.sidebar.error("Keine Verbindung zum Google Sheet.")

st.sidebar.markdown(f"**Aktuelle/r Labeler/in:** `{st.session_state.labeler_id or '(Bitte oben eingeben)'}`")

current_input_info = st.session_state.get('input_file_name', None)
if current_input_info: st.sidebar.markdown(f"**Input-Datei:** `{current_input_info}`")
else: st.sidebar.markdown("**Input-Datei:** -")

st.sidebar.markdown(f"**Datenbank:** Google Sheet")
st.sidebar.markdown(f"**Format Sheet:** Spalten `{', '.join(HEADER)}`")
st.sidebar.markdown("**Fortschritt:** Jedes Labeling wird als neue Zeile gespeichert.")

if st.session_state.get('initialized', False):
    total_urls_in_file = st.session_state.total_items
    # Zähle, wie viele Indizes im session_results-Dict sind (zuverlässiger als processed_urls_in_session)
    # oder verwende den aktuellen Index als Maß für den Fortschritt
    labeled_count = st.session_state.current_index # Zeigt an, *vor* welchem Item man ist
    # Wenn man fertig ist, ist current_index = total_items
    if st.session_state.current_index >= total_items:
         labeled_count = total_items

    st.sidebar.metric("URLs in Datei", total_urls_in_file)
    st.sidebar.metric("Aktuelles Item / Bearbeitet", f"{labeled_count+1 if labeled_count < total_items else labeled_count} / {total_urls_in_file}")
else:
    st.sidebar.metric("URLs in Datei", "-")
    st.sidebar.metric("Aktuelles Item / Bearbeitet", "-")

# Kleiner Hinweis auf Caching
st.sidebar.caption("Tweet-Vorschauen werden gecached.")