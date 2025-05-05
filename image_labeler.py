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

# === Google Sheets Setup ===
# Definiere die benötigten Berechtigungsbereiche
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Spaltennamen im Google Sheet (müssen mit Header übereinstimmen)
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_URL, COL_CATS, COL_COMMENT]

# Funktion zur Authentifizierung und zum Öffnen des Sheets
@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her und gibt das Worksheet-Objekt zurück."""
    try:
        # Versuche, Secrets zu laden (dies löst KEINEN st.* Befehl aus, der die UI zeichnet)
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]

        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1 # Nimm das erste Tabellenblatt

        # Überprüfe und schreibe Header, falls das Sheet leer ist
        # Diese Prüfung ist notwendig, bevor st.* Befehle für die Sidebar kommen
        header_written = False
        all_vals = worksheet.get_all_values() # Hole Werte einmal
        if not all_vals:
             worksheet.append_row(HEADER, value_input_option='USER_ENTERED')
             header_written = True
             # st.sidebar.info(f"Header-Zeile in '{sheet_name}' geschrieben.") # Nicht hier, da es zu früh ist

        # Gib das Worksheet zurück. Sidebar-Nachrichten kommen später.
        return worksheet, header_written, sheet_name
    except KeyError as e:
        st.error(f"Fehler: Secret '{e}' nicht in st.secrets gefunden. Überprüfe deine secrets.toml Konfiguration.")
        st.stop()
    except gspread.exceptions.SpreadsheetNotFound:
        # Hier können wir st.error verwenden, da set_page_config schon lief
        st.error(f"Fehler: Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', 'Name nicht gefunden')}' nicht gefunden. "
                 "Prüfe den Namen und die Freigabe für das Service Account.")
        st.stop()
    except Exception as e:
        st.error(f"Fehler beim Verbinden/Initialisieren von Google Sheets: {e}")
        st.stop()
        return None, False, None # Wird wegen st.stop() nicht erreicht

# Hole das Worksheet-Objekt und Statusinformationen
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

@st.cache_data(ttl=300) # Cache für 5 Minuten
def load_processed_urls_gsheet(_worksheet_ref): # Dummy-Parameter nicht mehr nötig, da worksheet global ist
    """Lädt bereits bearbeitete URLs aus dem Google Sheet."""
    processed_urls = set()
    if not _worksheet_ref:
        return processed_urls
    try:
        url_column_values = _worksheet_ref.col_values(1)
        if len(url_column_values) > 1:
            processed_urls = set(url_column_values[1:])
    except gspread.exceptions.APIError as e:
         st.error(f"Google Sheets API Fehler beim Lesen der URLs: {e}")
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Lesen der URLs aus Google Sheet: {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(uploaded_file_object):
    """Lädt alle URLs aus einem hochgeladenen Streamlit-Datei-Objekt."""
    urls = []
    if not uploaded_file_object:
        st.error("Kein Datei-Objekt zum Laden übergeben.")
        return urls
    try:
        df = pd.read_csv(uploaded_file_object, header=None, usecols=[0], skip_blank_lines=True)
        url_series = df.iloc[:, 0].dropna().astype(str)
        urls = url_series[url_series.str.startswith(("http://", "https://"))].unique().tolist()
    except pd.errors.EmptyDataError:
         st.warning(f"Input-Datei '{uploaded_file_object.name}' ist leer oder enthält keine URLs.")
    except IndexError:
         st.warning(f"Input-Datei '{uploaded_file_object.name}' scheint keine Spalten zu enthalten (Format?).")
    except Exception as e:
        st.error(f"Fehler beim Lesen der Input-CSV ('{uploaded_file_object.name}'): {e}")
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
    except gspread.exceptions.APIError as e:
        st.error(f"Google Sheets API Fehler beim Speichern für URL {url}: {e}")
        return False
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Speichern in Google Sheet: {e}")
        return False

# --- URL-Bereinigung & Tweet Embedding (unverändert) ---
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
    except requests.exceptions.Timeout:
        print(f"Timeout getting embed for: {tweet_url}")
        return None
    except requests.exceptions.RequestException as e:
        status = e.response.status_code if e.response is not None else "N/A"
        print(f"Info: Tweet oEmbed failed (Status: {status}, URL: {tweet_url}).")
        return None
    except Exception as e:
        st.warning(f"Fehler bei oEmbed-Verarbeitung für {tweet_url}: {e}", icon="❓")
        return None

# === Streamlit App Hauptteil ===

# Titel kommt NACH set_page_config
st.title("📊 URL-Kategorisierer (mit Google Sheets)")

# --- Initialisierung & Session State (unverändert) ---
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.input_file_name = None
    st.session_state.urls_to_process = []
    st.session_state.total_items = 0
    st.session_state.processed_urls_in_session = set()
    st.session_state.current_index = 0
    st.session_state.session_results = {}
    st.session_state.session_comments = {}

# --- Dateiauswahl (unverändert) ---
uploaded_file = st.file_uploader("1. Wähle die Input-CSV-Datei mit den Links (Erste Spalte = URL)", type=["csv"])

process_file = False
if uploaded_file is not None:
    if st.session_state.input_file_name != uploaded_file.name or not st.session_state.initialized:
        process_file = True
        st.session_state.input_file_name = uploaded_file.name
        # Reset state
        st.session_state.initialized = False
        st.session_state.urls_to_process = []
        st.session_state.total_items = 0
        st.session_state.processed_urls_in_session = set()
        st.session_state.current_index = 0
        st.session_state.session_results = {}
        st.session_state.session_comments = {}

if process_file:
    # Nur fortfahren, wenn die Worksheet-Verbindung erfolgreich war
    if worksheet:
        with st.spinner(f"Verarbeite '{uploaded_file.name}' und prüfe Google Sheet..."):
            all_input_urls = load_urls_from_input_csv(uploaded_file)
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
                    st.warning("Alle URLs aus der Input-Datei wurden bereits im Google Sheet gefunden oder die Datei enthält keine gültigen neuen URLs.")
                else:
                    st.success(f"{st.session_state.total_items} neue URLs zum Bearbeiten gefunden und gemischt.")
                    st.session_state.initialized = True
            elif not all_input_urls:
                 st.error("Input-Datei konnte nicht verarbeitet werden oder enthält keine URLs.")
                 st.session_state.initialized = False
    else:
        st.error("Verbindung zum Google Sheet fehlgeschlagen. Datei kann nicht verarbeitet werden.")
        st.session_state.initialized = False


# --- Haupt-Labeling-Interface (Logik größtenteils unverändert) ---
if st.session_state.get('initialized', False) and st.session_state.urls_to_process:
    total_items = st.session_state.total_items
    if st.session_state.current_index >= total_items:
         st.session_state.current_index = total_items # "Fertig"-Zustand

    # --- Prüfen ob fertig ---
    if st.session_state.current_index >= total_items:
        st.success("🎉 Alle neuen URLs aus der aktuellen Datei wurden bearbeitet!")
        st.balloons()
        st.info(f"Die Ergebnisse wurden laufend im Google Sheet '{connected_sheet_name}' gespeichert.")
        if worksheet: # Sicherstellen, dass worksheet existiert für URL
            try:
                sheet_url = worksheet.spreadsheet.url
                st.link_button("Google Sheet öffnen", sheet_url)
            except Exception:
                st.info("Link zum Sheet konnte nicht automatisch generiert werden.")

        if st.button("Neue Input-Datei laden"):
             st.session_state.initialized = False
             st.session_state.input_file_name = None
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
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True):
             st.session_state.current_index -= 1
             st.rerun()
    else:
        nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    progress_text = f"Link {current_idx + 1} von {total_items}"
    nav_cols_top[1].progress((current_idx + 1) / total_items, text=progress_text)
    st.divider()

    # --- Aktuelle URL und Einbettung (unverändert) ---
    current_url = st.session_state.urls_to_process[current_idx]
    st.subheader("Post Vorschau / Link")
    base_tweet_url = clean_tweet_url(current_url)
    embed_html = get_tweet_embed_html(base_tweet_url)
    if embed_html:
        st.components.v1.html(embed_html, height=650, scrolling=True)
        if base_tweet_url != current_url: st.caption(f"Original-URL (bereinigt): [{current_url}]({current_url})")
    else:
        st.markdown(f"**URL:** [{current_url}]({current_url})")
        if "twitter.com" in current_url or "x.com" in current_url:
             st.caption("Vorschau nicht verfügbar (Tweet gelöscht/privat, API-Problem o.ä.).")
        else:
            st.caption("Vorschau nur für X/Twitter Links verfügbar.")
        st.link_button("Link in neuem Tab öffnen", current_url)
    st.divider()

    # --- Kategorieauswahl und Kommentar (unverändert in der Logik) ---
    st.subheader("Kategorisierung")
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("**Wähle passende Kategorien:**")
        selected_categories_in_widgets = []
        default_selection = st.session_state.session_results.get(current_idx, [])
        for main_topic, sub_categories in CATEGORIES.items():
            with st.expander(f"**{main_topic}**", expanded=True):
                expander_key = f"multiselect_{current_idx}_{main_topic.replace(' ', '_').replace('&','_').replace('/','_')}"
                current_selection = st.multiselect(
                    label=" ", label_visibility="collapsed", options=sub_categories,
                    default=[cat for cat in default_selection if cat in sub_categories], key=expander_key )
                selected_categories_in_widgets.extend(current_selection)
        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets)))
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**"); st.info(", ".join(selected_categories_in_widgets))
        else:
            st.write("_Keine Kategorien ausgewählt._")
    with col2:
        default_comment = st.session_state.session_comments.get(current_idx, "")
        comment_key = f"comment_{current_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=200, key=comment_key)
    st.divider()

    # --- Navigationsbuttons (Unten) ---
    nav_cols_bottom = st.columns(7)
    # Zurück
    if current_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom", use_container_width=True):
             st.session_state.session_results[current_idx] = selected_categories_in_widgets
             st.session_state.session_comments[current_idx] = comment
             st.session_state.current_index -= 1
             st.rerun()
    else:
        nav_cols_bottom[0].button("⬅️ Zurück", key="back_bottom_disabled", disabled=True, use_container_width=True)
    # Speichern & Weiter
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        if not selected_categories_in_widgets:
             st.warning("Bitte wähle mindestens eine Kategorie aus.")
        elif not worksheet:
             st.error("Keine Verbindung zum Google Sheet zum Speichern.") # Sicherheitscheck
        else:
            categories_str = "; ".join(selected_categories_in_widgets)
            if save_categorization_gsheet(worksheet, current_url, categories_str, comment):
                st.session_state.session_results[current_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_idx] = comment
                st.session_state.processed_urls_in_session.add(current_url)
                st.session_state.current_index += 1
                st.rerun()
            else:
                st.error("Speichern in Google Sheet fehlgeschlagen. Siehe Fehlermeldung oben.")

# --- Fallback-Anzeigen (leicht angepasst) ---
elif not st.session_state.get('initialized', False) and uploaded_file is None:
    # Zeige nur, wenn die Worksheet-Verbindung erfolgreich war
    if worksheet:
        st.info("Bitte wähle eine Input-CSV-Datei aus, um zu starten.")
elif st.session_state.get('initialized', False) and not st.session_state.urls_to_process:
     st.warning("Keine *neuen* URLs zum Bearbeiten in der hochgeladenen Datei gefunden (alle bereits im Google Sheet).")
     if st.button("Andere Input-Datei laden"):
         st.session_state.initialized = False
         st.session_state.input_file_name = None
         st.rerun()

# --- Sidebar ---
# Sidebar wird erst hier aufgebaut, NACHDEM set_page_config lief und die Verbindung steht
st.sidebar.header("Info & Status")

# Zeige Verbindungsstatus basierend auf worksheet Objekt
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    if header_written_flag:
        st.sidebar.info(f"Header-Zeile wurde in '{connected_sheet_name}' geschrieben.")
    try:
        sheet_url = worksheet.spreadsheet.url
        st.sidebar.page_link(sheet_url, label="Sheet öffnen ↗️")
    except Exception:
        st.sidebar.info("Link zum Sheet konnte nicht generiert werden.")
else:
    # Fehlermeldung wurde bereits im Hauptbereich angezeigt
    st.sidebar.error("Keine Verbindung zum Google Sheet.")

if st.session_state.get('input_file_name'):
    st.sidebar.markdown(f"**Input:** `{st.session_state.input_file_name}`")
else:
    st.sidebar.markdown("**Input:** -")
st.sidebar.markdown(f"**Datenbank:** Google Sheet")
st.sidebar.markdown(f"""
- **Format Sheet:** Spalten `{', '.join(HEADER)}`
- **Fortschritt:** Wird nach Klick auf "Speichern & Weiter" im Sheet gesichert.
- **Einbettung:** Versucht, X/Twitter Posts einzubetten.
""")
if st.session_state.get('initialized', False):
    remaining = st.session_state.total_items - st.session_state.current_index
    processed_session = len(st.session_state.processed_urls_in_session)
    st.sidebar.metric("Verbleibende Links (aus Datei)", max(0, remaining))
    st.sidebar.metric("Gespeichert (diese Sitzung)", processed_session)
else:
    st.sidebar.metric("Verbleibende Links (aus Datei)", "-")
    st.sidebar.metric("Gespeichert (diese Sitzung)", "-")