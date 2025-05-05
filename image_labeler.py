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
# from datetime import datetime # Nicht mehr benötigt
# import pytz # Nicht mehr benötigt
import streamlit.components.v1 as components # Für HTML Einbettung

# --- DIES MUSS DER ERSTE STREAMLIT-BEFEHL SEIN ---
st.set_page_config(layout="wide", page_title="Dataset Labeler)")
# --- ENDE DES ERSTEN STREAMLIT-BEFEHLS ---

# === Pfad zur Standard-CSV-Datei ===
DEFAULT_CSV_PATH = "input.csv" # Diese Datei wird IMMER verwendet

# === Google Sheets Setup ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

# Spaltennamen im Google Sheet (REIHENFOLGE WICHTIG!) - Timestamp entfernt
# COL_TS = "Timestamp" # Nicht mehr benötigt
COL_LBL = "Labeler_ID"
COL_URL = "URL"
COL_CATS = "Kategorien"
COL_COMMENT = "Kommentar"
HEADER = [COL_LBL, COL_URL, COL_CATS, COL_COMMENT] # NEUE Header-Reihenfolge OHNE Timestamp

# Zeitzone für Zeitstempel - Nicht mehr benötigt
# TIMEZONE = pytz.timezone("Europe/Berlin")

# === Google Sheets Verbindung ===
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
        # Prüfe und schreibe Header, falls nötig (mit dem NEUEN Header ohne Timestamp)
        if not all_vals or all_vals[0] != HEADER:
            st.sidebar.warning(f"Header in '{sheet_name}' stimmt nicht mit {HEADER} überein oder fehlt. Schreibe korrekten Header...")
            try:
                # Intelligentes Update/Einfügen des Headers
                if not all_vals or len(all_vals[0]) != len(HEADER):
                    # Wenn Zeile 1 leer ist oder die Spaltenanzahl nicht passt, füge Header ein
                    # Vorher alte Header-Zeile löschen, falls vorhanden und Spaltenanzahl falsch war
                    if all_vals and len(all_vals[0]) != len(HEADER):
                        try:
                           worksheet.delete_rows(1) # Lösche die alte, falsche Header-Zeile
                           time.sleep(1) # Kurze Pause
                        except Exception as del_e:
                           st.sidebar.warning(f"Konnte alte Header-Zeile nicht löschen: {del_e}")
                    # Lösche ggf. ersten Inhalt, falls komplett leer oder nur eine leere Zeile
                    if not all_vals or (len(all_vals) == 1 and all(v == '' for v in all_vals[0])):
                        worksheet.clear()
                        time.sleep(1)
                    worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                else:
                    # Wenn Spaltenanzahl passt, nur überschreiben
                    cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]
                    worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')

                # Entferne leere Zeilen direkt nach dem Header
                worksheet = gc.open(sheet_name).sheet1 # Worksheet Objekt neu holen nach möglicher Änderung
                all_vals_after = worksheet.get_all_values()
                if len(all_vals_after) > 1 and all(v == '' for v in worksheet.row_values(2)):
                    worksheet.delete_rows(2)

                header_written = True
                st.sidebar.success(f"Header in '{sheet_name}' aktualisiert/geschrieben.")
            except Exception as he:
                st.sidebar.error(f"Konnte Header nicht schreiben: {he}")
        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt. Bitte überprüfen."); st.stop(); return None, False, None
    except gspread.exceptions.SpreadsheetNotFound: st.error(f"Google Sheet '{st.secrets.get('google_sheets', {}).get('sheet_name', '???')}' nicht gefunden."); st.stop(); return None, False, None
    except Exception as e: st.error(f"Fehler bei GSheets Verbindung: {e}"); st.stop(); return None, False, None

worksheet, header_written_flag, connected_sheet_name = connect_gsheet()

# === Einstellungen ===
CATEGORIES = {
    "Health": ["Lifestyle", "Mental Health", "Physical Health", "Healthcare System"],
    "Social": ["Education", "Family/Relationships", "Employment/Economy"],
    "Environment": ["Environmental Policies", "Energy Sector", "Natural/Man-made Disasters"],
}
ALL_CATEGORIES = [cat for sublist in CATEGORIES.values() for cat in sublist]

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
@st.cache_data(ttl=300)
def get_processed_urls_by_labeler(target_labeler_id):
    processed_urls = set()
    worksheet_obj, _, _ = connect_gsheet()
    if not worksheet_obj or not target_labeler_id:
        st.warning("Worksheet/Labeler ID fehlt beim Abrufen des Fortschritts.")
        return processed_urls
    print(f"DEBUG: Rufe verarbeitete URLs für Labeler '{target_labeler_id}' ab...")
    try:
        all_data = worksheet_obj.get_all_values()
        if not all_data or len(all_data) < 1: return processed_urls
        header_row = all_data[0]
        # Prüfe, ob der Header im Sheet dem erwarteten (neuen) Header entspricht
        if header_row != HEADER:
             st.warning(f"GSheet Header ({header_row}) stimmt nicht mit Code-Header ({HEADER}) überein. Fortschrittsprüfung könnte fehlerhaft sein.")
             # Versuche trotzdem, die Spalten zu finden
             try:
                 labeler_col_index = header_row.index(COL_LBL)
                 url_col_index = header_row.index(COL_URL)
             except ValueError as e:
                st.error(f"Benötigte Spalte '{e}' fehlt im Sheet-Header. Fortschritt kann nicht geladen werden.")
                return processed_urls
        else:
             # Header stimmt überein, verwende feste Indizes basierend auf HEADER
             try:
                 labeler_col_index = HEADER.index(COL_LBL)
                 url_col_index = HEADER.index(COL_URL)
             except ValueError:
                 st.error("Interner Fehler: Konnte Spaltenindizes nicht finden.")
                 return processed_urls

        for row in all_data[1:]:
            if len(row) > max(labeler_col_index, url_col_index) and row[labeler_col_index] and row[url_col_index]:
                if row[labeler_col_index] == target_labeler_id:
                    processed_urls.add(row[url_col_index].strip())
        print(f"DEBUG: {len(processed_urls)} verarbeitete URLs für '{target_labeler_id}' gefunden.")
    except gspread.exceptions.APIError as e: st.warning(f"GSheet API Fehler (Fortschritt laden): {e}")
    except Exception as e: st.warning(f"Fehler (Fortschritt laden): {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(file_path, source_name="Standarddatei"):
    """Lädt alle URLs aus einem Dateipfad und bereinigt sie."""
    urls = []
    if not file_path or not isinstance(file_path, str):
        st.error("Kein gültiger Dateipfad übergeben."); return urls
    try:
        with open(file_path, 'rb') as file_input_object:
            try:
                df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
            except UnicodeDecodeError:
                st.warning(f"UTF-8 Fehler bei '{source_name}', versuche latin-1...")
                file_input_object.seek(0)
                df = pd.read_csv(file_input_object, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)
        print(f"DEBUG: CSV gelesen ({source_name}), {len(df)} Zeilen.")
        url_series_raw = df.iloc[:, 0]
        url_series_str = url_series_raw.astype(str)
        url_series_nonan = url_series_str.replace('nan', pd.NA).dropna()
        url_series_stripped = url_series_nonan.str.strip()
        url_series_noempty = url_series_stripped[url_series_stripped != '']
        print(f"DEBUG: Nach Bereinigung, {len(url_series_noempty)} Zeilen übrig.")
        url_series_filtered = url_series_noempty[url_series_noempty.str.match(r'^https?://\S+$')]
        print(f"DEBUG: Nach Regex-Filter, {len(url_series_filtered)} Zeilen übrig.")
        urls = url_series_filtered.unique().tolist()
        print(f"DEBUG: Nach unique(), {len(urls)} URLs zurückgegeben.")
    except FileNotFoundError: st.error(f"Fehler: Datei '{file_path}' nicht gefunden.")
    except pd.errors.EmptyDataError: st.warning(f"Input '{source_name}' ist leer.")
    except IndexError: st.warning(f"Input '{source_name}' hat keine Spalten.")
    except Exception as e: st.error(f"Fehler beim Lesen/Verarbeiten von '{source_name}': {e}")
    return urls

# --- GEÄNDERTE Speicherfunktion (ohne Timestamp) ---
def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Hängt eine neue Zeile (OHNE Timestamp) an das Google Sheet an."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt."); return False
    try:
        # Zeitstempel wird nicht mehr generiert
        # now_ts = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z%z')

        # Daten in der korrekten Reihenfolge gemäß NEUEM HEADER
        data_row = [labeler_id, url, categories_str, comment] # Reihenfolge angepasst
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e:
         st.error(f"Sheets API Fehler (Speichern): {e}. Rate Limiting? Prüfe GCloud Quotas.")
         return False
    except Exception as e:
        st.error(f"Fehler beim Speichern in GSheet: {e}"); return False
# --- ENDE GEÄNDERTE Speicherfunktion ---


def clean_tweet_url(url):
    try:
        cleaned_url = url.split('?')[0]
        cleaned_url = re.sub(r"/(photo|video)/\d+$", "", cleaned_url)
        return cleaned_url
    except Exception: return url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    if not isinstance(tweet_url, str): return None
    try:
        parsed_url = urlparse(tweet_url)
        if parsed_url.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"] or "/status/" not in parsed_url.path:
            return None
    except Exception: return None
    cleaned_tweet_url = clean_tweet_url(tweet_url)
    api_url = f"https://publish.twitter.com/oembed?url={cleaned_tweet_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        return response.json().get("html")
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response is not None else 500
        print(f"HTTP/Netzwerk Fehler {status_code} für Embed {cleaned_tweet_url}: {e}")
        msg = f"Fehler ({status_code}) beim Laden der Vorschau."
        if status_code == 404: msg = "Tweet nicht gefunden (404)."
        elif status_code == 403: msg = "Zugriff verweigert (403)."
        elif isinstance(e, requests.exceptions.Timeout): msg = "Timeout beim Laden der Vorschau."
        return f"<p style='color:orange; font-family:sans-serif; border:1px solid orange; padding:10px; border-radius:5px;'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except Exception as e: st.warning(f"Generischer Embed Fehler {cleaned_tweet_url}: {e}"); return None

# === Streamlit App Hauptteil ===
st.title("📊 Dataset Labeler")

# --- Session State Initialisierung ---
if 'labeler_id' not in st.session_state: st.session_state.labeler_id = ""
if 'initialized' not in st.session_state: st.session_state.initialized = False
if 'input_file_name' not in st.session_state: st.session_state.input_file_name = DEFAULT_CSV_PATH
if 'urls_to_process' not in st.session_state: st.session_state.urls_to_process = []
if 'total_items' not in st.session_state: st.session_state.total_items = 0
if 'processed_urls_in_session' not in st.session_state: st.session_state.processed_urls_in_session = set()
if 'current_index' not in st.session_state: st.session_state.current_index = 0
if 'session_results' not in st.session_state: st.session_state.session_results = {}
if 'session_comments' not in st.session_state: st.session_state.session_comments = {}
if 'original_total_items' not in st.session_state: st.session_state.original_total_items = 0
if 'already_processed_count' not in st.session_state: st.session_state.already_processed_count = 0

# --- Labeler ID Eingabe ---
labeler_id_input = st.text_input(
    "👤 Bitte gib deinen Vornamen ein:", value=st.session_state.labeler_id, key="labeler_id_widget", help="Wird zum Speichern des Fortschritts verwendet."
)
st.session_state.labeler_id = labeler_id_input.strip()
if not st.session_state.labeler_id: st.warning("Bitte Labeler ID eingeben."); st.stop()
st.divider()

# --- Dateiverarbeitung (VEREINFACHT - nur Standarddatei) ---
trigger_processing = False
if not st.session_state.initialized:
    trigger_processing = True
    print("Triggering initial data processing...")

if trigger_processing and worksheet:
    print(f"Processing für: {DEFAULT_CSV_PATH}, Labeler: {st.session_state.labeler_id}")
    st.session_state.urls_to_process, st.session_state.total_items, st.session_state.processed_urls_in_session = [], 0, set()
    st.session_state.current_index, st.session_state.session_results, st.session_state.session_comments = 0, {}, {}
    st.session_state.input_file_name = DEFAULT_CSV_PATH
    st.session_state.original_total_items, st.session_state.already_processed_count = 0, 0

    with st.spinner(f"Verarbeite '{DEFAULT_CSV_PATH}' & prüfe Fortschritt..."):
        all_input_urls_cleaned = load_urls_from_input_csv(DEFAULT_CSV_PATH, source_name=DEFAULT_CSV_PATH)

        if all_input_urls_cleaned:
            st.session_state.original_total_items = len(all_input_urls_cleaned)
            print(f"DEBUG: {st.session_state.original_total_items} URLs aus Datei geladen.")
            current_labeler_id = st.session_state.labeler_id
            get_processed_urls_by_labeler.clear()
            processed_by_this_labeler = get_processed_urls_by_labeler(current_labeler_id)
            remaining_urls = [url for url in all_input_urls_cleaned if url.strip() not in processed_by_this_labeler]
            st.session_state.urls_to_process = remaining_urls
            st.session_state.total_items = len(remaining_urls)
            st.session_state.already_processed_count = st.session_state.original_total_items - st.session_state.total_items
            st.session_state.current_index = 0
            st.session_state.initialized = True

            if st.session_state.total_items > 0:
                st.success(f"{st.session_state.original_total_items} URLs gefunden. {st.session_state.already_processed_count} bereits von dir bearbeitet. {st.session_state.total_items} verbleibend.")
            else:
                 st.success(f"Super! Alle {st.session_state.original_total_items} URLs bereits von dir bearbeitet.")
        else:
             st.error(f"Konnte keine gültigen URLs in '{DEFAULT_CSV_PATH}' finden oder Datei fehlt.")
             st.session_state.initialized = False
elif trigger_processing and not worksheet:
    st.error("Sheet-Verbindung fehlgeschlagen.");
    st.session_state.initialized = False


# --- Haupt-Labeling-Interface ---
if st.session_state.get('initialized', False):
    remaining_items = st.session_state.total_items
    original_total = st.session_state.original_total_items
    processed_count = st.session_state.already_processed_count
    current_local_idx = st.session_state.current_index

    # Zustand: Alle URLs bearbeitet
    if remaining_items <= 0 or current_local_idx >= remaining_items:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Alle {original_total} URLs bearbeitet!")
        st.balloons()
        if worksheet:
            try: st.link_button("Google Sheet öffnen", worksheet.spreadsheet.url)
            except Exception: pass
        if st.button("App neu laden (Fortschritt bleibt)"):
             st.session_state.initialized = False
             st.cache_data.clear()
             get_processed_urls_by_labeler.clear()
             st.rerun()
        st.stop()

    current_url = st.session_state.urls_to_process[current_local_idx]

    # --- Navigation Oben ---
    nav_cols_top = st.columns([1, 3, 1])
    if current_local_idx > 0:
        if nav_cols_top[0].button("⬅️ Zurück", key="back_top", use_container_width=True): st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_top[0].button("⬅️ Zurück", key="back_top_disabled", disabled=True, use_container_width=True)
    if original_total > 0:
        current_global_item_number = processed_count + current_local_idx + 1
        progress_text = f"{st.session_state.labeler_id}: Item {current_global_item_number} / {original_total} ('{DEFAULT_CSV_PATH}')"
        nav_cols_top[1].progress((processed_count + current_local_idx) / original_total, text=progress_text)
    else: nav_cols_top[1].progress(0, text="Keine Items")
    can_go_forward = (current_local_idx + 1) < remaining_items
    next_local_idx_has_data = (current_local_idx + 1) in st.session_state.session_results
    skip_disabled = not can_go_forward or next_local_idx_has_data
    if nav_cols_top[2].button("Überspringen ➡️" if can_go_forward else "Letztes Item", key="skip_next_top", use_container_width=True, disabled=skip_disabled, help="Zum Speichern unteren Button nutzen."):
        if can_go_forward and not next_local_idx_has_data:
            st.session_state.session_results[current_local_idx], st.session_state.session_comments[current_local_idx] = [], "[Übersprungen]"
            st.session_state.current_index += 1; st.rerun()
        elif next_local_idx_has_data: st.toast("Nächstes Item hat Daten (aus Sitzung).", icon="⚠️")
    st.divider()

    # --- ZWEISPALTIGES LAYOUT ---
    left_column, right_column = st.columns([2, 1])

    # --- Linke Spalte: URL Anzeige & Einbettung ---
    with left_column:
        st.subheader("Post Vorschau / Link")
        base_tweet_url = clean_tweet_url(current_url)
        embed_html = get_tweet_embed_html(base_tweet_url)
        display_url = current_url
        if embed_html:
            components.html(embed_html, height=650, scrolling=True)
        else:
            st.markdown(f"**URL:** [{display_url}]({display_url})")
            if "twitter.com" in display_url or "x.com" in display_url: st.caption("Vorschau nicht verfügbar.")
            else: st.caption("Vorschau nur für X/Twitter.")
            st.link_button("Link in neuem Tab öffnen", display_url)

    # --- Rechte Spalte: Kategorieauswahl & Kommentar ---
    with right_column:
        st.subheader("Kategorisierung")
        saved_selection = st.session_state.session_results.get(current_local_idx, [])
        selected_categories_in_widgets = []

        st.markdown("**Wähle (eine) passende Kategorie:**")
        for main_topic, sub_categories in CATEGORIES.items():
            main_color = CATEGORY_COLORS.get(main_topic, "black")
            st.markdown(f'<h6 style="color:{main_color}; border-bottom: 1px solid {main_color}; margin-top: 10px; margin-bottom: 5px;">{main_topic}</h6>', unsafe_allow_html=True)
            for sub_cat in sub_categories:
                clean_sub_cat_key = re.sub(r'\W+', '', sub_cat)
                checkbox_key = f"cb_{current_local_idx}_{main_topic.replace(' ', '_')}_{clean_sub_cat_key}"
                is_checked_default = sub_cat in saved_selection
                is_checked_now = st.checkbox(sub_cat, value=is_checked_default, key=checkbox_key)
                if is_checked_now: selected_categories_in_widgets.append(sub_cat)
        st.markdown("---")

        selected_categories_in_widgets = sorted(list(set(selected_categories_in_widgets)))
        if selected_categories_in_widgets:
            st.write("**Ausgewählt:**")
            display_tags = []
            for cat in selected_categories_in_widgets:
                 cat_color = SUBCATEGORY_COLORS.get(cat, SUBCATEGORY_COLORS.get("DEFAULT_COLOR", "grey"))
                 display_tags.append(f'<span style="display: inline-block; color: {cat_color}; border: 1px solid {cat_color}; border-radius: 4px; padding: 1px 5px; margin: 2px; font-size: 0.85em;">{cat}</span>')
            st.markdown(" ".join(display_tags), unsafe_allow_html=True)
        else: st.write("_Keine Kategorien ausgewählt._")
        st.markdown("---")

        default_comment = st.session_state.session_comments.get(current_local_idx, "")
        comment_key = f"comment_{current_local_idx}"
        comment = st.text_area("Optionaler Kommentar:", value=default_comment, height=150, key=comment_key, placeholder="Notizen...")

    # --- Navigation Unten ---
    st.divider()
    nav_cols_bottom = st.columns(7)
    if current_local_idx > 0:
        if nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom", use_container_width=True):
            st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
            st.session_state.session_comments[current_local_idx] = comment
            st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_bottom[0].button("⬅️ Zurück ", key="back_bottom_disabled", disabled=True, use_container_width=True)
    if nav_cols_bottom[6].button("Speichern & Weiter ➡️", type="primary", key="save_next_bottom", use_container_width=True):
        current_labeler_id = st.session_state.labeler_id
        if not selected_categories_in_widgets: st.warning("Bitte mind. eine Kategorie wählen.")
        elif not worksheet: st.error("Keine GSheet Verbindung.")
        elif not current_labeler_id: st.error("Labeler ID fehlt.")
        else:
            categories_str = "; ".join(selected_categories_in_widgets)
            # Rufe GEÄNDERTE Speicherfunktion auf
            if save_categorization_gsheet(worksheet, current_labeler_id, display_url, categories_str, comment):
                st.session_state.session_results[current_local_idx] = selected_categories_in_widgets
                st.session_state.session_comments[current_local_idx] = comment
                st.session_state.processed_urls_in_session.add(current_local_idx)
                st.session_state.current_index += 1; st.rerun()
            else: st.error("Speichern fehlgeschlagen.")

# --- Fallback-Anzeige ---
elif not st.session_state.get('initialized', False) and st.session_state.labeler_id:
    st.warning("Initialisierung nicht abgeschlossen. Bitte prüfe Fehlermeldungen oben oder im Log.")

# --- Sidebar ---
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden mit: '{connected_sheet_name}'")
    try: st.sidebar.page_link(worksheet.spreadsheet.url, label="Sheet öffnen ↗️")
    except Exception: pass
else: st.sidebar.error("Keine GSheet Verbindung.")

st.sidebar.markdown(f"**Labeler/in:** `{st.session_state.labeler_id or '(fehlt)'}`")
st.sidebar.markdown(f"**Input-Datei:** `{DEFAULT_CSV_PATH}`")
# Zeige NEUEN Header im Sidebar an
st.sidebar.markdown(f"**DB:** Google Sheet | **Format:** `{', '.join(HEADER)}`")

if st.session_state.get('initialized', False):
    original_total = st.session_state.original_total_items
    processed_count = st.session_state.already_processed_count
    remaining_count = st.session_state.total_items
    current_local_idx = st.session_state.current_index
    current_global_item_number = processed_count + current_local_idx + 1
    if remaining_count == 0 and original_total > 0: current_global_item_number = original_total
    if original_total == 0: current_global_item_number = 0
    st.sidebar.metric("Gesamt (Datei)", original_total)
    st.sidebar.metric("Aktuell / Gesamt", f"{current_global_item_number} / {original_total}")
    st.sidebar.metric("Von dir gespeichert", processed_count)
    st.sidebar.metric("Noch offen (für dich)", remaining_count)
else:
    st.sidebar.metric("Gesamt (Datei)", "-"); st.sidebar.metric("Aktuell / Gesamt", "-")
    st.sidebar.metric("Von dir gespeichert", "-"); st.sidebar.metric("Noch offen (für dich)", "-")

st.sidebar.caption(f"GSheet Header: {'OK' if not header_written_flag else 'Aktualisiert'}")
st.sidebar.caption("Tweet-Vorschauen gecached.")
st.sidebar.caption("Fortschritt wird beim Start abgerufen.")