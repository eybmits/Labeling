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

# === Google Sheets Verbindung (VERBESSERT) ===
@st.cache_resource
def connect_gsheet():
    """Stellt Verbindung zu Google Sheets her, prüft/korrigiert den Header und gibt das Worksheet-Objekt zurück."""
    try:
        creds_dict = st.secrets["google_sheets"]["credentials_dict"]
        sheet_name = st.secrets["google_sheets"]["sheet_name"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        worksheet = gc.open(sheet_name).sheet1
        header_written = False # Flag, ob Header in *diesem* Durchlauf geändert wurde

        # Versuche, den aktuellen Inhalt zu lesen
        try:
            all_vals = worksheet.get_all_values()
        except gspread.exceptions.APIError as e:
             # Fehler beim Lesen (z.B. Berechtigungen)
             st.error(f"Fehler beim Lesen von Google Sheet '{sheet_name}': {e}. Prüfe Berechtigungen für Service Account.")
             st.stop()
             return None, False, None

        # Header prüfen und ggf. Korrektur-Flag setzen
        header_mismatch = False
        if not all_vals:
            header_mismatch = True
            st.sidebar.warning(f"Sheet '{sheet_name}' ist leer. Schreibe Header...")
        elif all_vals[0] != HEADER:
            header_mismatch = True
            st.sidebar.warning(f"Header in '{sheet_name}' ({all_vals[0]}) "
                               f"stimmt nicht mit Erwartung ({HEADER}) überein. Korrigiere...")

        # Header korrigieren, wenn nötig
        if header_mismatch:
            try:
                # --- ROBUSTERE KORREKTUR ---
                if not all_vals: # Fall 1: Sheet war komplett leer
                     # Ggf. vorher leeren, falls nur Müll drin war
                     # worksheet.clear()
                     # time.sleep(1)
                     worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED')
                     print("DEBUG: Header in leeres Sheet eingefügt.")
                else: # Fall 2: Sheet hat Inhalt, überschreibe Zeile 1 IMMER
                     cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]
                     worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')
                     print(f"DEBUG: Zeile 1 mit Header überschrieben: {HEADER}")
                # --- ENDE ROBUSTERE KORREKTUR ---

                # Kurze Pause kann helfen, damit GSheet die Änderung verarbeitet
                time.sleep(1.5) # Etwas länger zur Sicherheit
                header_written = True
                st.sidebar.success(f"Header in '{sheet_name}' geschrieben/korrigiert.")

                # ---- Cleanup nach Header schreiben/korrigieren ----
                try:
                    # Worksheet Objekt neu holen, da sich Struktur geändert haben könnte
                    worksheet = gc.open(sheet_name).sheet1
                    all_vals_after = worksheet.get_all_values()
                    # Entferne leere Zeilen direkt nach dem (jetzt hoffentlich korrekten) Header
                    if len(all_vals_after) > 1 and all(v == '' for v in worksheet.row_values(2)):
                         worksheet.delete_rows(2)
                         print("DEBUG: Leere Zeile 2 nach Header-Fix entfernt.")
                except Exception as cleanup_e:
                     st.sidebar.warning(f"Konnte nach Header-Fix nicht aufräumen: {cleanup_e}")
                # ---- Ende Cleanup ----

            except gspread.exceptions.APIError as he:
                 st.sidebar.error(f"API Fehler beim Schreiben des Headers: {he}")
            except Exception as he:
                st.sidebar.error(f"Allg. Fehler beim Schreiben des Headers: {he}")
                # Nicht stoppen, vielleicht funktioniert der Rest

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
             try: labeler_col_index, url_col_index = header_row.index(COL_LBL), header_row.index(COL_URL)
             except ValueError as e: st.error(f"Benötigte Spalte '{e}' fehlt im Sheet-Header."); return processed_urls
        else:
             try: labeler_col_index, url_col_index = HEADER.index(COL_LBL), HEADER.index(COL_URL)
             except ValueError: st.error("Interner Fehler: Konnte Spaltenindizes nicht finden."); return processed_urls

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
    urls = []
    if not file_path or not isinstance(file_path, str): st.error("Kein gültiger Dateipfad."); return urls
    try:
        with open(file_path, 'rb') as f:
            try: df = pd.read_csv(f, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
            except UnicodeDecodeError: f.seek(0); df = pd.read_csv(f, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)
        print(f"DEBUG: CSV ({source_name}), {len(df)} Zeilen.")
        s = df.iloc[:, 0].astype(str).replace('nan', pd.NA).dropna().str.strip()
        s = s[s != '']
        print(f"DEBUG: Nach Bereinigung, {len(s)} Zeilen.")
        s_filtered = s[s.str.match(r'^https?://\S+$')]
        print(f"DEBUG: Nach Regex, {len(s_filtered)} Zeilen.")
        urls = s_filtered.unique().tolist()
        print(f"DEBUG: Nach unique(), {len(urls)} URLs.")
    except FileNotFoundError: st.error(f"Datei '{file_path}' nicht gefunden.")
    except Exception as e: st.error(f"Fehler Lesen/Verarbeiten '{source_name}': {e}")
    return urls

def save_categorization_gsheet(worksheet_obj, labeler_id, url, categories_str, comment):
    """Hängt eine neue Zeile (OHNE Timestamp) an das Google Sheet an."""
    if not worksheet_obj: st.error("Keine Sheet-Verbindung zum Speichern."); return False
    if not labeler_id: st.error("Labeler ID fehlt."); return False
    try:
        data_row = [labeler_id, url, categories_str, comment] # Reihenfolge gemäß HEADER
        worksheet_obj.append_row(data_row, value_input_option='USER_ENTERED')
        return True
    except gspread.exceptions.APIError as e: st.error(f"Sheets API Fehler (Speichern): {e}"); return False
    except Exception as e: st.error(f"Fehler beim Speichern in GSheet: {e}"); return False

def clean_tweet_url(url):
    try: return re.sub(r"/(photo|video)/\d+$", "", url.split('?')[0])
    except Exception: return url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(tweet_url):
    if not isinstance(tweet_url, str): return None
    try:
        p = urlparse(tweet_url)
        if p.netloc not in ["twitter.com", "x.com", "www.twitter.com", "www.x.com"] or "/status/" not in p.path: return None
    except Exception: return None
    cleaned_url = clean_tweet_url(tweet_url)
    api = f"https://publish.twitter.com/oembed?url={cleaned_url}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try:
        r = requests.get(api, timeout=15); r.raise_for_status(); return r.json().get("html")
    except requests.exceptions.RequestException as e:
        sc = e.response.status_code if e.response is not None else 500; print(f"Embed Fehler {sc} {cleaned_url}: {e}")
        msg = f"Fehler ({sc}) Vorschau."; msg = "Tweet nicht gefunden (404)." if sc==404 else "Zugriff verweigert (403)." if sc==403 else msg
        if isinstance(e, requests.exceptions.Timeout): msg = "Timeout Vorschau."
        return f"<p style='color:orange;...'>{msg}</p><p><a href='{tweet_url}' target='_blank'>Link prüfen</a></p>"
    except Exception as e: st.warning(f"Generischer Embed Fehler {cleaned_url}: {e}"); return None

# === Streamlit App Hauptteil ===
st.title("📊 Dataset Labeler")

# --- Session State ---
if 'labeler_id' not in st.session_state: st.session_state.labeler_id = ""
if 'initialized' not in st.session_state: st.session_state.initialized = False
if 'input_file_name' not in st.session_state: st.session_state.input_file_name = DEFAULT_CSV_PATH
if 'urls_to_process' not in st.session_state: st.session_state.urls_to_process = []
if 'total_items' not in st.session_state: st.session_state.total_items = 0
if 'current_index' not in st.session_state: st.session_state.current_index = 0
if 'session_results' not in st.session_state: st.session_state.session_results = {}
if 'session_comments' not in st.session_state: st.session_state.session_comments = {}
if 'original_total_items' not in st.session_state: st.session_state.original_total_items = 0
if 'already_processed_count' not in st.session_state: st.session_state.already_processed_count = 0

# --- Labeler ID ---
labeler_id_input = st.text_input(
    "👤 Dein Vorname:", value=st.session_state.labeler_id, key="labeler_id_widget", help="Für Fortschritt speichern."
)
st.session_state.labeler_id = labeler_id_input.strip()
if not st.session_state.labeler_id: st.warning("Bitte Vornamen eingeben."); st.stop()
st.divider()

# --- Daten laden (nur beim ersten Mal) ---
if not st.session_state.initialized and worksheet:
    print("Initialisiere Daten...")
    st.session_state.urls_to_process, st.session_state.total_items = [], 0
    st.session_state.current_index, st.session_state.session_results, st.session_state.session_comments = 0, {}, {}
    st.session_state.input_file_name = DEFAULT_CSV_PATH
    st.session_state.original_total_items, st.session_state.already_processed_count = 0, 0

    with st.spinner(f"Lade '{DEFAULT_CSV_PATH}' & prüfe Fortschritt..."):
        all_urls = load_urls_from_input_csv(DEFAULT_CSV_PATH)
        if all_urls:
            st.session_state.original_total_items = len(all_urls)
            print(f"DEBUG: {len(all_urls)} URLs geladen.")
            curr_id = st.session_state.labeler_id
            get_processed_urls_by_labeler.clear()
            processed = get_processed_urls_by_labeler(curr_id)
            remaining = [url for url in all_urls if url.strip() not in processed]
            st.session_state.urls_to_process = remaining
            st.session_state.total_items = len(remaining)
            st.session_state.already_processed_count = len(all_urls) - len(remaining)
            st.session_state.current_index = 0
            st.session_state.initialized = True
            msg = f"{len(all_urls)} URLs gefunden. {st.session_state.already_processed_count} von dir bearbeitet. {len(remaining)} verbleibend." if len(remaining)>0 else f"Super! Alle {len(all_urls)} URLs von dir bearbeitet."
            st.success(msg)
        else: st.error(f"Keine URLs in '{DEFAULT_CSV_PATH}' gefunden."); st.session_state.initialized = False
elif not st.session_state.initialized and not worksheet: st.error("Sheet-Verbindung fehlt.")

# --- Hauptinterface ---
if st.session_state.get('initialized', False):
    remaining = st.session_state.total_items
    original = st.session_state.original_total_items
    processed = st.session_state.already_processed_count
    idx = st.session_state.current_index

    if remaining <= 0 or idx >= remaining:
        st.success(f"🎉 Super, {st.session_state.labeler_id}! Alle {original} URLs bearbeitet!")
        st.balloons();
        if worksheet: try: st.link_button("Google Sheet öffnen", worksheet.spreadsheet.url)
                      except: pass
        if st.button("App neu laden"): st.session_state.initialized=False; st.cache_data.clear(); get_processed_urls_by_labeler.clear(); st.rerun()
        st.stop()

    current_url = st.session_state.urls_to_process[idx]

    # Navigation Oben
    nav_cols = st.columns([1, 3, 1])
    if idx > 0: nav_cols[0].button("⬅️ Zurück", key="back_top", use_container_width=True, on_click=lambda: st.session_state.update(current_index=st.session_state.current_index - 1))
    else: nav_cols[0].button("⬅️ Zurück", key="back_top_dis", disabled=True, use_container_width=True)
    if original > 0:
        item_num = processed + idx + 1; prog_txt = f"{st.session_state.labeler_id}: Item {item_num}/{original} ('{DEFAULT_CSV_PATH}')"
        nav_cols[1].progress((processed + idx) / original, text=prog_txt)
    else: nav_cols[1].progress(0, text="Keine Items")
    can_fwd = (idx + 1) < remaining; next_has_data = (idx + 1) in st.session_state.session_results; skip_dis = not can_fwd or next_has_data
    if nav_cols[2].button("Überspringen ➡️" if can_fwd else "Letztes", key="skip", use_container_width=True, disabled=skip_dis, help="Zum Speichern unten klicken"):
        if can_fwd and not next_has_data:
            st.session_state.session_results[idx], st.session_state.session_comments[idx] = [], "[Übersprungen]"
            st.session_state.current_index += 1; st.rerun()
        elif next_has_data: st.toast("Nächstes Item hat Daten.", icon="⚠️")
    st.divider()

    # Layout
    left, right = st.columns([2, 1])

    with left: # Embed/Link
        st.subheader("Post Vorschau / Link")
        embed = get_tweet_embed_html(current_url)
        if embed: components.html(embed, height=650, scrolling=True)
        else:
            st.markdown(f"**URL:** [{current_url}]({current_url})")
            st.caption("Keine Vorschau.") if "twitter.com" in current_url or "x.com" in current_url else st.caption("Vorschau nur für X/Twitter.")
            st.link_button("Link öffnen", current_url)

    with right: # Kategorien/Kommentar
        st.subheader("Kategorisierung")
        saved_selection = st.session_state.session_results.get(idx, [])
        selected_cats = []
        st.markdown("**Wähle Kategorie(n):**")
        for main, subs in CATEGORIES.items():
            color = CATEGORY_COLORS.get(main, "black")
            st.markdown(f'<h6 style="color:{color}; border-bottom:1px solid {color}; margin:10px 0 5px 0;">{main}</h6>', unsafe_allow_html=True)
            for sub in subs:
                key = f"cb_{idx}_{main.replace(' ','_')}_{re.sub(r'[^a-zA-Z0-9]','',sub)}"
                checked = st.checkbox(sub, value=(sub in saved_selection), key=key)
                if checked: selected_cats.append(sub)
        st.markdown("---")
        selected_cats = sorted(list(set(selected_cats)))
        if selected_cats:
            st.write("**Ausgewählt:**")
            tags = [f'<span style="display:inline-block; color:{SUBCATEGORY_COLORS.get(c, "grey")}; border:1px solid {SUBCATEGORY_COLORS.get(c,"grey")}; border-radius:4px; padding:1px 5px; margin:2px; font-size:0.85em;">{c}</span>' for c in selected_cats]
            st.markdown(" ".join(tags), unsafe_allow_html=True)
        else: st.write("_Keine ausgewählt._")
        st.markdown("---")
        comment = st.text_area("Kommentar (optional):", value=st.session_state.session_comments.get(idx,""), height=150, key=f"comment_{idx}", placeholder="Notizen...")

    # Navigation Unten
    st.divider()
    nav_cols_b = st.columns(7)
    if idx > 0:
        if nav_cols_b[0].button("⬅️ Zurück ", key="back_b", use_container_width=True):
            st.session_state.session_results[idx], st.session_state.session_comments[idx] = selected_cats, comment
            st.session_state.current_index -= 1; st.rerun()
    else: nav_cols_b[0].button("⬅️ Zurück ", key="back_b_dis", disabled=True, use_container_width=True)
    if nav_cols_b[6].button("Speichern & Weiter ➡️", type="primary", key="save", use_container_width=True):
        if not selected_cats: st.warning("Bitte mind. eine Kategorie wählen.")
        elif not worksheet: st.error("Keine GSheet Verbindung.")
        elif not st.session_state.labeler_id: st.error("Labeler ID fehlt.")
        else:
            cats_str = "; ".join(selected_cats)
            if save_categorization_gsheet(worksheet, st.session_state.labeler_id, current_url, cats_str, comment):
                st.session_state.session_results[idx], st.session_state.session_comments[idx] = selected_cats, comment
                st.session_state.current_index += 1; st.rerun()
            else: st.error("Speichern fehlgeschlagen.")

elif not st.session_state.get('initialized', False) and st.session_state.labeler_id:
    st.warning("Initialisierung läuft oder fehlgeschlagen. Prüfe Logs/Fehler oben.")

# Sidebar
st.sidebar.header("Info & Status")
if worksheet:
    st.sidebar.success(f"Verbunden: '{connected_sheet_name}'")
    try: st.sidebar.page_link(worksheet.spreadsheet.url, label="Sheet öffnen ↗️")
    except: pass
else: st.sidebar.error("Keine GSheet Verbindung.")
st.sidebar.markdown(f"**Labeler:** `{st.session_state.labeler_id or '(fehlt)'}`")
st.sidebar.markdown(f"**Input:** `{DEFAULT_CSV_PATH}`")
st.sidebar.markdown(f"**Format:** `{', '.join(HEADER)}`")
if st.session_state.get('initialized', False):
    ot, pc, rc, ci = st.session_state.original_total_items, st.session_state.already_processed_count, st.session_state.total_items, st.session_state.current_index
    cgi = pc + ci + 1; cgi = ot if rc==0 and ot>0 else cgi; cgi = 0 if ot==0 else cgi
    st.sidebar.metric("Gesamt", ot); st.sidebar.metric("Aktuell/Gesamt", f"{cgi}/{ot}")
    st.sidebar.metric("Gespeichert", pc); st.sidebar.metric("Offen", rc)
else: st.sidebar.metric("Gesamt", "-"); st.sidebar.metric("Aktuell/Gesamt", "-"); st.sidebar.metric("Gespeichert", "-"); st.sidebar.metric("Offen", "-")
st.sidebar.caption(f"Header: {'OK' if not header_written_flag else 'Korrigiert'}")
st.sidebar.caption("Vorschauen gecached."); st.sidebar.caption("Fortschritt wird beim Start geladen.")