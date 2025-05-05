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

        try: all_vals = worksheet.get_all_values()
        except gspread.exceptions.APIError as e:
             st.error(f"Fehler beim Lesen von Google Sheet '{sheet_name}': {e}. Prüfe Berechtigungen."); st.stop(); return None, False, None

        header_mismatch = False
        if not all_vals:
            header_mismatch = True; st.sidebar.warning(f"Sheet '{sheet_name}' ist leer. Schreibe Header...")
        elif all_vals[0] != HEADER:
            header_mismatch = True; st.sidebar.warning(f"Header in '{sheet_name}' ({all_vals[0]}) != Erwartung ({HEADER}). Korrigiere...")

        if header_mismatch:
            try:
                if not all_vals: worksheet.insert_row(HEADER, 1, value_input_option='USER_ENTERED'); print("DEBUG: Header in leeres Sheet eingefügt.")
                else: cell_list = [gspread.Cell(1, i + 1, value) for i, value in enumerate(HEADER)]; worksheet.update_cells(cell_list, value_input_option='USER_ENTERED'); print(f"DEBUG: Zeile 1 mit Header überschrieben: {HEADER}")
                time.sleep(1.5); header_written = True; st.sidebar.success(f"Header in '{sheet_name}' geschrieben/korrigiert.")
                try:
                    worksheet = gc.open(sheet_name).sheet1; all_vals_after = worksheet.get_all_values()
                    if len(all_vals_after) > 1 and all(v == '' for v in worksheet.row_values(2)): worksheet.delete_rows(2); print("DEBUG: Leere Zeile 2 nach Header-Fix entfernt.")
                except Exception as cleanup_e: st.sidebar.warning(f"Konnte nach Header-Fix nicht aufräumen: {cleanup_e}")
            except gspread.exceptions.APIError as he: st.sidebar.error(f"API Fehler beim Schreiben des Headers: {he}")
            except Exception as he: st.sidebar.error(f"Allg. Fehler beim Schreiben des Headers: {he}")

        return worksheet, header_written, sheet_name
    except KeyError as e: st.error(f"Secret '{e}' fehlt."); st.stop(); return None, False, None
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

CATEGORY_COLORS = { "Health": "dodgerblue", "Social": "mediumseagreen", "Environment": "darkorange" }
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
    ws, _, _ = connect_gsheet()
    if not ws or not target_labeler_id: st.warning("Worksheet/ID fehlt für Fortschritt."); return processed_urls
    print(f"DEBUG: Lade Fortschritt für '{target_labeler_id}'...")
    try:
        all_data = ws.get_all_values(); header = all_data[0] if all_data else None
        if not header: return processed_urls
        if header != HEADER: st.warning(f"Sheet Header != Code Header. Prüfung evtl. fehlerhaft.");
        try: lbl_idx, url_idx = header.index(COL_LBL), header.index(COL_URL)
        except ValueError as e: st.error(f"Spalte '{e}' fehlt."); return processed_urls
        for row in all_data[1:]:
            if len(row)>max(lbl_idx,url_idx) and row[lbl_idx] and row[url_idx] and row[lbl_idx]==target_labeler_id:
                processed_urls.add(row[url_idx].strip())
        print(f"DEBUG: {len(processed_urls)} verarbeitete URLs gefunden.")
    except Exception as e: st.warning(f"Fehler Fortschritt laden: {e}")
    return processed_urls

@st.cache_data
def load_urls_from_input_csv(file_path, source_name="Standard"):
    urls = []; print(f"Lade URLs aus: {file_path}")
    if not file_path or not isinstance(file_path, str): st.error("Kein Pfad."); return urls
    try:
        with open(file_path, 'rb') as f:
            try: df = pd.read_csv(f, header=None, usecols=[0], skip_blank_lines=False, encoding='utf-8', skipinitialspace=True)
            except UnicodeDecodeError: f.seek(0); df = pd.read_csv(f, header=None, usecols=[0], skip_blank_lines=False, encoding='latin-1', skipinitialspace=True)
        s = df.iloc[:,0].astype(str).replace('nan',pd.NA).dropna().str.strip()[lambda x: x!='']
        s_f = s[s.str.match(r'^https?://\S+$')]; urls = s_f.unique().tolist()
        print(f"DEBUG: {len(df)} Zeilen gelesen -> {len(s)} bereinigt -> {len(s_f)} gültige URLs -> {len(urls)} unique.")
    except FileNotFoundError: st.error(f"Datei '{file_path}' nicht gefunden.")
    except Exception as e: st.error(f"Fehler Lesen '{source_name}': {e}")
    return urls

def save_categorization_gsheet(ws, lbl_id, url, cats_str, cmt):
    if not ws or not lbl_id: st.error("Sheet/Labeler fehlt für Speichern."); return False
    try: data_row = [lbl_id, url, cats_str, cmt]; ws.append_row(data_row, value_input_option='USER_ENTERED'); return True
    except Exception as e: st.error(f"Fehler Speichern: {e}"); return False

def clean_tweet_url(url):
    try: return re.sub(r"/(photo|video)/\d+$", "", url.split('?')[0])
    except: return url

@st.cache_data(ttl=3600)
def get_tweet_embed_html(url):
    if not isinstance(url, str): return None
    try: p = urlparse(url); assert p.netloc in ["twitter.com","x.com","www.twitter.com","www.x.com"] and "/status/" in p.path
    except: return None
    cleaned = clean_tweet_url(url); api=f"https://publish.twitter.com/oembed?url={cleaned}&maxwidth=550&omit_script=false&dnt=true&theme=dark"
    try: r=requests.get(api, timeout=15); r.raise_for_status(); return r.json().get("html")
    except requests.exceptions.RequestException as e:
        sc=e.response.status_code if e.response is not None else 500; print(f"Embed Fehler {sc} {cleaned}: {e}")
        msg=f"Fehler({sc})"; msg="Tweet? (404)" if sc==404 else "Zugriff? (403)" if sc==403 else msg; msg="Timeout" if isinstance(e, requests.exceptions.Timeout) else msg
        return f"<p style='color:orange;...'>{msg}</p><p><a href='{url}' target='_blank'>Link?</a></p>"
    except Exception as e: st.warning(f"Embed Fehler {cleaned}: {e}"); return None

# === Streamlit App ===
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
lbl_id = st.text_input("👤 Dein Vorname:", value=st.session_state.labeler_id, key="lbl_id_widget", help="Für Fortschritt.")
st.session_state.labeler_id = lbl_id.strip()
if not lbl_id: st.warning("Vornamen eingeben."); st.stop()
st.divider()

# --- Daten laden ---
if not st.session_state.initialized and worksheet:
    print("Initialisiere Daten...")
    st.session_state.update({ 'urls_to_process': [], 'total_items': 0, 'current_index': 0,
                              'session_results': {}, 'session_comments': {}, 'input_file_name': DEFAULT_CSV_PATH,
                              'original_total_items': 0, 'already_processed_count': 0 })
    with st.spinner(f"Lade '{DEFAULT_CSV_PATH}' & prüfe Fortschritt..."):
        all_urls = load_urls_from_input_csv(DEFAULT_CSV_PATH)
        if all_urls:
            st.session_state.original_total_items = len(all_urls); print(f"DEBUG: {len(all_urls)} URLs.")
            get_processed_urls_by_labeler.clear(); processed = get_processed_urls_by_labeler(lbl_id)
            remaining = [url for url in all_urls if url.strip() not in processed]
            st.session_state.update({ 'urls_to_process': remaining, 'total_items': len(remaining),
                                      'already_processed_count': len(all_urls)-len(remaining), 'current_index': 0, 'initialized': True })
            msg = f"{len(all_urls)} URLs. {len(all_urls)-len(remaining)} bearbeitet. {len(remaining)} offen." if len(remaining)>0 else f"Super! Alle {len(all_urls)} bearbeitet."
            st.success(msg)
        else: st.error(f"Keine URLs in '{DEFAULT_CSV_PATH}'."); st.session_state.initialized = False
elif not st.session_state.initialized and not worksheet: st.error("Sheet fehlt.")

# --- Hauptinterface ---
if st.session_state.get('initialized', False):
    rem, orig, proc, idx = st.session_state.total_items, st.session_state.original_total_items, st.session_state.already_processed_count, st.session_state.current_index

    if rem <= 0 or idx >= rem: # Alle bearbeitet
        st.success(f"🎉 Super, {lbl_id}! Alle {orig} URLs bearbeitet!"); st.balloons()
        # --- KORRIGIERTER TRY BLOCK ---
        if worksheet:
            try:
                st.link_button("Google Sheet öffnen", worksheet.spreadsheet.url)
            except Exception:
                pass # Fehler ignorieren
        # --- ENDE KORREKTUR ---
        if st.button("App neu laden"): st.session_state.initialized=False; st.cache_data.clear(); get_processed_urls_by_labeler.clear(); st.rerun()
        st.stop()

    url_now = st.session_state.urls_to_process[idx]

    # Nav Oben
    nav_t = st.columns([1,3,1]); item_n = proc+idx+1; prog_t=f"{lbl_id}: {item_n}/{orig} ('{DEFAULT_CSV_PATH}')" if orig>0 else "Keine Items"
    if idx>0: nav_t[0].button("⬅️", key="b_t", use_container_width=True, on_click=lambda: st.session_state.update(current_index=idx-1))
    else: nav_t[0].button("⬅️", key="b_t_d", disabled=True, use_container_width=True)
    nav_t[1].progress((proc+idx)/orig if orig>0 else 0, text=prog_t)
    can_fwd = (idx+1)<rem; next_has= (idx+1) in st.session_state.session_results; skip_d = not can_fwd or next_has
    if nav_t[2].button("➡️" if can_fwd else "🏁", key="s_t", use_container_width=True, disabled=skip_d, help="Überspringen"):
        if can_fwd and not next_has: st.session_state.session_results[idx], st.session_state.session_comments[idx] = [], "[Übersprungen]"; st.session_state.current_index+=1; st.rerun()
        elif next_has: st.toast("Nächstes hat Daten.", icon="⚠️")
    st.divider()

    # Layout
    left, right = st.columns([2,1])
    with left: # Embed
        st.subheader("Vorschau / Link"); embed = get_tweet_embed_html(url_now)
        if embed: components.html(embed, height=650, scrolling=True)
        else: st.markdown(f"**URL:** [{url_now}]({url_now})"); st.caption("Keine Vorschau."); st.link_button("Link öffnen", url_now)
    with right: # Kategorien
        st.subheader("Kategorisierung"); saved = st.session_state.session_results.get(idx,[]); selected=[]
        st.markdown("**Kategorie(n):**")
        for main, subs in CATEGORIES.items():
            color=CATEGORY_COLORS.get(main,"#000"); st.markdown(f'<h6 style="color:{color};border-bottom:1px solid {color};margin:10px 0 5px 0;">{main}</h6>', unsafe_allow_html=True)
            for sub in subs:
                key=f"cb_{idx}_{main.replace(' ','_')}_{re.sub(r'[^a-zA-Z0-9]','',sub)}"
                if st.checkbox(sub, value=(sub in saved), key=key): selected.append(sub)
        st.markdown("---"); selected=sorted(list(set(selected)))
        if selected:
            st.write("**Ausgewählt:**"); tags=[f'<span style="display:inline-block;color:{SUBCATEGORY_COLORS.get(c,"grey")};border:1px solid {SUBCATEGORY_COLORS.get(c,"grey")};border-radius:4px;padding:1px 5px;margin:2px;font-size:0.85em;">{c}</span>' for c in selected]
            st.markdown(" ".join(tags), unsafe_allow_html=True)
        else: st.write("_Keine._")
        st.markdown("---"); cmt=st.text_area("Kommentar:",value=st.session_state.session_comments.get(idx,""),height=150,key=f"cmt_{idx}",placeholder="...")

    # Nav Unten
    st.divider(); nav_b = st.columns(7)
    if idx>0:
        if nav_b[0].button("⬅️ ", key="b_b", use_container_width=True): st.session_state.session_results[idx],st.session_state.session_comments[idx]=selected,cmt; st.session_state.current_index-=1; st.rerun()
    else: nav_b[0].button("⬅️ ", key="b_b_d", disabled=True, use_container_width=True)
    if nav_b[6].button("Speichern ➡️", type="primary", key="save_b", use_container_width=True):
        if not selected: st.warning("Kategorie wählen.")
        elif not worksheet: st.error("Sheet fehlt.")
        elif not lbl_id: st.error("ID fehlt.")
        else:
            if save_categorization_gsheet(worksheet, lbl_id, url_now, "; ".join(selected), cmt):
                st.session_state.session_results[idx], st.session_state.session_comments[idx] = selected, cmt
                st.session_state.current_index+=1; st.rerun()
            else: st.error("Speichern fehlgeschlagen.")

elif not st.session_state.get('initialized', False) and lbl_id: st.warning("Initialisierung...")

# Sidebar
st.sidebar.header("Info & Status")
if worksheet: st.sidebar.success(f"Verbunden: '{connected_sheet_name}'"); try: st.sidebar.page_link(worksheet.spreadsheet.url, label="Sheet ↗️")
                except: pass
else: st.sidebar.error("Keine GSheet Verbindung.")
st.sidebar.markdown(f"**Labeler:** `{lbl_id or '(fehlt)'}`")
st.sidebar.markdown(f"**Input:** `{DEFAULT_CSV_PATH}`")
st.sidebar.markdown(f"**Format:** `{', '.join(HEADER)}`")
if st.session_state.get('initialized', False):
    ot,pc,rc,ci = st.session_state.original_total_items,st.session_state.already_processed_count,st.session_state.total_items,st.session_state.current_index
    cgi = pc+ci+1; cgi=ot if rc==0 and ot>0 else cgi; cgi=0 if ot==0 else cgi
    st.sidebar.metric("Gesamt",ot); st.sidebar.metric("Aktuell/Gesamt",f"{cgi}/{ot}")
    st.sidebar.metric("Gespeichert",pc); st.sidebar.metric("Offen",rc)
else: st.sidebar.metric("Gesamt","-"); st.sidebar.metric("Aktuell/Gesamt","-"); st.sidebar.metric("Gespeichert","-"); st.sidebar.metric("Offen","-")
st.sidebar.caption(f"Header: {'OK' if not header_written_flag else 'Korrigiert'}")
st.sidebar.caption("Vorschauen gecached."); st.sidebar.caption("Fortschritt beim Start geladen.")