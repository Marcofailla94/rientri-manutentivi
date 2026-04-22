
import os
import json
from datetime import datetime, date
from urllib.parse import quote
from io import BytesIO

import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials

APP_TITLE = "Rientri Manutentivi"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSET_FILE = os.path.join(BASE_DIR, "asset DOR.xlsx")
CONFIG_FILE = os.path.join(BASE_DIR, "config_email.json")
LOGO_FILE = os.path.join(BASE_DIR, "regionale.png")

GOOGLE_SHEET_NAME = "RientriManutentivi"

DR_LIST_DEFAULT = [
    'ABRUZZO', 'CALABRIA', 'CAMPANIA', 'FRIULI-VENEZIA GIULIA', 'LAZIO',
    'MARCHE', 'PIEMONTE', 'PUGLIA', 'SARDEGNA', 'SICILIA', 'TOSCANA',
    'TRENTINO-ALTO ADIGE', 'VENETO'
]

ALL_COLUMNS = [
    'ID',
    'DR',
    'ROTABILE',
    'IMC',
    'DATA ANORMALITA',
    'AVARIA',
    'N° AVVISO',
    'GRAVITA',
    'DATA PRESA IN CARICO',
    'DATA RIENTRO',
    'DATA NOTIFICA RIENTRO',
    'CONGRUENZA (SI/NO)',
    'NOTE RISCONTRO',
    'N° ORDINE',
    'DATA RESE/RIES',
    'STATO',
    'DATA CREAZIONE',
    'ULTIMO AGGIORNAMENTO',
    'MOTIVAZIONE CAMBIO RIENTRO',
    '__PowerAppsId__'
]

STATUS_COLORS = {
    'APERTA': '#ffd9d9',
    'IN_CARICO': '#fff2b3',
    'RIENTRATO': '#daf2d0',
    'TRATTATA': '#daf2d0',
    'FUORI_RANGE': '#ffd8a8'
}

STATUS_LABELS = {
    'APERTA': 'NON TRATTATA',
    'IN_CARICO': 'PRESA IN CARICO',
    'RIENTRATO': 'RIENTRATO IN IMPIANTO',
    'TRATTATA': 'TRATTATA'
}


def fmt_date(v):
    if v is None or v == '':
        return ''
    try:
        if pd.isna(v):
            return ''
    except Exception:
        pass
    try:
        return pd.to_datetime(v).strftime('%d/%m/%Y')
    except Exception:
        return str(v)


def txt(v):
    if v is None:
        return ''
    try:
        if pd.isna(v):
            return ''
    except Exception:
        pass
    return str(v).strip()


def send_outlook(to, cc, subject, body):
    return False, "Invio automatico non disponibile in questa versione. Usa 'Apri bozza email'."


def mailto(to, cc, subject, body):
    url = f"mailto:{quote(to)}?subject={quote(subject)}&body={quote(body)}"
    if cc:
        url += f"&cc={quote(cc)}"
    return url


def build_password_map(dr_list):
    pw = {
        'Control Room': 'Control Room4827',
        'Admin': 'Marco7314',
        'ABRUZZO': 'ABRUZZO1842',
        'CALABRIA': 'CALABRIA2951',
        'CAMPANIA': 'CAMPANIA4063',
        'FRIULI-VENEZIA GIULIA': 'FRIULI-VENEZIA GIULIA5174',
        'LAZIO': 'LAZIO6285',
        'MARCHE': 'MARCHE7396',
        'PIEMONTE': 'PIEMONTE8407',
        'PUGLIA': 'PUGLIA9518',
        'SARDEGNA': 'SARDEGNA1629',
        'SICILIA': 'SICILIA2730',
        'TOSCANA': 'TOSCANA3841',
        'TRENTINO-ALTO ADIGE': 'TRENTINO-ALTO ADIGE4952',
        'VENETO': 'VENETO5064'
    }
    for dr in dr_list:
        if dr not in pw:
            pw[dr] = f"{dr}0000"
    return pw


def init_auth_state():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'user_role' not in st.session_state:
        st.session_state.user_role = ''
    if 'login_error' not in st.session_state:
        st.session_state.login_error = ''


def logout():
    st.session_state.authenticated = False
    st.session_state.user_role = ''
    st.session_state.login_error = ''


def role_is_dr(role, dr_list):
    return role in dr_list


def apply_role_filter(df, role, dr_list):
    if role_is_dr(role, dr_list):
        return df[df['DR'].astype(str).str.strip() == role].copy()
    return df.copy()


def render_login(dr_list):
    st.set_page_config(page_title=APP_TITLE, layout='wide')
    render_header()

    st.subheader("Accesso applicazione")
    ruolo = st.selectbox('Seleziona profilo', ['Control Room', 'Admin'] + dr_list)
    password = st.text_input('Password', type='password')

    if st.button('Accedi', use_container_width=True):
        password_map = build_password_map(dr_list)
        if password == password_map.get(ruolo, ''):
            st.session_state.authenticated = True
            st.session_state.user_role = ruolo
            st.session_state.login_error = ''
            st.rerun()
        else:
            st.session_state.login_error = 'Password non corretta.'

    if st.session_state.login_error:
        st.error(st.session_state.login_error)

    st.stop()


def ensure_config(dr_list):
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    else:
        cfg = {
            'control_room_email': 'ControlRoomRegionale@trenitalia.it',
            'dr_referenti': {}
        }

    cfg.setdefault('dr_referenti', {})
    cfg.setdefault('control_room_email', 'ControlRoomRegionale@trenitalia.it')

    for dr in dr_list:
        cfg['dr_referenti'].setdefault(dr, {'to': '', 'cc': ''})

    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    return cfg


def find_column(df, candidates):
    normalized = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


@st.cache_data(show_spinner=False)
def load_asset():
    df = pd.read_excel(ASSET_FILE)
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    col_dr = find_column(df, ['DR', 'Dr', 'dr'])
    col_imc = find_column(df, ['IMC', 'Impianto Assegnatario', 'impianto assegnatario'])
    col_rot = find_column(df, ['codice manutentivo', 'Codice Manutentivo', 'ROTABILE', 'Rotabile'])

    if col_dr is None:
        raise ValueError("Nel file Asset DOR manca la colonna DR.")
    if col_imc is None:
        raise ValueError("Nel file Asset DOR manca la colonna IMC oppure Impianto Assegnatario.")
    if col_rot is None:
        raise ValueError("Nel file Asset DOR manca la colonna codice manutentivo oppure Codice Manutentivo.")

    out = pd.DataFrame()
    out['DR'] = df[col_dr].astype(str).str.strip()
    out['IMC'] = df[col_imc].astype(str).str.strip()
    out['ROTABILE'] = df[col_rot].astype(str).str.strip()

    out = out[
        (out['DR'] != '') &
        (out['IMC'] != '') &
        (out['ROTABILE'] != '') &
        (out['DR'].str.lower() != 'nan') &
        (out['IMC'].str.lower() != 'nan') &
        (out['ROTABILE'].str.lower() != 'nan')
    ].copy()

    out = out.drop_duplicates().reset_index(drop=True)
    return out


def connect_gsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        dict(st.secrets["gcp_service_account"]),
        scope
    )
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    return sheet


def ensure_gsheet_headers():
    sheet = connect_gsheet()
    header = sheet.row_values(1)

    if not header:
        sheet.append_row(ALL_COLUMNS)
        return

    header = [str(x).strip() for x in header]
    if header != ALL_COLUMNS:
        raise ValueError(
            "Le intestazioni del Google Sheet non coincidono con quelle attese. "
            "Controlla la prima riga del foglio."
        )


def load_segnalazioni():
    ensure_gsheet_headers()
    sheet = connect_gsheet()
    values = sheet.get_all_values()

    if not values or len(values) == 1:
        df = pd.DataFrame(columns=ALL_COLUMNS)
    else:
        header = [str(c).strip() for c in values[0]]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)

    for c in ALL_COLUMNS:
        if c not in df.columns:
            df[c] = ''

    df = df[ALL_COLUMNS]
    df['STATO'] = df['STATO'].replace('', pd.NA).fillna('TRATTATA')
    return df


def save_df(df):
    ensure_gsheet_headers()
    sheet = connect_gsheet()
    df_to_save = df.copy().fillna('').astype(str)

    all_rows = [ALL_COLUMNS] + df_to_save[ALL_COLUMNS].values.tolist()
    sheet.clear()
    sheet.update('A1', all_rows)


def dataframe_to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Archivio')
    output.seek(0)
    return output.getvalue()


def next_id(df):
    prefix = datetime.now().strftime('%Y%m%d')
    if df.empty or 'ID' not in df.columns:
        return f'{prefix}-001'
    existing = df['ID'].astype(str).fillna('')
    count = existing[existing.str.startswith(prefix + '-')].shape[0] + 1
    return f'{prefix}-{count:03d}'


def get_deadline_days(gravita):
    gravita = txt(gravita).lower()
    if gravita == 'rientro immediato':
        return 0
    if gravita == 'rientro entro 24 h':
        return 1
    if gravita == 'rientro entro 48 h':
        return 2
    if gravita == 'rientro entro 72 h':
        return 3
    return None


def is_out_of_range(row):
    limite = get_deadline_days(row.get('GRAVITA', ''))
    if limite is None:
        return False

    data_anom = pd.to_datetime(row.get('DATA ANORMALITA', ''), errors='coerce')
    data_rientro = pd.to_datetime(row.get('DATA RIENTRO', ''), errors='coerce')

    if pd.isna(data_anom) or pd.isna(data_rientro):
        return False

    delta = (data_rientro.normalize() - data_anom.normalize()).days
    return delta > limite


def subject_new(row):
    return f"{row['GRAVITA']} {row['ROTABILE']} - {row['IMC']} - {row['DR']}"


def body_new(row):
    pairs = [
        ('ID Segnalazione', row['ID']),
        ('DR', row['DR']),
        ('IMC', row['IMC']),
        ('Rotabile', row['ROTABILE']),
        ('Data anormalità', fmt_date(row['DATA ANORMALITA'])),
        ('Avaria', row['AVARIA']),
        ('N° avviso', row['N° AVVISO']),
        ('Gravità', row['GRAVITA'])
    ]
    bullets = '\n'.join([f'- {k}: {v}' for k, v in pairs])
    return (
        "Buongiorno,\n"
        "si richiede quanto in oggetto.\n\n"
        f"Di seguito i dettagli della segnalazione:\n{bullets}\n\n"
        "Si prega di prendere in carico la richiesta entro 8 ore dalla ricezione di questa mail."
    )


def subject_takeover(row):
    return f"Presa in carico rientro {row['ROTABILE']} - {row['IMC']} - {row['DR']}"


def body_takeover(row):
    return f"""Buongiorno,
si comunica la presa visione e presa in carico della richiesta.
È stato stabilito con la SOR Regionale il rientro manutentivo nella seguente data:

- Data rientro: {fmt_date(row["DATA RIENTRO"])}
- Rotabile: {row["ROTABILE"]}
- IMC: {row["IMC"]}
- DR: {row["DR"]}
- Gravità: {row["GRAVITA"]}
- N° avviso: {row["N° AVVISO"]}
"""


def subject_notify_return(row):
    return f"Notifica rientro in impianto {row['ROTABILE']} - {row['IMC']} - {row['DR']}"


def body_notify_return(row):
    return f"""Buongiorno,
si comunica che il rotabile risulta rientrato presso l'impianto di manutenzione.

Dettagli della segnalazione:
- ID Segnalazione: {row['ID']}
- Rotabile: {row['ROTABILE']}
- IMC: {row['IMC']}
- DR: {row['DR']}
- Gravità: {row['GRAVITA']}
- N° avviso: {row['N° AVVISO']}
- Data presa in carico: {fmt_date(row['DATA PRESA IN CARICO'])}
- Data rientro pianificata: {fmt_date(row['DATA RIENTRO'])}
- Data notifica rientro in impianto: {fmt_date(row['DATA NOTIFICA RIENTRO'])}
"""


def subject_reschedule(row):
    return f"Variazione data rientro {row['ROTABILE']} - {row['IMC']} - {row['DR']}"


def body_reschedule(row, old_date, new_date, motivo):
    return f"""Buongiorno,
si comunica una variazione della data di rientro manutentivo precedentemente stabilita.

Dettagli della segnalazione:
- ID Segnalazione: {row['ID']}
- Rotabile: {row['ROTABILE']}
- IMC: {row['IMC']}
- DR: {row['DR']}
- Gravità: {row['GRAVITA']}
- N° avviso: {row['N° AVVISO']}

Variazione rientro:
- Data rientro precedente: {old_date}
- Nuova data rientro: {new_date}
- Motivazione cambio rientro: {motivo}
"""


def subject_closed(row):
    return f"Chiusura rientro {row['ROTABILE']} - {row['IMC']} - {row['DR']}"


def body_closed(row):
    pairs = [
        ('Rotabile', row['ROTABILE']),
        ('DR', row['DR']),
        ('IMC', row['IMC']),
        ('Congruenza', row['CONGRUENZA (SI/NO)']),
        ('Note riscontro', row['NOTE RISCONTRO']),
        ('N° ordine', row['N° ORDINE']),
        ('Data RESE/RIES', fmt_date(row['DATA RESE/RIES']))
    ]
    bullets = '\n'.join([f'- {k}: {v}' for k, v in pairs])
    return (
        "Buongiorno,\n"
        "si comunica l'avvenuta conclusione dell'intervento manutentivo.\n\n"
        f"Dettagli finali:\n{bullets}"
    )


def card(row):
    bg = STATUS_COLORS['FUORI_RANGE'] if is_out_of_range(row) else STATUS_COLORS.get(row['STATO'], '#fff')
    motivazione = txt(row.get('MOTIVAZIONE CAMBIO RIENTRO', ''))

    stato_label = STATUS_LABELS.get(row['STATO'], row['STATO'])
    if is_out_of_range(row):
        stato_label = f"{stato_label} - FUORI RANGE"

    extra = ""
    if motivazione:
        extra += f'<div style="margin-top:6px;color:#7a5c00;"><b>Motivazione ultimo cambio rientro:</b> {motivazione}</div>'
    if txt(row.get('DATA NOTIFICA RIENTRO', '')):
        extra += f'<div style="margin-top:6px;color:#0b6b2e;"><b>Data notifica rientro:</b> {fmt_date(row["DATA NOTIFICA RIENTRO"])}</div>'

    st.markdown(
        f'''
        <div style="background:{bg}; border:1px solid #d0d7de; border-radius:12px; padding:12px; margin-bottom:10px;">
          <div style="font-weight:700;">{txt(row['ID'])} | {txt(row['ROTABILE'])}</div>
          <div style="color:#555;">DR: {txt(row['DR'])} | IMC: {txt(row['IMC'])} | Gravità: {txt(row['GRAVITA'])} | Stato: {stato_label}</div>
          <div style="color:#555;">Data anormalità: {fmt_date(row['DATA ANORMALITA'])} | N° avviso: {txt(row['N° AVVISO'])}</div>
          <div style="margin-top:8px;"><b>Avaria:</b> {txt(row['AVARIA'])}</div>
          {extra}
        </div>
        ''',
        unsafe_allow_html=True
    )


def render_header():
    st.markdown('''
    <style>
    .hdr{background:#ffd84d;border:2px solid #d0b03a;border-radius:12px;padding:16px 18px;margin-bottom:18px;}
    .t1{color:#0b6b2e;font-weight:800;font-size:30px;line-height:1.1;margin:0;}
    .t2{color:#0b6b2e;font-weight:800;font-size:22px;line-height:1.2;margin:6px 0 0 0;}
    .t3{color:#0b6b2e;font-weight:800;font-size:18px;line-height:1.2;margin:4px 0 0 0;}
    .pill{display:inline-block;margin-top:10px;padding:6px 12px;border-radius:999px;background:#0b6b2e;color:#fff;font-weight:700;}
    .legend-box{padding:8px 12px;border-radius:10px;border:1px solid #ddd;font-weight:700;text-align:center;}
    </style>
    ''', unsafe_allow_html=True)

    c1, c2 = st.columns([4, 1])

    with c1:
        st.markdown('''
        <div class="hdr">
            <div class="t1">Direzione Operations Regionale</div>
            <div class="t2">Manutenzione Regionale</div>
            <div class="t3">Maintenance & Standard Engineering</div>
            <div class="pill">Rientri Manutentivi</div>
        </div>
        ''', unsafe_allow_html=True)

    with c2:
        if os.path.exists(LOGO_FILE):
            st.image(LOGO_FILE, use_container_width=True)
        else:
            st.warning('Inserisci regionale.png nella cartella del progetto.')


def render_legend():
    st.markdown("**Legenda colori**")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown('<div class="legend-box" style="background:#ffd9d9;">Rosso = Non trattata</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="legend-box" style="background:#fff2b3;">Giallo = Presa in carico</div>', unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="legend-box" style="background:#daf2d0;">Verde = Rientrato / Trattata</div>', unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="legend-box" style="background:#ffd8a8;">Arancione = Fuori range</div>', unsafe_allow_html=True)


def init_mail_state():
    if 'mail_link' not in st.session_state:
        st.session_state.mail_link = ''
    if 'mail_subject' not in st.session_state:
        st.session_state.mail_subject = ''
    if 'mail_body' not in st.session_state:
        st.session_state.mail_body = ''
    if 'mail_message' not in st.session_state:
        st.session_state.mail_message = ''
    if 'mail_context' not in st.session_state:
        st.session_state.mail_context = ''


def set_pending_mail(context, link, subject, body, message):
    st.session_state.mail_context = context
    st.session_state.mail_link = link
    st.session_state.mail_subject = subject
    st.session_state.mail_body = body
    st.session_state.mail_message = message


def clear_pending_mail():
    st.session_state.mail_context = ''
    st.session_state.mail_link = ''
    st.session_state.mail_subject = ''
    st.session_state.mail_body = ''
    st.session_state.mail_message = ''


def render_pending_mail(context):
    if st.session_state.mail_context == context and st.session_state.mail_link:
        st.info(st.session_state.mail_message)
        st.link_button(
            'Apri bozza email',
            st.session_state.mail_link,
            use_container_width=True
        )
        with st.expander("Mostra testo email"):
            st.text_input("Oggetto", st.session_state.mail_subject, key=f"obj_{context}")
            st.text_area("Corpo", st.session_state.mail_body, height=220, key=f"body_{context}")
        if st.button("Chiudi bozza pronta", key=f"close_mail_{context}", use_container_width=True):
            clear_pending_mail()
            st.rerun()


def main():
    init_auth_state()

    try:
        asset = load_asset()
        dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    except Exception as e:
        st.set_page_config(page_title=APP_TITLE, layout='wide')
        render_header()
        st.error(f"Errore inizializzazione asset: {e}")
        st.stop()

    if not st.session_state.authenticated:
        render_login(dr_list)

    st.set_page_config(page_title=APP_TITLE, layout='wide')
    init_mail_state()
    render_header()
    render_legend()

    current_role = st.session_state.user_role
    is_control_room = current_role == 'Control Room'
    is_admin = current_role == 'Admin'
    is_dr_user = role_is_dr(current_role, dr_list)

    with st.sidebar:
        st.success(f"Accesso eseguito come: {current_role}")
        st.button("Logout", on_click=logout, use_container_width=True)

    try:
        cfg = ensure_config(dr_list)
        df_all = load_segnalazioni()
    except Exception as e:
        st.error(f"Errore inizializzazione applicazione: {e}")
        st.stop()

    df = apply_role_filter(df_all, current_role, dr_list)

    m1, m2, m3 = st.columns(3)
    m1.metric('Non trattate', int((df['STATO'] == 'APERTA').sum()))
    m2.metric('Prese in carico', int((df['STATO'] == 'IN_CARICO').sum()))
    m3.metric('Verdi (Rientrate/Trattate)', int(((df['STATO'] == 'RIENTRATO') | (df['STATO'] == 'TRATTATA')).sum()))

    if is_dr_user:
        tab2, tab3, tab4, tab5 = st.tabs([
            'Non trattate / In carico (a cura GT)',
            'Segnalazioni in carico (a cura GT o referente IMC)',
            'Configurazione email',
            'Archivio'
        ])
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            'Nuova segnalazione',
            'Non trattate / In carico (a cura GT)',
            'Segnalazioni in carico (a cura GT o referente IMC)',
            'Configurazione email',
            'Archivio'
        ])

        with tab1:
            st.subheader('Nuova segnalazione')
            render_pending_mail('new')

            c1, c2 = st.columns(2)

            with c1:
                dr_options = sorted(asset['DR'].dropna().astype(str).str.strip().unique().tolist())
                dr_sel = st.selectbox("DR", dr_options)

                asset_dr = asset[asset['DR'] == dr_sel].copy()

                imc_list = sorted(asset_dr['IMC'].dropna().astype(str).str.strip().unique().tolist())
                opzioni_imc = imc_list + ["ALTRO / INSERIMENTO MANUALE"]
                imc_scelto = st.selectbox("IMC (Impianto assegnatario)", opzioni_imc)

                if imc_scelto == "ALTRO / INSERIMENTO MANUALE":
                    imc_finale = st.text_input("Inserisci IMC manualmente")
                    asset_imc = asset_dr.copy()
                else:
                    imc_finale = imc_scelto
                    asset_imc = asset_dr[asset_dr['IMC'] == imc_scelto].copy()

                rotabili_list = sorted(asset_imc['ROTABILE'].dropna().astype(str).str.strip().unique().tolist())
                opzioni_rotabile = rotabili_list + ["ALTRO / INSERIMENTO MANUALE"]
                rotabile_scelto = st.selectbox("ROTABILE", opzioni_rotabile)

                if rotabile_scelto == "ALTRO / INSERIMENTO MANUALE":
                    rotabile_finale = st.text_input("Inserisci rotabile manualmente")
                else:
                    rotabile_finale = rotabile_scelto

            with st.form('new_form'):
                c3, c4 = st.columns(2)

                with c3:
                    data_an = st.date_input("DATA ANORMALITA", value=date.today(), format="DD/MM/YYYY")
                    grav = st.selectbox(
                        "GRAVITA",
                        [
                            "Rientro immediato",
                            "Rientro da turno manutentivo",
                            "Rientro entro 24 H",
                            "Rientro entro 48 H",
                            "Rientro entro 72 H"
                        ]
                    )

                with c4:
                    n_avviso = st.text_input('N° AVVISO')
                    avaria = st.text_area('AVARIA', height=120)
                    invia = st.checkbox(
                        'Invia email ai referenti regionali subito dopo il salvataggio',
                        value=True
                    )

                ok = st.form_submit_button('Salva segnalazione e apri bozza mail', use_container_width=True)

            if ok:
                if not (dr_sel and txt(imc_finale) and txt(rotabile_finale) and avaria.strip()):
                    st.error('Compila tutti i campi obbligatori.')
                else:
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    row = {
                        'ID': next_id(df_all),
                        'DR': dr_sel,
                        'ROTABILE': txt(rotabile_finale),
                        'IMC': txt(imc_finale),
                        'DATA ANORMALITA': pd.to_datetime(data_an).strftime('%Y-%m-%d'),
                        'AVARIA': avaria.strip(),
                        'N° AVVISO': txt(n_avviso),
                        'GRAVITA': grav,
                        'DATA PRESA IN CARICO': '',
                        'DATA RIENTRO': '',
                        'DATA NOTIFICA RIENTRO': '',
                        'CONGRUENZA (SI/NO)': '',
                        'NOTE RISCONTRO': '',
                        'N° ORDINE': '',
                        'DATA RESE/RIES': '',
                        'STATO': 'APERTA',
                        'DATA CREAZIONE': now,
                        'ULTIMO AGGIORNAMENTO': now,
                        'MOTIVAZIONE CAMBIO RIENTRO': '',
                        '__PowerAppsId__': ''
                    }

                    df_all = pd.concat([df_all, pd.DataFrame([row])], ignore_index=True)
                    save_df(df_all)
                    st.success(f"Segnalazione {row['ID']} salvata.")

                    ref = cfg['dr_referenti'].get(dr_sel, {'to': '', 'cc': ''})
                    if invia and ref.get('to'):
                        sub, body = subject_new(row), body_new(row)
                        set_pending_mail(
                            'new',
                            mailto(ref['to'], ref.get('cc', ''), sub, body),
                            sub,
                            body,
                            "Bozza email pronta per i referenti regionali."
                        )
                        st.rerun()
                    elif invia:
                        st.warning('Email referenti non configurate per questa DR.')

    with tab2:
        st.subheader('Segnalazioni non trattate / In carico (a cura GT)')
        render_pending_mail('takeover')

        if is_dr_user:
            filtro_tab2 = current_role
            st.info(f"Visualizzazione limitata alla DR: {current_role}")
        else:
            filtro_tab2 = st.selectbox('Filtra per DR', ['TUTTE'] + dr_list, key='filtro_tab2')

        aperte = df[df['STATO'] == 'APERTA'].copy()
        if filtro_tab2 != 'TUTTE':
            aperte = aperte[aperte['DR'] == filtro_tab2]

        if aperte.empty:
            st.info('Non ci sono segnalazioni aperte.')

        for _, row in aperte.sort_values(by='DATA CREAZIONE', ascending=False).iterrows():
            card(row)
            with st.expander(f"Azioni segnalazione {row['ID']}"):
                data_rientro = st.date_input(
                    f"DATA RIENTRO - {row['ID']}",
                    value=date.today(),
                    format='DD/MM/YYYY',
                    key=f"rientro_{row['ID']}"
                )
                invia = st.checkbox(
                    'Invia email a Control Room Regionale',
                    value=True,
                    key=f"mail_presa_{row['ID']}"
                )

                if st.button(f"Presa in carico e apri bozza mail {row['ID']}", key=f"presa_{row['ID']}", use_container_width=True):
                    mask = df_all['ID'].astype(str) == str(row['ID'])
                    df_all.loc[mask, 'DATA PRESA IN CARICO'] = datetime.now().strftime('%Y-%m-%d')
                    df_all.loc[mask, 'DATA RIENTRO'] = pd.to_datetime(data_rientro).strftime('%Y-%m-%d')
                    df_all.loc[mask, 'STATO'] = 'IN_CARICO'
                    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    save_df(df_all)

                    upd = df_all.loc[mask].iloc[0].to_dict()
                    to = cfg.get('control_room_email', '')

                    if invia and to:
                        sub, body = subject_takeover(upd), body_takeover(upd)
                        set_pending_mail(
                            'takeover',
                            mailto(to, '', sub, body),
                            sub,
                            body,
                            "Bozza email pronta per la presa in carico."
                        )
                        st.rerun()

                    st.success('Segnalazione presa in carico.')

        st.divider()
        st.subheader('Segnalazioni prese in carico')

        carico = df[df['STATO'] == 'IN_CARICO'].copy()
        if filtro_tab2 != 'TUTTE':
            carico = carico[carico['DR'] == filtro_tab2]

        if carico.empty:
            st.info('Non ci sono segnalazioni in carico.')

        for _, row in carico.sort_values(by='ULTIMO AGGIORNAMENTO', ascending=False).iterrows():
            card(row)
            st.caption(
                f"Data presa in carico: {fmt_date(row['DATA PRESA IN CARICO'])} | "
                f"Data rientro: {fmt_date(row['DATA RIENTRO'])}"
            )

    with tab3:
        st.subheader('Segnalazioni in carico (a cura GT o referente IMC)')
        render_pending_mail('notify_return')
        render_pending_mail('reschedule')
        render_pending_mail('close')

        carico_tab3 = df[df['STATO'].isin(['IN_CARICO', 'RIENTRATO'])].copy()

        if is_dr_user:
            filtro_tab3 = current_role
            st.info(f"Visualizzazione limitata alla DR: {current_role}")
        else:
            filtro_tab3 = st.selectbox('Filtra per DR', ['TUTTE'] + dr_list, key='filtro_tab3')

        if filtro_tab3 != 'TUTTE':
            carico_tab3 = carico_tab3[carico_tab3['DR'] == filtro_tab3]

        if carico_tab3.empty:
            st.info('Non ci sono segnalazioni in carico.')

        for _, row in carico_tab3.sort_values(by='ULTIMO AGGIORNAMENTO', ascending=False).iterrows():
            card(row)
            st.caption(
                f"Data presa in carico: {fmt_date(row['DATA PRESA IN CARICO'])} | "
                f"Data rientro attuale: {fmt_date(row['DATA RIENTRO'])} | "
                f"Data notifica rientro: {fmt_date(row['DATA NOTIFICA RIENTRO'])}"
            )

            with st.expander(f"Notifica rientro {row['ID']}"):
                data_notifica_rientro = st.date_input(
                    'Data effettiva rientro in impianto',
                    value=pd.to_datetime(row['DATA NOTIFICA RIENTRO']).date() if txt(row['DATA NOTIFICA RIENTRO']) else date.today(),
                    format='DD/MM/YYYY',
                    key=f"notifica_rientro_{row['ID']}"
                )
                invia_notifica = st.checkbox(
                    'Invia email di notifica rientro alla Control Room Regionale',
                    value=True,
                    key=f"mail_notifica_rientro_{row['ID']}"
                )

                if st.button(f"Notifica rientro e apri bozza mail {row['ID']}", key=f"btn_notifica_rientro_{row['ID']}", use_container_width=True):
                    mask = df_all['ID'].astype(str) == str(row['ID'])
                    df_all.loc[mask, 'DATA NOTIFICA RIENTRO'] = pd.to_datetime(data_notifica_rientro).strftime('%Y-%m-%d')
                    df_all.loc[mask, 'STATO'] = 'RIENTRATO'
                    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    save_df(df_all)

                    upd = df_all.loc[mask].iloc[0].to_dict()
                    to = cfg.get('control_room_email', '')

                    if invia_notifica and to:
                        sub, body = subject_notify_return(upd), body_notify_return(upd)
                        set_pending_mail(
                            'notify_return',
                            mailto(to, '', sub, body),
                            sub,
                            body,
                            "Bozza email pronta per la notifica di rientro in impianto."
                        )
                        st.rerun()

                    st.success('Rientro notificato correttamente.')

            with st.expander(f"Riprogramma / Ritratta segnalazione {row['ID']}"):
                nuova_data = st.date_input(
                    'Nuova DATA RIENTRO',
                    value=pd.to_datetime(row['DATA RIENTRO']).date() if txt(row['DATA RIENTRO']) else date.today(),
                    format='DD/MM/YYYY',
                    key=f"nuova_data_{row['ID']}"
                )
                motivazione = st.text_area(
                    'Motivazione cambio rientro',
                    key=f"motivo_rientro_{row['ID']}",
                    height=100
                )
                invia_variazione = st.checkbox(
                    'Invia email di variazione alla Control Room Regionale',
                    value=True,
                    key=f"mail_variazione_{row['ID']}"
                )

                if st.button(f"Aggiorna rientro e apri bozza mail {row['ID']}", key=f"update_rientro_{row['ID']}", use_container_width=True):
                    if not txt(motivazione):
                        st.error('Inserisci la motivazione del cambio rientro.')
                    else:
                        mask = df_all['ID'].astype(str) == str(row['ID'])
                        old_date = fmt_date(df_all.loc[mask, 'DATA RIENTRO'].iloc[0])

                        df_all.loc[mask, 'DATA RIENTRO'] = pd.to_datetime(nuova_data).strftime('%Y-%m-%d')
                        df_all.loc[mask, 'MOTIVAZIONE CAMBIO RIENTRO'] = txt(motivazione)
                        df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        save_df(df_all)

                        upd = df_all.loc[mask].iloc[0].to_dict()
                        new_date = fmt_date(upd['DATA RIENTRO'])
                        to = cfg.get('control_room_email', '')

                        if invia_variazione and to:
                            sub, body = subject_reschedule(upd), body_reschedule(upd, old_date, new_date, txt(motivazione))
                            set_pending_mail(
                                'reschedule',
                                mailto(to, '', sub, body),
                                sub,
                                body,
                                "Bozza email pronta per la variazione del rientro."
                            )
                            st.rerun()

                        st.success('Data rientro aggiornata correttamente.')

            with st.expander(f"Completa lavorazione {row['ID']}"):
                c1, c2 = st.columns(2)

                with c1:
                    congr = st.selectbox('CONGRUENZA (SI/NO)', ['SI', 'NO'], key=f"cong_tab3_{row['ID']}")
                    ordine = st.text_input('N° ORDINE', key=f"ord_tab3_{row['ID']}")

                with c2:
                    data_rese = st.date_input(
                        'DATA RESE/RIES',
                        value=date.today(),
                        format='DD/MM/YYYY',
                        key=f"rese_tab3_{row['ID']}"
                    )
                    invia = st.checkbox(
                        'Invia email di chiusura alla Control Room Regionale',
                        value=True,
                        key=f"mail_close_tab3_{row['ID']}"
                    )

                note = st.text_area('NOTE RISCONTRO', key=f"note_tab3_{row['ID']}", height=100)

                if st.button(f"Chiudi segnalazione e apri bozza mail {row['ID']}", key=f"close_tab3_{row['ID']}", use_container_width=True):
                    mask = df_all['ID'].astype(str) == str(row['ID'])
                    df_all.loc[mask, 'CONGRUENZA (SI/NO)'] = congr
                    df_all.loc[mask, 'NOTE RISCONTRO'] = note
                    df_all.loc[mask, 'N° ORDINE'] = ordine
                    df_all.loc[mask, 'DATA RESE/RIES'] = pd.to_datetime(data_rese).strftime('%Y-%m-%d')
                    df_all.loc[mask, 'STATO'] = 'TRATTATA'
                    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    save_df(df_all)

                    upd = df_all.loc[mask].iloc[0].to_dict()
                    to = cfg.get('control_room_email', '')

                    if invia and to:
                        sub, body = subject_closed(upd), body_closed(upd)
                        set_pending_mail(
                            'close',
                            mailto(to, '', sub, body),
                            sub,
                            body,
                            "Bozza email pronta per la chiusura della segnalazione."
                        )
                        st.rerun()

                    st.success('Segnalazione chiusa e archiviata.')

    with tab4:
        st.subheader('Configurazione email regionali')

        if is_dr_user:
            st.info(f"Puoi modificare solo i riferimenti della tua DR: {current_role}")
            rows = [{
                'DR': current_role,
                'EMAIL_TO': cfg.get('dr_referenti', {}).get(current_role, {}).get('to', ''),
                'EMAIL_CC': cfg.get('dr_referenti', {}).get(current_role, {}).get('cc', '')
            }]

            with st.form("config_email_form_dr"):
                st.text_input(
                    'Email Control Room Regionale',
                    value=cfg.get('control_room_email', ''),
                    disabled=True
                )

                edit = st.data_editor(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    num_rows='fixed',
                    hide_index=True,
                    key='editor_email_dr_single'
                )

                salva_cfg = st.form_submit_button('Salva configurazione email', use_container_width=True)

            if salva_cfg:
                dr_nome = current_role
                cfg['dr_referenti'][dr_nome] = {
                    'to': txt(edit.iloc[0]['EMAIL_TO']),
                    'cc': txt(edit.iloc[0]['EMAIL_CC'])
                }

                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)

                st.success('Configurazione email aggiornata.')
                st.rerun()

        else:
            with st.form("config_email_form"):
                control = st.text_input(
                    'Email Control Room Regionale',
                    value=cfg.get('control_room_email', '')
                )

                rows = [
                    {
                        'DR': dr,
                        'EMAIL_TO': cfg.get('dr_referenti', {}).get(dr, {}).get('to', ''),
                        'EMAIL_CC': cfg.get('dr_referenti', {}).get(dr, {}).get('cc', '')
                    }
                    for dr in dr_list
                ]

                edit = st.data_editor(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    num_rows='fixed',
                    hide_index=True,
                    key='editor_email_dr'
                )

                salva_cfg = st.form_submit_button('Salva configurazione email', use_container_width=True)

            if salva_cfg:
                out = {
                    'control_room_email': txt(control),
                    'dr_referenti': {}
                }

                for _, r in edit.iterrows():
                    dr_nome = txt(r['DR'])
                    out['dr_referenti'][dr_nome] = {
                        'to': txt(r['EMAIL_TO']),
                        'cc': txt(r['EMAIL_CC'])
                    }

                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(out, f, ensure_ascii=False, indent=2)

                st.success('Configurazione email aggiornata.')
                st.rerun()

    with tab5:
        st.subheader('Archivio completo')

        filtro1, filtro2, filtro3 = st.columns(3)

        with filtro1:
            rotabili_archivio = sorted([
                x for x in df['ROTABILE'].dropna().astype(str).str.strip().unique().tolist()
                if x and x.lower() != 'nan'
            ])
            rotabile_sel = st.selectbox(
                'Filtra per ROTABILE',
                ['TUTTI'] + rotabili_archivio,
                key='archivio_rotabile'
            )

        with filtro2:
            data_presa_da = st.date_input(
                'DATA PRESA IN CARICO da',
                value=None,
                format='DD/MM/YYYY',
                key='archivio_data_da'
            )

        with filtro3:
            data_presa_a = st.date_input(
                'DATA PRESA IN CARICO a',
                value=None,
                format='DD/MM/YYYY',
                key='archivio_data_a'
            )

        view_raw = df.copy()

        if rotabile_sel != 'TUTTI':
            view_raw = view_raw[view_raw['ROTABILE'].astype(str).str.strip() == rotabile_sel]

        data_presa_series = pd.to_datetime(view_raw['DATA PRESA IN CARICO'], errors='coerce')

        if data_presa_da is not None:
            view_raw = view_raw[data_presa_series >= pd.to_datetime(data_presa_da)]
            data_presa_series = pd.to_datetime(view_raw['DATA PRESA IN CARICO'], errors='coerce')

        if data_presa_a is not None:
            view_raw = view_raw[data_presa_series <= pd.to_datetime(data_presa_a)]

        st.caption(f"Record trovati: {len(view_raw)}")

        view = view_raw.copy()
        for c in ['DATA ANORMALITA', 'DATA PRESA IN CARICO', 'DATA RIENTRO', 'DATA NOTIFICA RIENTRO', 'DATA RESE/RIES']:
            view[c] = view[c].apply(fmt_date)

        view = view.fillna('')

        c1, c2 = st.columns([3, 1])

        with c1:
            st.dataframe(view, use_container_width=True, hide_index=True)

        with c2:
            excel_bytes = dataframe_to_excel_bytes(view)
            nome_file = f"Archivio_Rientri_Manutentivi_Filtrato_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

            st.download_button(
                label="Scarica Excel filtrato",
                data=excel_bytes,
                file_name=nome_file,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


if __name__ == '__main__':
    main()





