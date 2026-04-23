
import os
import json
from io import BytesIO
from datetime import datetime, date
from urllib.parse import quote

import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file
from sqlalchemy import create_engine, text

APP_TITLE = "Rientri Manutentivi"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSET_FILE = os.path.join(BASE_DIR, "asset DOR.xlsx")
CONFIG_FILE = os.path.join(BASE_DIR, "config_email.json")
LOGO_FILE = os.path.join(BASE_DIR, "regionale.png")
SECRETS_FILE = os.path.join(BASE_DIR, "secrets.toml")
MYSQL_TABLE_NAME = "rientri_manutentivi"

DR_LIST_DEFAULT = [
    'ABRUZZO', 'CALABRIA', 'CAMPANIA', 'FRIULI-VENEZIA GIULIA', 'LAZIO',
    'MARCHE', 'PIEMONTE', 'PUGLIA', 'SARDEGNA', 'SICILIA', 'TOSCANA',
    'TRENTINO-ALTO ADIGE', 'VENETO'
]

ALL_COLUMNS = [
    'ID', 'DR', 'ROTABILE', 'IMC', 'DATA ANORMALITA', 'AVARIA', 'N° AVVISO',
    'GRAVITA', 'DATA PRESA IN CARICO', 'DATA RIENTRO', 'DATA NOTIFICA RIENTRO',
    'CONGRUENZA (SI/NO)', 'NOTE RISCONTRO', 'N° ORDINE', 'DATA RESE/RIES',
    'STATO', 'DATA CREAZIONE', 'ULTIMO AGGIORNAMENTO', 'MOTIVAZIONE CAMBIO RIENTRO',
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

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "rientri-manutentivi-marco-secret-key")


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


def normalize_date_for_db(v):
    value = txt(v)
    if not value:
        return None
    dt = pd.to_datetime(value, errors='coerce')
    if pd.isna(dt):
        return None
    return dt.strftime('%Y-%m-%d')


def normalize_timestamp_for_db(v):
    value = txt(v)
    if not value:
        return None
    dt = pd.to_datetime(value, errors='coerce')
    if pd.isna(dt):
        return None
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def mailto(to, cc, subject, body):
    url = f"mailto:{quote(to)}?subject={quote(subject)}&body={quote(body)}"
    if cc:
        url += f"&cc={quote(cc)}"
    return url


def load_database_url():
    if os.path.exists(SECRETS_FILE):
        with open(SECRETS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABASE_URL"):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    raise ValueError("DATABASE_URL non trovato in secrets.toml")


_ENGINE = None
def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(load_database_url(), pool_recycle=280, pool_pre_ping=True)
    return _ENGINE


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


def role_is_dr(role, dr_list):
    return role in dr_list


def apply_role_filter(df, role, dr_list):
    if role_is_dr(role, dr_list):
        return df[df['DR'].astype(str).str.strip() == role].copy()
    return df.copy()


def find_column(df, candidates):
    normalized = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


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

    return out.drop_duplicates().reset_index(drop=True)


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


def save_config(cfg):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def load_segnalazioni():
    query = f"""
        SELECT
            id AS `ID`,
            dr AS `DR`,
            rotabile AS `ROTABILE`,
            imc AS `IMC`,
            data_anormalita AS `DATA ANORMALITA`,
            avaria AS `AVARIA`,
            n_avviso AS `N° AVVISO`,
            gravita AS `GRAVITA`,
            data_presa_in_carico AS `DATA PRESA IN CARICO`,
            data_rientro AS `DATA RIENTRO`,
            data_notifica_rientro AS `DATA NOTIFICA RIENTRO`,
            congruenza AS `CONGRUENZA (SI/NO)`,
            note_riscontro AS `NOTE RISCONTRO`,
            n_ordine AS `N° ORDINE`,
            data_rese_ries AS `DATA RESE/RIES`,
            stato AS `STATO`,
            data_creazione AS `DATA CREAZIONE`,
            ultimo_aggiornamento AS `ULTIMO AGGIORNAMENTO`,
            motivazione_cambio_rientro AS `MOTIVAZIONE CAMBIO RIENTRO`,
            powerapps_id AS `__PowerAppsId__`
        FROM {MYSQL_TABLE_NAME}
        ORDER BY ultimo_aggiornamento DESC, data_creazione DESC
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)

    for c in ALL_COLUMNS:
        if c not in df.columns:
            df[c] = ''

    df = df[ALL_COLUMNS]
    df['STATO'] = df['STATO'].replace('', pd.NA).fillna('TRATTATA')
    return df


def save_df(df):
    df_to_save = df.copy().fillna('')

    delete_sql = text(f"DELETE FROM {MYSQL_TABLE_NAME}")
    insert_sql = text(f"""
        INSERT INTO {MYSQL_TABLE_NAME} (
            id, dr, rotabile, imc, data_anormalita, avaria, n_avviso, gravita,
            data_presa_in_carico, data_rientro, data_notifica_rientro,
            congruenza, note_riscontro, n_ordine, data_rese_ries,
            stato, data_creazione, ultimo_aggiornamento,
            motivazione_cambio_rientro, powerapps_id
        ) VALUES (
            :id, :dr, :rotabile, :imc, :data_anormalita, :avaria, :n_avviso, :gravita,
            :data_presa_in_carico, :data_rientro, :data_notifica_rientro,
            :congruenza, :note_riscontro, :n_ordine, :data_rese_ries,
            :stato, :data_creazione, :ultimo_aggiornamento,
            :motivazione_cambio_rientro, :powerapps_id
        )
    """)

    with get_engine().begin() as conn:
        conn.execute(delete_sql)
        for _, row in df_to_save.iterrows():
            conn.execute(insert_sql, {
                "id": txt(row['ID']),
                "dr": txt(row['DR']),
                "rotabile": txt(row['ROTABILE']),
                "imc": txt(row['IMC']),
                "data_anormalita": normalize_date_for_db(row['DATA ANORMALITA']),
                "avaria": txt(row['AVARIA']),
                "n_avviso": txt(row['N° AVVISO']),
                "gravita": txt(row['GRAVITA']),
                "data_presa_in_carico": normalize_date_for_db(row['DATA PRESA IN CARICO']),
                "data_rientro": normalize_date_for_db(row['DATA RIENTRO']),
                "data_notifica_rientro": normalize_date_for_db(row['DATA NOTIFICA RIENTRO']),
                "congruenza": txt(row['CONGRUENZA (SI/NO)']),
                "note_riscontro": txt(row['NOTE RISCONTRO']),
                "n_ordine": txt(row['N° ORDINE']),
                "data_rese_ries": normalize_date_for_db(row['DATA RESE/RIES']),
                "stato": txt(row['STATO']),
                "data_creazione": normalize_timestamp_for_db(row['DATA CREAZIONE']),
                "ultimo_aggiornamento": normalize_timestamp_for_db(row['ULTIMO AGGIORNAMENTO']),
                "motivazione_cambio_rientro": txt(row['MOTIVAZIONE CAMBIO RIENTRO']),
                "powerapps_id": txt(row['__PowerAppsId__'])
            })


def dataframe_to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Archivio')
    output.seek(0)
    return output


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

    return (data_rientro.normalize() - data_anom.normalize()).days > limite


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


def require_login():
    if not session.get('authenticated'):
        return redirect(url_for('login'))
    return None


@app.context_processor
def inject_globals():
    return {
        'app_title': APP_TITLE,
        'status_colors': STATUS_COLORS,
        'status_labels': STATUS_LABELS,
        'current_role': session.get('user_role', ''),
        'is_dr_user': session.get('user_role') in session.get('dr_list', [])
    }


@app.route('/logo')
def logo():
    return send_file(LOGO_FILE)


@app.route('/logout')
def logout():
    session.clear()
    flash('Logout eseguito.', 'info')
    return redirect(url_for('login'))


@app.route('/')
def root():
    return redirect(url_for('dashboard') if session.get('authenticated') else url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    asset = load_asset()
    dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    session['dr_list'] = dr_list

    if request.method == 'POST':
        ruolo = request.form.get('ruolo', '')
        password = request.form.get('password', '')
        if password == build_password_map(dr_list).get(ruolo, ''):
            session['authenticated'] = True
            session['user_role'] = ruolo
            flash('Accesso eseguito correttamente.', 'success')
            return redirect(url_for('dashboard'))
        flash('Password non corretta.', 'danger')

    return render_template('login.html', dr_list=dr_list)


@app.route('/dashboard')
def dashboard():
    rl = require_login()
    if rl:
        return rl

    asset = load_asset()
    dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    cfg = ensure_config(dr_list)
    df_all = load_segnalazioni()
    current_role = session['user_role']
    is_dr_user = role_is_dr(current_role, dr_list)
    df = apply_role_filter(df_all, current_role, dr_list)

    filtro_dr = request.args.get('filtro_dr', current_role if is_dr_user else 'TUTTE')
    archivio_rotabile = request.args.get('rotabile', 'TUTTI')

    aperte = df[df['STATO'] == 'APERTA'].copy()
    carico = df[df['STATO'] == 'IN_CARICO'].copy()
    carico_tab3 = df[df['STATO'].isin(['IN_CARICO', 'RIENTRATO'])].copy()

    if filtro_dr != 'TUTTE':
        aperte = aperte[aperte['DR'] == filtro_dr]
        carico = carico[carico['DR'] == filtro_dr]
        carico_tab3 = carico_tab3[carico_tab3['DR'] == filtro_dr]

    rotabili_archivio = sorted([
        x for x in df['ROTABILE'].dropna().astype(str).str.strip().unique().tolist()
        if x and x.lower() != 'nan'
    ])

    view_raw = df.copy()
    if archivio_rotabile != 'TUTTI':
        view_raw = view_raw[view_raw['ROTABILE'].astype(str).str.strip() == archivio_rotabile]

    for c in ['DATA ANORMALITA', 'DATA PRESA IN CARICO', 'DATA RIENTRO', 'DATA NOTIFICA RIENTRO', 'DATA RESE/RIES']:
        view_raw[c] = view_raw[c].apply(fmt_date)

    return render_template(
        'dashboard.html',
        dr_list=dr_list,
        cfg=cfg,
        filtro_dr=filtro_dr,
        aperte=aperte.sort_values(by='DATA CREAZIONE', ascending=False).to_dict('records'),
        carico=carico.sort_values(by='ULTIMO AGGIORNAMENTO', ascending=False).to_dict('records'),
        carico_tab3=carico_tab3.sort_values(by='ULTIMO AGGIORNAMENTO', ascending=False).to_dict('records'),
        is_dr_user=is_dr_user,
        current_role=current_role,
        rotabili_archivio=rotabili_archivio,
        view_raw=view_raw.fillna('').to_dict('records'),
        non_trattate=int((df['STATO'] == 'APERTA').sum()),
        prese_in_carico=int((df['STATO'] == 'IN_CARICO').sum()),
        verdi=int(((df['STATO'] == 'RIENTRATO') | (df['STATO'] == 'TRATTATA')).sum()),
        today=date.today().strftime('%Y-%m-%d')
    )


@app.post('/nuova')
def nuova():
    rl = require_login()
    if rl:
        return rl

    current_role = session['user_role']
    asset = load_asset()
    dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    if role_is_dr(current_role, dr_list):
        flash('Le DR non possono inserire nuove segnalazioni.', 'danger')
        return redirect(url_for('dashboard'))

    cfg = ensure_config(dr_list)
    df_all = load_segnalazioni()

    dr_sel = txt(request.form.get('dr'))
    imc_finale = txt(request.form.get('imc_finale'))
    rotabile_finale = txt(request.form.get('rotabile_finale'))
    avaria = txt(request.form.get('avaria'))
    data_an = txt(request.form.get('data_an'))
    grav = txt(request.form.get('gravita'))
    n_avviso = txt(request.form.get('n_avviso'))
    invia = request.form.get('invia') == 'on'

    if not (dr_sel and imc_finale and rotabile_finale and avaria):
        flash('Compila tutti i campi obbligatori.', 'danger')
        return redirect(url_for('dashboard'))

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = {
        'ID': next_id(df_all),
        'DR': dr_sel,
        'ROTABILE': rotabile_finale,
        'IMC': imc_finale,
        'DATA ANORMALITA': pd.to_datetime(data_an).strftime('%Y-%m-%d'),
        'AVARIA': avaria,
        'N° AVVISO': n_avviso,
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
    flash(f"Segnalazione {row['ID']} salvata.", 'success')

    ref = cfg['dr_referenti'].get(dr_sel, {'to': '', 'cc': ''})
    if invia and ref.get('to'):
        return redirect(mailto(ref['to'], ref.get('cc', ''), subject_new(row), body_new(row)))

    if invia:
        flash('Email referenti non configurate per questa DR.', 'warning')

    return redirect(url_for('dashboard'))


@app.post('/presa_in_carico/<segn_id>')
def presa_in_carico(segn_id):
    rl = require_login()
    if rl:
        return rl

    cfg = ensure_config(session.get('dr_list', DR_LIST_DEFAULT))
    df_all = load_segnalazioni()
    mask = df_all['ID'].astype(str) == str(segn_id)

    if not mask.any():
        flash('Segnalazione non trovata.', 'danger')
        return redirect(url_for('dashboard'))

    df_all.loc[mask, 'DATA PRESA IN CARICO'] = datetime.now().strftime('%Y-%m-%d')
    df_all.loc[mask, 'DATA RIENTRO'] = pd.to_datetime(txt(request.form.get('data_rientro'))).strftime('%Y-%m-%d')
    df_all.loc[mask, 'STATO'] = 'IN_CARICO'
    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_df(df_all)

    upd = df_all.loc[mask].iloc[0].to_dict()
    flash('Segnalazione presa in carico.', 'success')

    if request.form.get('invia') == 'on' and cfg.get('control_room_email'):
        return redirect(mailto(cfg['control_room_email'], '', subject_takeover(upd), body_takeover(upd)))

    return redirect(url_for('dashboard'))


@app.post('/notifica_rientro/<segn_id>')
def notifica_rientro(segn_id):
    rl = require_login()
    if rl:
        return rl

    cfg = ensure_config(session.get('dr_list', DR_LIST_DEFAULT))
    df_all = load_segnalazioni()
    mask = df_all['ID'].astype(str) == str(segn_id)

    if not mask.any():
        flash('Segnalazione non trovata.', 'danger')
        return redirect(url_for('dashboard'))

    df_all.loc[mask, 'DATA NOTIFICA RIENTRO'] = pd.to_datetime(txt(request.form.get('data_notifica'))).strftime('%Y-%m-%d')
    df_all.loc[mask, 'STATO'] = 'RIENTRATO'
    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_df(df_all)

    upd = df_all.loc[mask].iloc[0].to_dict()
    flash('Rientro notificato correttamente.', 'success')

    if request.form.get('invia_notifica') == 'on' and cfg.get('control_room_email'):
        return redirect(mailto(cfg['control_room_email'], '', subject_notify_return(upd), body_notify_return(upd)))

    return redirect(url_for('dashboard'))


@app.post('/riprogramma/<segn_id>')
def riprogramma(segn_id):
    rl = require_login()
    if rl:
        return rl

    cfg = ensure_config(session.get('dr_list', DR_LIST_DEFAULT))
    df_all = load_segnalazioni()
    motivazione = txt(request.form.get('motivazione'))

    if not motivazione:
        flash('Inserisci la motivazione del cambio rientro.', 'danger')
        return redirect(url_for('dashboard'))

    mask = df_all['ID'].astype(str) == str(segn_id)
    if not mask.any():
        flash('Segnalazione non trovata.', 'danger')
        return redirect(url_for('dashboard'))

    old_date = fmt_date(df_all.loc[mask, 'DATA RIENTRO'].iloc[0])
    df_all.loc[mask, 'DATA RIENTRO'] = pd.to_datetime(txt(request.form.get('nuova_data'))).strftime('%Y-%m-%d')
    df_all.loc[mask, 'MOTIVAZIONE CAMBIO RIENTRO'] = motivazione
    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_df(df_all)

    upd = df_all.loc[mask].iloc[0].to_dict()
    flash('Data rientro aggiornata correttamente.', 'success')

    if request.form.get('invia_variazione') == 'on' and cfg.get('control_room_email'):
        return redirect(mailto(
            cfg['control_room_email'],
            '',
            subject_reschedule(upd),
            body_reschedule(upd, old_date, fmt_date(upd['DATA RIENTRO']), motivazione)
        ))

    return redirect(url_for('dashboard'))


@app.post('/chiudi/<segn_id>')
def chiudi(segn_id):
    rl = require_login()
    if rl:
        return rl

    cfg = ensure_config(session.get('dr_list', DR_LIST_DEFAULT))
    df_all = load_segnalazioni()
    mask = df_all['ID'].astype(str) == str(segn_id)

    if not mask.any():
        flash('Segnalazione non trovata.', 'danger')
        return redirect(url_for('dashboard'))

    df_all.loc[mask, 'CONGRUENZA (SI/NO)'] = txt(request.form.get('congruenza'))
    df_all.loc[mask, 'NOTE RISCONTRO'] = txt(request.form.get('note'))
    df_all.loc[mask, 'N° ORDINE'] = txt(request.form.get('ordine'))
    df_all.loc[mask, 'DATA RESE/RIES'] = pd.to_datetime(txt(request.form.get('data_rese'))).strftime('%Y-%m-%d')
    df_all.loc[mask, 'STATO'] = 'TRATTATA'
    df_all.loc[mask, 'ULTIMO AGGIORNAMENTO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_df(df_all)

    upd = df_all.loc[mask].iloc[0].to_dict()
    flash('Segnalazione chiusa e archiviata.', 'success')

    if request.form.get('invia_close') == 'on' and cfg.get('control_room_email'):
        return redirect(mailto(cfg['control_room_email'], '', subject_closed(upd), body_closed(upd)))

    return redirect(url_for('dashboard'))


@app.post('/salva_config')
def salva_config():
    rl = require_login()
    if rl:
        return rl

    asset = load_asset()
    dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    cfg = ensure_config(dr_list)
    current_role = session.get('user_role', '')

    if role_is_dr(current_role, dr_list):
        cfg['dr_referenti'][current_role] = {
            'to': txt(request.form.get('email_to')),
            'cc': txt(request.form.get('email_cc'))
        }
    else:
        cfg['control_room_email'] = txt(request.form.get('control_room_email'))
        for dr in dr_list:
            cfg['dr_referenti'][dr] = {
                'to': txt(request.form.get(f'to_{dr}', '')),
                'cc': txt(request.form.get(f'cc_{dr}', ''))
            }

    save_config(cfg)
    flash('Configurazione email aggiornata.', 'success')
    return redirect(url_for('dashboard'))


@app.get('/export_archivio')
def export_archivio():
    rl = require_login()
    if rl:
        return rl

    asset = load_asset()
    dr_list = sorted(set(DR_LIST_DEFAULT) | set(asset['DR'].dropna().astype(str).str.strip().tolist()))
    df_all = load_segnalazioni()
    df = apply_role_filter(df_all, session.get('user_role'), dr_list)

    rotabile = request.args.get('rotabile', 'TUTTI')
    data_da = request.args.get('data_da')
    data_a = request.args.get('data_a')

    view_raw = df.copy()
    if rotabile != 'TUTTI':
        view_raw = view_raw[view_raw['ROTABILE'].astype(str).str.strip() == rotabile]

    data_presa_series = pd.to_datetime(view_raw['DATA PRESA IN CARICO'], errors='coerce')
    if data_da:
        view_raw = view_raw[data_presa_series >= pd.to_datetime(data_da)]
        data_presa_series = pd.to_datetime(view_raw['DATA PRESA IN CARICO'], errors='coerce')
    if data_a:
        view_raw = view_raw[data_presa_series <= pd.to_datetime(data_a)]

    view = view_raw.copy()
    for c in ['DATA ANORMALITA', 'DATA PRESA IN CARICO', 'DATA RIENTRO', 'DATA NOTIFICA RIENTRO', 'DATA RESE/RIES']:
        view[c] = view[c].apply(fmt_date)

    output = dataframe_to_excel_bytes(view.fillna(''))
    filename = f"Archivio_Rientri_Manutentivi_Filtrato_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == '__main__':
    app.run(debug=True)







