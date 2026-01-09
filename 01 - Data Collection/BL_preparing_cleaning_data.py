import pandas as pd
import os
import glob
import re

# --- 1. KONFIGURATION ---
SOURCE_FOLDER = '/workspaces/ARM_Gruppe-5/01 - Data Collection/exporte' 
OUTPUT_FILE = '/workspaces/ARM_Gruppe-5/01 - Data Collection/ARM_Master_Table.csv'

LAYER_CONFIGS = {
    "10060": {"pivot": ["haushaltsgrosse"], "value": ["wert"]},
    "10080": {"pivot": ["nationalitaet"], "value": ["anzahl_personen"]}, 
    "10180": {"pivot": ["staatsangehoerigkeit_kategorie"], "value": ["anzahl"]}, 
    "10200": {"pivot": [], "value": ["falle", "flache_in_m2", "quadratmeterpreis_chf"]},
    "10230": {"pivot": [], "value": ["neu_erstellte_wohnungen"]},
    "10580": {"pivot": ["indikator"], "value": ["wert"]},
    "10630": {"pivot": ["indikator"], "value": ["wert"]}, 
    "10680": {"pivot": [], "value": ["anfangsbestand","geburten", "todesfaelle", "zuzuege", "wegzuege", "wanderungssaldo", "endbestand"]},
    "12070": {"pivot": [], "value": ["falle", "flache_in_m2", "quadratmeterpreis_chf"]},
    "12880": {"pivot": [], "value": ["anzahl_bewilligungen"]},
    "12900": {"pivot": [], "value": ["anzahl_bauprojekte"]},
    "13010": {"pivot": ["anzahl_zimmer"], "value": ["schatzwert"]},
    "13030": {"pivot": ["bewohnertyp_text"], "value": ["schatzwert"]}
}

BFS_NAME_MAP = {}

def master_slug(s):
    if pd.isna(s) or s == "": return "unknown"
    s = str(s).lower().strip().replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
    s = s.replace('+', 'plus').replace('/', '_').replace('.', '').replace('-', '_')
    return re.sub(r'[^a-z0-9]+', '_', s).strip('_')

def parse_years_rollout(val):
    val_str = str(val).strip()
    range_match = re.match(r'(\d{4})[/-](\d{4})', val_str)
    if range_match:
        start, end = int(range_match.group(1)), int(range_match.group(2))
        return list(range(start, end + 1))
    single_match = re.search(r'(\d{4})', val_str)
    return [int(single_match.group(1))] if single_match else []

def clean_and_pivot_v26(df, layer_id, config):
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 1. ID-Mapping
    bfs_p = ['bfs_gemeindenummer', 'gemeinde_code', 'gemeinde_nummer', 'bfs_nummer', 'bfs_code', 'gem_nr']
    gem_p = ['gemeinde_text', 'gemeindename', 'gemeinde', 'bezeichnung']
    bfs_col = next((c for c in df.columns if any(p in c for p in bfs_p)), None)
    gem_col = next((c for c in df.columns if c != bfs_col and any(p in c for p in gem_p)), None)
    
    if not bfs_col: return pd.DataFrame()
    df = df.rename(columns={bfs_col: 'bfs_nummer', gem_col: 'gemeinde'})

    # 2. Filter Bezirke
    df = df[~df['gemeinde'].astype(str).str.contains(r'^bezirk|^kanton|^total', case=False, na=False)].copy()

    # 3. Jahr ausrollen (x64-Multiplikation) & Filter ab 2010
    y_col = 'periode' if 'periode' in df.columns else 'jahr'
    if y_col not in df.columns: return pd.DataFrame()
    
    df['jahr_list'] = df[y_col].apply(parse_years_rollout)
    df = df.explode('jahr_list').reset_index(drop=True)
    df['jahr'] = df['jahr_list']
    df = df[df['jahr'] >= 2010].copy()

    # 4. Werte säubern
    val_cols = [v for v in config['value'] if v in df.columns]
    for v in val_cols:
        df[v] = pd.to_numeric(df[v].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0.0)

    # 5. Aggregation & Pivot
    id_keys = ['jahr', 'bfs_nummer']
    p_cols = config.get('pivot', [])
    actual_p_cols = [pc for pc in p_cols if pc in df.columns]
    
    if actual_p_cols:
        df['p_header'] = df[actual_p_cols].astype(str).agg('_'.join, axis=1).apply(master_slug)
        v_col = val_cols[0]
        df_final = df.pivot_table(index=id_keys, columns='p_header', values=v_col, aggfunc='sum').reset_index()
        
    else:
        df_final = df.groupby(id_keys)[val_cols].sum().reset_index()

    # Gemeinde-Mapping und Branding
    df_final['gemeinde'] = df_final['bfs_nummer'].map(BFS_NAME_MAP)
    df_final.columns = [c if c in id_keys or c == 'gemeinde' else f"ds{layer_id}_{master_slug(c)}" for c in df_final.columns]
    return df_final

# --- HAUPTPROZESS ---
csv_files = glob.glob(os.path.join(SOURCE_FOLDER, "*.csv"))

for f in csv_files:
    m = re.search(r'(\d{5})', os.path.basename(f))
    if m:
        try:
            tmp = pd.read_csv(f, nrows=100)
            tmp.columns = [c.lower().strip() for c in tmp.columns]
            b_c = next((c for c in tmp.columns if any(p in c for p in ['bfs_gemeindenummer', 'bfs_nummer'])), None)
            g_c = next((c for c in tmp.columns if c != b_c and any(p in c for p in ['gemeinde_text', 'gemeinde'])), None)
            if b_c and g_c:
                for _, r in tmp[[b_c, g_c]].dropna().iterrows():
                    BFS_NAME_MAP[int(r[b_c])] = str(r[g_c]).split(' (')[0].strip()
        except: continue

master_df = None
for f in csv_files:
    m = re.search(r'(\d{5})', os.path.basename(f))
    if m and m.group(1) in LAYER_CONFIGS:
        print(f"-> Integriere Layer {m.group(1)}...")
        try:
            trans = clean_and_pivot_v26(pd.read_csv(f), m.group(1), LAYER_CONFIGS[m.group(1)])
            if trans.empty: continue
            if master_df is None:
                master_df = trans
            else:
                master_df = pd.merge(master_df, trans, on=['jahr', 'bfs_nummer', 'gemeinde'], how='outer')
        except Exception as e:
            print(f"   ❌ Fehler bei {m.group(1)}: {e}")

if master_df is not None:
    master_df = master_df.groupby(['jahr', 'bfs_nummer', 'gemeinde'], as_index=False).first()
    id_cols = ['jahr', 'bfs_nummer', 'gemeinde']
    data_cols = [c for c in master_df.columns if c not in id_cols]
    data_cols.sort(key=lambda x: int(re.search(r'ds(\d+)', x).group(1)) if re.search(r'ds(\d+)', x) else 99999)
    master_df = master_df[id_cols + data_cols]
    master_df = master_df.sort_values(by=['gemeinde', 'jahr'])
    master_df.fillna(0.0).to_csv(OUTPUT_FILE, index=False)
    print(f"\n✨ FERTIG! Master-Tabelle erstellt.")