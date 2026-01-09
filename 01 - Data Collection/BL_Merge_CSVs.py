import pandas as pd
import os
import re

# --- 1. KONFIGURATION ---
SOURCE_FOLDER = '/workspaces/ARM_Gruppe-5/01 - Data Collection/exporte' 
OUTPUT_FILE = '01 - Data Collection/master_data.csv'

config_mapping = {
    "10060": ("gemeinde", "jahr"),
    "10080": ("gemeinde", "jahr"),
    "10180": ("bfs_bezeichnung", "jahr"),
    "10200": ("gemeinde", "jahr"),
    "10230": ("gemeinde", "jahr"),
    "10580": ("gemeinde", "jahr"),
    "10630": ("gemeinde", "jahr"),
    "10680": ("gemeinde", "jahr"),
    "12070": ("gemeinde", "jahr"),
    "12880": ("gemeindename", "jahr"),
    "12900": ("gemeindename", "jahr"),
    "13010": ("gemeinde_text", "periode"), # Periode: 2010/2014 etc.
    "13030": ("gemeinde_text", "periode"), # Periode: 2015/2019 etc.
}

def slugify(s):
    if pd.isna(s): return "unknown"
    s = str(s).lower().strip().replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')

def parse_year_range(val):
    """Wandelt '2010/2014' in [2010, 2011, 2012, 2013, 2014] um."""
    val = str(val).strip()
    # Prüfen auf Format YYYY/YYYY oder YYYY-YYYY
    match_range = re.match(r'(\d{4})[/-](\d{4})', val)
    if match_range:
        start = int(match_range.group(1))
        end = int(match_range.group(2))
        return list(range(start, end + 1))
    
    # Normalfall: Nur ein Jahr (nimmt die ersten 4 Ziffern)
    match_single = re.search(r'(\d{4})', val)
    if match_single:
        return [int(match_single.group(1))]
    
    return []

def clean_dataframe(df, m_col, y_col, ds_id):
    df.columns = [c.lower().strip() for c in df.columns]
    m_col = m_col.lower()
    y_col = y_col.lower()

    # 1. Standard-Spalten umbenennen
    df = df.rename(columns={m_col: 'gemeinde', y_col: 'jahr_raw'})
    
    # 2. Gemeinde-Namen säubern
    df['gemeinde'] = df['gemeinde'].astype(str).str.split(' (', expand=False, regex=False).str[0].str.strip()
    
    # --- NEU: EXPANSION DER JAHRE ---
    # Erstellt eine Liste von Jahren für jede Zeile
    df['jahr'] = df['jahr_raw'].apply(parse_year_range)
    # Erzeugt für jedes Jahr in der Liste eine eigene Zeile (Kartesisches Produkt pro Zeile)
    df = df.explode('jahr')
    
    df = df.dropna(subset=['jahr', 'gemeinde'])
    df['jahr'] = df['jahr'].astype(int)

    # Bezirks- und Kantons-Summen entfernen
    df = df[~df['gemeinde'].str.contains(r'bezirk|kanton|total', case=False, na=False)].copy()

    # --- PIVOT LOGIK ---
    pivot_col = None
    value_col = None

    if 'nationalitaet' in df.columns:
        pivot_col, value_col = 'nationalitaet', 'anzahl_personen'
    elif 'indikator' in df.columns:
        pivot_col = 'indikator'
        value_col = 'anzahl' if 'anzahl' in df.columns else 'wert'
    elif 'anzahl_zimmer' in df.columns:
        pivot_col, value_col = 'anzahl_zimmer', 'schatzwert'
    elif 'bewohnertyp_text' in df.columns:
        pivot_col, value_col = 'bewohnertyp_text', 'schatzwert'
    elif 'haushaltsgrosse' in df.columns:
        pivot_col, value_col = 'haushaltsgrosse', 'wert'

    if pivot_col and value_col:
        df[pivot_col] = df[pivot_col].apply(slugify)
        df = df.pivot_table(index=['jahr', 'gemeinde'], 
                            columns=pivot_col, 
                            values=value_col, 
                            aggfunc='sum').reset_index()
    else:
        df = df.groupby(['jahr', 'gemeinde']).sum(numeric_only=True).reset_index()
    
    df.columns = [f"ds{ds_id}_{c}" if c not in ['jahr', 'gemeinde'] else c for c in df.columns]
    return df

# (Der Rest des Main-Merge-Prozesses bleibt identisch zu deinem Skript)
# --- MAIN MERGE PROCESS ---
master_df = None
print("🚀 Starte Zusammenführung der Layer mit Perioden-Expansion...")

for ds_id in sorted(config_mapping.keys()):
    m_col, y_col = config_mapping[ds_id]
    file_path = os.path.join(SOURCE_FOLDER, f"export_{ds_id}.csv")
    
    if os.path.exists(file_path):
        try:
            df_raw = pd.read_csv(file_path)
            if df_raw.empty: continue
            
            df_cleaned = clean_dataframe(df_raw, m_col, y_col, ds_id)
            
            if master_df is None:
                master_df = df_cleaned
            else:
                master_df = pd.merge(master_df, df_cleaned, on=['jahr', 'gemeinde'], how='outer')
            
            print(f"  ✅ Layer {ds_id} erfolgreich integriert.")
        except Exception as e:
            print(f"  ❌ Fehler bei Layer {ds_id}: {e}")

if master_df is not None:
    id_cols = ['jahr', 'gemeinde']
    data_cols = sorted([c for c in master_df.columns if c not in id_cols], 
                       key=lambda x: int(re.search(r'ds(\d+)', x).group(1)) if re.search(r'ds(\d+)', x) else 0)
    master_df = master_df[id_cols + data_cols]
    master_df = master_df.sort_values(by=['gemeinde', 'jahr'])
    master_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\n✨ Fertig! Struktur: {master_df.shape[0]} Zeilen x {master_df.shape[1]} Spalten.")