import pandas as pd

# --- KONFIGURATION ---
INPUT_FILE = '/workspaces/ARM_Gruppe-5/01 - Data Collection/ARM_Master_Table.csv'
OUTPUT_FILE = '/workspaces/ARM_Gruppe-5/01 - Data Collection/ARM_Master_Table_final.csv'

def calculate_branded_features(df):
    # --- 1. DURCHSCHNITTLICHE HAUSHALTSGRÖSSE (Branding ds10630) ---
    # Berechnung basierend auf den Rohdaten-Spalten von Layer 10060
    hh_map = {
        'ds10060_1_person': 1,
        'ds10060_2_personen': 2,
        'ds10060_3_personen': 3,
        'ds10060_4_personen': 4,
        'ds10060_5_personen': 5,
        'ds10060_6_oder_mehr_personen': 6
    }
    
    existing_hh = [c for c in hh_map.keys() if c in df.columns]
    
    if existing_hh:
        total_hh = df[existing_hh].sum(axis=1)
        weighted_sum = sum(df[col] * hh_map[col] for col in existing_hh)
        # Neuer Name laut Anweisung: ds10630_avg_haushaltsgrosse
        df['ds10630_avg_haushaltsgrosse'] = (weighted_sum / total_hh).fillna(0).round(2)
    
    # --- 2. NATIONALITÄTEN-VERHÄLTNIS (Branding ds10080) ---
    if 'ds10080_ausland' in df.columns and 'ds10080_schweiz' in df.columns:
        total_pop = df['ds10080_ausland'] + df['ds10080_schweiz']
        
        # Neue Namen laut Anweisung: ds10080_anteil_auslaender_pct / _schweizer_pct
        df['ds10080_anteil_auslaender_pct'] = (df['ds10080_ausland'] / total_pop * 100).fillna(0).round(2)
        df['ds10080_anteil_schweizer_pct'] = (df['ds10080_schweiz'] / total_pop * 100).fillna(0).round(2)

    return df

# --- HAUPTPROZESS ---
try:
    print(f"Lese Master-Tabelle ein...")
    df = pd.read_csv(INPUT_FILE)
    
    print("Berechne branded Features...")
    df = calculate_branded_features(df)
    
    # Definition der neuen Spalten für die Sortierung
    new_cols = [
        'ds10630_avg_haushaltsgrosse', 
        'ds10080_anteil_auslaender_pct', 
        'ds10080_anteil_schweizer_pct'
    ]
    
    # Sortierung: ID-Felder, dann die neuen Layer-Werte, dann der Rest
    id_cols = ['jahr', 'bfs_nummer', 'gemeinde']
    other_cols = [c for c in df.columns if c not in id_cols + new_cols]
    
    df_final = df[id_cols + new_cols + other_cols]
    
    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"✨ Fertig! Die branded Features wurden in {OUTPUT_FILE} gespeichert.")
    
except Exception as e:
    print(f"❌ Fehler: {e}")