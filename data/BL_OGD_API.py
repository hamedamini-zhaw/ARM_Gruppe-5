import requests
import pandas as pd
import time

def fetch_dataset(dataset_id, where_clause=""):
    """Holt Daten von der data.bl.ch API mit Pagination."""
    base_url = f"https://data.bl.ch/api/explore/v2.1/catalog/datasets/{dataset_id}/records"
    all_records = []
    limit = 100
    offset = 0
    
    while True:
        params = {"limit": limit, "offset": offset, "where": where_clause}
        try:
            response = requests.get(base_url, params=params, timeout=30)
            if response.status_code != 200:
                print(f"  ❌ Fehler {response.status_code} bei ID {dataset_id}")
                break
            
            payload = response.json()
            results = payload.get("results", [])
            if not results:
                break
                
            all_records.extend(results)
            if len(results) < limit:
                break
            offset += limit
            time.sleep(0.1) # Schutz für die API-Stabilität
        except Exception as e:  
            print(f"  ❌ Verbindungsfehler: {e}")
            break
            
    return pd.DataFrame(all_records)

# --- KONFIGURATION ---

# Mapping: Layer-ID -> Name der Gemeinde-Spalte in diesem Datensatz
config = {
    "12880": "gemeindename",
    "12900": "gemeindename",
    "13030": "gemeinde_text",
    "13010": "gemeinde_text",
    "10680": "gemeinde",
    "12070": "gemeinde",
    "10200": "gemeinde",
    "10180": "bfs_bezeichnung",
    "10230": "gemeinde",
    "10060": "gemeinde",
    "10580": "gemeinde",
    "10630": "gemeinde", # Steuerdaten (wird speziell gefiltert)
    "10080": "gemeinde", # Nationalität
}

# Deine Ziel-Gemeinden
target_municipalities = [
    "Anwil", "Arisdorf", "Arlesheim", "Augst", "Itingen", 
    "Muttenz", "Waldenburg", "Sissach", "Liestal", "Prattln"
]

print(f"🚀 Starte Download-Prozess für {len(config)} Datensätze...")

for ds_id, m_col in config.items():
    # 1. Basis-Filter für die Gemeinden erstellen
    # Sucht nach "Gemeindename*" (findet auch Varianten mit "(BL)")
    m_filters = [f'{m_col} like "{m}*"' for m in target_municipalities]
    where_query = "(" + " OR ".join(m_filters) + ")"

    # 2. SPEZIAL-FILTER für Layer 10630 (Steuern)
    # Reduziert die Daten auf die zwei wichtigen Indikatoren
    if ds_id == "10630":
        indikator_filter = '(indikator = "Anzahl_Steuerpflichtige" OR indikator = "Steuerbares_Einkommen_CHF")'
        where_query += f" AND {indikator_filter}"

    print(f"⏳ Verarbeite ID {ds_id} ...", end="\r")
    
    # API Abfrage ausführen
    df = fetch_dataset(ds_id, where_query)
    
    if not df.empty:
        # Als CSV speichern für das Master-Skript
        filename = f"export_{ds_id}.csv"
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"  ✅ ID {ds_id}: {len(df)} Zeilen gespeichert.")
    else:
        print(f"  ℹ️ ID {ds_id}: Keine Daten gefunden (Spaltennamen prüfen).")

print("\n✨ Alle Exporte liegen im Ordner bereit.")