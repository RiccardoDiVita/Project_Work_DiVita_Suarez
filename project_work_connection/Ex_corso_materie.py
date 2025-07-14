import datetime
# Aggiorna questa riga di importazione
from file_di_config import fetch_api_data, execute_query, get_all_id_corso_anno_from_db # <--- AGGIUNTO get_all_id_corso_anno_from_db

def transform_corso_materie_data(raw_data: list) -> list:
    """
    Trasforma i dati grezzi delle materie del corso nel formato DB desiderato,
    gestendo le conversioni di tipo e i valori mancanti in base ai dati forniti.
    """
    transformed_records = []
    
    def parse_numeric_or_none(num_str):
        if isinstance(num_str, (int, float)): # Se è già un numero, restituiscilo così
            return num_str
        if isinstance(num_str, str) and num_str:
            try:
                # Sostituisce la virgola con un punto per la conversione a float
                return float(num_str.replace(',', '.'))
            except ValueError:
                return None
        return None

    for record in raw_data:
        # PRIMO CONTROLLO: Assicurati che 'record' sia un dizionario
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue # Salta questo elemento e passa al successivo

        try:
            transformed_record = {
                "id_corso_anno_fk": int(record.get("idCorsoAnno")) if record.get("idCorsoAnno") else None,
                "codice_corso_materia": record.get("CodiceCorso"), # Corretto il nome della chiave qui
                "id_materia": int(record.get("idMateria")) if record.get("idMateria") else None,
                "nome_materia": record.get("Materia"),
                "codice_materia": record.get("CodiceMateria"),
                
                # Conversione a NUMERIC (float) per tutti i campi "Ore"
                "ore_previste": parse_numeric_or_none(record.get("OrePreviste")),
                "ore_effettuate": parse_numeric_or_none(record.get("OreEffettuate")),
                "ore_effettuate_fad": parse_numeric_or_none(record.get("OreEffettuateFAD")),
                "ore_effettuate_monte_ore": parse_numeric_or_none(record.get("OreEffettuateMonteOre")),
                "ore_effettuate_monte_ore_fad": parse_numeric_or_none(record.get("OreEffettuateMonteOreFAD")),
                "ore_effettuate_no_monte_ore": parse_numeric_or_none(record.get("OreEffettuateNoMonteOre")),
                "ore_pianificate": parse_numeric_or_none(record.get("OrePianificate")),
                "ore_pianificate_monte_ore": parse_numeric_or_none(record.get("OrePianificateMonteOre")),
                "ore_pianificate_no_monte_ore": parse_numeric_or_none(record.get("OrePianificateNoMonteOre")),
                "ore_da_pianificare": parse_numeric_or_none(record.get("OreDaPianificare"))
            }
            
            # Controllo essenziale per la chiave primaria composita
            if transformed_record["id_corso_anno_fk"] is None or transformed_record["id_materia"] is None:
                print(f"❌ Record materia ignorato: 'idCorsoAnno' o 'idMateria' è mancante/non valido. Record originale: {record}")
                continue

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record materia: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record materia: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'corso_materie'.")
    return transformed_records


def load_corso_materie_data(data: list):
    """Carica i dati trasformati delle materie del corso nel database."""
    if not data:
        print("Nessun dato da caricare per 'corso_materie'.")
        return

    # SCHEMA AGGIORNATO DELLA TABELLA 'corso_materie' - CAMPI ORE A NUMERIC
    create_table_query = """
    CREATE TABLE IF NOT EXISTS corso_materie (
        id_corso_anno_fk INT NOT NULL,                 -- FK verso 'corsi' (id_corso_anno)
        id_materia INT NOT NULL,                       -- ID univoco della materia
        codice_corso_materia VARCHAR(255),             -- Codice del corso (dall'API 'corso_materie')
        nome_materia VARCHAR(255) NOT NULL,            -- Nome della materia
        codice_materia VARCHAR(255),                   -- Codice specifico della materia
        ore_previste NUMERIC(10, 2),                   -- CAMBIATO A NUMERIC
        ore_effettuate NUMERIC(10, 2),                 -- CAMBIATO A NUMERIC
        ore_effettuate_fad NUMERIC(10, 2),             -- CAMBIATO A NUMERIC
        ore_effettuate_monte_ore NUMERIC(10, 2),       -- CAMBIATO A NUMERIC
        ore_effettuate_monte_ore_fad NUMERIC(10, 2),   -- CAMBIATO A NUMERIC
        ore_effettuate_no_monte_ore NUMERIC(10, 2),    -- CAMBIATO A NUMERIC
        ore_pianificate NUMERIC(10, 2),                -- CAMBIATO A NUMERIC
        ore_pianificate_monte_ore NUMERIC(10, 2),      -- CAMBIATO A NUMERIC
        ore_pianificate_no_monte_ore NUMERIC(10, 2),   -- CAMBIATO A NUMERIC
        ore_da_pianificare NUMERIC(10, 2),             -- CAMBIATO A NUMERIC
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id_corso_anno_fk, id_materia),    -- Chiave primaria composita
        CONSTRAINT fk_corso_materia_corso FOREIGN KEY (id_corso_anno_fk) REFERENCES corsi (id_corso_anno)
    );
    """
    execute_query(create_table_query)
    print("Tabella 'corso_materie' verificata/creata.")

    # INSERT/UPDATE AGGIORNATO CON TUTTE LE NUOVE COLONNE E SEGNAPOSTO
    insert_query = """
    INSERT INTO corso_materie (
        id_corso_anno_fk, id_materia, codice_corso_materia, nome_materia, codice_materia,
        ore_previste, ore_effettuate, ore_effettuate_fad, ore_effettuate_monte_ore,
        ore_effettuate_monte_ore_fad, ore_effettuate_no_monte_ore, ore_pianificate,
        ore_pianificate_monte_ore, ore_pianificate_no_monte_ore, ore_da_pianificare
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_corso_anno_fk, id_materia) DO UPDATE SET
        codice_corso_materia = EXCLUDED.codice_corso_materia,
        nome_materia = EXCLUDED.nome_materia,
        codice_materia = EXCLUDED.codice_materia,
        ore_previste = EXCLUDED.ore_previste,
        ore_effettuate = EXCLUDED.ore_effettuate,
        ore_effettuate_fad = EXCLUDED.ore_effettuate_fad,
        ore_effettuate_monte_ore = EXCLUDED.ore_effettuate_monte_ore,
        ore_effettuate_monte_ore_fad = EXCLUDED.ore_effettuate_monte_ore_fad,
        ore_effettuate_no_monte_ore = EXCLUDED.ore_effettuate_no_monte_ore,
        ore_pianificate = EXCLUDED.ore_pianificate,
        ore_pianificate_monte_ore = EXCLUDED.ore_pianificate_monte_ore,
        ore_pianificate_no_monte_ore = EXCLUDED.ore_pianificate_no_monte_ore,
        ore_da_pianificare = EXCLUDED.ore_da_pianificare,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_corso_anno_fk'],
                record['id_materia'],
                record['codice_corso_materia'],
                record['nome_materia'],
                record['codice_materia'],
                record['ore_previste'],
                record['ore_effettuate'],
                record['ore_effettuate_fad'],
                record['ore_effettuate_monte_ore'],
                record['ore_effettuate_monte_ore_fad'],
                record['ore_effettuate_no_monte_ore'],
                record['ore_pianificate'],
                record['ore_pianificate_monte_ore'],
                record['ore_pianificate_no_monte_ore'],
                record['ore_da_pianificare']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento della materia '{record.get('nome_materia', 'N/A')}' per corso '{record.get('id_corso_anno_fk', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'corso_materie'.")

def run_Ex_corso_materie(): # Nome della funzione coerente con il nome del file
    """
    Orchestra il processo ETL per l'endpoint 'corso_materie',
    iterando su tutti gli id_corso_anno.
    """
    print("\n--- Avvio processo ETL per 'corso_materie' ---")
    try:
        # --- SCRIPT SQL PER CANCELLARE LA TABELLA (OPZIONALE) ---
        print("🗑️ Tentativo di eliminare la tabella 'corso_materie' (se esiste)...")
        drop_table_corso_materie_query = """
        DROP TABLE IF EXISTS corso_materie CASCADE;
        """
        execute_query(drop_table_corso_materie_query)
        print("✅ Tabella 'corso_materie' eliminata (o non esisteva).")
        # --- FINE SCRIPT SQL DI CANCELLAZIONE ---

        # Questa chiamata userà ora la funzione importata da file_di_config.py
        all_corso_ids = get_all_id_corso_anno_from_db() 
        if not all_corso_ids:
            print("🛑 Nessun 'id_corso_anno' trovato nel database. Assicurati che 'Ex_corsi.py' sia stato eseguito.")
            return

        all_raw_data = []
        for corso_id in all_corso_ids:
            raw_data_for_id = fetch_api_data(endpoint="corso_materie", params={"idCorsoAnno": corso_id})
            all_raw_data.extend(raw_data_for_id)

        if not all_raw_data:
            print("Nessun dato grezzo recuperato per 'corso_materie'. Terminazione.")
            return

        transformed_data = transform_corso_materie_data(all_raw_data)
        if not transformed_data:
            print("Nessun dato trasformato per 'corso_materie'. Terminazione.")
            return

        load_corso_materie_data(transformed_data)
        print("--- Processo ETL per 'corso_materie' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'corso_materie': {e}")

if __name__ == "__main__":
    run_Ex_corso_materie()