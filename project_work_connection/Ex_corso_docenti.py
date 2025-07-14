import datetime # Utile se ci sono campi data
from file_di_config import fetch_api_data, execute_query, get_all_id_corso_anno_from_db

# NOTA: La funzione transform_corso_docenti_data ora accetta anche 'id_corso_anno' come parametro
def transform_corso_docenti_data(raw_data: list, id_corso_anno: int) -> list:
    """
    Trasforma i dati grezzi dei docenti associati ai corsi nel formato DB desiderato.
    Aggiunge l'id_corso_anno ai record trasformati poiché non è incluso nella risposta API.
    """
    transformed_records = []
    
    for record in raw_data:
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue

        try:
            transformed_record = {
                # Campi chiave (inclusi quelli per la chiave primaria composita)
                "id_corso_anno_fk": id_corso_anno, # Preso dal parametro della funzione
                "id_docente_fk": record.get("idUtente"), # ID del docente dalla API

                # Dettagli del Docente (denormalizzati)
                "cognome_docente": record.get("Cognome"),
                "nome_docente": record.get("Nome"),
                "nome_materia_insegnata": record.get("Materia"), # Nome della materia insegnata in quel corso
                "monte_ore_assegnato": int(record.get("MonteOre")) if record.get("MonteOre") else None,
                "ore_lavorate": int(record.get("OreLavorate")) if record.get("OreLavorate") else None,
            }
            
            # Controllo essenziale per la chiave primaria composita
            if transformed_record["id_corso_anno_fk"] is None or transformed_record["id_docente_fk"] is None:
                print(f"❌ Record corso_docenti ignorato: ID Corso Anno o ID Docente mancante/non valido. Record originale: {record}")
                continue

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record corso_docenti: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record corso_docenti: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'corso_docenti'.")
    return transformed_records


def load_corso_docenti_data(data: list):
    """Carica i dati trasformati dei docenti associati ai corsi nel database."""
    if not data:
        print("Nessun dato da caricare per 'corso_docenti'.")
        return

    # **SCHEMA ESATTO DELLA TABELLA 'corso_docenti' - AGGIORNATO (FK docente rimossa)!**
    # Chiave primaria composita: (id_corso_anno_fk, id_docente_fk)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS corso_docenti (
        id_corso_anno_fk INT NOT NULL,                 -- FK verso 'corsi' (id_corso_anno)
        id_docente_fk VARCHAR(255) NOT NULL,           -- ID del docente

        cognome_docente VARCHAR(255),
        nome_docente VARCHAR(255),
        nome_materia_insegnata VARCHAR(255),
        monte_ore_assegnato INT,
        ore_lavorate INT,

        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id_corso_anno_fk, id_docente_fk), -- Chiave primaria composita
        CONSTRAINT fk_corso_docenti_corso FOREIGN KEY (id_corso_anno_fk) REFERENCES corsi (id_corso_anno)
        -- *** NOTA: Il vincolo FOREIGN KEY "fk_corso_docenti_docente" È STATO RIMOSSO ***
        -- Questo è stato fatto per permettere l'inserimento di dati anche se gli ID docente
        -- non sono presenti nella tabella 'docenti', a causa di inconsistenza nei dati API.
    );
    """
    execute_query(create_table_query)
    print("Tabella 'corso_docenti' verificata/creata.")

    # **INSERT/UPDATE AGGIORNATO CON LE NUOVE COLONNE**
    insert_query = """
    INSERT INTO corso_docenti (
        id_corso_anno_fk, id_docente_fk, cognome_docente, nome_docente, 
        nome_materia_insegnata, monte_ore_assegnato, ore_lavorate
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_corso_anno_fk, id_docente_fk) DO UPDATE SET
        cognome_docente = EXCLUDED.cognome_docente,
        nome_docente = EXCLUDED.nome_docente,
        nome_materia_insegnata = EXCLUDED.nome_materia_insegnata,
        monte_ore_assegnato = EXCLUDED.monte_ore_assegnato,
        ore_lavorate = EXCLUDED.ore_lavorate,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_corso_anno_fk'],
                record['id_docente_fk'],
                record['cognome_docente'],
                record['nome_docente'],
                record['nome_materia_insegnata'],
                record['monte_ore_assegnato'],
                record['ore_lavorate']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento docente '{record.get('id_docente_fk', 'N/A')}' per corso '{record.get('id_corso_anno_fk', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'corso_docenti'.")


def run_Ex_corso_docenti(): # Nome della funzione coerente con il nome del file
    """
    Orchestra il processo ETL per l'endpoint 'corso_docenti',
    iterando su tutti gli id_corso_anno.
    """
    print("\n--- Avvio processo ETL per 'corso_docenti' ---")
    try:
        # --- SCRIPT SQL PER CANCELLARE LA TABELLA (OPZIONALE) ---
        print("🗑️ Tentativo di eliminare la tabella 'corso_docenti' (se esiste)...")
        drop_table_corso_docenti_query = """
        DROP TABLE IF EXISTS corso_docenti CASCADE;
        """
        execute_query(drop_table_corso_docenti_query)
        print("✅ Tabella 'corso_docenti' eliminata (o non esisteva).")
        # --- FINE SCRIPT SQL DI CANCELLAZIONE ---

        all_corso_ids = get_all_id_corso_anno_from_db()
        if not all_corso_ids:
            print("🛑 Nessun 'id_corso_anno' trovato nel database. Assicurati che 'Ex_corsi.py' sia stato eseguito.")
            return

        all_transformed_data = [] # Modificato per raccogliere dati già trasformati
        for corso_id in all_corso_ids:
            # Effettua la chiamata API per ogni ID corso
            raw_data_for_id = fetch_api_data(endpoint="corso_docenti", params={"idCorsoAnno": corso_id})
            
            # Trasforma i dati per questo ID corso e aggiungi l'ID corso ai record
            if raw_data_for_id: # Solo se ci sono dati per questo ID corso
                transformed_batch = transform_corso_docenti_data(raw_data_for_id, id_corso_anno=corso_id)
                all_transformed_data.extend(transformed_batch)

        if not all_transformed_data: # Controlla la lista aggregata di dati trasformati
            print("Nessun dato grezzo o trasformato recuperato per 'corso_docenti'. Terminazione.")
            return

        load_corso_docenti_data(all_transformed_data) 
        print("--- Processo ETL per 'corso_docenti' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'corso_docenti': {e}")

if __name__ == "__main__":
    run_Ex_corso_docenti()