from file_di_config import fetch_api_data, execute_query, API_KEY # API_KEY è importata ma non usata per il controllo
import datetime

def transform_corsi_data(raw_data: list) -> list:
    """
    Trasforma i dati grezzi dei corsi nel formato Python/DB desiderato,
    gestendo le conversioni di tipo e i valori mancanti.
    """
    transformed_records = []
    for record in raw_data:
        try:
            # Funzione helper per convertire stringhe data "DD/MM/YYYY" in oggetti date
            # Restituisce None se la stringa è vuota o None
            def parse_date_or_none(date_str):
                if date_str:
                    try:
                        return datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
                    except ValueError:
                        print(f"⚠️ Formato data non valido '{date_str}' nel record: {record}. Impostazione a NULL.")
                        return None
                return None

            # Esegui la trasformazione mappando i nomi dei campi API ai nomi delle colonne del DB
            transformed_record = {
                "id_corso_anno": int(record.get("idCorsoAnno")) if record.get("idCorsoAnno") else None,
                "codice_corso": record.get("CodiceCorso"),
                "nome_corso": record.get("Corso"),
                "anno": int(record.get("Anno")) if record.get("Anno") else None,
                "sezione": record.get("Sezione"),
                "data_inizio": parse_date_or_none(record.get("DataInizio")),
                "data_fine": parse_date_or_none(record.get("DataFine")),
                "data_inizio_stage": parse_date_or_none(record.get("DataInizioStage")),
                "data_fine_stage": parse_date_or_none(record.get("DataFineStage")),
                "num_iscritti": int(record.get("Iscritti")) if record.get("Iscritti") else None
            }
            # Un controllo extra per il campo chiave primaria
            if transformed_record["id_corso_anno"] is None:
                print(f"❌ Record ignorato: 'idCorsoAnno' è mancante o non convertibile a INT. Record: {record}")
                continue # Salta questo record se la chiave primaria è nulla

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'corsi'.")
    return transformed_records


def load_corsi_data(data: list):
    """Carica i dati trasformati dei corsi nel database."""
    if not data:
        print("Nessun dato da caricare per 'corsi'.")
        return

    # **SCHEMA ESATTO DELLA TABELLA 'corsi' - ADATTATO AI NUOVI NOMI E TIPI**
    create_table_query = """
    CREATE TABLE IF NOT EXISTS corsi (
        id_corso_anno INT PRIMARY KEY,       -- Da 'idCorsoAnno'
        codice_corso VARCHAR(255) NOT NULL,  -- Da 'CodiceCorso'
        nome_corso TEXT NOT NULL,            -- Da 'Corso'
        anno INT,                            -- Da 'Anno'
        sezione VARCHAR(50),                 -- Da 'Sezione'
        data_inizio DATE NOT NULL,           -- Da 'DataInizio'
        data_fine DATE NOT NULL,             -- Da 'DataFine'
        data_inizio_stage DATE,              -- Da 'DataInizioStage' (potrebbe essere NULL)
        data_fine_stage DATE,                -- Da 'DataFineStage' (potrebbe essere NULL)
        num_iscritti INT,                    -- Da 'Iscritti'
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    execute_query(create_table_query)
    print("Tabella 'corsi' verificata/creata.")

    # **INSERT/UPDATE AGGIORNATO CON TUTTE LE NUOVE COLONNE E SEGNAPOSTO**
    insert_query = """
    INSERT INTO corsi (
        id_corso_anno, codice_corso, nome_corso, anno, sezione,
        data_inizio, data_fine, data_inizio_stage, data_fine_stage, num_iscritti
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_corso_anno) DO UPDATE SET
        codice_corso = EXCLUDED.codice_corso,
        nome_corso = EXCLUDED.nome_corso,
        anno = EXCLUDED.anno,
        sezione = EXCLUDED.sezione,
        data_inizio = EXCLUDED.data_inizio,
        data_fine = EXCLUDED.data_fine,
        data_inizio_stage = EXCLUDED.data_inizio_stage,
        data_fine_stage = EXCLUDED.data_fine_stage,
        num_iscritti = EXCLUDED.num_iscritti,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            # L'ordine dei valori qui DEVE CORRISPONDERE all'ordine delle colonne nell'INSERT query sopra
            execute_query(insert_query, (
                record['id_corso_anno'],
                record['codice_corso'],
                record['nome_corso'],
                record['anno'],
                record['sezione'],
                record['data_inizio'],
                record['data_fine'],
                record['data_inizio_stage'],
                record['data_fine_stage'],
                record['num_iscritti']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento del record '{record.get('id_corso_anno', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'corsi'.")


def run_Ex_corsi():
    """Orchestra il processo ETL per l'endpoint 'corsi'."""
    print("\n--- Avvio processo ETL per 'corsi' ---")
    try:
        # --- SCRIPT SQL PER CANCELLARE LE TABELLE PRIMA DELL'ELABORAZIONE ---
        print("🗑️ Tentativo di eliminare le tabelle 'corsi' e 'clienti' (se esistono)...")
        
        # Elimina la tabella corsi. CASCADE elimina anche eventuali dipendenze (es. chiavi esterne).
        drop_table_corsi_query = """
        DROP TABLE IF EXISTS corsi CASCADE;
        """
        execute_query(drop_table_corsi_query)
        
        # Elimina la tabella clienti. (Assicurati che questo sia il nome corretto se esiste)
        drop_table_clienti_query = """
        DROP TABLE IF EXISTS clienti CASCADE;
        """
        execute_query(drop_table_clienti_query)
        
        print("✅ Tabelle 'corsi' e 'clienti' eliminate (o non esistevano).")
        # --- FINE SCRIPT SQL DI CANCELLAZIONE ---

        raw_data = fetch_api_data(endpoint="corsi")
        if not raw_data: return

        transformed_data = transform_corsi_data(raw_data)
        if not transformed_data: return

        load_corsi_data(transformed_data)
        print("--- Processo ETL per 'corsi' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'corsi': {e}")

if __name__ == "__main__":
    # Avvia direttamente il processo ETL per i corsi
    run_Ex_corsi()