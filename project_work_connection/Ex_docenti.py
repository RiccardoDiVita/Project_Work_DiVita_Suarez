# Importa le funzioni e le configurazioni da file_di_config
from file_di_config import fetch_api_data, execute_query, API_KEY

def transform_docenti_data(raw_data: list) -> list:
    """
    Trasforma i dati grezzi dei docenti nel formato DB desiderato.
    """
    transformed_records = []
    for record in raw_data:
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue

        try:
            transformed_record = {
                "id_docente": record.get("idUtente"), # Sembra essere l'ID univoco del docente
                "cognome_docente": record.get("Cognome"),
                "nome_docente": record.get("Nome"),
                "email_docente": record.get("Email")
            }
            
            if transformed_record["id_docente"] is None:
                print(f"❌ Record docente ignorato: 'idUtente' è mancante. Record: {record}")
                continue

            transformed_records.append(transformed_record)

        except Exception as e:
            print(f"⚠️ Errore durante la trasformazione del record docente: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'docenti'.")
    return transformed_records


def load_docenti_data(data: list):
    """Carica i dati trasformati dei docenti nel database."""
    if not data:
        print("Nessun dato da caricare per 'docenti'.")
        return

    create_table_query = """
    CREATE TABLE IF NOT EXISTS docenti (
        id_docente VARCHAR(255) PRIMARY KEY,     -- Da 'idUtente' (presunto ID univoco)
        cognome_docente VARCHAR(255) NOT NULL,   -- Da 'Cognome'
        nome_docente VARCHAR(255) NOT NULL,      -- Da 'Nome'
        email_docente VARCHAR(255),              -- Da 'Email'
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    execute_query(create_table_query)
    print("Tabella 'docenti' verificata/creata.")

    insert_query = """
    INSERT INTO docenti (
        id_docente, cognome_docente, nome_docente, email_docente
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id_docente) DO UPDATE SET
        cognome_docente = EXCLUDED.cognome_docente,
        nome_docente = EXCLUDED.nome_docente,
        email_docente = EXCLUDED.email_docente,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_docente'],
                record['cognome_docente'],
                record['nome_docente'],
                record['email_docente']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento record '{record.get('id_docente', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'docenti'.")


def run_Ex_docenti(): # Nome della funzione coerente con il nome del file
    """Orchestra il processo ETL per l'endpoint 'docenti'."""
    print("\n--- Avvio processo ETL per 'docenti' ---")
    try:
        # --- SCRIPT SQL PER CANCELLARE LA TABELLA DOCENTI ---
        print("🗑️ Tentativo di eliminare la tabella 'docenti' (se esiste)...")
        drop_table_docenti_query = """
        DROP TABLE IF EXISTS docenti CASCADE;
        """
        execute_query(drop_table_docenti_query)
        print("✅ Tabella 'docenti' eliminata (o non esisteva).")
        # --- FINE SCRIPT SQL DI CANCELLAZIONE ---

        raw_data = fetch_api_data(endpoint="docenti") # Chiamata all'endpoint 'docenti'
        if not raw_data: return

        transformed_data = transform_docenti_data(raw_data)
        if not transformed_data: return

        load_docenti_data(transformed_data)
        print("--- Processo ETL per 'docenti' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'docenti': {e}")

if __name__ == "__main__":
    run_Ex_docenti()