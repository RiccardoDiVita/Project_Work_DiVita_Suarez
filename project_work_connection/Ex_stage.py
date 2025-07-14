import datetime
# Importa le funzioni e le configurazioni da file_di_config
from file_di_config import fetch_api_data, execute_query # get_all_id_corso_anno_from_db non serve più qui

# NOTA: La funzione transform_stage_data non riceverà più id_corso_anno come parametro esplicito
def transform_stage_data(raw_data: list) -> list: # rimosso **kwargs o id_corso_anno
    """
    Trasforma i dati grezzi degli stage nel formato DB desiderato,
    basandosi sulla struttura API fornita.
    """
    transformed_records = []
    
    def parse_date_or_none(date_str):
        if date_str:
            try:
                # Assumiamo il formato data "DD/MM/YYYY"
                return datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
            except ValueError:
                print(f"⚠️ Formato data '{date_str}' non valido. Impostazione a NULL.")
                return None
        return None

    for record in raw_data:
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue

        try:
            transformed_record = {
                # Campi chiave per la PK composita
                "id_alunno_fk": record.get("idAlunno"),
                "id_corso_anno_fk": int(record.get("idCorsoAnno")) if record.get("idCorsoAnno") else None,
                "data_inizio_stage": parse_date_or_none(record.get("DataInizioStage")),

                # Dettagli Alunno (denormalizzati)
                "cognome_alunno": record.get("Cognome"),
                "nome_alunno": record.get("Nome"),
                
                # Dettagli Corso (denormalizzati)
                "codice_corso_stage": record.get("CodiceCorso"), # Codice del corso associato allo stage
                "nome_corso_stage": record.get("Corso"),       # Nome del corso associato allo stage

                # Dettagli Azienda
                "id_azienda_stage": record.get("Azienda"),       # ID/Codice dell'azienda (dal campo "Azienda")
                "partita_iva_azienda": record.get("PI"),         # Partita IVA (da "PI")

                # Dettagli Stage
                "data_fine_stage": parse_date_or_none(record.get("DataFineStage")),
            }
            
            # Controllo essenziale per la chiave primaria composita
            if (transformed_record["id_alunno_fk"] is None or 
                transformed_record["id_corso_anno_fk"] is None or 
                transformed_record["data_inizio_stage"] is None):
                print(f"❌ Record stage ignorato: Campi chiave (idAlunno, idCorsoAnno, DataInizioStage) mancanti/non validi. Record originale: {record}")
                continue

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record stage: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record stage: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'stage'.")
    return transformed_records


def load_stage_data(data: list):
    """Carica i dati trasformati degli stage nel database."""
    if not data:
        print("Nessun dato da caricare per 'stage'.")
        return

    # **SCHEMA ESATTO DELLA TABELLA 'stage' - ADATTATO ALLA NUOVA STRUTTURA E PK!**
    # Chiave primaria composita: (id_alunno_fk, id_corso_anno_fk, data_inizio_stage)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stage (
        id_alunno_fk VARCHAR(255) NOT NULL,            -- FK all'alunno
        id_corso_anno_fk INT NOT NULL,                 -- FK al corso-anno
        data_inizio_stage DATE NOT NULL,               -- Data inizio stage (parte della PK)
        
        data_fine_stage DATE,
        id_azienda_stage VARCHAR(255),                 -- ID/Codice azienda (da "Azienda")
        partita_iva_azienda VARCHAR(255),              -- Partita IVA (da "PI")
        codice_corso_stage VARCHAR(255),               -- Codice del corso (da "CodiceCorso")
        nome_corso_stage TEXT,                         -- Nome del corso (da "Corso")
        cognome_alunno VARCHAR(255),                   -- Da "Cognome"
        nome_alunno VARCHAR(255),                      -- Da "Nome"
        
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id_alunno_fk, id_corso_anno_fk, data_inizio_stage), -- Chiave primaria composita
        
        -- Vincoli di chiave esterna (adattati o rimossi se necessario)
        CONSTRAINT fk_stage_corso FOREIGN KEY (id_corso_anno_fk) REFERENCES corsi (id_corso_anno)
        -- *** NOTA: Il vincolo FOREIGN KEY "fk_stage_alunno" È STATO RIMOSSO ***
        -- Questo è stato fatto per permettere l'inserimento di dati anche se le iscrizioni
        -- non sono presenti nella tabella 'iscrizioni_dettaglio', a causa di inconsistenza
        -- nei dati API o di un caricamento parziale di iscrizioni.
    );
    """
    execute_query(create_table_query)
    print("Tabella 'stage' verificata/creata.")

    # **INSERT/UPDATE AGGIORNATO CON LE COLONNE**
    insert_query = """
    INSERT INTO stage (
        id_alunno_fk, id_corso_anno_fk, data_inizio_stage,
        data_fine_stage, id_azienda_stage, partita_iva_azienda,
        codice_corso_stage, nome_corso_stage, cognome_alunno, nome_alunno
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_alunno_fk, id_corso_anno_fk, data_inizio_stage) DO UPDATE SET
        data_fine_stage = EXCLUDED.data_fine_stage,
        id_azienda_stage = EXCLUDED.id_azienda_stage,
        partita_iva_azienda = EXCLUDED.partita_iva_azienda,
        codice_corso_stage = EXCLUDED.codice_corso_stage,
        nome_corso_stage = EXCLUDED.nome_corso_stage,
        cognome_alunno = EXCLUDED.cognome_alunno,
        nome_alunno = EXCLUDED.nome_alunno,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_alunno_fk'],
                record['id_corso_anno_fk'],
                record['data_inizio_stage'],
                record['data_fine_stage'],
                record['id_azienda_stage'],
                record['partita_iva_azienda'],
                record['codice_corso_stage'],
                record['nome_corso_stage'],
                record['cognome_alunno'],
                record['nome_alunno']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento stage alunno '{record.get('id_alunno_fk', 'N/A')}' corso '{record.get('id_corso_anno_fk', 'N/A')}' data '{record.get('data_inizio_stage', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'stage'.")


def run_Ex_stage(): # Nome della funzione coerente con il nome del file
    """
    Orchestra il processo ETL per l'endpoint 'stage',
    recuperando i dati per intervalli di date.
    """
    print("\n--- Avvio processo ETL per 'stage' ---")
    try:
        # --- SCRIPT SQL PER CANCELLARE LA TABELLA (OPZIONALE) ---
        print("🗑️ Tentativo di eliminare la tabella 'stage' (se esiste)...")
        drop_table_stage_query = """
        DROP TABLE IF EXISTS stage CASCADE;
        """
        execute_query(drop_table_stage_query)
        print("✅ Tabella 'stage' eliminata (o non esisteva).")
        # --- FINE SCRIPT SQL DI CANCELLAZIONE ---

        all_transformed_data = []
        
        # Definisci l'intervallo di date per la query API
        start_date = datetime.date(2000, 1, 1) # Inizio dal 1° Gennaio 2000
        end_date = datetime.date.today()       # Fino alla data odierna

        # Formatta le date come stringhe "DD/MM/YYYY" per l'API
        data_da_str = start_date.strftime("%d/%m/%Y")
        data_a_str = end_date.strftime("%d/%m/%Y")

        print(f"🔍 Recupero dati 'stage' da {data_da_str} a {data_a_str}...")
        # L'API Stage non richiede iterazione per idCorsoAnno, ma solo un range di date
        raw_data = fetch_api_data(endpoint="stage", params={"DataDa": data_da_str, "DataA": data_a_str})
        
        if raw_data:
            # Passa i dati grezzi direttamente alla trasformazione (idCorsoAnno è già nel record)
            transformed_batch = transform_stage_data(raw_data) 
            all_transformed_data.extend(transformed_batch)
        else:
            print(f"⚠️ Nessun dato grezzo recuperato per 'stage' nell'intervallo {data_da_str} - {data_a_str}.")

        if not all_transformed_data:
            print("Nessun dato grezzo o trasformato recuperato per 'stage'. Terminazione.")
            return

        load_stage_data(all_transformed_data)
        print("--- Processo ETL per 'stage' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'stage': {e}")

if __name__ == "__main__":
    run_Ex_stage()