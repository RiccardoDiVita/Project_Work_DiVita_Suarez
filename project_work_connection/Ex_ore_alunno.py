import datetime # Anche se non ci sono date API, importiamo per completezza
# Importa le funzioni e le configurazioni da file_di_config
from file_di_config import fetch_api_data, execute_query, get_all_id_corso_anno_from_db

# NOTA: La funzione transform_ore_alunno_data accetta anche 'id_corso_anno' come parametro
def transform_ore_alunno_data(raw_data: list, id_corso_anno: int) -> list:
    """
    Trasforma i dati grezzi delle ore alunno nel formato DB desiderato.
    Aggiunge l'id_corso_anno ai record trasformati.
    """
    transformed_records = []
    
    # Funzione helper per convertire stringhe numeriche (anche con virgola) a float (NUMERIC nel DB)
    def parse_numeric_or_none(num_str):
        if isinstance(num_str, (int, float)): # Se è già un numero, restituiscilo così
            return num_str
        if isinstance(num_str, str) and num_str:
            try:
                # Sostituisce la virgola con un punto e converte a float
                return float(num_str.replace(',', '.'))
            except ValueError:
                return None
        return None

    # Funzione helper per convertire stringhe a intero, gestendo valori mancanti
    def parse_int_or_none(int_str):
        if isinstance(int_str, int):
            return int_str
        if isinstance(int_str, str) and int_str:
            try:
                return int(int_str)
            except ValueError:
                return None
        return None

    for record in raw_data:
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue

        try:
            transformed_record = {
                # Campi chiave per la PK composita
                "id_corso_anno_fk": id_corso_anno, # Preso dal parametro della funzione
                "id_alunno_fk": record.get("idAlunno"),
                "materia_insegnata": record.get("Materia"), # La materia è parte della PK per unicità

                # Dettagli Alunno (denormalizzati)
                "cognome_alunno": record.get("Cognome"),
                "nome_alunno": record.get("Nome"),
                "cf_alunno": record.get("CF"),
                
                # Dettagli Ore/Minuti/Voti
                "ore_previste": parse_numeric_or_none(record.get("OrePreviste")),
                "ore_assenza_iniziali": parse_numeric_or_none(record.get("OreAssenzaIniziali")),
                "ore_presenza_iniziali": parse_numeric_or_none(record.get("OrePresenzaIniziali")),
                "minuti_presenza": parse_int_or_none(record.get("MinutiPresenza")),
                "minuti_lezione": parse_int_or_none(record.get("MinutiLezione")),
                "voto_medio": parse_numeric_or_none(record.get("VotoMedio"))
            }
            
            # Controllo essenziale per la chiave primaria composita
            if (transformed_record["id_corso_anno_fk"] is None or 
                transformed_record["id_alunno_fk"] is None or 
                transformed_record["materia_insegnata"] is None): # Materia è ora PK
                print(f"❌ Record ore_alunno ignorato: Campi chiave mancanti/non validi. Record originale: {record}")
                continue

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record ore_alunno: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record ore_alunno: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'ore_alunno'.")
    return transformed_records


def load_ore_alunno_data(data: list):
    """Carica i dati trasformati delle ore alunno nel database."""
    if not data:
        print("Nessun dato da caricare per 'ore_alunno'.")
        return

    # **SCHEMA ESATTO DELLA TABELLA 'ore_alunno' - FK 'fk_ore_alunno_iscrizione' RIMOSSA!**
    # Chiave primaria composita: (id_corso_anno_fk, id_alunno_fk, materia_insegnata)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ore_alunno (
        id_corso_anno_fk INT NOT NULL,              -- FK verso 'corsi' (id_corso_anno)
        id_alunno_fk VARCHAR(255) NOT NULL,         -- ID Alunno
        materia_insegnata VARCHAR(255) NOT NULL,    -- Materia è parte della PK per unicità

        cognome_alunno VARCHAR(255),
        nome_alunno VARCHAR(255),
        cf_alunno VARCHAR(255),

        ore_previste NUMERIC(10, 2),
        ore_assenza_iniziali NUMERIC(10, 2),
        ore_presenza_iniziali NUMERIC(10, 2),
        minuti_presenza INT,
        minuti_lezione INT,
        voto_medio NUMERIC(5, 2),                   -- Voto medio (presunto decimale per flessibilità)

        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id_corso_anno_fk, id_alunno_fk, materia_insegnata), -- Chiave primaria composita
        CONSTRAINT fk_ore_alunno_corso FOREIGN KEY (id_corso_anno_fk) REFERENCES corsi (id_corso_anno)
        -- *** NOTA: Il vincolo FOREIGN KEY "fk_ore_alunno_iscrizione" È STATO RIMOSSO ***
        -- Questo è stato fatto per permettere l'inserimento di dati anche se le iscrizioni
        -- non sono presenti nella tabella 'iscrizioni_dettaglio', a causa di inconsistenza
        -- nei dati API o di un caricamento parziale di iscrizioni.
    );
    """
    execute_query(create_table_query)
    print("Tabella 'ore_alunno' verificata/creata.")

    # **INSERT/UPDATE AGGIORNATO CON LE COLONNE**
    insert_query = """
    INSERT INTO ore_alunno (
        id_corso_anno_fk, id_alunno_fk, materia_insegnata,
        cognome_alunno, nome_alunno, cf_alunno,
        ore_previste, ore_assenza_iniziali, ore_presenza_iniziali,
        minuti_presenza, minuti_lezione, voto_medio
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_corso_anno_fk, id_alunno_fk, materia_insegnata) DO UPDATE SET
        cognome_alunno = EXCLUDED.cognome_alunno,
        nome_alunno = EXCLUDED.nome_alunno,
        cf_alunno = EXCLUDED.cf_alunno,
        ore_previste = EXCLUDED.ore_previste,
        ore_assenza_iniziali = EXCLUDED.ore_assenza_iniziali,
        ore_presenza_iniziali = EXCLUDED.ore_presenza_iniziali,
        minuti_presenza = EXCLUDED.minuti_presenza,
        minuti_lezione = EXCLUDED.minuti_lezione,
        voto_medio = EXCLUDED.voto_medio,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_corso_anno_fk'],
                record['id_alunno_fk'],
                record['materia_insegnata'],
                record['cognome_alunno'],
                record['nome_alunno'],
                record['cf_alunno'],
                record['ore_previste'],
                record['ore_assenza_iniziali'],
                record['ore_presenza_iniziali'],
                record['minuti_presenza'],
                record['minuti_lezione'],
                record['voto_medio']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento ore alunno per '{record.get('id_alunno_fk', 'N/A')}' corso '{record.get('id_corso_anno_fk', 'N/A')}' materia '{record.get('materia_insegnata', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'ore_alunno'.")


def run_Ex_ore_alunno(): # Nome della funzione coerente con il nome del file
    """
    Orchestra il processo ETL per l'endpoint 'ore_alunno',
    iterando su tutti gli id_corso_anno.
    """
    print("\n--- Avvio processo ETL per 'ore_alunno' ---")
    try:
        # ---------------------------------------------------------------------
        # SCRIPT SQL PER CANCELLARE LA TABELLA (OPZIONALE) --- Questa è la riga a cui ti riferivi!
        # ---------------------------------------------------------------------
        print("🗑️ Tentativo di eliminare la tabella 'ore_alunno' (se esiste)...")
        drop_table_ore_alunno_query = """
        DROP TABLE IF EXISTS ore_alunno CASCADE;
        """
        execute_query(drop_table_ore_alunno_query)
        print("✅ Tabella 'ore_alunno' eliminata (o non esisteva).")
        # ---------------------------------------------------------------------

        all_corso_ids = get_all_id_corso_anno_from_db()
        if not all_corso_ids:
            print("🛑 Nessun 'id_corso_anno' trovato nel database. Assicurati che 'Ex_corsi.py' sia stato eseguito.")
            return

        all_transformed_data = []
        for corso_id in all_corso_ids:
            # Effettua la chiamata API per ogni ID corso
            raw_data_for_id = fetch_api_data(endpoint="ore_alunno", params={"idCorsoAnno": corso_id})
            
            if raw_data_for_id:
                transformed_batch = transform_ore_alunno_data(raw_data_for_id, id_corso_anno=corso_id)
                all_transformed_data.extend(transformed_batch)

        if not all_transformed_data:
            print("Nessun dato grezzo o trasformato recuperato per 'ore_alunno'. Terminazione.")
            return

        load_ore_alunno_data(all_transformed_data)
        print("--- Processo ETL per 'ore_alunno' completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'ore_alunno': {e}")

if __name__ == "__main__":
    run_Ex_ore_alunno()