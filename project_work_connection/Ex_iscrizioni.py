import datetime
# Assicurati che file_di_config.py contenga fetch_api_data e execute_query
from file_di_config import fetch_api_data, execute_query, API_KEY # API_KEY è importata ma non usata per il controllo

def transform_iscrizioni_data(raw_data: list) -> list:
    """
    Trasforma i dati grezzi delle iscrizioni/dettagli alunno-corso
    nel formato Python/DB desiderato.
    """
    transformed_records = []
    for record in raw_data:
        # Controllo iniziale per assicurarsi che l'elemento sia un dizionario
        if not isinstance(record, dict):
            print(f"⚠️ Errore: Trovato un elemento non-dizionario in raw_data: {record}. Ignorato.")
            continue

        try:
            # Funzione helper per convertire stringhe data "DD/MM/YYYY" in oggetti date
            def parse_date_or_none(date_str):
                if date_str:
                    try:
                        return datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
                    except ValueError:
                        print(f"⚠️ Formato data '{date_str}' non valido per DataNascita. Impostazione a NULL. Record: {record.get('idAlunno')}")
                        return None
                return None

            # Funzione helper per convertire stringhe "true"/"false" in boolean
            def parse_boolean_or_none(bool_str):
                if isinstance(bool_str, str):
                    if bool_str.lower() == 'true': return True
                    if bool_str.lower() == 'false': return False
                return None # Se non è "true" o "false', considera Nullo

            transformed_record = {
                # Dettagli Alunno
                "id_alunno": record.get("idAlunno"), # Stringa come chiave
                "cognome": record.get("Cognome"),
                "nome": record.get("Nome"),
                "cf": record.get("CF"),
                "data_nascita": parse_date_or_none(record.get("DataNascita")),
                "sesso": record.get("Sesso"),
                "email": record.get("Email"),
                "voto_diploma": record.get("VotoDiploma"), # Stringa, può essere vuoto
                "alunno_attivo": parse_boolean_or_none(record.get("AlunnoAttivo")),
                "ritirato_corso": parse_boolean_or_none(record.get("RitiratoCorso")),

                # Dettagli Corso (FK a tabella corsi)
                "id_corso_anno_fk": int(record.get("idCorsoAnno")) if record.get("idCorsoAnno") else None,
                "codice_corso": record.get("CodiceCorso"),
                "nome_corso_completo": record.get("Corso"), # Nome completo per evitare ambiguità con 'corsi.nome_corso'

                # Dettagli specifici dell'Iscrizione/Valutazione
                "voto_pagella": record.get("VotoPagella"), # Stringa, può essere vuoto
                "credito": record.get("Credito"),           # Stringa, può essere vuoto
                "voto_ammissione_esame": record.get("VotoAmissioneEsame"), # Stringa, può essere vuoto
                "esito_finale": record.get("EsitoFinale")   # Stringa, può essere vuoto
            }
            
            # Controllo essenziale per i componenti della chiave primaria composita
            if transformed_record["id_alunno"] is None or transformed_record["id_corso_anno_fk"] is None:
                print(f"❌ Record iscrizione ignorato: 'idAlunno' o 'idCorsoAnno' è mancante/non valido. Record: {record}")
                continue

            transformed_records.append(transformed_record)

        except (ValueError, TypeError) as e:
            print(f"⚠️ Errore di conversione tipo o campo mancante nel record iscrizione: {record} - Errore: {e}")
            continue
        except Exception as e:
            print(f"⚠️ Errore generico durante la trasformazione del record iscrizione: {record} - Errore: {e}")
            continue
    print(f"➡️ Trasformati {len(transformed_records)} record per 'iscrizioni'.")
    return transformed_records


def load_iscrizioni_data(data: list):
    """Carica i dati trasformati delle iscrizioni nel database."""
    if not data:
        print("Nessun dato da caricare per 'iscrizioni'.")
        return

    # SCHEMA COMPLETO DELLA TABELLA 'iscrizioni_dettaglio'
    create_table_query = """
    CREATE TABLE IF NOT EXISTS iscrizioni_dettaglio (
        id_alunno VARCHAR(255) NOT NULL,            -- Da 'idAlunno' (hash/UUID)
        id_corso_anno_fk INT NOT NULL,              -- Da 'idCorsoAnno' (FK verso la tabella 'corsi')

        cognome_alunno VARCHAR(255),                -- Da 'Cognome'
        nome_alunno VARCHAR(255),                   -- Da 'Nome'
        cf_alunno VARCHAR(255),                     -- Da 'CF'
        data_nascita_alunno DATE,                   -- Da 'DataNascita'
        sesso_alunno VARCHAR(10),                   -- Da 'Sesso'
        email_alunno VARCHAR(255),                  -- Da 'Email'
        voto_diploma_alunno VARCHAR(50),            -- Da 'VotoDiploma'
        alunno_attivo BOOLEAN,                      -- Da 'AlunnoAttivo'
        ritirato_corso BOOLEAN,                     -- Da 'RitiratoCorso'

        codice_corso VARCHAR(255),                  -- Da 'CodiceCorso'
        nome_corso_completo TEXT,                   -- Da 'Corso'

        voto_pagella VARCHAR(50),                   -- Da 'VotoPagella'
        credito VARCHAR(50),                        -- Da 'Credito'
        voto_ammissione_esame VARCHAR(50),          -- Da 'VotoAmissioneEsame'
        esito_finale VARCHAR(50),                   -- Da 'EsitoFinale'
        
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id_alunno, id_corso_anno_fk),  -- Chiave primaria composita
        
        -- Chiave esterna (facoltativa ma consigliata se la tabella 'corsi' esiste)
        CONSTRAINT fk_iscrizioni_corso FOREIGN KEY (id_corso_anno_fk) REFERENCES corsi (id_corso_anno)
    );
    """
    execute_query(create_table_query)
    print("Tabella 'iscrizioni_dettaglio' verificata/creata.")

    # INSERT/UPDATE AGGIORNATO CON TUTTE LE NUOVE COLONNE E SEGNAPOSTO
    insert_query = """
    INSERT INTO iscrizioni_dettaglio (
        id_alunno, id_corso_anno_fk, cognome_alunno, nome_alunno, cf_alunno,
        data_nascita_alunno, sesso_alunno, email_alunno, voto_diploma_alunno,
        alunno_attivo, ritirato_corso, codice_corso, nome_corso_completo,
        voto_pagella, credito, voto_ammissione_esame, esito_finale
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_alunno, id_corso_anno_fk) DO UPDATE SET
        cognome_alunno = EXCLUDED.cognome_alunno,
        nome_alunno = EXCLUDED.nome_alunno,
        cf_alunno = EXCLUDED.cf_alunno,
        data_nascita_alunno = EXCLUDED.data_nascita_alunno,
        sesso_alunno = EXCLUDED.sesso_alunno,
        email_alunno = EXCLUDED.email_alunno,
        voto_diploma_alunno = EXCLUDED.voto_diploma_alunno,
        alunno_attivo = EXCLUDED.alunno_attivo,
        ritirato_corso = EXCLUDED.ritirato_corso,
        codice_corso = EXCLUDED.codice_corso,
        nome_corso_completo = EXCLUDED.nome_corso_completo,
        voto_pagella = EXCLUDED.voto_pagella,
        credito = EXCLUDED.credito,
        voto_ammissione_esame = EXCLUDED.voto_ammissione_esame,
        esito_finale = EXCLUDED.esito_finale,
        updated_at = NOW();
    """
    
    records_processed = 0
    for record in data:
        try:
            execute_query(insert_query, (
                record['id_alunno'],
                record['id_corso_anno_fk'],
                record['cognome'],
                record['nome'],
                record['cf'],
                record['data_nascita'],
                record['sesso'],
                record['email'],
                record['voto_diploma'],
                record['alunno_attivo'],
                record['ritirato_corso'],
                record['codice_corso'],
                record['nome_corso_completo'],
                record['voto_pagella'],
                record['credito'],
                record['voto_ammissione_esame'],
                record['esito_finale']
            ))
            records_processed += 1
        except Exception as e:
            print(f"❌ Errore durante l'inserimento/aggiornamento del record alunno '{record.get('id_alunno', 'N/A')}' e corso '{record.get('id_corso_anno_fk', 'N/A')}': {e}")
    print(f"✅ Caricati/Aggiornati {records_processed} record nella tabella 'iscrizioni_dettaglio'.")


def run_Ex_iscrizioni(): # Nome della funzione coerente con il nome del file
    """Orchestra il processo ETL per l'endpoint 'iscrizioni' (dettaglio)."""
    print("\n--- Avvio processo ETL per 'iscrizioni' (dettaglio) ---")
    try:
        # ---------------------------------------------------------------------
        # SCRIPT SQL PER CANCELLARE LA TABELLA 'iscrizioni_dettaglio'
        # ---------------------------------------------------------------------
        print("🗑️ Tentativo di eliminare la tabella 'iscrizioni_dettaglio' (se esiste)...")
        drop_table_iscrizioni_query = """
        DROP TABLE IF EXISTS iscrizioni_dettaglio CASCADE;
        """
        execute_query(drop_table_iscrizioni_query)
        print("✅ Tabella 'iscrizioni_dettaglio' eliminata (o non esisteva).")
        # ---------------------------------------------------------------------

        # Lista degli anni accademici da cui recuperare i dati.
        # Genera gli anni accademici come stringhe singole (es. '2015', '2016')
        # partendo dal 2015 fino all'anno solare corrente (2025).
        start_year = 2015
        current_calendar_year = datetime.datetime.now().year # Anno corrente: 2025
        
        # Genera una lista di stringhe di anni singoli: ['2015', '2016', ..., '2025']
        # range(start_year, current_calendar_year + 1) include l'anno finale.
        academic_years = [str(year) for year in range(start_year, current_calendar_year + 1)]

        all_raw_data = []
        if not academic_years:
            print("🛑 Nessun 'AnnoAccademico' specificato. Impossibile recuperare i dati delle iscrizioni.")
            return

        for year_str in academic_years: # Itera sulle stringhe degli anni (es. '2015')
            print(f"🔄 Recupero dati iscrizioni per Anno Accademico: {year_str}...")
            # Effettua la chiamata API per ogni AnnoAccademico
            # L'API si aspetta 'AnnoAccademico' come parametro di query, ora come singola stringa dell'anno
            raw_data_for_year = fetch_api_data(endpoint="iscrizioni", params={"AnnoAccademico": year_str})
            all_raw_data.extend(raw_data_for_year) # Aggiungi i dati a una lista unica

        if not all_raw_data:
            print("Nessun dato grezzo recuperato per 'iscrizioni'. Terminazione.")
            return

        transformed_data = transform_iscrizioni_data(all_raw_data)
        if not transformed_data: return

        load_iscrizioni_data(transformed_data)
        print("--- Processo ETL per 'iscrizioni' (dettaglio) completato con successo! ---")
    except Exception as e:
        print(f"🔴 Errore critico nel processo ETL per 'iscrizioni' (dettaglio): {e}")

if __name__ == "__main__":
    run_Ex_iscrizioni() # Esegui la funzione principale per l'ETL delle iscrizioni