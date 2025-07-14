import psycopg2 # Libreria per connettersi e interagire con i database PostgreSQL
from psycopg2.extras import RealDictCursor # Permette di recuperare i risultati delle query come liste di dizionari (più comodo da usare)
import requests # Libreria per effettuare richieste HTTP (GET, POST, ecc.) alle API
from requests.exceptions import HTTPError, RequestException # Classi di eccezioni specifiche per gli errori HTTP e di richiesta

# --- Sezione di Configurazione dell'API ---
# Qui definisci gli URL base, il prefisso e la chiave di autenticazione per la tua API.

API_BASE_URL = "https://api-pw25-grhhckd5abhdhccd.italynorth-01.azurewebsites.net/"
API_PREFIX = "api/" # Prefisso comune per gli endpoint dell'API (es. /api/corsi, /api/docenti)

# !!! PUNTO CRITICO: INSERISCI QUI IL TUO VERO TOKEN API !!!
# Questo valore verrà usato per autenticare le richieste API.
API_KEY = "key_dhA9D87y" # Sostituisci "key_dhA9D87y" con il tuo token API reale.

# Header per l'autenticazione. La maggior parte delle API con token Bearer richiede questo formato.
# Nota importante: c'è uno SPAZIO tra "Bearer" e la chiave API.
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# --- Sezione di Configurazione del Database PostgreSQL ---
# Qui definisci i parametri per la connessione al tuo database PostgreSQL (su Azure).

DB_CONFIG = {
    'host': "project-work-db.postgres.database.azure.com", # Indirizzo del server del tuo database PostgreSQL su Azure
    'port': 5432,                                          # Porta standard per PostgreSQL
    'dbname': "postgres",                                  # Nome del database a cui connettersi
    'user': "michelangelo",                                # Nome utente per l'accesso al database
    'password': "Sugumi2004",                              # Password per l'utente del database
    'sslmode': 'require'                                   # Forza l'uso di una connessione SSL/TLS cifrata (obbligatorio per Azure PostgreSQL)
}

# --- Funzioni di Utilità per l'Interazione con il Database ---

def get_connection():
    """
    Stabilisce e restituisce una connessione al database PostgreSQL utilizzando le configurazioni definite.
    In caso di successo, stampa un messaggio positivo. In caso di errore, stampa un messaggio e rilancia l'eccezione.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG) # Tenta di connettersi al DB usando le credenziali in DB_CONFIG
        print("✅ Connessione al database riuscita!") # Messaggio di conferma in caso di successo
        return conn
    except Exception as e:
        print(f"❌ Errore nella connessione al database: {e}") # Messaggio di errore se la connessione fallisce
        raise # Rilancia l'eccezione per permettere al chiamante di gestire l'errore


def execute_query(query, params=None, fetch_results=False):
    """
    Esegue una query SQL sul database.
    - query (str): L'istruzione SQL da eseguire.
    - params (tuple, list, dict, optional): Parametri da passare alla query per prevenire SQL injection.
    - fetch_results (bool): Se True, recupera e ritorna i risultati della query (per SELECT).
                            Se False, esegue il commit della transazione (per INSERT, UPDATE, DELETE, CREATE TABLE).
    """
    conn = None # Inizializza la variabile di connessione a None
    try:
        conn = get_connection() # Ottiene una nuova connessione al database
        # Crea un cursore per eseguire le operazioni sul database.
        # RealDictCursor fa sì che i risultati delle SELECT siano dizionari (chiave=nome colonna).
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params) # Esegue la query con i parametri forniti
            if fetch_results:
                return cur.fetchall() # Se fetch_results è True, recupera tutti i risultati della query
            else:
                conn.commit() # Se fetch_results è False, committa le modifiche al database
    except Exception as e:
        if conn: # Controlla se la connessione è stata stabilita prima di provare il rollback
            conn.rollback() # Annulla tutte le modifiche fatte durante la transazione in caso di errore
        print(f"❌ Errore durante l'esecuzione della query: {e}") # Stampa l'errore SQL
        raise e # Rilancia l'eccezione per la gestione degli errori a livello superiore
    finally:
        if conn: # Controlla se la connessione è stata stabilita prima di provare a chiuderla
            conn.close() # Chiude la connessione al database, rilasciando le risorse


# --- Funzioni di Utilità per l'Interazione con l'API ---

def fetch_api_data(endpoint: str, params: dict = None) -> list:
    """
    Estrae dati da un endpoint API specifico, con parametri opzionali per query string.
    Gestisce risposte API che potrebbero contenere metadati (es. {"valid": true, "data": [...]}).
    """
    url = f"{API_BASE_URL}{API_PREFIX}{endpoint}"
    
    # Stampa l'URL e gli eventuali parametri per debug
    if params:
        print(f"▶️ Richiesta per '{url}' con parametri: {params}...")
    else:
        print(f"▶️ Richiesta per '{url}'...")

    try:
        # Effettua la richiesta GET all'API. Passa gli HEADERS per l'autenticazione e i 'params' per i filtri.
        response = requests.get(url, headers=HEADERS, timeout=10, params=params)
        response.raise_for_status() # Controlla lo stato della risposta HTTP (es. se è 4xx o 5xx, lancia un'eccezione HTTPError)
        json_response = response.json() # Tenta di decodificare la risposta JSON

        # Gestisce vari formati di risposta API per estrarre i record reali:
        # 1. Risposta standard: un dizionario con chiave 'data' che contiene una lista di record.
        if isinstance(json_response, dict) and "data" in json_response and isinstance(json_response["data"], list):
            actual_data_records = json_response["data"]
        # 2. Risposta specifica per "nessun dato": un dizionario con 'valid': False e un messaggio.
        #    Questo non è un errore HTTP, ma un'indicazione che non ci sono dati per la richiesta.
        elif isinstance(json_response, dict) and "valid" in json_response and json_response["valid"] is False:
            print(f"⚠️ API per '{endpoint}' con parametri {params}: {json_response.get('message', 'Nessun dato disponibile').strip()}. Trattato come 0 record.")
            actual_data_records = [] # Tratta questo come un set di dati vuoto.
        # 3. Risposta semplice: la risposta è direttamente una lista di record.
        elif isinstance(json_response, list):
            actual_data_records = json_response
        # 4. Qualsiasi altra struttura inaspettata.
        else:
            print(f"❌ Errore: La struttura della risposta JSON per '{endpoint}' non è stata riconosciuta. Risposta completa: {json_response}")
            return []

        print(f"✅ {endpoint}: ricevuti {len(actual_data_records)} record.") # Conferma il numero di record ricevuti
        return actual_data_records # Restituisce solo la lista dei record di dati effettivi

    except (HTTPError, RequestException) as e:
        # Gestisce errori specifici di richiesta HTTP (es. 403 Forbidden, errore di rete)
        print(f"❌ Errore API per '{endpoint}' con parametri {params}: {e}")
        return []
    except ValueError as e: # Gestisce errori di decodifica JSON (es. la risposta non è un JSON valido)
        print(f"❌ Errore di decodifica JSON per '{endpoint}': {e}. Risposta testuale: {response.text}")
        return []


def get_all_id_corso_anno_from_db() -> list:
    """
    Recupera tutti gli 'id_corso_anno' distinti dalla tabella 'corsi' nel database.
    Questa funzione è utile quando altri endpoint API richiedono 'idCorsoAnno' come parametro
    per recuperare i loro dati (es. 'corso_materie', 'corso_docenti').
    """
    print("🔎 Recupero id_corso_anno dalla tabella 'corsi' dal DB...")
    # La query recupera tutti gli ID distinti, escludendo i valori NULL per sicurezza.
    query = "SELECT DISTINCT id_corso_anno FROM corsi WHERE id_corso_anno IS NOT NULL;"
    try:
        # Esegue la query e recupera i risultati come lista di dizionari.
        results = execute_query(query, fetch_results=True)
        # Estrae il valore 'id_corso_anno' da ogni dizionario e crea una lista di ID.
        ids = [row['id_corso_anno'] for row in results if row and 'id_corso_anno' in row]
        print(f"✅ Trovati {len(ids)} ID corso anno nel database.")
        return ids
    except Exception as e:
        print(f"❌ Errore nel recupero degli ID corso anno dal DB: {e}")
        return []

# --- Blocco di Esecuzione Principale del File (Solo per Scopo di Test/Avviso) ---
# Questo codice viene eseguito SOLO se file_di_config.py è lo script principale avviato (es. python file_di_config.py).
# Non viene eseguito quando file_di_config.py viene importato da altri script.
if __name__ == "__main__":
    # Questo messaggio indica che il file di configurazione è stato caricato.
    print("File_config.py caricato.")