# Importa la funzione di avvio ETL da ogni script specifico
# Assicurati che i nomi dei file e delle funzioni corrispondano ai tuoi script (es. run_Ex_corsi)
from Ex_corsi import run_Ex_corsi
from Ex_docenti import run_Ex_docenti
from Ex_iscrizioni import run_Ex_iscrizioni
from Ex_corso_materie import run_Ex_corso_materie
from Ex_corso_docenti import run_Ex_corso_docenti
from Ex_ore_alunno import run_Ex_ore_alunno
from Ex_stage import run_Ex_stage

# Non è necessario importare API_KEY qui, la validazione è nel file_di_config.py
# e i singoli script ETL gestiscono la loro esecuzione.

def run_all_etl_processes():
    """
    Avvia tutti i processi ETL per i vari endpoint nell'ordine corretto.
    Questo rispetta le dipendenze tra le tabelle per evitare errori di chiave esterna.
    """

    print("\n🚀 Avvio di tutti i processi ETL configurati (ordine di esecuzione per dipendenze)...")

    # --- Esegui i processi ETL nell'ordine di dipendenza ---
    
    print("\n----- Esecuzione ETL per CORSI -----")
    run_Ex_corsi() # Popola 'corsi' (base per molte FK)
    
    print("\n----- Esecuzione ETL per DOCENTI -----")
    run_Ex_docenti() # Popola 'docenti' (base per 'corso_docenti')
    
    print("\n----- Esecuzione ETL per ISCRIZIONI (Dettaglio) -----")
    run_Ex_iscrizioni() # Popola 'iscrizioni_dettaglio' (dipende da 'corsi', base per 'ore_alunno', 'stage')
    
    print("\n----- Esecuzione ETL per CORSO_MATERIE -----")
    run_Ex_corso_materie() # Dipende da 'corsi'
    
    print("\n----- Esecuzione ETL per CORSO_DOCENTI -----")
    run_Ex_corso_docenti() # Dipende da 'corsi' e 'docenti'
    
    print("\n----- Esecuzione ETL per ORE_ALUNNO -----")
    run_Ex_ore_alunno() # Dipende da 'corsi' e 'iscrizioni_dettaglio'
    
    print("\n----- Esecuzione ETL per STAGE -----")
    run_Ex_stage() # Dipende da 'corsi' e 'iscrizioni_dettaglio'

    print("\n🎉 Tutti i processi ETL configurati sono stati eseguiti.")

if __name__ == "__main__":
    run_all_etl_processes()