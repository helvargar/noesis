# Noesis AI - Multi-tenant BYO-LLM SaaS

Noesis AI è una piattaforma SaaS progettata per consentire alle aziende di interrogare la propria base di conoscenza (Database SQL e Documenti) utilizzando i propri modelli di linguaggio (OpenAI, Anthropic, ecc.).

## 🚀 Caratteristiche Principali

-   **BYO-LLM (Bring Your Own LLM)**: I clienti inseriscono la propria API Key, riducendo i costi per il fornitore SaaS.
-   **Security First**: API Key cifrate a riposo (AES-256). Solo operazioni `SELECT` consentite con whitelist di tabelle.
-   **RAG + SQL Routing**: Il sistema sceglie automaticamente se interrogare il database o i documenti in base alla domanda.
-   **Isolamento Multi-tenant**: Ogni tenant ha il proprio indice vettoriale e i propri permessi database dedicati.
-   **Dashboard Admin**: Interfaccia moderna per la gestione dei tenant e degli utenti.

## 🛠️ Installazione

1. **Crea un virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Installa le dipendenze**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Prepara i dati di esempio (opzionale)**:
   ```bash
   python3 scripts/setup_sample_data.py
   ```

4. **Avvia il server**:
   ```bash
   python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

## 🖥️ Utilizzo Dashboard

Accedi all'interfaccia via browser: `http://localhost:8000`

**Credenziali Admin Default:**
- **Email**: `admin@noesis.ai`
- **Password**: `GeiAdmin01`

### Flusso di Configurazione:
1. **Login**: Accedi con le credenziali admin.
2. **Nuova Azienda**: Clicca su "+ Nuovo Tenant", inserisci il nome e la tua OpenAI/Anthropic API Key.
3. **Database**: Passa alla tab "Database", attiva la connessione e usa il path assoluto per il DB di esempio: `/Users/administrator/Workspaces/noesis/data/sample_data.db` (tipo SQLite).
4. **Documenti**: Trascina i file dalla cartella `data/sample_docs` nella tab "Documenti".
5. **Chat**: Una volta salvato, clicca su "Test" sulla card del tenant per fare domande come:
   - *"Quanti ordini ha fatto Mario Rossi?"* (Interroga SQL)
   - *"Qual è la policy sui rimborsi?"* (Interroga Documenti)

## 🐳 Architettura Tecnica

-   **Backend**: FastAPI (Python)
-   **Orchestrazione**: LlamaIndex
-   **Sicurezza**: Passlib (Hashing), PyJWT, Cryptography (Fernet)
-   **Database**: SQLite/Postgres (SQLAlchemy)
-   **Frontend**: Vanilla JS + CSS (Glassmorphism design)

---
*Progettato con ❤️ dal team Advanced Agentic Coding di Google DeepMind.*
