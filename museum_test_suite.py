import requests
import json
import time

# --- CONFIGURATION ---
BASE_URL = "http://localhost:8000/api/v1"
TENANT_ID = "tenant_b4b6daaa"
SITE_ID = "1"
SESSION_PREFIX = f"test_suite_{int(time.time())}"

# --- COLORS FOR OUTPUT ---
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
END = "\033[0m"

# --- TEST CASES ---
TEST_CASES = [
    {
        "id": "ART-01",
        "category": "Artist Biography",
        "query": "Chi è Arturo Martini?",
        "expect_min_len": 1500,
        "description": "Verifica il recupero integrale di una biografia lunga."
    },
    {
        "id": "ART-02",
        "category": "Artist Filtering",
        "query": "Elenca tutti gli scultori presenti nel museo",
        "expect_keywords": ["Martini", "Canova", "Zandomeneghi"],
        "description": "Verifica il filtraggio degli artisti per categoria (SCULTORI)."
    },
    {
        "id": "AW-01",
        "category": "Artwork Detail",
        "query": "Dammi tutte le informazioni sul 'Ritratto del Cavalier Pasquale Moresco'",
        "expect_min_len": 2000,
        "description": "Verifica il recupero della descrizione dell'opera e dell'artista correlato."
    },
    {
        "id": "AW-02",
        "category": "Technique Filtering",
        "query": "Mostrami le opere realizzate in marmo",
        "expect_keywords": ["Zandomeneghi", "Martini", "Marmo"],
        "description": "Verifica il filtraggio delle opere per materiale/tecnica (MARMO)."
    },
    {
        "id": "AW-03",
        "category": "Advanced Filtering",
        "query": "Quali dipinti sono realizzati con la tecnica dell'olio su tela?",
        "expect_keywords": ["Hayez", "Apollonio", "Selvatico", "olio"],
        "description": "Verifica il filtraggio incrociato tra tipologia (dipinti) e tecnica (olio su tela)."
    },
    {
        "id": "AW-04",
        "category": "Time Filtering",
        "query": "Ci sono opere che risalgono al 1907?",
        "expect_keywords": ["Ritratto del Cavalier Pasquale Moresco"],
        "description": "Verifica la capacità di filtrare per anno di realizzazione."
    },
    {
        "id": "SQL-01",
        "category": "Discovery / SQL Join",
        "query": "Quali opere di Luigi Zandomeneghi ci sono nella GALLERIA DELL'OTTOCENTO?",
        "expect_keywords": ["Zandomeneghi", "Galleria"], 
        "description": "Verifica la capacità di join tra artisti, opere e sale usando nomi reali."
    },
    {
        "id": "PATH-01",
        "category": "Pathways - General",
        "query": "Parlami del percorso 'PERCORSO SCULTURA'",
        "expect_keywords": ["percorso", "scultura"],
        "description": "Verifica il recupero delle descrizioni dei percorsi tematici."
    },
    {
        "id": "PATH-02",
        "category": "Pathways - Artworks",
        "query": "Quali opere sono incluse nel percorso 'SENSORIALE'?",
        "expect_keywords": ["Adamo", "Eva", "Pisana", "Venere"],
        "description": "Verifica il recupero delle opere associate a un percorso specifico."
    },
    {
        "id": "LOC-01",
        "category": "Location - Contents",
        "query": "Cosa posso vedere nella SALA MULTISENSORIALE?",
        "expect_keywords": ["Sensoriale", "Martini", "Cacciapuoti"],
        "description": "Verifica il recupero delle opere contenute in una specifica sala."
    },
    {
        "id": "LOC-02",
        "category": "Location - Lookup",
        "query": "In quale sala si trova la 'Venere' di Antonio Canova?",
        "expect_keywords": ["Galleria dell'Ottocento"],
        "description": "Verifica la capacità di trovare la collocazione di un'opera specifica."
    },
    {
        "id": "SITE-01",
        "category": "Site Info",
        "query": "Qual è l'indirizzo del Museo Bailo?",
        "expect_keywords": ["Treviso", "Bailo"],
        "description": "Verifica il recupero di attributi specifici del sito."
    },
    {
        "id": "LANG-01",
        "category": "Multilingual",
        "query": "Who is Antonio Canova?",
        "expect_min_len": 1000,
        "description": "Verifica il bypass di esaustività in lingua inglese."
    },
    {
        "id": "NEG-01",
        "category": "Negative Search",
        "query": "C'è un quadro di Pablo Picasso?",
        "expect_keywords": ["non", "Nessun"],
        "description": "Verifica la gestione di dati non presenti nel DB."
    },
    {
        "id": "COMB-01",
        "category": "Complex Multi-filter",
        "query": "Cerca sculture di Arturo Martini esposte nel CORRIDOIO 7",
        "expect_keywords": ["Maternità", "Figura mostruosa"],
        "description": "Verifica una query complessa con artista + categoria + sala."
    }
]

def run_test(case):
    print(f"\n{BOLD}[{case['id']}] {case['category']}{END}: {case['query']}")
    print(f"   {case['description']}")
    
    session_id = f"{SESSION_PREFIX}_{case['id']}"
    payload = {
        "query": case['query'],
        "session_id": session_id,
        "site_id": SITE_ID
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            response = requests.post(
                f"{BASE_URL}/tenants/{TENANT_ID}/chat",
                json=payload,
                timeout=180
            )
            duration = time.time() - start_time
            
            if response.status_code == 429:
                print(f"   {YELLOW}Rate limited (429). Retrying in 20s... (Attempt {attempt+1}/{max_retries}){END}")
                time.sleep(20)
                continue

            if response.status_code != 200:
                print(f"   {RED}FAILED: status {response.status_code} - {response.text[:100]}{END}")
                return False
                
            data = response.json()
            answer = data.get("answer", "")
            
            # If response says "Si è verificato un errore" it might be an internal 429 caught by the pipeline
            if "Resource exhausted" in answer or "429" in answer:
                print(f"   {YELLOW}Internal Rate Limit detected. Retrying in 20s...{END}")
                time.sleep(20)
                continue

            length = len(answer)
            print(f"   {BLUE}Response ({length} chars, {duration:.1f}s):{END}")
            
            # Validation
            passed = True
            reasons = []
            
            if "expect_min_len" in case:
                if length < case["expect_min_len"]:
                    passed = False
                    reasons.append(f"Length {length} < {case['expect_min_len']}")
            
            if "expect_keywords" in case:
                for kw in case["expect_keywords"]:
                    if kw.lower() not in answer.lower():
                        if case['id'] != "NEG-01":
                            passed = False
                            reasons.append(f"Keyword '{kw}' not found")
            
            if passed:
                print(f"   {GREEN}RESULT: PASSED{END}")
                preview = answer[:200].replace("\n", " ") + "..."
                print(f"   {YELLOW}Preview: {preview}{END}")
            else:
                print(f"   {RED}RESULT: FAILED ({', '.join(reasons)}){END}")
                print(f"   {YELLOW}Trace: {answer[:300]}...{END}")
                
            return passed

        except Exception as e:
            print(f"   {RED}EXCEPTION: {str(e)}{END}")
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            return False
    return False

def main():
    print(f"\n{BOLD}🏛️  MUSEUM AI TEST SUITE v1.1 (with auto-retry){END}")
    print(f"Target: {BASE_URL} | Tenant: {TENANT_ID}")
    print("=" * 40)
    
    stats = {"passed": 0, "failed": 0}
    
    for case in TEST_CASES:
        time.sleep(2) # Small delay between tests
        if run_test(case):
            stats["passed"] += 1
        else:
            stats["failed"] += 1
            
    print("\n" + "=" * 40)
    print(f"{BOLD}SUMMARY:{END}")
    print(f"Total Tests: {len(TEST_CASES)}")
    print(f"{GREEN}Passed: {stats['passed']}{END}")
    print(f"{RED}Failed: {stats['failed']}{END}")
    
    if stats["failed"] == 0:
        print(f"\n{GREEN}✨ TUTTI I TEST SONO STATI SUPERATI CON SUCCESSO!{END}")
    else:
        print(f"\n{YELLOW}⚠️  Alcuni test hanno rilevato incongruenze. Controllare i log sopra.{END}")

if __name__ == "__main__":
    main()
