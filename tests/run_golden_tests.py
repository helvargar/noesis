#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║           NOESIS - SUITE DI TEST GOLDEN DATASET                 ║
║                                                                  ║
║  Esegue tutte le domande del golden_dataset.json contro          ║
║  l'endpoint /api/v1/tenants/{tenant_id}/chat e valida            ║
║  le risposte secondo le regole definite nel dataset.             ║
║                                                                  ║
║  Uso: python tests/run_golden_tests.py [--verbose] [--category]  ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import time
import sys
import os
import argparse
import requests
from datetime import datetime
from pathlib import Path
from typing import Optional


# ─────────────────────── Configurazione ───────────────────────
BASE_URL = os.getenv("NOESIS_TEST_URL", "http://localhost:8000")
TENANT_ID = os.getenv("NOESIS_TEST_TENANT", "tenant_b4b6daaa")
SITE_ID = os.getenv("NOESIS_TEST_SITE", "1")
TIMEOUT_SECONDS = 30
GOLDEN_DATASET_PATH = Path(__file__).parent / "golden_dataset.json"
RESULTS_DIR = Path(__file__).parent / "results"


# ─────────────────────── Colori Console ───────────────────────
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


def colored(text: str, color: str) -> str:
    return f"{color}{text}{Colors.RESET}"


# ─────────────────────── Validazione ───────────────────────
class TestResult:
    def __init__(self, test_id: str, category: str, query: str, description: str):
        self.test_id = test_id
        self.category = category
        self.query = query
        self.description = description
        self.passed = False
        self.response = ""
        self.errors: list[str] = []
        self.response_time: float = 0.0
        self.http_status: int = 0

    @property
    def status_icon(self) -> str:
        return "✅" if self.passed else "❌"

    def to_dict(self) -> dict:
        return {
            "test_id": self.test_id,
            "category": self.category,
            "query": self.query,
            "passed": self.passed,
            "response_time_s": round(self.response_time, 2),
            "errors": self.errors,
            "response_preview": self.response[:200] + "..." if len(self.response) > 200 else self.response
        }


def validate_response(
    response_text: str,
    expected_keywords: list[str],
    forbidden_keywords: list[str],
    expect_data: bool,
    max_time: Optional[float],
    actual_time: float
) -> list[str]:
    """Valida la risposta e restituisce una lista di errori (vuota = OK)."""
    errors = []
    resp_lower = response_text.lower()

    # 1. Controlla keyword attese (almeno una deve essere presente)
    if expected_keywords:
        found_any = any(kw.lower() in resp_lower for kw in expected_keywords)
        if not found_any:
            errors.append(
                f"KEYWORD MANCANTE: Nessuna delle keyword attese trovata: {expected_keywords}"
            )

    # 2. Controlla keyword vietate (nessuna deve essere presente)
    for fk in forbidden_keywords:
        if fk.lower() in resp_lower:
            errors.append(f"KEYWORD VIETATA trovata: '{fk}'")

    # 3. Se expect_data=True, la risposta non deve sembrare un errore
    if expect_data:
        error_phrases = [
            "non ho trovato", "non sono riuscito", "errore tecnico",
            "problema tecnico", "riprova più tardi"
        ]
        for phrase in error_phrases:
            if phrase in resp_lower:
                errors.append(f"ERRORE INATTESO: risposta contiene '{phrase}' ma ci si aspettavano dati")
                break

    # 4. La risposta non deve essere vuota
    if not response_text.strip():
        errors.append("RISPOSTA VUOTA")

    # 5. Controllo latenza
    if max_time and actual_time > max_time:
        errors.append(f"TIMEOUT: {actual_time:.1f}s > {max_time}s")

    return errors


# ─────────────────────── Esecuzione Test ───────────────────────
def run_single_test(test_case: dict, verbose: bool = False) -> TestResult:
    """Esegue un singolo test case."""
    result = TestResult(
        test_id=test_case["id"],
        category=test_case["category"],
        query=test_case["query"],
        description=test_case["description"]
    )

    url = f"{BASE_URL}/api/v1/tenants/{TENANT_ID}/chat"
    payload = {
        "query": test_case["query"],
        "session_id": f"golden_test_{test_case['id']}_{int(time.time())}",
        "site_id": SITE_ID,
        "stream": True
    }

    try:
        start = time.time()
        resp = requests.post(url, json=payload, timeout=TIMEOUT_SECONDS, stream=True)
        result.http_status = resp.status_code

        if resp.status_code != 200:
            result.errors.append(f"HTTP {resp.status_code}: {resp.text[:200]}")
            result.response_time = time.time() - start
            return result

        # Leggi la risposta in streaming
        full_response = ""
        for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
            if chunk:
                full_response += chunk

        result.response_time = time.time() - start
        result.response = full_response.strip()

        # Valida la risposta
        result.errors = validate_response(
            response_text=result.response,
            expected_keywords=test_case.get("expected_keywords", []),
            forbidden_keywords=test_case.get("forbidden_keywords", []),
            expect_data=test_case.get("expect_data", True),
            max_time=test_case.get("max_response_time_seconds"),
            actual_time=result.response_time
        )
        result.passed = len(result.errors) == 0

    except requests.exceptions.ConnectionError:
        result.errors.append(f"CONNESSIONE FALLITA: Il server non risponde su {BASE_URL}")
    except requests.exceptions.Timeout:
        result.errors.append(f"TIMEOUT: Nessuna risposta entro {TIMEOUT_SECONDS}s")
    except Exception as e:
        result.errors.append(f"ERRORE IMPREVISTO: {str(e)}")

    return result


def run_all_tests(
    category_filter: Optional[str] = None,
    verbose: bool = False
) -> list[TestResult]:
    """Carica il golden dataset ed esegue tutti i test."""

    # Carica dataset
    if not GOLDEN_DATASET_PATH.exists():
        print(colored(f"❌ File golden_dataset.json non trovato: {GOLDEN_DATASET_PATH}", Colors.RED))
        sys.exit(1)

    with open(GOLDEN_DATASET_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    tests = dataset.get("tests", [])
    if category_filter:
        tests = [t for t in tests if t["category"] == category_filter]

    if not tests:
        print(colored(f"⚠️  Nessun test trovato" + (f" per la categoria '{category_filter}'" if category_filter else ""), Colors.YELLOW))
        sys.exit(1)

    # Header
    print()
    print(colored("╔══════════════════════════════════════════════════════════════╗", Colors.CYAN))
    print(colored("║           NOESIS - GOLDEN TEST SUITE                        ║", Colors.CYAN))
    print(colored("╚══════════════════════════════════════════════════════════════╝", Colors.CYAN))
    print(f"  Server:    {BASE_URL}")
    print(f"  Tenant:    {TENANT_ID}")
    print(f"  Test:      {len(tests)}" + (f" (filtro: {category_filter})" if category_filter else ""))
    print(f"  Ora:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(colored("─" * 64, Colors.DIM))
    print()

    results: list[TestResult] = []
    for i, test in enumerate(tests, 1):
        test_id = test["id"]
        category = test["category"]
        query = test["query"]

        # Stampa progresso
        print(f"  [{i:2d}/{len(tests)}] {colored(test_id, Colors.BOLD):20s} │ {query[:45]:45s}", end=" ", flush=True)

        result = run_single_test(test, verbose)
        results.append(result)

        # Risultato inline
        time_str = f"{result.response_time:.1f}s"
        if result.passed:
            print(colored(f"✅ PASS ({time_str})", Colors.GREEN))
        else:
            print(colored(f"❌ FAIL ({time_str})", Colors.RED))

        # Dettagli errore se verbose
        if verbose and not result.passed:
            for err in result.errors:
                print(colored(f"           └─ {err}", Colors.RED))
            if result.response:
                preview = result.response[:150].replace("\n", " ")
                print(colored(f"           └─ Risposta: \"{preview}...\"", Colors.DIM))
            print()

    return results


# ─────────────────────── Report ───────────────────────
def print_report(results: list[TestResult]) -> bool:
    """Stampa il report finale e salva su file. Ritorna True se tutti i test passano."""

    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]
    total = len(results)
    pct = (len(passed) / total * 100) if total > 0 else 0
    avg_time = sum(r.response_time for r in results) / total if total > 0 else 0

    print()
    print(colored("═" * 64, Colors.CYAN))
    print(colored("                    REPORT FINALE", Colors.BOLD))
    print(colored("═" * 64, Colors.CYAN))
    print()

    # Score
    if pct == 100:
        score_color = Colors.GREEN
        grade = "🏆 PERFETTO"
    elif pct >= 80:
        score_color = Colors.GREEN
        grade = "✅ BUONO"
    elif pct >= 60:
        score_color = Colors.YELLOW
        grade = "⚠️  SUFFICIENTE"
    else:
        score_color = Colors.RED
        grade = "❌ INSUFFICIENTE"

    print(f"  Voto: {colored(grade, score_color)}")
    print(f"  Successo: {colored(f'{len(passed)}/{total} ({pct:.0f}%)', score_color)}")
    print(f"  Tempo medio: {avg_time:.1f}s")
    print()

    # Dettagli per categoria
    categories = {}
    for r in results:
        if r.category not in categories:
            categories[r.category] = {"passed": 0, "total": 0}
        categories[r.category]["total"] += 1
        if r.passed:
            categories[r.category]["passed"] += 1

    print(f"  {'CATEGORIA':<25s} {'RISULTATO':>12s}")
    print(f"  {'─' * 25} {'─' * 12}")
    for cat, stats in sorted(categories.items()):
        cat_pct = stats["passed"] / stats["total"] * 100 if stats["total"] > 0 else 0
        cat_color = Colors.GREEN if cat_pct == 100 else (Colors.YELLOW if cat_pct >= 50 else Colors.RED)
        p = stats["passed"]
        t = stats["total"]
        label = f"{p}/{t} ({cat_pct:.0f}%)"
        print(f"  {cat:<25s} {colored(label, cat_color)}")

    # Fallimenti
    if failed:
        print()
        print(colored("  ─── TEST FALLITI ───", Colors.RED))
        for r in failed:
            print(f"  {colored('❌', Colors.RED)} {r.test_id}: {r.query}")
            for err in r.errors:
                print(f"     └─ {err}")
        print()

    # Salva report JSON
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = RESULTS_DIR / f"golden_report_{timestamp}.json"

    report_data = {
        "timestamp": datetime.now().isoformat(),
        "server": BASE_URL,
        "tenant_id": TENANT_ID,
        "summary": {
            "total": total,
            "passed": len(passed),
            "failed": len(failed),
            "success_rate": round(pct, 1),
            "avg_response_time_s": round(avg_time, 2),
            "grade": grade
        },
        "categories": categories,
        "tests": [r.to_dict() for r in results]
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    print(colored(f"  📄 Report salvato: {report_path}", Colors.DIM))
    print(colored("═" * 64, Colors.CYAN))
    print()

    return len(failed) == 0


# ─────────────────────── Main ───────────────────────
def main():
    parser = argparse.ArgumentParser(description="Noesis Golden Test Suite")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostra dettagli sugli errori")
    parser.add_argument("--category", "-c", type=str, help="Filtra per categoria (es. ricerca_artista)")
    parser.add_argument("--list-categories", action="store_true", help="Elenca le categorie disponibili")
    args = parser.parse_args()

    if args.list_categories:
        with open(GOLDEN_DATASET_PATH, "r", encoding="utf-8") as f:
            dataset = json.load(f)
        cats = sorted(set(t["category"] for t in dataset.get("tests", [])))
        print("\nCategorie disponibili:")
        for c in cats:
            count = sum(1 for t in dataset["tests"] if t["category"] == c)
            print(f"  • {c} ({count} test)")
        print()
        return

    results = run_all_tests(
        category_filter=args.category,
        verbose=args.verbose
    )

    all_passed = print_report(results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
