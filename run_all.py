"""
run_all.py — Master Runner

Runs all analytics scripts in parallel, then starts the FastAPI server.

Execution order:
  Phase 0 (sequential): schema init → store dimension → ETL data load
  Phase 1 (parallel):   independent analytics scripts
  Phase 2 (parallel):   scripts that depend on Phase 1 outputs
  Phase 3 (parallel):   new analytics modules (pricing, drift, clv_prediction)
  Phase 4:              uvicorn (FastAPI server)

Usage:
  python run_all.py              # run ETL + analytics + start server
  python run_all.py --no-server  # run ETL + analytics only, skip server
  python run_all.py --skip-etl   # skip Phase 0 (assume DB already loaded)
  python run_all.py --skip-phase1  # skip phase 1 (use existing CSVs)
"""

import subprocess
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

PYTHON = sys.executable

# Colour codes (work on Windows 10+ and most terminals)
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def run_script(name: str, script: str) -> dict:
    """Run a single Python script as a subprocess, capture output."""
    start = time.time()
    try:
        result = subprocess.run(
            [PYTHON, script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        elapsed = time.time() - start
        ok = result.returncode == 0
        return {
            "name"   : name,
            "script" : script,
            "ok"     : ok,
            "elapsed": elapsed,
            "stdout" : result.stdout,
            "stderr" : result.stderr,
        }
    except Exception as e:
        return {
            "name"   : name,
            "script" : script,
            "ok"     : False,
            "elapsed": time.time() - start,
            "stdout" : "",
            "stderr" : str(e),
        }


def run_phase(phase_name: str, scripts: list) -> list:
    """Run a list of (name, script) pairs in parallel, print live status."""
    print(f"\n{BOLD}{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {phase_name}{RESET}")
    print(f"{BOLD}{CYAN}{'-'*60}{RESET}")

    for name, _ in scripts:
        print(f"  {YELLOW}STARTING{RESET}  {name}")

    results = []
    with ThreadPoolExecutor(max_workers=len(scripts)) as pool:
        futures = {pool.submit(run_script, name, script): name
                   for name, script in scripts}
        for future in as_completed(futures):
            r = future.result()
            symbol = f"{GREEN}DONE   {RESET}" if r["ok"] else f"{RED}FAILED {RESET}"
            print(f"  {symbol}  {r['name']:45s}  {r['elapsed']:6.1f}s")
            if not r["ok"] and r["stderr"]:
                lines = r["stderr"].strip().splitlines()
                for line in lines[-3:]:
                    print(f"           {RED}{line}{RESET}")
            results.append(r)

    return results


def print_summary(all_results: list, total_elapsed: float):
    passed = [r for r in all_results if r["ok"]]
    failed = [r for r in all_results if not r["ok"]]

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  ANALYTICS SUMMARY{RESET}")
    print(f"{'='*60}")
    print(f"  Total time : {total_elapsed:.1f}s")
    print(f"  Passed     : {GREEN}{len(passed)}/{len(all_results)}{RESET}")
    if failed:
        print(f"  Failed     : {RED}{', '.join(r['name'] for r in failed)}{RESET}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Run all analytics + FastAPI server")
    parser.add_argument("--no-server",    action="store_true",
                        help="Skip starting the FastAPI server after analytics")
    parser.add_argument("--skip-etl",     action="store_true",
                        help="Skip Phase 0 ETL (assume DB already populated)")
    parser.add_argument("--skip-phase1",  action="store_true",
                        help="Skip Phase 1 (assume CSVs already generated)")
    args = parser.parse_args()

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  RETAIL ANALYTICS -- MASTER RUNNER v2{RESET}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{BOLD}{'='*60}{RESET}")

    overall_start = time.time()
    all_results   = []

    # ── Phase 0: schema init + ETL data load (sequential) ────────────────────
    if not args.skip_etl:
        print(f"\n{BOLD}{CYAN}{'-'*60}{RESET}")
        print(f"{BOLD}{CYAN}  PHASE 0 — ETL: Schema Init + Data Load{RESET}")
        print(f"{BOLD}{CYAN}{'-'*60}{RESET}")

        etl_steps = [
            ("Schema Init",         "init_schema.py"),
            ("Store Dimension",     "load_store_dimension.py"),
            ("ETL Data Load",       "master_incremental_multisource_etl.py"),
        ]
        etl_ok = True
        for name, script in etl_steps:
            print(f"  {YELLOW}RUNNING{RESET}  {name}")
            r = run_script(name, script)
            symbol = f"{GREEN}DONE   {RESET}" if r["ok"] else f"{RED}FAILED {RESET}"
            print(f"  {symbol}  {r['name']:45s}  {r['elapsed']:6.1f}s")
            if not r["ok"]:
                lines = r["stderr"].strip().splitlines()
                for line in lines[-5:]:
                    print(f"           {RED}{line}{RESET}")
                etl_ok = False
                break

        if not etl_ok:
            print(f"\n{RED}Phase 0 ETL failed — analytics may be incomplete.{RESET}")
    else:
        print(f"\n{YELLOW}Phase 0 skipped (--skip-etl){RESET}")

    # ── Phase 1: independent core scripts ────────────────────────────────────
    if not args.skip_phase1:
        phase1 = [
            ("Advanced Analytics  (RFM/ABC/CLV/Cohort/Basket)", "test_advanced_analytics.py"),
            ("Price Elasticity",                                 "price_elasticity.py"),
            ("Customer Journey",                                 "customer_journey.py"),
            ("Demand Forecasting",                               "demand_forecasting.py"),
            ("Geographic Analysis",                              "geographic_analysis.py"),
            ("Anomaly Detection",                                "anomaly_detection.py"),
            ("Seasonality Analysis",                             "seasonality_analysis.py"),
            ("Customer Segmentation (K-Means/PCA)",              "customer_segmentation.py"),
            ("Store Performance Analysis",                       "store_performance.py"),
            ("Product Recommendation Engine",                    "product_recommendations.py"),
        ]
        r1 = run_phase("PHASE 1 — Independent Analytics (running in parallel)", phase1)
        all_results.extend(r1)
    else:
        print(f"\n{YELLOW}Phase 1 skipped (--skip-phase1){RESET}")

    # ── Phase 2: scripts that depend on Phase 1 outputs ───────────────────────
    phase2 = [
        ("Churn Prediction    (needs RFM/CLV CSVs)",    "ml_churn_prediction.py"),
        ("Inventory Optimisation (needs forecast CSV)",  "inventory_optimization.py"),
    ]
    r2 = run_phase("PHASE 2 — Dependent Analytics (running in parallel)", phase2)
    all_results.extend(r2)

    # ── Phase 3: new analytics (depend on Phase 1+2) ──────────────────────────
    phase3 = [
        ("Pricing Optimizer   (needs elasticity CSV)",   "pricing_optimizer.py"),
        ("CLV Prediction      (needs CLV/RFM CSVs)",     "clv_prediction.py"),
        ("Drift Monitor       (needs RFM/churn/seg CSVs)","monitor_drift.py"),
    ]
    r3 = run_phase("PHASE 3 — New Analytics Modules (running in parallel)", phase3)
    all_results.extend(r3)

    total_elapsed = time.time() - overall_start
    print_summary(all_results, total_elapsed)

    # ── Phase 4: start server ─────────────────────────────────────────────────
    if args.no_server:
        print("Server skipped (--no-server). Analytics complete.\n")
        sys.exit(0 if all(r["ok"] for r in all_results) else 1)

    failed = [r for r in all_results if not r["ok"]]
    if failed:
        print(f"{YELLOW}Warning: {len(failed)} script(s) failed. Starting server anyway...{RESET}\n")

    print(f"{BOLD}{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}{CYAN}  PHASE 4 -- Starting FastAPI Server{RESET}")
    print(f"{BOLD}{CYAN}{'-'*60}{RESET}")
    print(f"  Landing Page: http://localhost:8000/")
    print(f"  Dashboard   : http://localhost:8000/dashboard")
    print(f"  API Docs    : http://localhost:8000/docs")
    print(f"  Press Ctrl+C to stop\n")

    subprocess.run([PYTHON, "-m", "uvicorn", "main:app",
                    "--host", "0.0.0.0", "--port", "8000", "--reload"])


if __name__ == "__main__":
    main()
