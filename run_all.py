"""
run_all.py — Master Runner

Starts the FastAPI server immediately, then runs all analytics scripts in the
background so the API is available within seconds instead of minutes.

Execution order:
  Server start (immediate): uvicorn boots in ~5s, serves stale/cached data
  Phase 0 (sequential):     schema init → store dimension → ETL data load
  Phase 1 (parallel):       independent analytics scripts
  Phase 2 (parallel):       scripts that depend on Phase 1 outputs
  Phase 3 (parallel):       new analytics modules (pricing, drift, clv_prediction)

Pipeline progress is written to pipeline_status.json and exposed via
GET /api/pipeline/status.

Usage:
  python run_all.py              # start server immediately + run ETL + analytics
  python run_all.py --no-server  # run ETL + analytics only, skip server
  python run_all.py --skip-etl   # skip Phase 0 (assume DB already loaded)
  python run_all.py --skip-phase1  # skip phase 1 (use existing CSVs)
"""

import json
import os
import signal
import subprocess
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

PYTHON      = sys.executable
STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_status.json")

# Colour codes
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ── Status helpers ────────────────────────────────────────────────────────────

def _write_status(phase: str, detail: str = "", completed: list | None = None,
                  failed: list | None = None, final: bool = False):
    data = {
        "status"           : "complete" if final else "running",
        "phase"            : phase,
        "detail"           : detail,
        "completed_phases" : completed or [],
        "failed_scripts"   : failed or [],
        "updated_at"       : datetime.utcnow().isoformat() + "Z",
    }
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass  # non-fatal


# ── Script runner ─────────────────────────────────────────────────────────────

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


# ── Server helpers ────────────────────────────────────────────────────────────

def _start_server() -> subprocess.Popen:
    """Launch uvicorn in the background (no --reload to avoid CSV-write restarts)."""
    print(f"\n{BOLD}{CYAN}{'-'*60}{RESET}")
    print(f"{BOLD}{CYAN}  Booting FastAPI server…{RESET}")
    print(f"{BOLD}{CYAN}{'-'*60}{RESET}")
    proc = subprocess.Popen(
        [PYTHON, "-m", "uvicorn", "main:app",
         "--host", "0.0.0.0", "--port", "8000"],
    )
    print(f"  uvicorn PID {proc.pid} — waiting for /health…")
    return proc


def _wait_for_server(timeout: int = 60) -> bool:
    """Poll /health until the server responds or timeout expires."""
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen("http://localhost:8000/health", timeout=2)
            return True
        except Exception:
            time.sleep(1)
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run all analytics + FastAPI server")
    parser.add_argument("--no-server",   action="store_true",
                        help="Skip starting the FastAPI server")
    parser.add_argument("--skip-etl",   action="store_true",
                        help="Skip Phase 0 ETL (assume DB already populated)")
    parser.add_argument("--skip-phase1", action="store_true",
                        help="Skip Phase 1 (assume CSVs already generated)")
    args = parser.parse_args()

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  RETAIL ANALYTICS -- MASTER RUNNER v3{RESET}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{BOLD}{'='*60}{RESET}")

    _write_status("starting", "Server booting")

    # ── Start server immediately ──────────────────────────────────────────────
    server_proc = None
    if not args.no_server:
        server_proc = _start_server()
        if _wait_for_server(timeout=60):
            print(f"  {GREEN}Server ready{RESET}  http://localhost:8000/")
        else:
            print(f"  {YELLOW}Server not responding after 60s — continuing anyway{RESET}")

    overall_start = time.time()
    all_results   = []
    completed_phases: list[str] = []
    failed_scripts:   list[str] = []

    # ── Phase 0: schema init + ETL data load (sequential) ────────────────────
    if not args.skip_etl:
        _write_status("phase_0", "Schema init + ETL data load", completed_phases, failed_scripts)
        print(f"\n{BOLD}{CYAN}{'-'*60}{RESET}")
        print(f"{BOLD}{CYAN}  PHASE 0 — ETL: Schema Init + Data Load{RESET}")
        print(f"{BOLD}{CYAN}{'-'*60}{RESET}")

        etl_steps = [
            ("Schema Init",     "init_schema.py"),
            ("Store Dimension", "load_store_dimension.py"),
            ("ETL Data Load",   "master_incremental_multisource_etl.py"),
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
                failed_scripts.append(name)
                etl_ok = False
                break

        if etl_ok:
            completed_phases.append("phase_0")
        else:
            print(f"\n{RED}Phase 0 ETL failed — analytics may be incomplete.{RESET}")
    else:
        print(f"\n{YELLOW}Phase 0 skipped (--skip-etl){RESET}")
        completed_phases.append("phase_0_skipped")

    # ── Phase 1: independent core scripts ────────────────────────────────────
    if not args.skip_phase1:
        _write_status("phase_1", "Independent analytics (parallel)", completed_phases, failed_scripts)
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
        failed_scripts.extend(r["name"] for r in r1 if not r["ok"])
        completed_phases.append("phase_1")
    else:
        print(f"\n{YELLOW}Phase 1 skipped (--skip-phase1){RESET}")
        completed_phases.append("phase_1_skipped")

    # ── Phase 2: scripts that depend on Phase 1 outputs ──────────────────────
    _write_status("phase_2", "Dependent analytics (parallel)", completed_phases, failed_scripts)
    phase2 = [
        ("Churn Prediction    (needs RFM/CLV CSVs)",   "ml_churn_prediction.py"),
        ("Inventory Optimisation (needs forecast CSV)", "inventory_optimization.py"),
    ]
    r2 = run_phase("PHASE 2 — Dependent Analytics (running in parallel)", phase2)
    all_results.extend(r2)
    failed_scripts.extend(r["name"] for r in r2 if not r["ok"])
    completed_phases.append("phase_2")

    # ── Phase 3: new analytics (depend on Phase 1+2) ─────────────────────────
    _write_status("phase_3", "New analytics modules (parallel)", completed_phases, failed_scripts)
    phase3 = [
        ("Pricing Optimizer   (needs elasticity CSV)",    "pricing_optimizer.py"),
        ("CLV Prediction      (needs CLV/RFM CSVs)",      "clv_prediction.py"),
        ("Drift Monitor       (needs RFM/churn/seg CSVs)", "monitor_drift.py"),
    ]
    r3 = run_phase("PHASE 3 — New Analytics Modules (running in parallel)", phase3)
    all_results.extend(r3)
    failed_scripts.extend(r["name"] for r in r3 if not r["ok"])
    completed_phases.append("phase_3")

    total_elapsed = time.time() - overall_start
    print_summary(all_results, total_elapsed)

    _write_status(
        phase="complete",
        detail=f"All phases done in {total_elapsed:.1f}s",
        completed=completed_phases,
        failed=failed_scripts,
        final=True,
    )

    # ── Wait for server (or exit) ─────────────────────────────────────────────
    if args.no_server:
        print("Server skipped (--no-server). Analytics complete.\n")
        sys.exit(0 if not failed_scripts else 1)

    if failed_scripts:
        print(f"{YELLOW}Warning: {len(failed_scripts)} script(s) failed.{RESET}\n")

    if server_proc is not None:
        print(f"{BOLD}{CYAN}  Analytics complete — server running (PID {server_proc.pid}).  Ctrl+C to stop.{RESET}\n")
        try:
            server_proc.wait()
        except KeyboardInterrupt:
            server_proc.send_signal(signal.SIGTERM)
            server_proc.wait()


if __name__ == "__main__":
    main()
