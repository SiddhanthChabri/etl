"""
routers/etl.py — ETL Pipeline Trigger Endpoints
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException
from datetime import datetime
import subprocess
import os

router = APIRouter()

# Track last run status in memory (use Redis/DB in production)
_status = {
    "etl":       {"last_run": None, "status": "never run"},
    "analytics": {"last_run": None, "status": "never run"},
    "quality":   {"last_run": None, "status": "never run"},
}


def _run_script(name: str, script: str):
    _status[name]["status"] = "running"
    _status[name]["last_run"] = datetime.now().isoformat()
    try:
        result = subprocess.run(
            ["python", script],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            _status[name]["status"] = "success"
            _status[name]["output"] = result.stdout[-500:]  # last 500 chars
        else:
            _status[name]["status"] = "failed"
            _status[name]["error"]  = result.stderr[-500:]
    except subprocess.TimeoutExpired:
        _status[name]["status"] = "timeout"
    except Exception as e:
        _status[name]["status"] = "error"
        _status[name]["error"]  = str(e)


@router.get("/status", summary="Get status of all ETL jobs")
def get_status():
    return _status


@router.post("/run", summary="Trigger incremental ETL pipeline")
def run_etl(background_tasks: BackgroundTasks):
    if _status["etl"]["status"] == "running":
        raise HTTPException(status_code=409, detail="ETL is already running")
    background_tasks.add_task(_run_script, "etl", "incremental_etl.py")
    return {"message": "✅ ETL pipeline started", "check_status": "/etl/status"}


@router.post("/analytics/refresh", summary="Re-run all analytics (RFM, ABC, CLV, etc.)")
def refresh_analytics(background_tasks: BackgroundTasks):
    if _status["analytics"]["status"] == "running":
        raise HTTPException(status_code=409, detail="Analytics refresh is already running")
    background_tasks.add_task(_run_script, "analytics", "test_advanced_analytics.py")
    return {"message": "✅ Analytics refresh started", "check_status": "/etl/status"}


@router.post("/quality/check", summary="Run data quality checks")
def run_quality_check(background_tasks: BackgroundTasks):
    if _status["quality"]["status"] == "running":
        raise HTTPException(status_code=409, detail="Quality check is already running")
    background_tasks.add_task(_run_script, "quality", "data_quality_checks.py")
    return {"message": "✅ Quality check started", "check_status": "/etl/status"}