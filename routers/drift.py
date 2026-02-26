"""
routers/drift.py - Model Drift Detection Endpoints

Endpoints:
  GET /api/drift/summary         - overall health + drifted features
  GET /api/drift/features        - all feature drift scores (paginated)
  GET /api/drift/module/{module} - drift results for a specific module
  GET /api/drift/alerts          - only features with drift status != STABLE
"""

import json
import os
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

CSV    = "drift_results.csv"
REPORT = "models/drift_report.json"


def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(
            status_code=404,
            detail=CSV + " not found. Run monitor_drift.py first.",
        )
    df = pd.read_csv(CSV)
    return df.where(pd.notnull(df), None)


def _load_report() -> dict:
    if not os.path.exists(REPORT):
        raise HTTPException(status_code=404, detail="Drift report not found.")
    with open(REPORT) as f:
        return json.load(f)


@router.get("/summary", summary="Overall model drift health summary")
def get_drift_summary():
    report = _load_report()
    return {
        "generated_at"    : report.get("generated_at"),
        "overall_health"  : report.get("overall_health"),
        "total_checks"    : report.get("total_checks"),
        "status_breakdown": report.get("status_breakdown"),
        "modules_checked" : report.get("modules_checked"),
        "drifted_features": report.get("drifted_features"),
    }


@router.get("/features", summary="All drift scores per feature (paginated)")
def get_all_features(
    module: str = Query(None, description="Filter by module (RFM|Segmentation|Churn)"),
    status: str = Query(None, description="Filter by status (STABLE|SLIGHT_DRIFT|SIGNIFICANT_DRIFT)"),
    limit : int = Query(100,  description="Max records"),
    offset: int = Query(0,    description="Pagination offset"),
):
    df = _load_df()
    if module:
        df = df[df["module"].str.lower() == module.lower()]
    if status:
        df = df[df["status"].str.upper() == status.upper()]
    total = len(df)
    page  = df.sort_values("psi", ascending=False).iloc[offset: offset + limit]
    return {
        "total" : total,
        "limit" : limit,
        "offset": offset,
        "computed_at": datetime.utcnow().isoformat(),
        "data"  : page.to_dict("records"),
    }


@router.get("/module/{module_name}", summary="Drift results for a specific module")
def get_module_drift(module_name: str):
    df  = _load_df()
    sub = df[df["module"].str.lower() == module_name.lower()]
    if sub.empty:
        raise HTTPException(
            status_code=404,
            detail="Module not found. Available: " + str(df["module"].unique().tolist()),
        )
    return {
        "module"     : module_name,
        "computed_at": datetime.utcnow().isoformat(),
        "features"   : sub.sort_values("psi", ascending=False).to_dict("records"),
    }


@router.get("/alerts", summary="Features with drift status != STABLE")
def get_alerts():
    df     = _load_df()
    alerts = df[df["status"] != "STABLE"].sort_values("psi", ascending=False)
    return {
        "alert_count": len(alerts),
        "computed_at": datetime.utcnow().isoformat(),
        "alerts"     : alerts.to_dict("records"),
    }
