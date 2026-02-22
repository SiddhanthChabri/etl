"""
routers/dashboard.py — Dashboard & File Download Endpoints
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
import pandas as pd
import glob
import os
import math

router = APIRouter()


def _sanitize(val):
    """Convert NaN / inf / -inf to None for JSON compliance."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, 2)
    except (TypeError, ValueError):
        return val


# ── Landing Page ──────────────────────────────────────────────────────────────
@router.get("/", response_class=HTMLResponse,
            summary="API Landing Page", include_in_schema=False)
def index():
    path = "static/index.html"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="index.html not found in static/")
    return FileResponse(path, media_type="text/html")


# ── Dashboard ─────────────────────────────────────────────────────────────────
@router.get("/dashboard", summary="Serve HTML Analytics Dashboard",
            response_class=HTMLResponse)
def serve_dashboard():
    path = "static/dashboard.html"
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail="dashboard.html not found in static/."
        )
    return FileResponse(path, media_type="text/html")


# ── Excel Download ────────────────────────────────────────────────────────────
@router.get("/download/excel", summary="Download latest Excel report")
def download_excel():
    files = glob.glob("analytics_dashboard_*.xlsx") + glob.glob("static/analytics_dashboard_*.xlsx")
    if not files:
        raise HTTPException(
            status_code=404,
            detail="Excel file not found. Run generate_excel_dashboard.py first."
        )
    latest = sorted(files)[-1]
    return FileResponse(
        latest,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="analytics_report.xlsx"
    )


# ── Cohort Matrix ─────────────────────────────────────────────────────────────
@router.get("/api/cohort", summary="Get cohort retention matrix as JSON")
def get_cohort():
    path = "cohort_retention_matrix.csv"
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail="cohort_retention_matrix.csv not found. Run test_advanced_analytics.py first."
        )

    df = pd.read_csv(path, index_col=0)

    # Normalise index and column names to strings
    df.columns = [str(c) for c in df.columns]
    df.index   = [str(i) for i in df.index]

    # Sanitize every cell — converts NaN/inf to None (JSON null)
    matrix = {}
    for cohort in df.index:
        matrix[cohort] = {}
        for month in df.columns:
            matrix[cohort][month] = _sanitize(df.loc[cohort, month])

    return {
        "cohorts": df.index.tolist(),
        "months":  df.columns.tolist(),
        "matrix":  matrix
    }


# ── Combined Summary ──────────────────────────────────────────────────────────
@router.get("/api/summary", summary="Overall analytics summary (RFM + ABC + CLV)")
def get_summary():
    summary = {}

    try:
        rfm = pd.read_csv("rfm_analysis_results.csv")
        summary["rfm"] = {
            "total_customers": int(len(rfm)),
            "segments":        rfm["segment"].value_counts().to_dict(),
            "avg_monetary":    round(float(rfm["monetary"].mean()), 2),
        }
    except Exception as e:
        summary["rfm"] = {"error": str(e)}

    try:
        abc = pd.read_csv("abc_analysis_results.csv")
        summary["abc"] = {
            "total_products":     int(len(abc)),
            "total_revenue":      round(float(abc["total_revenue"].sum()), 2),
            "class_distribution": abc["abc_class"].value_counts().to_dict(),
        }
    except Exception as e:
        summary["abc"] = {"error": str(e)}

    try:
        clv = pd.read_csv("clv_analysis_results.csv")
        summary["clv"] = {
            "total_customers": int(len(clv)),
            "avg_clv":         round(float(clv["clv_discounted"].mean()), 2),
            "total_clv":       round(float(clv["clv_discounted"].sum()), 2),
            "segments":        clv["clv_segment"].value_counts().to_dict(),
        }
    except Exception as e:
        summary["clv"] = {"error": str(e)}

    return summary


# ── Health Check ──────────────────────────────────────────────────────────────
@router.get("/health", summary="Check health of all key data files")
def health_check():
    files = {
        "dashboard_html":        "static/dashboard.html",
        "rfm_results":           "rfm_analysis_results.csv",
        "abc_results":           "abc_analysis_results.csv",
        "clv_results":           "clv_analysis_results.csv",
        "cohort_matrix":         "cohort_retention_matrix.csv",
        "market_basket_results": "market_basket_results.csv",
        "churn_predictions":     "churn_predictions.csv",
        "churn_model":           "models/churn_model.pkl",
        "churn_features":        "models/churn_features.json",
    }
    status = {k: os.path.exists(v) for k, v in files.items()}
    all_ok = all(status.values())
    return {
        "status": "healthy" if all_ok else "degraded",
        "all_ok": all_ok,
        "files":  status
    }
