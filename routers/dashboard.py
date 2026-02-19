"""
routers/dashboard.py — Dashboard & File Download Endpoints
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
import pandas as pd
import json
import glob
import os

router = APIRouter()

@router.get("/", response_class=HTMLResponse,
            summary="API Landing Page", include_in_schema=False)
def index():
    return FileResponse("static/index.html", media_type="text/html")


@router.get("/dashboard", summary="Serve HTML Analytics Dashboard",
            response_class=HTMLResponse)
def serve_dashboard():
    # Find latest HTML dashboard
    files = glob.glob("static/analytics_dashboard_*.html") + glob.glob("analytics_dashboard_*.html")
    if not files:
        raise HTTPException(status_code=404,
                            detail="Dashboard not found. Run generate_analytics_dashboard.py first.")
    return FileResponse(sorted(files)[-1], media_type="text/html")


@router.get("/download/excel", summary="Download latest Excel report")
def download_excel():
    files = glob.glob("analytics_dashboard_*.xlsx")
    if not files:
        raise HTTPException(status_code=404,
                            detail="Excel file not found. Run generate_excel_dashboard.py first.")
    latest = sorted(files)[-1]
    return FileResponse(
        latest,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="analytics_report.xlsx"
    )


@router.get("/api/cohort", summary="Get cohort retention matrix as JSON")
def get_cohort():
    path = "cohort_retention_matrix.csv"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Cohort matrix not found.")
    df = pd.read_csv(path, index_col=0)
    df = df.where(pd.notnull(df), None)
    return {
        "cohorts":  df.index.tolist(),
        "months":   df.columns.tolist(),
        "matrix":   df.to_dict("index")
    }


@router.get("/api/summary", summary="Overall analytics summary (all modules)")
def get_summary():
    summary = {}
    try:
        rfm = pd.read_csv("rfm_analysis_results.csv")
        summary["rfm"] = {
            "total_customers": len(rfm),
            "segments": rfm["segment"].value_counts().to_dict(),
            "avg_monetary": round(float(rfm["monetary"].mean()), 2),
        }
    except Exception:
        summary["rfm"] = {"error": "not available"}

    try:
        abc = pd.read_csv("abc_analysis_results.csv")
        summary["abc"] = {
            "total_products": len(abc),
            "total_revenue": round(float(abc["total_revenue"].sum()), 2),
            "class_distribution": abc["abc_class"].value_counts().to_dict(),
        }
    except Exception:
        summary["abc"] = {"error": "not available"}

    try:
        clv = pd.read_csv("clv_analysis_results.csv")
        summary["clv"] = {
            "total_customers": len(clv),
            "avg_clv": round(float(clv["clv_discounted"].mean()), 2),
            "total_clv": round(float(clv["clv_discounted"].sum()), 2),
            "segments": clv["clv_segment"].value_counts().to_dict(),
        }
    except Exception:
        summary["clv"] = {"error": "not available"}

    return summary