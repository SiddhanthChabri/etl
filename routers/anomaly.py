"""
routers/anomaly.py — Sales Anomaly Detection API Endpoints

Endpoints:
  GET /api/anomaly/summary          — KPIs, severity breakdown, top anomalies
  GET /api/anomaly/daily            — full daily series with anomaly flags
  GET /api/anomaly/alerts           — only anomalous days (filterable)
  GET /api/anomaly/monthly          — anomaly counts by month
  GET /api/anomaly/product/{id}     — anomalies for a specific product
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "anomaly_results.csv"
METADATA_JSON = "models/anomaly_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Anomaly metadata not found. Run anomaly_detection.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run anomaly_detection.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Anomaly detection summary & top anomalies")
def get_summary():
    """
    Global KPIs: total trading days, anomaly count/rate, severity breakdown,
    and the top 10 most extreme anomalous days.
    """
    meta = _meta()
    return {
        "generated_at"          : meta.get("generated_at"),
        "rolling_window_days"   : meta.get("rolling_window_days"),
        "z_thresholds"          : meta.get("z_thresholds"),
        "total_trading_days"    : meta.get("total_trading_days"),
        "overall_anomaly_count" : meta.get("overall_anomaly_count"),
        "overall_anomaly_rate"  : meta.get("overall_anomaly_rate"),
        "severity_distribution" : meta.get("severity_distribution"),
        "product_anomaly_count" : meta.get("product_anomaly_count"),
        "products_analysed"     : meta.get("products_analysed"),
        "top_anomalies"         : meta.get("top_anomalies"),
    }


# ── Full daily series ─────────────────────────────────────────────────────────

@router.get("/daily", summary="Full daily revenue series with anomaly flags")
def get_daily(
    anomalies_only: bool = Query(False, description="Return only anomalous days"),
    severity      : str  = Query(None,  description="Filter by severity: Medium/High/Critical"),
):
    """
    Returns the complete daily series including revenue, rolling mean,
    z-score, and anomaly flags. Useful for time-series charts.
    """
    meta   = _meta()
    series = meta.get("daily_series", [])

    if anomalies_only:
        series = [r for r in series if r.get("is_anomaly")]
    if severity:
        series = [r for r in series if str(r.get("severity", "")).lower() == severity.lower()]

    return {"total": len(series), "data": series}


# ── Anomaly alerts ────────────────────────────────────────────────────────────

@router.get("/alerts", summary="Anomalous days sorted by severity")
def get_alerts(
    scope    : str = Query(None,       description="overall or product"),
    metric   : str = Query(None,       description="revenue or price"),
    severity : str = Query(None,       description="Medium / High / Critical"),
    direction: str = Query(None,       description="Spike or Drop"),
    limit    : int = Query(50,         description="Max records"),
    offset   : int = Query(0,          description="Pagination offset"),
):
    df = _load_df()

    if scope:
        df = df[df["scope"].str.lower() == scope.lower()]
    if metric:
        df = df[df["metric"].str.lower() == metric.lower()]
    if severity:
        df = df[df["severity"].str.lower() == severity.lower()]
    if direction:
        df = df[df["direction"].str.lower() == direction.lower()]

    total = len(df)
    page  = df.sort_values("z_score", ascending=True).iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "data": _df_records(page)}


# ── Monthly counts ────────────────────────────────────────────────────────────

@router.get("/monthly", summary="Anomaly count aggregated by month")
def get_monthly():
    meta = _meta()
    return meta.get("monthly_anomaly_counts", [])


# ── Product anomalies ─────────────────────────────────────────────────────────

@router.get("/product/{product_id}", summary="Anomalies for a specific product")
def get_product_anomalies(product_id: str, metric: str = Query("revenue")):
    df = _load_df()
    result = df[
        (df["product_id"].astype(str) == str(product_id)) &
        (df["metric"] == metric)
    ]
    if result.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No anomalies found for product '{product_id}' with metric '{metric}'.",
        )
    return {
        "product_id": product_id,
        "metric"    : metric,
        "count"     : len(result),
        "data"      : _df_records(result),
    }
