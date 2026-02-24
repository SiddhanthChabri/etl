"""
routers/seasonality.py -- Sales Seasonality & Trend Analysis API

Endpoints:
  GET /api/seasonality/summary        -- KPIs + top-level metrics
  GET /api/seasonality/decomposition  -- daily trend/seasonal/residual series
  GET /api/seasonality/patterns       -- DoW, month, quarter averages
  GET /api/seasonality/yoy            -- year x month revenue & growth matrix
  GET /api/seasonality/heatmap        -- year x week-of-year revenue heatmap
  GET /api/seasonality/peaks          -- top peak and trough trading days
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "seasonality_results.csv"
METADATA_JSON = "models/seasonality_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Seasonality metadata not found. Run seasonality_analysis.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run seasonality_analysis.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Seasonality KPIs and top-level metrics")
def get_summary():
    """
    Returns global KPIs:
    - Date range, total/active days, total & avg daily revenue
    - Trend direction & strength, seasonal strength
    - Best day of week, best month
    - Weekend vs weekday split
    - Best/worst month per year
    """
    meta = _meta()
    return {
        "generated_at"          : meta.get("generated_at"),
        "date_range_start"      : meta.get("date_range_start"),
        "date_range_end"        : meta.get("date_range_end"),
        "total_days"            : meta.get("total_days"),
        "active_days"           : meta.get("active_days"),
        "total_revenue"         : meta.get("total_revenue"),
        "avg_daily_revenue"     : meta.get("avg_daily_revenue"),
        "max_daily_revenue"     : meta.get("max_daily_revenue"),
        "min_daily_revenue"     : meta.get("min_daily_revenue"),
        "best_day_of_week"      : meta.get("best_day_of_week"),
        "best_month"            : meta.get("best_month"),
        "trend_direction"       : meta.get("trend_direction"),
        "trend_strength"        : meta.get("trend_strength"),
        "seasonal_strength"     : meta.get("seasonal_strength"),
        "trend_slope_pct_per_day": meta.get("trend_slope_pct_per_day"),
        "weekend_split"         : meta.get("weekend_split"),
        "best_worst_months"     : meta.get("best_worst_months"),
    }


# ── Decomposition series ──────────────────────────────────────────────────────

@router.get("/decomposition", summary="Daily revenue decomposition (trend + seasonal + residual)")
def get_decomposition(
    year: int = Query(None, description="Filter to a single year"),
):
    """
    Returns the daily time series with columns:
    date, revenue, trend, seasonal_component, residual, trend_30d
    Optionally filtered to a single year.
    """
    meta   = _meta()
    series = meta.get("daily_series", [])
    if year:
        series = [r for r in series if r.get("date", "").startswith(str(year))]
    return {"total": len(series), "data": series}


# ── Seasonal patterns ─────────────────────────────────────────────────────────

@router.get("/patterns", summary="Day-of-week, month, and quarter average revenue patterns")
def get_patterns():
    """
    Returns three pattern sets:
    - dow_patterns   : Mon-Sun average revenue + index vs overall avg
    - month_patterns : Jan-Dec average + total revenue + index
    - quarter_patterns : Q1-Q4 breakdown
    """
    meta = _meta()
    return {
        "dow_patterns"     : meta.get("dow_patterns", []),
        "month_patterns"   : meta.get("month_patterns", []),
        "quarter_patterns" : meta.get("quarter_patterns", []),
    }


# ── Year-over-year ────────────────────────────────────────────────────────────

@router.get("/yoy", summary="Year-over-year monthly revenue and growth matrix")
def get_yoy():
    """
    Returns a matrix of year × month revenue totals and YoY growth percentages.
    Useful for heatmap or grouped bar charts.
    """
    meta = _meta()
    return meta.get("yoy_matrix", {})


# ── Week-of-year heatmap ──────────────────────────────────────────────────────

@router.get("/heatmap", summary="Year x week-of-year revenue heatmap data")
def get_heatmap():
    """
    Returns revenue totals per (year, week-of-year) for a calendar heatmap.
    """
    meta = _meta()
    return meta.get("week_heatmap", {})


# ── Peak / trough days ────────────────────────────────────────────────────────

@router.get("/peaks", summary="Top peak and trough trading days")
def get_peaks(n: int = Query(10, description="Number of peak/trough days to return")):
    """
    Returns the n highest-revenue (peaks) and n lowest-revenue (troughs)
    non-zero trading days.
    """
    meta = _meta()
    pt   = meta.get("peak_trough", {})
    return {
        "peaks"  : pt.get("peaks",   [])[:n],
        "troughs": pt.get("troughs", [])[:n],
    }


# ── Per-day CSV lookup ────────────────────────────────────────────────────────

@router.get("/daily", summary="Full daily series from CSV (filterable)")
def get_daily(
    year  : int = Query(None, description="Filter by year"),
    month : int = Query(None, description="Filter by month (1-12)"),
    dow   : str = Query(None, description="Day of week, e.g. Monday"),
    limit : int = Query(500, description="Max rows"),
    offset: int = Query(0,   description="Pagination offset"),
):
    df = _load_df()
    if year:
        df = df[df["year"] == year]
    if month:
        df = df[df["month"] == month]
    if dow:
        df = df[df["day_of_week"].str.lower() == dow.lower()]
    total = len(df)
    page  = df.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset,
            "data": _df_records(page)}
