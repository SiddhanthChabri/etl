"""
routers/geographic.py — Geographic Sales Analysis API Endpoints

Endpoints:
  GET /api/geo/summary              — global stats, top regions, KPIs
  GET /api/geo/regions              — all regions with full metrics
  GET /api/geo/region/{region}      — single region detail + YoY + top products
  GET /api/geo/trends               — YoY revenue by region (for charts)
  GET /api/geo/monthly              — monthly revenue for top regions
  GET /api/geo/top-products/{region}— top products for a given region
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "geographic_results.csv"
METADATA_JSON = "models/geographic_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run geographic_analysis.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Geographic metadata not found. Run geographic_analysis.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Global geographic sales summary & KPIs")
def get_summary():
    """
    Global KPIs: total revenue, transactions, customers, years covered,
    top region, and full ranked region list with revenue shares.
    """
    meta = _meta()
    return {
        "generated_at"         : meta.get("generated_at"),
        "total_regions"        : meta.get("total_regions"),
        "total_revenue"        : meta.get("total_revenue"),
        "total_transactions"   : meta.get("total_transactions"),
        "total_customers"      : meta.get("total_customers"),
        "years_covered"        : meta.get("years_covered"),
        "top_region"           : meta.get("top_region"),
        "top_region_revenue"   : meta.get("top_region_revenue"),
        "top_region_share_pct" : meta.get("top_region_share_pct"),
        "regions"              : meta.get("regions"),
    }


# ── All Regions ───────────────────────────────────────────────────────────────

@router.get("/regions", summary="All regions with sales metrics (sortable)")
def get_regions(
    sort_by   : str  = Query("total_revenue", description="Sort column"),
    ascending : bool = Query(False,           description="Sort ascending"),
    limit     : int  = Query(50,              description="Max records"),
    exclude_uk: bool = Query(False,           description="Exclude United Kingdom"),
):
    df = _load_df()
    if exclude_uk:
        df = df[df["region"] != "United Kingdom"]

    valid = ["total_revenue", "total_transactions", "unique_customers",
             "avg_order_value", "revenue_share_pct", "revenue_per_customer"]
    if sort_by not in valid:
        sort_by = "total_revenue"

    df = df.sort_values(sort_by, ascending=ascending).head(limit)
    return _df_records(df)


# ── Single Region ─────────────────────────────────────────────────────────────

@router.get("/region/{region}", summary="Full profile for a single region")
def get_region(region: str):
    """
    Returns the region's summary metrics, YoY revenue breakdown,
    and top 5 products by revenue.
    """
    df = _load_df()
    row = df[df["region"].str.lower() == region.lower()]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found.")

    meta = _meta()

    # YoY for this region
    yoy_all = meta.get("yoy_by_region", [])
    yoy_row = next(
        (r for r in yoy_all if str(r.get("region", "")).lower() == region.lower()),
        {}
    )

    # Top products
    top_prods = meta.get("top_products_by_region", {})
    matched_key = next(
        (k for k in top_prods if k.lower() == region.lower()), None
    )
    products = top_prods.get(matched_key, []) if matched_key else []

    return {
        "summary"      : _df_records(row)[0],
        "yoy_revenue"  : yoy_row,
        "top_products" : products,
    }


# ── YoY Trends ────────────────────────────────────────────────────────────────

@router.get("/trends", summary="Year-over-year revenue by region")
def get_trends(
    top_n     : int  = Query(10,   description="Top N regions to include"),
    exclude_uk: bool = Query(False, description="Exclude United Kingdom"),
):
    """
    Returns YoY revenue pivot for the top N regions.
    Useful for multi-line trend charts.
    """
    meta     = _meta()
    yoy_all  = meta.get("yoy_by_region", [])
    regions  = meta.get("regions", [])

    # Get top N region names by total revenue
    top_names = [r["region"] for r in regions if not exclude_uk or r["region"] != "United Kingdom"][:top_n]

    filtered = [r for r in yoy_all if r.get("region") in top_names]
    return {
        "years"  : meta.get("years_covered"),
        "regions": filtered,
    }


# ── Monthly Trend ─────────────────────────────────────────────────────────────

@router.get("/monthly", summary="Monthly revenue trend for top regions")
def get_monthly(
    top_n: int = Query(6, description="Top N regions"),
):
    meta    = _meta()
    monthly = meta.get("monthly_trend", [])
    regions = meta.get("regions", [])

    top_names = [r["region"] for r in regions][:top_n]
    filtered  = [r for r in monthly if r.get("region") in top_names]
    return {
        "top_regions": top_names,
        "data"       : filtered,
    }


# ── Top Products by Region ────────────────────────────────────────────────────

@router.get("/top-products/{region}", summary="Top products by revenue for a region")
def get_top_products(region: str, limit: int = Query(10)):
    meta      = _meta()
    top_prods = meta.get("top_products_by_region", {})

    matched_key = next(
        (k for k in top_prods if k.lower() == region.lower()), None
    )
    if not matched_key:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found.")

    products = top_prods[matched_key][:limit]
    return {"region": matched_key, "products": products}
