"""
routers/store_performance.py -- Store Performance Analysis API

Endpoints:
  GET /api/store/summary           -- Global KPIs + tier counts
  GET /api/store/stores            -- All stores with KPIs (filterable/sortable)
  GET /api/store/{store_id}        -- Single store detail + top products
  GET /api/store/top               -- Top N stores by revenue
  GET /api/store/trends            -- Monthly revenue series (top-10 stores)
  GET /api/store/products/{store_id} -- Top products for a specific store
  GET /api/store/yoy               -- Year-over-year revenue by store
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "store_performance_results.csv"
METADATA_JSON = "models/store_performance_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Store metadata not found. Run store_performance.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run store_performance.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Store performance global KPIs")
def get_summary():
    """
    Returns top-level KPIs:
    total_stores, total_revenue, total_txns, total_customers, overall_aov,
    top_store_name, top_store_share, tier_counts, donut chart data.
    """
    meta = _meta()
    return {
        "generated_at"    : meta.get("generated_at"),
        "total_stores"    : meta.get("total_stores"),
        "total_revenue"   : meta.get("total_revenue"),
        "total_txns"      : meta.get("total_txns"),
        "total_customers" : meta.get("total_customers"),
        "overall_aov"     : meta.get("overall_aov"),
        "top_store_name"  : meta.get("top_store_name"),
        "top_store_share" : meta.get("top_store_share"),
        "tier_counts"     : meta.get("tier_counts"),
        "donut"           : meta.get("donut"),
        "quarterly"       : meta.get("quarterly"),
    }


# ── All stores ────────────────────────────────────────────────────────────────

@router.get("/stores", summary="All stores with KPIs (filterable)")
def get_stores(
    tier    : str = Query(None, description="Filter by tier: A, B, or C"),
    country : str = Query(None, description="Filter by country/region"),
    limit   : int = Query(50,   description="Max rows"),
    offset  : int = Query(0,    description="Pagination offset"),
):
    """
    Returns per-store KPIs sorted by revenue descending.
    Filterable by tier (A/B/C) and country.
    """
    df = _load_df()
    if tier:
        df = df[df["tier"].str.upper() == tier.upper()]
    if country:
        df = df[df["country"].str.lower() == country.lower()]
    total = len(df)
    page  = df.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset,
            "data": _df_records(page)}


# ── Top N stores ──────────────────────────────────────────────────────────────

@router.get("/top", summary="Top N stores by revenue")
def get_top(n: int = Query(10, description="Number of stores to return")):
    """Returns the top n stores ranked by total revenue."""
    df = _load_df()
    top = df.nlargest(n, "total_revenue")
    return {"n": n, "data": _df_records(top)}


# ── Single store ──────────────────────────────────────────────────────────────

@router.get("/{store_id}", summary="Single store detail with top products")
def get_store(store_id: int):
    """
    Returns full KPIs for a store plus its top-10 products by revenue.
    """
    df = _load_df()
    row = df[df["store_id"] == store_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Store ID {store_id} not found.")
    record = _df_records(row)[0]

    meta = _meta()
    tp   = meta.get("top_products", {})
    # Look up by store_key (int key in JSON is stored as string)
    store_key = record.get("store_key")
    products  = tp.get(str(store_key), tp.get(int(store_key), {}))
    record["top_products"] = products.get("products", [])

    bm = None
    if record.get("best_month_year") and record.get("best_month_month"):
        bm = {
            "year"   : record.get("best_month_year"),
            "month"  : record.get("best_month_month"),
            "revenue": record.get("best_month_revenue"),
        }
    record["best_month"] = bm
    return record


# ── Monthly trends ────────────────────────────────────────────────────────────

@router.get("/trends/monthly", summary="Monthly revenue series for top-10 stores")
def get_trends(
    store_name: str = Query(None, description="Filter to a single store by name"),
):
    """
    Returns monthly revenue data for the top-10 stores by total revenue.
    Optionally filter to a single store.
    """
    meta   = _meta()
    trends = meta.get("monthly_trends", [])
    if store_name:
        trends = [r for r in trends if r.get("store_name", "").lower() == store_name.lower()]
    return {"total": len(trends), "data": trends}


# ── Top products for a store ──────────────────────────────────────────────────

@router.get("/products/{store_id}", summary="Top products by revenue for a specific store")
def get_store_products(
    store_id: int,
    n       : int = Query(10, description="Number of top products to return"),
):
    """
    Returns the top n products by revenue for the given store.
    """
    df       = _load_df()
    row      = df[df["store_id"] == store_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Store ID {store_id} not found.")
    store_key = int(row.iloc[0]["store_key"])

    meta = _meta()
    tp   = meta.get("top_products", {})
    data = tp.get(str(store_key), tp.get(store_key, {}))
    products = data.get("products", [])[:n]
    return {
        "store_id"  : store_id,
        "store_name": data.get("store_name", ""),
        "n"         : n,
        "products"  : products,
    }


# ── YoY revenue ───────────────────────────────────────────────────────────────

@router.get("/yoy/all", summary="Year-over-year revenue per store")
def get_yoy(
    store_name: str = Query(None, description="Filter to a single store by name"),
):
    """
    Returns annual revenue records per store with YoY growth %.
    Optionally filter to a single store.
    """
    meta    = _meta()
    yoy     = meta.get("yoy", {})
    records = yoy.get("records", [])
    if store_name:
        records = [r for r in records if r.get("store_name", "").lower() == store_name.lower()]
    return {
        "years"  : yoy.get("years", []),
        "total"  : len(records),
        "data"   : records,
    }
