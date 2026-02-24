"""
routers/inventory.py — Inventory Optimisation API Endpoints

Endpoints:
  GET /api/inventory/summary              — metadata, risk distribution, cost totals
  GET /api/inventory/products             — all products (paginated, filterable)
  GET /api/inventory/product/{id}         — single product detail
  GET /api/inventory/alerts               — Critical / High stockout-risk products
  GET /api/inventory/categories           — EOQ & cost aggregated by category
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "inventory_optimization_results.csv"
METADATA_JSON = "models/inventory_metadata.json"

RISK_LEVELS = ["Critical", "High", "Medium", "Low"]


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run inventory_optimization.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    """NaN-safe conversion to JSON records."""
    return json.loads(df.to_json(orient="records"))


def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Inventory metadata not found. Run inventory_optimization.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Inventory optimisation summary & metadata")
def get_summary():
    """
    Global overview: risk distribution, average EOQ / safety stock / ROP,
    total annual inventory cost, and model assumptions.
    """
    meta = _meta()
    return {
        "generated_at"               : meta.get("generated_at"),
        "lead_time_days"             : meta.get("lead_time_days"),
        "service_level_pct"          : meta.get("service_level_pct"),
        "ordering_cost"              : meta.get("ordering_cost"),
        "holding_cost_rate"          : meta.get("holding_cost_rate"),
        "products_analysed"          : meta.get("products_analysed"),
        "risk_distribution"          : meta.get("risk_distribution"),
        "avg_eoq"                    : meta.get("avg_eoq"),
        "avg_safety_stock"           : meta.get("avg_safety_stock"),
        "avg_reorder_point"          : meta.get("avg_reorder_point"),
        "total_annual_holding_cost"  : meta.get("total_annual_holding_cost"),
        "total_annual_ordering_cost" : meta.get("total_annual_ordering_cost"),
        "total_annual_inventory_cost": meta.get("total_annual_inventory_cost"),
        "top_alerts"                 : meta.get("top_alerts"),
    }


# ── All Products ──────────────────────────────────────────────────────────────

@router.get("/products", summary="All products with inventory parameters (paginated)")
def get_all_products(
    category     : str  = Query(None, description="Filter by product category"),
    risk         : str  = Query(None, description="Filter by stockout risk level (Critical/High/Medium/Low)"),
    limit        : int  = Query(100,  description="Max records"),
    offset       : int  = Query(0,    description="Pagination offset"),
    sort_by      : str  = Query("annual_demand", description="Sort column"),
    ascending    : bool = Query(False, description="Sort ascending"),
):
    df = _load_df()

    if category:
        df = df[df["category"].str.lower() == category.lower()]
    if risk:
        df = df[df["stockout_risk"].str.lower() == risk.lower()]

    valid_sort = ["annual_demand", "eoq", "safety_stock", "reorder_point",
                  "total_annual_cost", "cv", "avg_daily_demand"]
    if sort_by not in valid_sort:
        sort_by = "annual_demand"

    total = len(df)
    page  = df.sort_values(sort_by, ascending=ascending).iloc[offset: offset + limit]
    return {
        "total"   : total,
        "limit"   : limit,
        "offset"  : offset,
        "data"    : _df_records(page),
    }


# ── Single Product ────────────────────────────────────────────────────────────

@router.get("/product/{product_id}", summary="Inventory parameters for a single product")
def get_product(product_id: str):
    """
    Returns full inventory profile: demand stats, safety stock, ROP, EOQ,
    forecast-adjusted metrics, and stockout risk.
    """
    df     = _load_df()
    result = df[df["product_id"].astype(str) == str(product_id)]

    if result.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Product '{product_id}' not found in inventory results.",
        )
    return _df_records(result)[0]


# ── Alerts: Critical / High risk ──────────────────────────────────────────────

@router.get("/alerts", summary="Products at Critical or High stockout risk")
def get_alerts(
    risk_level: str = Query("Critical", description="Minimum risk level (Critical or High)"),
    limit     : int = Query(50),
    category  : str = Query(None),
):
    """
    Products with highly volatile demand that need priority attention.
    Sorted by coefficient of variation (CV) descending.
    """
    df = _load_df()

    if risk_level.lower() == "critical":
        df = df[df["stockout_risk"] == "Critical"]
    else:
        df = df[df["stockout_risk"].isin(["Critical", "High"])]

    if category:
        df = df[df["category"].str.lower() == category.lower()]

    total = len(df)
    page  = df.sort_values("cv", ascending=False).head(limit)
    return {
        "risk_level" : risk_level,
        "total"      : total,
        "data"       : _df_records(page),
    }


# ── By Category ───────────────────────────────────────────────────────────────

@router.get("/categories", summary="Inventory metrics aggregated by product category")
def get_categories():
    """
    Category-level rollup: average EOQ, safety stock, ROP,
    total annual cost, and alert counts.
    """
    meta = _meta()
    cat  = meta.get("category_summary", [])
    return sorted(cat, key=lambda x: x.get("total_annual_cost", 0), reverse=True)
