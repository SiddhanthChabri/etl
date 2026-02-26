"""
routers/pricing.py — Dynamic Pricing Recommendations Endpoints

Endpoints:
  GET /api/pricing/summary          — overall stats + top opportunities
  GET /api/pricing/products         — all recommendations (paginated + filterable)
  GET /api/pricing/product/{id}     — single product recommendation
  GET /api/pricing/action/{action}  — filter by action (RAISE_PRICE / LOWER_PRICE / HOLD)
  GET /api/pricing/top              — top N by expected revenue lift
"""

import json
import os
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

CSV  = "pricing_optimizer_results.csv"
META = "models/pricing_optimizer_metadata.json"


def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{CSV} not found. Run pricing_optimizer.py first.",
        )
    df = pd.read_csv(CSV)
    return df.where(pd.notnull(df), None)


def _load_meta() -> dict:
    if not os.path.exists(META):
        raise HTTPException(status_code=404, detail="Pricing metadata not found.")
    with open(META) as f:
        return json.load(f)


@router.get("/summary", summary="Pricing optimisation summary & top opportunities")
def get_pricing_summary():
    meta = _load_meta()
    df   = _load_df()

    avg_lift = round(float(
        df[df["action"].isin(["RAISE_PRICE", "LOWER_PRICE"])]["expected_revenue_lift_pct"].mean()
    ), 2) if not df.empty else 0

    return {
        "generated_at"       : meta.get("generated_at"),
        "total_products"     : meta.get("total_products"),
        "action_breakdown"   : meta.get("action_breakdown"),
        "confidence_breakdown": meta.get("confidence_breakdown"),
        "assumed_margin_pct" : meta.get("assumed_margin_pct"),
        "max_adjustment_pct" : meta.get("max_adjustment_pct"),
        "avg_revenue_lift_pct": avg_lift,
        "top_opportunities"  : meta.get("top_opportunities"),
    }


@router.get("/products", summary="All pricing recommendations (paginated)")
def get_all_products(
    action    : str  = Query(None, description="RAISE_PRICE | LOWER_PRICE | HOLD"),
    confidence: str  = Query(None, description="HIGH | MEDIUM | LOW"),
    category  : str  = Query(None, description="Filter by product category"),
    limit     : int  = Query(100,  description="Max records"),
    offset    : int  = Query(0,    description="Pagination offset"),
):
    df = _load_df()
    if action:
        df = df[df["action"].str.upper() == action.upper()]
    if confidence:
        df = df[df["confidence"].str.upper() == confidence.upper()]
    if category:
        df = df[df["category"].str.lower() == category.lower()]

    total = len(df)
    page  = df.iloc[offset: offset + limit]
    return {
        "total" : total,
        "limit" : limit,
        "offset": offset,
        "computed_at": datetime.utcnow().isoformat(),
        "data"  : page.to_dict("records"),
    }


@router.get("/product/{product_id}", summary="Pricing recommendation for one product")
def get_product(product_id: str):
    df = _load_df()
    row = df[df["product_id"].astype(str) == str(product_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found.")
    return row.iloc[0].to_dict()


@router.get("/action/{action}", summary="All products for a specific pricing action")
def get_by_action(
    action: str,
    limit : int = Query(50, description="Max records"),
    offset: int = Query(0,  description="Pagination offset"),
):
    valid = {"RAISE_PRICE", "LOWER_PRICE", "HOLD"}
    if action.upper() not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action '{action}'. Use one of: {valid}",
        )
    df    = _load_df()
    df    = df[df["action"].str.upper() == action.upper()]
    total = len(df)
    page  = df.sort_values("expected_revenue_lift_pct", ascending=False).iloc[offset: offset + limit]
    return {
        "action": action.upper(),
        "total" : total,
        "limit" : limit,
        "offset": offset,
        "computed_at": datetime.utcnow().isoformat(),
        "data"  : page.to_dict("records"),
    }


@router.get("/top", summary="Top N products by expected revenue lift")
def get_top(
    n         : int  = Query(10,  description="Number of top products"),
    confidence: str  = Query(None, description="Filter by confidence level"),
):
    df = _load_df()
    df = df[df["action"].isin(["RAISE_PRICE", "LOWER_PRICE"])]
    if confidence:
        df = df[df["confidence"].str.upper() == confidence.upper()]
    top = df.nlargest(n, "expected_revenue_lift_pct")
    return {
        "n"         : n,
        "computed_at": datetime.utcnow().isoformat(),
        "data"      : top.to_dict("records"),
    }
