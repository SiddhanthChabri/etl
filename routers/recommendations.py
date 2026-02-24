"""
routers/recommendations.py -- Product Recommendation Engine API

Endpoints:
  GET /api/recommendations/summary                -- model stats + popular + top recommended
  GET /api/recommendations/popular                -- cold-start fallback products
  GET /api/recommendations/customer/{customer_id} -- top-10 recs for a customer
  GET /api/recommendations/customers              -- all customers with their rec count
  GET /api/recommendations/product/{product_id}   -- top-10 similar products for a product
  GET /api/recommendations/top                    -- most recommended products overall
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "product_recommendations.csv"
METADATA_JSON = "models/recommendations_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Recommendations metadata not found. Run product_recommendations.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run product_recommendations.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Recommendation engine summary & model stats")
def get_summary():
    """
    Returns model statistics, top recommended products overall,
    and popular products (cold-start fallback).
    """
    meta = _meta()
    return {
        "generated_at"         : meta.get("generated_at"),
        "n_customers"          : meta.get("n_customers"),
        "n_products"           : meta.get("n_products"),
        "n_pairs"              : meta.get("n_pairs"),
        "sparsity_pct"         : meta.get("sparsity_pct"),
        "avg_similarity"       : meta.get("avg_similarity"),
        "avg_recs_per_customer": meta.get("avg_recs_per_customer"),
        "customers_served"     : meta.get("customers_served"),
        "coverage_pct"         : meta.get("coverage_pct"),
        "top_recommended"      : meta.get("top_recommended", []),
        "popular_products"     : meta.get("popular_products", []),
    }


# ── Popular products (cold-start) ─────────────────────────────────────────────

@router.get("/popular", summary="Top products by unique buyer count (cold-start fallback)")
def get_popular(n: int = Query(20, description="Number of popular products to return")):
    """
    Returns the most broadly purchased products — used as cold-start
    recommendations for new / sparse customers.
    """
    meta = _meta()
    popular = meta.get("popular_products", [])[:n]
    return {"n": len(popular), "data": popular}


# ── Per-customer recommendations ──────────────────────────────────────────────

@router.get("/customer/{customer_id}", summary="Top-10 recommendations for a specific customer")
def get_customer_recs(customer_id: str):
    """
    Returns the top-10 recommended products for the given customer_id,
    scored by item-item collaborative filtering.
    Falls back to popular products if the customer is not found.
    """
    meta    = _meta()
    c_recs  = meta.get("customer_recs", {})

    # Search by customer_id string (keys are customer_key ints stored as strings)
    match = None
    for ck_str, val in c_recs.items():
        if val.get("customer_id") == customer_id:
            match = {"customer_key": int(ck_str), **val}
            break

    if match is None:
        popular = meta.get("popular_products", [])[:10]
        return {
            "customer_id"    : customer_id,
            "found"          : False,
            "fallback"       : "popular_products",
            "recommendations": popular,
        }

    return {
        "customer_id"    : customer_id,
        "customer_key"   : match["customer_key"],
        "customer_name"  : match.get("customer_name", ""),
        "found"          : True,
        "fallback"       : None,
        "recommendations": match.get("recommendations", []),
    }


# ── All customers list ─────────────────────────────────────────────────────────

@router.get("/customers", summary="All customers with recommendation counts (paginated)")
def get_customers(
    limit : int = Query(50, description="Max rows"),
    offset: int = Query(0,  description="Pagination offset"),
    search: str = Query(None, description="Filter by customer_name substring"),
):
    """
    Returns a paginated list of all customers with their number of recommendations.
    """
    df = _load_df()
    summary = (
        df.groupby(["customer_key", "customer_id", "customer_name"])
          .agg(rec_count=("rank", "count"), max_score=("rec_score", "max"))
          .reset_index()
          .sort_values("max_score", ascending=False)
    )
    if search:
        summary = summary[summary["customer_name"].str.contains(search, case=False, na=False)]
    total = len(summary)
    page  = summary.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset,
            "data": _df_records(page)}


# ── Product similarity lookup ──────────────────────────────────────────────────

@router.get("/product/{product_id}", summary="Top-10 similar products for a given product")
def get_similar_products(product_id: str):
    """
    Returns the top-10 most similar products to the given product_id,
    based on item-item cosine similarity from purchase co-occurrence.
    """
    meta     = _meta()
    item_sim = meta.get("item_similarity", {})

    # Find the product_key for this product_id
    # item_similarity keys are product_key strings
    target_key = None
    for pk_str, similar_list in item_sim.items():
        # The product_id isn't in item_sim keys directly — scan all similar entries
        # to find which key corresponds to this product_id using the CSV
        pass

    # Load CSV to resolve product_id → product_key
    df = _load_df()
    matches = df[df["product_id"] == product_id]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Product ID '{product_id}' not found.")
    product_key = int(matches.iloc[0]["product_key"])
    product_name = str(matches.iloc[0]["product_name"])

    similar = item_sim.get(str(product_key), [])
    return {
        "product_id"   : product_id,
        "product_key"  : product_key,
        "product_name" : product_name,
        "n_similar"    : len(similar),
        "similar"      : similar,
    }


# ── Top recommended products overall ──────────────────────────────────────────

@router.get("/top", summary="Top products by number of customers they're recommended to")
def get_top_recommended(n: int = Query(20, description="Number of top products to return")):
    """
    Returns products most frequently appearing in customer recommendation lists,
    indicating broadly popular but not yet universally purchased items.
    """
    meta     = _meta()
    top_recs = meta.get("top_recommended", [])[:n]
    return {"n": len(top_recs), "data": top_recs}
