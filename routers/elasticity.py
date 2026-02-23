"""
routers/elasticity.py — Price Elasticity API Endpoints

Endpoints:
  GET /api/elasticity/summary              — distribution, metadata, extremes
  GET /api/elasticity/products             — all products (paginated, filterable)
  GET /api/elasticity/product/{id}         — single product detail
  GET /api/elasticity/type/{type}          — products by elasticity type
  GET /api/elasticity/top/elastic          — most price-sensitive products
  GET /api/elasticity/top/inelastic        — least price-sensitive products
  GET /api/elasticity/categories           — elasticity aggregated by category
"""

import json
import os
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

ELASTICITY_CSV  = "price_elasticity_results.csv"
METADATA_JSON   = "models/price_elasticity_metadata.json"

ELASTICITY_TYPES = [
    "Highly Elastic", "Elastic", "Inelastic", "Highly Inelastic", "Positive"
]


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_elasticity() -> pd.DataFrame:
    if not os.path.exists(ELASTICITY_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{ELASTICITY_CSV} not found. Run price_elasticity.py first.",
        )
    df = pd.read_csv(ELASTICITY_CSV)
    df = df.where(pd.notnull(df), None)
    return df


def load_metadata() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Elasticity metadata not found. Run price_elasticity.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Price elasticity summary & distribution")
def get_summary():
    """
    Overview: elasticity type distribution, average elasticity,
    most/least price-sensitive products, and model metadata.
    """
    meta = load_metadata()
    df   = load_elasticity()
    sig  = df[df["is_significant"] == True]

    type_dist = df["elasticity_type"].value_counts().to_dict()

    return {
        "generated_at"           : meta.get("generated_at"),
        "method"                 : meta.get("method"),
        "products_analysed"      : meta.get("products_analysed"),
        "significant_results"    : meta.get("significant_results"),
        "significance_pct"       : meta.get("significance_pct"),
        "avg_elasticity"         : meta.get("avg_elasticity"),
        "median_elasticity"      : meta.get("median_elasticity"),
        "elasticity_distribution": type_dist,
        "most_elastic_product"   : meta.get("most_elastic_product"),
        "most_inelastic_product" : meta.get("most_inelastic_product"),
        "significant_only_stats" : {
            "count"            : int(len(sig)),
            "avg_elasticity"   : round(float(sig["price_elasticity"].mean()), 4) if len(sig) else None,
            "median_elasticity": round(float(sig["price_elasticity"].median()), 4) if len(sig) else None,
        },
    }


# ── All Products ──────────────────────────────────────────────────────────────

@router.get("/products", summary="All products with price elasticity (paginated)")
def get_all_products(
    category       : str  = Query(None,  description="Filter by product category"),
    elasticity_type: str  = Query(None,  description="Filter by elasticity type"),
    significant_only: bool = Query(False, description="Only return statistically significant results (p < 0.05)"),
    limit          : int  = Query(100,   description="Max records to return"),
    offset         : int  = Query(0,     description="Pagination offset"),
):
    df = load_elasticity()

    if category:
        df = df[df["category"].str.lower() == category.lower()]
    if elasticity_type:
        df = df[df["elasticity_type"].str.lower() == elasticity_type.lower()]
    if significant_only:
        df = df[df["is_significant"] == True]

    total = len(df)
    page  = df.sort_values("price_elasticity").iloc[offset: offset + limit]
    return {
        "total"          : total,
        "limit"          : limit,
        "offset"         : offset,
        "significant_only": significant_only,
        "data"           : page.to_dict("records"),
    }


# ── Single Product ────────────────────────────────────────────────────────────

@router.get("/product/{product_id}", summary="Price elasticity for a single product")
def get_product(product_id: str):
    """
    Returns elasticity coefficient, confidence stats, price range,
    and a plain-English interpretation.
    """
    df     = load_elasticity()
    result = df[df["product_id"].astype(str) == str(product_id)]

    if result.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Product '{product_id}' not found in elasticity results.",
        )
    return result.iloc[0].to_dict()


# ── By Elasticity Type ────────────────────────────────────────────────────────

@router.get("/type/{elasticity_type}", summary="Products filtered by elasticity type")
def get_by_type(
    elasticity_type: str,
    significant_only: bool = Query(False),
    limit           : int  = Query(50),
    offset          : int  = Query(0),
):
    valid = [t.lower() for t in ELASTICITY_TYPES]
    if elasticity_type.lower().replace("-", " ") not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type. Choose from: {ELASTICITY_TYPES}",
        )

    df = load_elasticity()
    df = df[df["elasticity_type"].str.lower() == elasticity_type.lower().replace("-", " ")]

    if significant_only:
        df = df[df["is_significant"] == True]

    total = len(df)
    page  = df.sort_values("price_elasticity").iloc[offset: offset + limit]
    return {
        "elasticity_type": elasticity_type,
        "total"          : total,
        "data"           : page.to_dict("records"),
    }


# ── Top Elastic ───────────────────────────────────────────────────────────────

@router.get("/top/elastic", summary="Top N most price-sensitive products")
def get_top_elastic(
    n               : int  = Query(20, description="Number of products"),
    significant_only: bool = Query(True, description="Only statistically significant results"),
    category        : str  = Query(None),
):
    """Products where demand drops most sharply when price increases."""
    df = load_elasticity()
    df = df[df["price_elasticity"] < 0]

    if significant_only:
        df = df[df["is_significant"] == True]
    if category:
        df = df[df["category"].str.lower() == category.lower()]

    top = df.nsmallest(n, "price_elasticity")
    return top.to_dict("records")


# ── Top Inelastic ─────────────────────────────────────────────────────────────

@router.get("/top/inelastic", summary="Top N least price-sensitive products")
def get_top_inelastic(
    n               : int  = Query(20),
    significant_only: bool = Query(True),
    category        : str  = Query(None),
):
    """Products where demand is least affected by price changes — safe to raise prices."""
    df = load_elasticity()
    df = df[df["price_elasticity"] < 0]

    if significant_only:
        df = df[df["is_significant"] == True]
    if category:
        df = df[df["category"].str.lower() == category.lower()]

    top = df.nlargest(n, "price_elasticity")
    return top.to_dict("records")


# ── By Category ───────────────────────────────────────────────────────────────

@router.get("/categories", summary="Average price elasticity aggregated by category")
def get_by_category(significant_only: bool = Query(False)):
    """
    Category-level summary: average elasticity, distribution of types,
    and count of significant products.
    """
    df = load_elasticity()
    if significant_only:
        df = df[df["is_significant"] == True]

    agg = (
        df.groupby("category")
        .agg(
            product_count     =("product_id",        "count"),
            avg_elasticity    =("price_elasticity",  "mean"),
            median_elasticity =("price_elasticity",  "median"),
            significant_count =("is_significant",    "sum"),
            avg_price         =("avg_price",         "mean"),
        )
        .reset_index()
        .round(4)
        .sort_values("avg_elasticity")
    )

    return agg.to_dict("records")


# ── Types List ────────────────────────────────────────────────────────────────

@router.get("/types", summary="List all elasticity type labels")
def list_types():
    return {"types": ELASTICITY_TYPES}
