"""
routers/abc.py — ABC Analysis Endpoints
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import os

router = APIRouter()
CSV = "abc_analysis_results.csv"


def load_abc() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(status_code=404, detail=f"{CSV} not found. Run analytics first.")
    df = pd.read_csv(CSV)
    df = df.where(pd.notnull(df), None)
    return df


@router.get("/products", summary="Get all ABC classified products")
def get_all_abc(
    abc_class: str = Query(None, description="Filter by class: A, B or C"),
    category:  str = Query(None, description="Filter by product category"),
    limit:     int = Query(100,  description="Max records"),
    offset:    int = Query(0,    description="Pagination offset")
):
    df = load_abc()
    if abc_class:
        df = df[df["abc_class"].str.upper() == abc_class.upper()]
    if category:
        df = df[df["category"].str.lower() == category.lower()]
    total = len(df)
    df = df.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "data": df.to_dict("records")}


@router.get("/product/{product_id}", summary="Get ABC class for a single product")
def get_product_abc(product_id: str):
    df = load_abc()
    result = df[df["product_id"].astype(str) == str(product_id)]
    if result.empty:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found")
    return result.iloc[0].to_dict()


@router.get("/classes/summary", summary="ABC class distribution summary")
def get_class_summary():
    df = load_abc()
    total_revenue = df["total_revenue"].sum()
    summary = (
        df.groupby("abc_class")
        .agg(
            product_count=("product_id",    "count"),
            total_revenue=("total_revenue", "sum"),
        )
        .reset_index()
        .sort_values("abc_class")
    )
    summary["revenue_pct"] = (summary["total_revenue"] / total_revenue * 100).round(2)
    summary["product_pct"] = (summary["product_count"] / len(df) * 100).round(2)
    return summary.to_dict("records")


@router.get("/top", summary="Top N products by revenue")
def get_top_products(n: int = Query(10)):
    df = load_abc()
    top = df.nlargest(n, "total_revenue")[
        ["product_id", "product_name", "category",
         "total_revenue", "abc_class"]
    ]
    return top.to_dict("records")


@router.get("/categories", summary="List all product categories")
def get_categories():
    df = load_abc()
    return {"categories": sorted(df["category"].dropna().unique().tolist())}