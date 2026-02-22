"""
routers/basket.py — Market Basket Endpoints
"""

from fastapi import APIRouter, Query, HTTPException
import pandas as pd
import os

router = APIRouter()
CSV = "market_basket_results.csv"


def load_basket() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(status_code=404, detail=f"{CSV} not found. Run analytics first.")
    df = pd.read_csv(CSV)
    df = df.where(pd.notnull(df), None)
    return df


@router.get("/rules", summary="Get all association rules")
def get_all_rules(
    min_confidence: float = Query(0.0,  description="Minimum confidence threshold"),
    min_support:    float = Query(0.0,  description="Minimum support threshold"),
    limit:          int   = Query(50),
    offset:         int   = Query(0)
):
    df = load_basket()
    if "confidence" in df.columns:
        df = df[df["confidence"] >= min_confidence]
    if "support" in df.columns:
        df = df[df["support"] >= min_support]
    total = len(df)
    df = df.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "data": df.to_dict("records")}


@router.get("/recommendations/{product_name}", summary="Get products frequently bought with a given product")
def get_recommendations(product_name: str, limit: int = Query(5)):
    df = load_basket()
    # Check both antecedents and consequents columns
    recs = pd.DataFrame()
    if "antecedents" in df.columns and "consequents" in df.columns:
        mask = (
            df["antecedents"].str.lower().str.contains(product_name.lower(), na=False) |
            df["consequents"].str.lower().str.contains(product_name.lower(), na=False)
        )
        recs = df[mask]
    if recs.empty:
        return {"product": product_name, "recommendations": [], "message": "No associations found"}
    recs = recs.sort_values("confidence", ascending=False).head(limit)
    return {"product": product_name, "recommendations": recs.to_dict("records")}


@router.get("/top")
def get_basket_top(n: int = Query(50)):
    path = "market_basket_results.csv"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="market_basket_results.csv not found.")

    df = pd.read_csv(path)

    # Compute lift = confidence_a_to_b / support(b)
    # Since we don't have support_b, use confidence_a_to_b as proxy sort key
    # Sort by confidence_a_to_b descending
    if "confidence_a_to_b" in df.columns:
        df = df.sort_values("confidence_a_to_b", ascending=False)

    df = df.head(n)

    # Rename to standard names the dashboard expects
    df = df.rename(columns={
        "product_a"        : "antecedents",
        "product_b"        : "consequents",
        "confidence_a_to_b": "confidence",
        "confidence_b_to_a": "confidence_b_to_a",
        "count"            : "transaction_count"
    })

    # Compute lift approximation: confidence / support
    df["lift"] = (df["confidence"] / df["support"]).round(4)

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    return df.to_dict("records")
