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


@router.get("/top", summary="Top N strongest association rules by lift")
def get_top_rules(n: int = Query(10)):
    df = load_basket()
    if "lift" in df.columns:
        df = df.nlargest(n, "lift")
    return df.head(n).to_dict("records")