"""
routers/clv.py — CLV Analysis Endpoints
"""

from fastapi import APIRouter, Query, HTTPException
import pandas as pd
import os

router = APIRouter()
CSV = "clv_analysis_results.csv"


def load_clv() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(status_code=404, detail=f"{CSV} not found. Run analytics first.")
    df = pd.read_csv(CSV)
    df = df.where(pd.notnull(df), None)
    return df


@router.get("/customers", summary="Get all CLV scores")
def get_all_clv(
    segment: str = Query(None, description="Filter by CLV segment"),
    limit:   int = Query(100),
    offset:  int = Query(0)
):
    df = load_clv()
    if segment:
        df = df[df["clv_segment"].str.lower() == segment.lower()]
    total = len(df)
    df = df.sort_values("clv_discounted", ascending=False).iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "data": df.to_dict("records")}


@router.get("/customer/{customer_id}", summary="Get CLV for a single customer")
def get_customer_clv(customer_id: str):
    df = load_clv()
    result = df[df["customer_id"].astype(str) == str(customer_id)]
    if result.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    return result.iloc[0].to_dict()


@router.get("/segments/summary", summary="CLV segment distribution")
def get_clv_summary():
    df = load_clv()
    summary = (
        df.groupby("clv_segment")
        .agg(
            customer_count=("customer_id",      "count"),
            avg_clv=       ("clv_discounted",   "mean"),
            total_clv=     ("clv_discounted",   "sum"),
            avg_purchases= ("purchase_count",   "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("avg_clv", ascending=False)
    )
    return summary.to_dict("records")


@router.get("/top", summary="Top N customers by CLV")
def get_top_clv(n: int = Query(10)):
    df = load_clv()
    top = df.nlargest(n, "clv_discounted")[
        ["customer_id", "customer_name", "state",
         "purchase_count", "total_revenue", "clv_discounted", "clv_segment"]
    ]
    return top.to_dict("records")