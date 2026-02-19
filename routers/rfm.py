"""
routers/rfm.py — RFM Analysis Endpoints
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import os

router = APIRouter()
CSV = "rfm_analysis_results.csv"


def load_rfm() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(status_code=404, detail=f"{CSV} not found. Run analytics first.")
    df = pd.read_csv(CSV)
    df = df.where(pd.notnull(df), None)  # NaN → None (JSON safe)
    return df


@router.get("/customers", summary="Get all RFM customer segments")
def get_all_rfm(
    segment: str = Query(None, description="Filter by segment name"),
    limit:   int = Query(100,  description="Max records to return"),
    offset:  int = Query(0,    description="Pagination offset")
):
    df = load_rfm()
    if segment:
        df = df[df["segment"].str.lower() == segment.lower()]
    total = len(df)
    df = df.iloc[offset: offset + limit]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": df.to_dict("records")
    }


@router.get("/customer/{customer_id}", summary="Get RFM data for a single customer")
def get_customer_rfm(customer_id: str):
    df = load_rfm()
    result = df[df["customer_id"].astype(str) == str(customer_id)]
    if result.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    return result.iloc[0].to_dict()


@router.get("/segments/summary", summary="RFM segment distribution summary")
def get_segment_summary():
    df = load_rfm()
    summary = (
        df.groupby("segment")
        .agg(
            customer_count=("customer_id", "count"),
            avg_recency=("recency",   "mean"),
            avg_frequency=("frequency", "mean"),
            avg_monetary=("monetary",  "mean"),
            total_revenue=("monetary",  "sum"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    return summary.to_dict("records")


@router.get("/segments/list", summary="List all unique RFM segments")
def get_segment_list():
    df = load_rfm()
    return {"segments": sorted(df["segment"].dropna().unique().tolist())}


@router.get("/top", summary="Top N customers by monetary value")
def get_top_customers(n: int = Query(10, description="Number of top customers")):
    df = load_rfm()
    top = df.nlargest(n, "monetary")[
        ["customer_id", "customer_name", "state", "recency",
         "frequency", "monetary", "segment"]
    ]
    return top.to_dict("records")