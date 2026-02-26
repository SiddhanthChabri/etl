"""
routers/clv_prediction.py — Forward-Looking CLV Prediction Endpoints

Endpoints:
  GET /api/clv-predict/summary          — model info + segment breakdown
  GET /api/clv-predict/customers        — all predictions (paginated)
  GET /api/clv-predict/customer/{id}    — single customer prediction
  GET /api/clv-predict/top              — top N by predicted CLV
  GET /api/clv-predict/segment/{seg}    — all customers in a CLV segment
"""

import json
import os
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

CSV  = "clv_predictions.csv"
META = "models/clv_prediction_metadata.json"


def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{CSV} not found. Run clv_prediction.py first.",
        )
    df = pd.read_csv(CSV)
    return df.where(pd.notnull(df), None)


def _load_meta() -> dict:
    if not os.path.exists(META):
        raise HTTPException(status_code=404, detail="CLV prediction metadata not found.")
    with open(META) as f:
        return json.load(f)


@router.get("/summary", summary="CLV prediction model summary & segment breakdown")
def get_summary():
    meta = _load_meta()
    return {
        "generated_at"          : meta.get("generated_at"),
        "method"                : meta.get("method"),
        "prediction_period_days": meta.get("prediction_period_days"),
        "clv_horizon_months"    : meta.get("clv_horizon_months"),
        "discount_rate"         : meta.get("discount_rate"),
        "total_customers"       : meta.get("total_customers"),
        "avg_predicted_clv"     : meta.get("avg_predicted_clv"),
        "avg_p_alive"           : meta.get("avg_p_alive"),
        "segment_breakdown"     : meta.get("segment_breakdown"),
        "top_10_predicted"      : meta.get("top_10_predicted"),
    }


@router.get("/customers", summary="All CLV predictions (paginated)")
def get_all(
    segment: str = Query(None, description="Filter by predicted_clv_segment"),
    limit  : int = Query(100,  description="Max records"),
    offset : int = Query(0,    description="Pagination offset"),
):
    df = _load_df()
    if segment:
        df = df[df["predicted_clv_segment"].str.lower() == segment.lower()]
    total = len(df)
    page  = df.sort_values("predicted_clv", ascending=False).iloc[offset: offset + limit]
    return {
        "total"      : total,
        "limit"      : limit,
        "offset"     : offset,
        "computed_at": datetime.utcnow().isoformat(),
        "data"       : page.to_dict("records"),
    }


@router.get("/customer/{customer_id}", summary="CLV prediction for one customer")
def get_customer(customer_id: str):
    df  = _load_df()
    row = df[df["customer_id"].astype(str) == str(customer_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found.")
    return row.iloc[0].to_dict()


@router.get("/top", summary="Top N customers by predicted CLV")
def get_top(n: int = Query(10, description="Number of top customers")):
    df = _load_df()
    top = df.nlargest(n, "predicted_clv")
    return {
        "n"          : n,
        "computed_at": datetime.utcnow().isoformat(),
        "data"       : top.to_dict("records"),
    }


@router.get("/segment/{segment}", summary="All customers in a predicted CLV segment")
def get_segment(
    segment: str,
    limit  : int = Query(100, description="Max records"),
    offset : int = Query(0,   description="Pagination offset"),
):
    df  = _load_df()
    sub = df[df["predicted_clv_segment"].str.lower() == segment.lower()]
    if sub.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Segment '{segment}' not found. Available: {df['predicted_clv_segment'].dropna().unique().tolist()}",
        )
    total = len(sub)
    page  = sub.sort_values("predicted_clv", ascending=False).iloc[offset: offset + limit]
    return {
        "segment"    : segment,
        "total"      : total,
        "limit"      : limit,
        "offset"     : offset,
        "computed_at": datetime.utcnow().isoformat(),
        "data"       : page.to_dict("records"),
    }
