"""
routers/forecast.py — Demand Forecasting API Endpoints

Serves pre-computed demand forecasts from demand_forecasting.py output.

Endpoints:
  GET /api/forecast/summary              — model info + top products
  GET /api/forecast/products             — all forecast rows (paginated)
  GET /api/forecast/product/{id}         — full time series for one product
  GET /api/forecast/top                  — top N by expected demand
  GET /api/forecast/category/{category}  — aggregate category forecast
  GET /api/forecast/categories           — list all categories
  GET /api/forecast/accuracy             — model accuracy metrics
"""

import json
import os
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

FORECAST_CSV  = "demand_forecast_results.csv"
METADATA_JSON = "models/demand_forecast_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_forecast() -> pd.DataFrame:
    if not os.path.exists(FORECAST_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{FORECAST_CSV} not found. Run demand_forecasting.py first.",
        )
    df = pd.read_csv(FORECAST_CSV)
    df = df.where(pd.notnull(df), None)
    return df


def load_metadata() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Forecast metadata not found. Run demand_forecasting.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Demand forecast summary & model info")
def get_forecast_summary():
    """
    Returns model metadata, top forecasted products,
    and total expected demand by category.
    """
    meta   = load_metadata()
    df     = load_forecast()
    future = df[df["is_forecast"] == True]

    top_products = (
        future
        .groupby(["product_id", "product_name", "category"])["predicted_quantity"]
        .sum()
        .reset_index()
        .nlargest(10, "predicted_quantity")
        .round(2)
        .to_dict("records")
    )

    category_forecast = (
        future
        .groupby("category")["predicted_quantity"]
        .sum()
        .round(2)
        .to_dict()
    )

    return {
        "generated_at"        : meta.get("generated_at"),
        "method"              : meta.get("method"),
        "horizon_days"        : meta.get("horizon_days"),
        "products_forecast"   : meta.get("products_forecast"),
        "data_through"        : meta.get("data_through"),
        "total_forecasted_qty": round(float(future["predicted_quantity"].sum()), 2),
        "top_demand_products" : top_products,
        "forecast_by_category": category_forecast,
    }


# ── All Products ──────────────────────────────────────────────────────────────

@router.get("/products", summary="All demand forecasts (paginated)")
def get_all_forecasts(
    category   : str  = Query(None,  description="Filter by product category"),
    future_only: bool = Query(True,  description="Return only future forecast rows"),
    limit      : int  = Query(100,   description="Max records to return"),
    offset     : int  = Query(0,     description="Pagination offset"),
):
    df = load_forecast()

    if future_only:
        df = df[df["is_forecast"] == True]
    if category:
        df = df[df["category"].str.lower() == category.lower()]

    total = len(df)
    page  = df.iloc[offset: offset + limit]
    return {
        "total"      : total,
        "limit"      : limit,
        "offset"     : offset,
        "future_only": future_only,
        "data"       : page.to_dict("records"),
    }


# ── Single Product ────────────────────────────────────────────────────────────

@router.get("/product/{product_id}", summary="Full forecast time series for one product")
def get_product_forecast(
    product_id : str,
    future_only: bool = Query(False, description="Return only future dates"),
):
    """
    Returns the complete historical + forecast time series for a product,
    along with its accuracy metrics and metadata.
    """
    df     = load_forecast()
    result = df[df["product_id"].astype(str) == str(product_id)]

    if result.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Product '{product_id}' not found in forecast results.",
        )

    if future_only:
        result = result[result["is_forecast"] == True]

    meta  = load_metadata()
    pmeta = next(
        (p for p in meta.get("product_details", [])
         if str(p["product_id"]) == str(product_id)),
        {},
    )

    return {
        "product_id"          : product_id,
        "product_name"        : result["product_name"].iloc[0],
        "category"            : result["category"].iloc[0],
        "method"              : meta.get("method"),
        "horizon_days"        : meta.get("horizon_days"),
        "accuracy"            : pmeta.get("accuracy"),
        "avg_daily_qty"       : pmeta.get("avg_daily_qty"),
        "total_historical_qty": pmeta.get("total_historical_qty"),
        "timeseries"          : result.to_dict("records"),
    }


# ── Top Products ──────────────────────────────────────────────────────────────

@router.get("/top", summary="Top N products by expected demand")
def get_top_demand_products(
    n       : int = Query(10,  description="Number of top products"),
    category: str = Query(None, description="Filter by category"),
):
    df = load_forecast()
    df = df[df["is_forecast"] == True]

    if category:
        df = df[df["category"].str.lower() == category.lower()]
        if df.empty:
            raise HTTPException(
                status_code=404, detail=f"Category '{category}' not found."
            )

    top = (
        df.groupby(["product_id", "product_name", "category"])
        .agg(
            total_forecasted_qty=("predicted_quantity", "sum"),
            avg_daily_forecast   =("predicted_quantity", "mean"),
        )
        .reset_index()
        .nlargest(n, "total_forecasted_qty")
        .round(2)
    )
    return top.to_dict("records")


# ── Category Aggregate ────────────────────────────────────────────────────────

@router.get("/category/{category}", summary="Aggregate demand forecast for a category")
def get_category_forecast(category: str):
    """
    Aggregates all product forecasts within a category into a single time series.
    Useful for category-level inventory planning.
    """
    df = load_forecast()
    df = df[df["category"].str.lower() == category.lower()]

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"Category '{category}' not found."
        )

    agg = (
        df.groupby("forecast_date")
        .agg(
            total_predicted=("predicted_quantity", "sum"),
            total_lower    =("lower_bound",         "sum"),
            total_upper    =("upper_bound",         "sum"),
            is_forecast    =("is_forecast",         "first"),
        )
        .reset_index()
        .round(2)
        .sort_values("forecast_date")
    )

    future_total = round(
        float(df[df["is_forecast"] == True]["predicted_quantity"].sum()), 2
    )

    return {
        "category"           : category,
        "products_in_category": int(df["product_id"].nunique()),
        "total_forecasted_qty": future_total,
        "timeseries"         : agg.to_dict("records"),
    }


# ── Categories List ───────────────────────────────────────────────────────────

@router.get("/categories", summary="List all categories available in forecast data")
def list_categories():
    df = load_forecast()
    return {
        "categories": sorted(df["category"].dropna().unique().tolist())
    }


# ── Accuracy Metrics ──────────────────────────────────────────────────────────

@router.get("/accuracy", summary="Model accuracy metrics across all forecasted products")
def get_accuracy_metrics():
    """
    Returns aggregate accuracy (MAE, RMSE, MAPE) across all products,
    plus per-product breakdown.
    """
    meta    = load_metadata()
    details = meta.get("product_details", [])

    maes  = [d["accuracy"]["mae"]  for d in details if d.get("accuracy") and d["accuracy"].get("mae")  is not None]
    rmses = [d["accuracy"]["rmse"] for d in details if d.get("accuracy") and d["accuracy"].get("rmse") is not None]
    mapes = [d["accuracy"]["mape"] for d in details if d.get("accuracy") and d["accuracy"].get("mape") is not None]

    return {
        "method"      : meta.get("method"),
        "n_products"  : len(details),
        "avg_mae"     : round(float(np.mean(maes)),  3) if maes  else None,
        "avg_rmse"    : round(float(np.mean(rmses)), 3) if rmses else None,
        "avg_mape_pct": round(float(np.mean(mapes)), 2) if mapes else None,
        "note"        : "MAPE computed only on days with actual demand > 0",
        "per_product" : [
            {
                "product_id"  : d["product_id"],
                "product_name": d["product_name"],
                "category"    : d["category"],
                **d.get("accuracy", {}),
            }
            for d in details
        ],
    }
