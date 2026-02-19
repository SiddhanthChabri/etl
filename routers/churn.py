"""
routers/churn.py — Churn Prediction Endpoints
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import pandas as pd
import numpy as np
import joblib
import json
import os

router = APIRouter()

MODEL_PATH    = "models/churn_model.pkl"
FEATURES_PATH = "models/churn_features.json"
META_PATH     = "models/churn_model_metadata.json"
CSV_PATH      = "churn_predictions.csv"


def load_model():
    if not os.path.exists(MODEL_PATH):
        raise HTTPException(status_code=404,
            detail="Model not found. Run ml_churn_prediction.py first.")
    model    = joblib.load(MODEL_PATH)
    features = json.load(open(FEATURES_PATH))
    return model, features


def load_predictions() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise HTTPException(status_code=404,
            detail="Predictions not found. Run ml_churn_prediction.py first.")
    df = pd.read_csv(CSV_PATH)
    df = df.where(pd.notnull(df), None)
    return df


@router.get("/summary", summary="Churn prediction summary & model metrics")
def get_churn_summary():
    if not os.path.exists(META_PATH):
        raise HTTPException(status_code=404, detail="Model metadata not found.")
    meta = json.load(open(META_PATH))
    df   = load_predictions()

    dist = df["churn_risk_tier"].value_counts().to_dict()
    return {
        "model":       meta["model_name"],
        "trained_at":  meta["trained_at"],
        "metrics":     meta["metrics"],
        "top_features":meta["top_features"][:5],
        "risk_distribution": dist,
        "total_customers": len(df),
        "predicted_churners": int(df["churn_prediction"].sum()),
        "churn_rate_pct": round(df["churn_prediction"].mean() * 100, 2),
    }


@router.get("/customer/{customer_id}", summary="Get churn prediction for a customer")
def get_customer_churn(customer_id: str):
    df     = load_predictions()
    result = df[df["customer_id"].astype(str) == str(customer_id)]
    if result.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    row = result.iloc[0].to_dict()
    row["churn_probability_pct"] = round(float(row.get("churn_probability", 0)) * 100, 2)
    return row


@router.get("/risk/{tier}", summary="Get all customers in a risk tier")
def get_customers_by_risk(
    tier:   str = "critical",
    limit:  int = Query(50),
    offset: int = Query(0)
):
    tier_map = {
        "critical": "Critical Risk",
        "high":     "High Risk",
        "medium":   "Medium Risk",
        "low":      "Low Risk"
    }
    tier_label = tier_map.get(tier.lower())
    if not tier_label:
        raise HTTPException(status_code=400,
            detail=f"Invalid tier. Choose from: {list(tier_map.keys())}")
    df    = load_predictions()
    df    = df[df["churn_risk_tier"] == tier_label]
    total = len(df)
    df    = df.sort_values("churn_probability", ascending=False).iloc[offset:offset+limit]
    return {"tier": tier_label, "total": total, "data": df.to_dict("records")}


@router.get("/top-at-risk", summary="Top N high-value customers most likely to churn")
def get_top_at_risk(n: int = Query(20)):
    """High monetary value + high churn probability = biggest business risk"""
    df = load_predictions()
    df["risk_score"] = df["churn_probability"] * df["monetary"]
    top = df.nlargest(n, "risk_score")[
        ["customer_id", "customer_name", "state",
         "monetary", "frequency", "churn_probability",
         "churn_risk_tier", "segment"]
    ]
    top["churn_probability_pct"] = (top["churn_probability"] * 100).round(2)
    return top.to_dict("records")


class PredictRequest(BaseModel):
    recency:   float
    frequency: float
    monetary:  float
    r_score:   int = 3
    f_score:   int = 3
    m_score:   int = 3
    rfm_value: float = 333


@router.post("/predict", summary="Predict churn for a custom input")
def predict_custom(req: PredictRequest):
    """
    Provide RFM values directly and get a churn probability prediction.
    Useful for scoring new/hypothetical customers.
    """
    model, features = load_model()
    rfm_df = pd.read_csv("rfm_analysis_results.csv")

    input_data = {
        "recency":   req.recency,
        "frequency": req.frequency,
        "monetary":  req.monetary,
        "r_score":   req.r_score,
        "f_score":   req.f_score,
        "m_score":   req.m_score,
        "rfm_value": req.rfm_value,
        "avg_order_value":      req.monetary / max(req.frequency, 1),
        "revenue_per_day":      req.monetary / max(req.recency, 1),
        "recency_x_frequency":  req.recency  * req.frequency,
        "monetary_x_frequency": req.monetary * req.frequency,
        "log_monetary":         np.log1p(req.monetary),
        "log_frequency":        np.log1p(req.frequency),
        "log_recency":          np.log1p(req.recency),
        "segment_score":        3,
    }
    # Fill any extra features with 0
    row = pd.DataFrame([{f: input_data.get(f, 0) for f in features}])
    prob = float(model.predict_proba(row)[0][1])

    if prob >= 0.75: risk = "Critical Risk"
    elif prob >= 0.50: risk = "High Risk"
    elif prob >= 0.25: risk = "Medium Risk"
    else: risk = "Low Risk"

    return {
        "churn_probability":     round(prob, 4),
        "churn_probability_pct": round(prob * 100, 2),
        "churn_prediction":      int(prob >= 0.5),
        "risk_tier":             risk,
        "input":                 req.dict()
    }