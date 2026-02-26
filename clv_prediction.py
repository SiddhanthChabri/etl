"""
clv_prediction.py — Forward-Looking CLV Prediction (BG/NBD + Gamma-Gamma)

Uses the lifetimes library (BG/NBD + Gamma-Gamma models) to predict:
  - Expected future transactions per customer (next 90 days)
  - Expected average order value
  - Predicted CLV for next 12 months

Falls back to a simple parametric model if lifetimes is not installed.

Output: clv_predictions.csv
        models/clv_prediction_metadata.json
"""

import json
import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CLV_CSV    = os.path.join(BASE_DIR, "clv_analysis_results.csv")
RFM_CSV    = os.path.join(BASE_DIR, "rfm_analysis_results.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "clv_predictions.csv")
META_PATH  = os.path.join(BASE_DIR, "models", "clv_prediction_metadata.json")

PREDICTION_PERIOD  = 90    # days to predict future transactions
CLV_HORIZON_MONTHS = 12    # months for CLV calculation
DISCOUNT_RATE      = 0.10  # annual discount rate
PENALIZER          = 0.001


def _try_lifetimes(df_rfm: pd.DataFrame, df_clv: pd.DataFrame) -> tuple:
    """Attempt BG/NBD + Gamma-Gamma modelling with lifetimes library."""
    from lifetimes import BetaGeoFitter, GammaGammaFitter

    merged = df_rfm.merge(
        df_clv[["customer_id", "lifespan_days", "avg_purchase_value", "purchase_count"]],
        on="customer_id", how="left"
    )

    max_T = float(merged["lifespan_days"].max() or 365.0)

    summary = pd.DataFrame({
        "customer_id"   : merged["customer_id"],
        "frequency"     : (merged["frequency"] - 1).clip(lower=0),
        "recency"       : merged["lifespan_days"].fillna(0),
        "T"             : max_T,
        "monetary_value": merged.get(
            "avg_purchase_value",
            merged["monetary"] / merged["frequency"].clip(lower=1)
        ).fillna(0),
    }).dropna()

    summary = summary[(summary["T"] > 0) & (summary["monetary_value"] > 0)]
    if len(summary) < 10:
        raise ValueError("insufficient_data")

    bgf = BetaGeoFitter(penalizer_coef=PENALIZER)
    bgf.fit(summary["frequency"], summary["recency"], summary["T"])

    repeat = summary[summary["frequency"] > 0]
    if len(repeat) < 5:
        raise ValueError("insufficient_repeat_customers")

    ggf = GammaGammaFitter(penalizer_coef=PENALIZER)
    ggf.fit(repeat["frequency"], repeat["monetary_value"])

    summary["predicted_purchases"] = bgf.conditional_expected_number_of_purchases_up_to_time(
        PREDICTION_PERIOD, summary["frequency"], summary["recency"], summary["T"]
    ).round(4)

    summary["predicted_avg_value"] = ggf.conditional_expected_average_profit(
        summary["frequency"], summary["monetary_value"]
    ).round(4)

    annual_factor = CLV_HORIZON_MONTHS / (PREDICTION_PERIOD / 30.0)
    summary["predicted_clv"] = (
        summary["predicted_purchases"] * annual_factor
        * summary["predicted_avg_value"]
        / (1 + DISCOUNT_RATE)
    ).round(2)

    summary["p_alive"] = bgf.conditional_probability_alive(
        summary["frequency"], summary["recency"], summary["T"]
    ).round(4)

    model_info = {
        "method"             : "BG/NBD + Gamma-Gamma (lifetimes library)",
        "bgf_params"         : {k: round(float(v), 4) for k, v in bgf.params_.items()},
        "ggf_params"         : {k: round(float(v), 4) for k, v in ggf.params_.items()},
        "training_customers" : len(summary),
    }
    return summary, model_info


def _fallback_model(df_rfm: pd.DataFrame, df_clv: pd.DataFrame) -> tuple:
    """Simple parametric CLV projection when lifetimes is unavailable."""
    merged = df_rfm.merge(
        df_clv[["customer_id", "lifespan_days", "avg_purchase_value",
                "purchase_count", "clv"]],
        on="customer_id", how="left"
    )

    max_rec = float(merged["recency"].max() or 1)
    merged["p_alive"] = np.exp(-0.5 * merged["recency"] / max_rec)

    freq     = merged["frequency"].clip(lower=1)
    lifespan = merged.get("lifespan_days", pd.Series([365.0] * len(merged))).fillna(365).clip(lower=1)
    daily_rate = freq / lifespan
    merged["predicted_purchases"] = (daily_rate * PREDICTION_PERIOD * merged["p_alive"]).round(4)

    avg_val = merged.get("avg_purchase_value", merged["monetary"] / freq).fillna(0)
    merged["predicted_avg_value"] = avg_val.round(4)

    annual_factor = CLV_HORIZON_MONTHS / (PREDICTION_PERIOD / 30.0)
    merged["predicted_clv"] = (
        merged["predicted_purchases"] * annual_factor
        * merged["predicted_avg_value"]
        / (1 + DISCOUNT_RATE)
    ).round(2)

    model_info = {
        "method": "Parametric (exponential decay — install 'lifetimes' for BG/NBD)",
    }
    return merged, model_info


def _clv_segment(val: float, p33: float, p67: float) -> str:
    if val >= p67:
        return "High Potential"
    if val >= p33:
        return "Medium Potential"
    return "Low Potential"


def main():
    os.makedirs(os.path.join(BASE_DIR, "models"), exist_ok=True)

    for path, name in [(CLV_CSV, "clv_analysis_results.csv"),
                       (RFM_CSV, "rfm_analysis_results.csv")]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} not found. Run advanced_analytics.py first.")

    df_clv = pd.read_csv(CLV_CSV)
    df_rfm = pd.read_csv(RFM_CSV)
    print(f"[CLVPrediction] Loaded {len(df_rfm)} customers.")

    try:
        summary, model_info = _try_lifetimes(df_rfm, df_clv)
        method = "bgnbd"
        print("[CLVPrediction] Using BG/NBD + Gamma-Gamma model.")
    except Exception as e:
        print(f"[CLVPrediction] BG/NBD failed ({e}). Using fallback model.")
        summary, model_info = _fallback_model(df_rfm, df_clv)
        method = "fallback"

    if method == "bgnbd":
        out = summary[["customer_id", "predicted_purchases",
                        "predicted_avg_value", "predicted_clv", "p_alive"]].copy()
        out = out.merge(
            df_rfm[["customer_id", "customer_name", "state", "segment"]],
            on="customer_id", how="left"
        )
    else:
        out = summary[[
            "customer_id", "customer_name", "state", "segment",
            "predicted_purchases", "predicted_avg_value", "predicted_clv", "p_alive"
        ]].copy()

    p33 = out["predicted_clv"].quantile(0.33)
    p67 = out["predicted_clv"].quantile(0.67)
    out["predicted_clv_segment"] = out["predicted_clv"].apply(
        lambda v: _clv_segment(v, p33, p67)
    )
    out["prediction_period_days"] = PREDICTION_PERIOD
    out["clv_horizon_months"]     = CLV_HORIZON_MONTHS
    out["computed_at"]            = datetime.utcnow().isoformat()

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"[CLVPrediction] Saved {len(out)} predictions -> {OUTPUT_CSV}")

    segment_counts = out["predicted_clv_segment"].value_counts().to_dict()
    top10 = out.nlargest(10, "predicted_clv")[
        ["customer_id", "customer_name", "predicted_clv",
         "predicted_purchases", "p_alive", "predicted_clv_segment"]
    ].to_dict("records")

    meta = {
        "generated_at"          : datetime.utcnow().isoformat(),
        "method"                : model_info.get("method"),
        "model_params"          : model_info,
        "prediction_period_days": PREDICTION_PERIOD,
        "clv_horizon_months"    : CLV_HORIZON_MONTHS,
        "discount_rate"         : DISCOUNT_RATE,
        "total_customers"       : len(out),
        "avg_predicted_clv"     : round(float(out["predicted_clv"].mean()), 2),
        "avg_p_alive"           : round(float(out["p_alive"].mean()), 4),
        "segment_breakdown"     : segment_counts,
        "top_10_predicted"      : top10,
    }

    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[CLVPrediction] Metadata -> {META_PATH}")
    print(f"[CLVPrediction] Segments: {segment_counts}")


if __name__ == "__main__":
    main()
