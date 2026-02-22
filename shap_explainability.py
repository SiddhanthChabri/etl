import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import shap
import joblib
import json
import os
import numpy as np
import pandas as pd

from logger_config import setup_logger
logger = setup_logger("shap_explainability")

# ── Paths ─────────────────────────────────────────────────────────────────────
MODEL_PATH      = "models/churn_model.pkl"
FEATURES_PATH   = "models/churn_features.json"
METADATA_PATH   = "models/churn_model_metadata.json"
CHURN_CSV       = "churn_predictions.csv"
RFM_CSV         = "rfm_analysis_results.csv"
SHAP_OUTPUT_DIR = "static/shap"

os.makedirs(SHAP_OUTPUT_DIR, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _load_artifacts():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError("Churn model not found. Run ml_churn_prediction.py first.")
    if not os.path.exists(FEATURES_PATH):
        raise FileNotFoundError("churn_features.json not found.")
    model = joblib.load(MODEL_PATH)
    with open(FEATURES_PATH) as f:
        features = json.load(f)
    return model, features


def _build_feature_matrix(features: list):
    rfm   = pd.read_csv(RFM_CSV)
    churn = pd.read_csv(CHURN_CSV)

    df = rfm.merge(
        churn[["customer_id", "churn_probability", "churn_prediction"]],
        on="customer_id", how="inner"
    )

    missing = [f for f in features if f not in df.columns]
    if missing:
        logger.warning(f"Missing features in data: {missing}. Filling with 0.")
        for col in missing:
            df[col] = 0

    X = df[features].copy()
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
    return X, df


# ── Core SHAP computation ─────────────────────────────────────────────────────
def compute_shap_values(sample_size: int = 500):
    model, features = _load_artifacts()
    X, _ = _build_feature_matrix(features)

    if len(X) > sample_size:
        X_sample = X.sample(n=sample_size, random_state=42).reset_index(drop=True)
    else:
        X_sample = X.reset_index(drop=True)

    logger.info(f"Computing SHAP values on {len(X_sample)} samples...")

    model_type = type(model).__name__

    if model_type in ("RandomForestClassifier", "GradientBoostingClassifier"):
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer(X_sample)
        if shap_values.values.ndim == 3:
            shap_values_class1 = shap.Explanation(
                values        = shap_values.values[:, :, 1],
                base_values   = shap_values.base_values[:, 1],
                data          = shap_values.data,
                feature_names = features
            )
        else:
            shap_values_class1 = shap_values
    else:
        explainer = shap.LinearExplainer(model, X_sample)
        raw = explainer.shap_values(X_sample)
        shap_values_class1 = shap.Explanation(
            values        = raw if raw.ndim == 2 else raw[:, :, 1],
            base_values   = np.full(len(X_sample), explainer.expected_value),
            data          = X_sample.values,
            feature_names = features
        )

    logger.info("SHAP values computed successfully.")
    return explainer, shap_values_class1, X_sample, features


# ── Waterfall PNG (still used for per-customer endpoint) ─────────────────────
def generate_waterfall_plot(customer_id: str) -> str:
    """
    Generates a waterfall PNG for one customer.
    Used by GET /api/churn/explain/customer/{id}/waterfall
    """
    model, features = _load_artifacts()
    X, df = _build_feature_matrix(features)

    if "customer_id" not in df.columns:
        raise ValueError("customer_id column not found in merged dataframe.")

    match = df[df["customer_id"].astype(str) == str(customer_id)]
    if match.empty:
        raise ValueError(f"Customer ID '{customer_id}' not found in data.")

    row_idx = match.index[0]
    X_row   = X.iloc[[row_idx]]

    model_type = type(model).__name__

    if model_type in ("RandomForestClassifier", "GradientBoostingClassifier"):
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer(X_row)
        if shap_values.values.ndim == 3:
            sv = shap.Explanation(
                values        = shap_values.values[0, :, 1],
                base_values   = shap_values.base_values[0, 1],
                data          = shap_values.data[0],
                feature_names = features
            )
        else:
            sv = shap_values[0]
    else:
        explainer = shap.LinearExplainer(model, X)
        raw = explainer.shap_values(X_row)
        sv = shap.Explanation(
            values        = raw[0] if raw.ndim == 2 else raw[0, :, 1],
            base_values   = explainer.expected_value,
            data          = X_row.values[0],
            feature_names = features
        )

    out_path = os.path.join(SHAP_OUTPUT_DIR, f"shap_waterfall_{customer_id}.png")

    plt.close("all")
    shap.plots.waterfall(sv, max_display=12, show=False)

    fig = plt.gcf()
    fig.set_size_inches(12, 8)
    fig.suptitle(f"Churn Explanation — Customer {customer_id}", fontsize=13, y=1.01)

    fig.savefig(out_path, dpi=150, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close("all")

    logger.info(f"Waterfall plot saved → {out_path}")
    return out_path


# ── Plotly JSON data functions ────────────────────────────────────────────────
def get_bar_plot_data() -> dict:
    """
    Returns bar chart data as JSON for Plotly rendering in browser.
    Used by GET /api/churn/explain/bar-data
    """
    _, shap_values, _, features = compute_shap_values()
    mean_abs = np.abs(shap_values.values).mean(axis=0)

    pairs           = sorted(zip(features, mean_abs), key=lambda x: x[1])
    sorted_features = [p[0] for p in pairs]
    sorted_values   = [round(float(p[1]), 6) for p in pairs]

    return {
        "features": sorted_features,
        "values":   sorted_values
    }


def get_summary_plot_data() -> dict:
    """
    Returns beeswarm data as JSON for Plotly rendering in browser.
    Used by GET /api/churn/explain/summary-data
    """
    _, shap_values, X_sample, features = compute_shap_values()

    shap_array    = shap_values.values
    feature_array = X_sample.values

    f_min      = feature_array.min(axis=0)
    f_max      = feature_array.max(axis=0)
    denom      = np.where((f_max - f_min) == 0, 1, f_max - f_min)
    normalized = (feature_array - f_min) / denom

    points = []
    for fi, feat in enumerate(features):
        for si in range(len(X_sample)):
            points.append({
                "feature"   : feat,
                "shap_value": round(float(shap_array[si, fi]), 6),
                "feat_value": round(float(normalized[si, fi]), 4)
            })

    return {
        "features": features,
        "points":   points
    }


# ── JSON output functions ─────────────────────────────────────────────────────
def get_global_feature_importance() -> list:
    """
    Returns ranked feature importance as JSON-serialisable list.
    Used by GET /api/churn/explain/importance
    """
    _, shap_values, _, features = compute_shap_values()
    mean_abs = np.abs(shap_values.values).mean(axis=0)
    return sorted(
        [
            {"feature": f, "mean_shap": round(float(v), 6)}
            for f, v in zip(features, mean_abs)
        ],
        key=lambda x: x["mean_shap"],
        reverse=True
    )


def get_customer_shap_breakdown(customer_id: str) -> dict:
    """
    Returns per-feature SHAP values for one customer as JSON.
    Used by GET /api/churn/explain/customer/{id}
    """
    model, features = _load_artifacts()
    X, df = _build_feature_matrix(features)

    match = df[df["customer_id"].astype(str) == str(customer_id)]
    if match.empty:
        raise ValueError(f"Customer ID '{customer_id}' not found.")

    row_idx  = match.index[0]
    X_row    = X.iloc[[row_idx]]
    row_data = df.iloc[row_idx]

    model_type = type(model).__name__

    if model_type in ("RandomForestClassifier", "GradientBoostingClassifier"):
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer(X_row)
        if shap_values.values.ndim == 3:
            vals       = shap_values.values[0, :, 1].tolist()
            base_value = float(shap_values.base_values[0, 1])
        else:
            vals       = shap_values.values[0].tolist()
            base_value = float(shap_values.base_values[0])
    else:
        explainer  = shap.LinearExplainer(model, X)
        raw        = explainer.shap_values(X_row)
        vals       = (raw[0] if raw.ndim == 2 else raw[0, :, 1]).tolist()
        base_value = float(explainer.expected_value)

    breakdown = [
        {
            "feature"    : feat,
            "value"      : round(float(X_row[feat].values[0]), 4),
            "shap_value" : round(sv, 6),
            "direction"  : "increases_churn" if sv > 0 else "decreases_churn"
        }
        for feat, sv in zip(features, vals)
    ]
    breakdown.sort(key=lambda x: abs(x["shap_value"]), reverse=True)

    return {
        "customer_id"      : customer_id,
        "churn_probability": round(float(row_data.get("churn_probability", 0)), 4),
        "risk_tier"        : str(row_data.get("risk_tier", "Unknown")),
        "base_value"       : round(base_value, 6),
        "explanation"      : breakdown
    }
