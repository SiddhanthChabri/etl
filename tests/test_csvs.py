"""
tests/test_csvs.py — Validate that analytics CSVs exist and have correct schema
"""

import os
import pytest
import pandas as pd


BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(filename: str) -> pd.DataFrame:
    path = os.path.join(BASE, filename)
    if not os.path.exists(path):
        pytest.skip(f"{filename} not yet generated — run analytics first")
    return pd.read_csv(path)


# ── RFM ───────────────────────────────────────────────────────────────────────

def test_rfm_csv_schema():
    df = _load("rfm_analysis_results.csv")
    required = {"customer_id", "frequency", "monetary", "recency", "segment"}
    assert required.issubset(df.columns), f"Missing: {required - set(df.columns)}"
    assert len(df) > 0, "RFM CSV is empty"
    assert df["monetary"].min() >= 0, "Negative monetary values found"


def test_rfm_segments_are_valid():
    df = _load("rfm_analysis_results.csv")
    assert df["segment"].notna().all(), "Null segments found"


# ── ABC ───────────────────────────────────────────────────────────────────────

def test_abc_csv_schema():
    df = _load("abc_analysis_results.csv")
    assert "abc_class" in df.columns
    valid_classes = {"A", "B", "C"}
    actual = set(df["abc_class"].unique())
    assert actual.issubset(valid_classes), f"Unexpected classes: {actual - valid_classes}"


# ── CLV ───────────────────────────────────────────────────────────────────────

def test_clv_csv_schema():
    df = _load("clv_analysis_results.csv")
    required = {"customer_id", "clv", "clv_segment"}
    assert required.issubset(df.columns)
    assert (df["clv"] >= 0).all(), "Negative CLV values found"


# ── Churn ─────────────────────────────────────────────────────────────────────

def test_churn_csv_schema():
    df = _load("churn_predictions.csv")
    assert "customer_id" in df.columns
    # churn_risk_tier or churn_probability should be present
    assert "churn_risk_tier" in df.columns or "churn_probability" in df.columns


# ── Forecast ──────────────────────────────────────────────────────────────────

def test_forecast_csv_schema():
    df = _load("demand_forecast_results.csv")
    required = {"forecast_date", "predicted_quantity", "product_id", "is_forecast"}
    assert required.issubset(df.columns)
    assert len(df) > 0


# ── Price Elasticity ──────────────────────────────────────────────────────────

def test_elasticity_csv_schema():
    df = _load("price_elasticity_results.csv")
    required = {"product_id", "price_elasticity", "elasticity_type"}
    assert required.issubset(df.columns)


# ── Inventory ─────────────────────────────────────────────────────────────────

def test_inventory_csv_schema():
    df = _load("inventory_optimization_results.csv")
    required = {"product_id", "eoq", "reorder_point", "safety_stock"}
    assert required.issubset(df.columns)
    assert (df["eoq"] >= 0).all(), "Negative EOQ values found"


# ── Store Performance ─────────────────────────────────────────────────────────

def test_store_csv_schema():
    df = _load("store_performance_results.csv")
    assert "store_id" in df.columns or "store_key" in df.columns
    assert "total_revenue" in df.columns


# ── Recommendations ───────────────────────────────────────────────────────────

def test_recommendations_csv_schema():
    df = _load("product_recommendations.csv")
    assert "customer_id" in df.columns
    assert "product_id" in df.columns
    assert len(df) > 0


# ── Pricing Optimizer ─────────────────────────────────────────────────────────

def test_pricing_csv_schema():
    df = _load("pricing_optimizer_results.csv")
    required = {"product_id", "action", "expected_revenue_lift_pct", "confidence"}
    assert required.issubset(df.columns)
    valid_actions = {"RAISE_PRICE", "LOWER_PRICE", "HOLD"}
    actual = set(df["action"].unique())
    assert actual.issubset(valid_actions)


# ── Drift Monitor ─────────────────────────────────────────────────────────────

def test_drift_csv_schema():
    df = _load("drift_results.csv")
    required = {"module", "feature", "psi", "status"}
    assert required.issubset(df.columns)
    valid_statuses = {"STABLE", "SLIGHT_DRIFT", "SIGNIFICANT_DRIFT"}
    actual = set(df["status"].unique())
    assert actual.issubset(valid_statuses)


# ── CLV Predictions ───────────────────────────────────────────────────────────

def test_clv_predictions_csv_schema():
    df = _load("clv_predictions.csv")
    required = {"customer_id", "predicted_clv", "p_alive", "predicted_clv_segment"}
    assert required.issubset(df.columns)
    assert (df["p_alive"] >= 0).all()
    assert (df["p_alive"] <= 1).all()
