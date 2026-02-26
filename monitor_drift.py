"""
monitor_drift.py - Model Drift Detection

Computes Population Stability Index (PSI) for key features across
time windows to detect if model input distributions have shifted.

PSI thresholds:
  < 0.1   : No drift (stable)
  0.1-0.2 : Slight drift (monitor)
  > 0.2   : Significant drift (retrain)

Output: drift_results.csv
        models/drift_report.json
"""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(BASE_DIR, "drift_results.csv")
REPORT     = os.path.join(BASE_DIR, "models", "drift_report.json")


def compute_psi(expected: np.ndarray, actual: np.ndarray, buckets: int = 10) -> float:
    """Population Stability Index between two distributions."""
    # Cast to float so np.isnan works on any numeric dtype (int, object, etc.)
    expected = np.array(expected, dtype=float)
    actual   = np.array(actual,   dtype=float)
    expected = expected[~np.isnan(expected)]
    actual   = actual[~np.isnan(actual)]
    if len(expected) == 0 or len(actual) == 0:
        return 0.0

    breakpoints = np.percentile(expected, np.linspace(0, 100, buckets + 1))
    breakpoints[0]  -= 1e-8
    breakpoints[-1] += 1e-8

    exp_counts = np.histogram(expected, bins=breakpoints)[0]
    act_counts = np.histogram(actual,   bins=breakpoints)[0]

    exp_pct = np.maximum(exp_counts / len(expected), 1e-6)
    act_pct = np.maximum(act_counts / len(actual),   1e-6)

    psi = float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))
    return round(psi, 4)


def drift_label(psi: float) -> str:
    if psi < 0.1:
        return "STABLE"
    if psi < 0.2:
        return "SLIGHT_DRIFT"
    return "SIGNIFICANT_DRIFT"


def check_rfm_drift() -> list:
    path = os.path.join(BASE_DIR, "rfm_analysis_results.csv")
    if not os.path.exists(path):
        return []

    df = pd.read_csv(path)
    features = ["recency", "frequency", "monetary"]
    results  = []

    for feat in features:
        if feat not in df.columns:
            continue
        vals = pd.to_numeric(df[feat], errors="coerce").dropna().values
        n    = len(vals)
        if n < 20:
            continue

        # Split first half (reference) vs second half (current) as proxy
        ref  = vals[: n // 2]
        curr = vals[n // 2 :]
        psi  = compute_psi(ref, curr)
        results.append({
            "module"   : "RFM",
            "feature"  : feat,
            "psi"      : psi,
            "status"   : drift_label(psi),
            "ref_mean" : round(float(np.mean(ref)),  2),
            "curr_mean": round(float(np.mean(curr)), 2),
            "ref_std"  : round(float(np.std(ref)),   2),
            "curr_std" : round(float(np.std(curr)),  2),
            "ref_n"    : len(ref),
            "curr_n"   : len(curr),
        })
    return results


def check_segmentation_drift() -> list:
    path = os.path.join(BASE_DIR, "customer_segments_km.csv")
    if not os.path.exists(path):
        return []

    df = pd.read_csv(path)
    features = [c for c in df.columns
                if c not in ("customer_id", "cluster", "cluster_label")]
    results  = []

    for feat in features:
        if feat not in df.columns:
            continue
        vals = pd.to_numeric(df[feat], errors="coerce").dropna().values
        n    = len(vals)
        if n < 20:
            continue
        ref  = vals[: n // 2]
        curr = vals[n // 2 :]
        psi  = compute_psi(ref, curr)
        results.append({
            "module"   : "Segmentation",
            "feature"  : feat,
            "psi"      : psi,
            "status"   : drift_label(psi),
            "ref_mean" : round(float(np.mean(ref)),  2),
            "curr_mean": round(float(np.mean(curr)), 2),
            "ref_std"  : round(float(np.std(ref)),   2),
            "curr_std" : round(float(np.std(curr)),  2),
            "ref_n"    : len(ref),
            "curr_n"   : len(curr),
        })
    return results


def check_churn_drift() -> list:
    path = os.path.join(BASE_DIR, "churn_predictions.csv")
    if not os.path.exists(path):
        return []

    df = pd.read_csv(path)
    features = [c for c in df.columns
                if c not in ("customer_id", "customer_name", "risk_tier",
                             "churn_label", "state", "city")]
    results  = []

    for feat in features:
        try:
            vals = pd.to_numeric(df[feat], errors="coerce").dropna().values
        except Exception:
            continue
        n = len(vals)
        if n < 20:
            continue
        ref  = vals[: n // 2]
        curr = vals[n // 2 :]
        psi  = compute_psi(ref, curr)
        results.append({
            "module"   : "Churn",
            "feature"  : feat,
            "psi"      : psi,
            "status"   : drift_label(psi),
            "ref_mean" : round(float(np.mean(ref)),  2),
            "curr_mean": round(float(np.mean(curr)), 2),
            "ref_std"  : round(float(np.std(ref)),   2),
            "curr_std" : round(float(np.std(curr)),  2),
            "ref_n"    : len(ref),
            "curr_n"   : len(curr),
        })
    return results


def main():
    os.makedirs(os.path.join(BASE_DIR, "models"), exist_ok=True)

    print("[DriftMonitor] Running drift detection...")
    all_results = []
    all_results.extend(check_rfm_drift())
    all_results.extend(check_segmentation_drift())
    all_results.extend(check_churn_drift())

    if not all_results:
        print("[DriftMonitor] No data found for drift analysis.")
        return

    df = pd.DataFrame(all_results)
    df.to_csv(OUTPUT_CSV, index=False)
    print("[DriftMonitor] Saved " + str(len(df)) + " drift checks to " + OUTPUT_CSV)

    status_counts = df["status"].value_counts().to_dict()
    drifted = df[df["status"] != "STABLE"][
        ["module", "feature", "psi", "status"]
    ].to_dict("records")

    report = {
        "generated_at"    : datetime.utcnow().isoformat(),
        "total_checks"    : len(df),
        "status_breakdown": status_counts,
        "overall_health"  : (
            "HEALTHY" if not drifted
            else "WARNING" if all(d["status"] == "SLIGHT_DRIFT" for d in drifted)
            else "CRITICAL"
        ),
        "drifted_features": drifted,
        "modules_checked" : df["module"].unique().tolist(),
    }

    with open(REPORT, "w") as f:
        json.dump(report, f, indent=2)

    print("[DriftMonitor] Overall health: " + report["overall_health"])
    print("[DriftMonitor] Status breakdown: " + str(status_counts))
    if drifted:
        print("[DriftMonitor] Drifted features: " + str([d["feature"] for d in drifted]))


if __name__ == "__main__":
    main()
