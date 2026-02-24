"""
ml_churn_prediction.py
Churn Prediction using RFM features + RandomForest
Pipeline: Train → Evaluate → Save model → Expose via FastAPI
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (classification_report, confusion_matrix,
                             roc_auc_score, roc_curve, accuracy_score,
                             precision_score, recall_score, f1_score)
from sklearn.pipeline import Pipeline
from sklearn.inspection import permutation_importance
import joblib
import json
import os
from datetime import datetime
from logger_config import setup_logger

logger = setup_logger('churn_prediction')


# ── 1. CHURN LABELLING STRATEGY ───────────────────────────────────────────
def label_churn(df: pd.DataFrame, recency_threshold: int = None) -> pd.DataFrame:
    """
    Label a customer as churned (1) if their recency > threshold.
    Threshold defaults to the 75th percentile of recency — customers
    who haven't purchased in a long time relative to peers.
    """
    if recency_threshold is None:
        recency_threshold = int(df["recency"].quantile(0.75))
    df = df.copy()
    df["churned"] = (df["recency"] > recency_threshold).astype(int)
    logger.info(f"  Churn threshold (recency): {recency_threshold} days")
    logger.info(f"  Churned customers:         {df['churned'].sum():,} "
                f"({df['churned'].mean()*100:.1f}%)")
    logger.info(f"  Active customers:          {(df['churned']==0).sum():,} "
                f"({(df['churned']==0).mean()*100:.1f}%)")
    return df, recency_threshold


# ── 2. FEATURE ENGINEERING ───────────────────────────────────────────────
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a rich feature set from RFM + CLV data.
    """
    df = df.copy()

    # Core RFM scores
    features = ["recency", "frequency", "monetary",
                "r_score", "f_score", "m_score", "rfm_value"]

    # Derived features
    df["avg_order_value"]      = df["monetary"] / df["frequency"].replace(0, np.nan)
    df["revenue_per_day"]      = df["monetary"] / df["recency"].replace(0, np.nan)
    df["recency_x_frequency"]  = df["recency"] * df["frequency"]
    df["monetary_x_frequency"] = df["monetary"] * df["frequency"]
    df["log_monetary"]         = np.log1p(df["monetary"])
    df["log_frequency"]        = np.log1p(df["frequency"])
    df["log_recency"]          = np.log1p(df["recency"])

    # RFM segment encoded
    seg_map = {
        "Champions": 8, "Loyal Customers": 7, "Potential Loyalists": 6,
        "Recent Customers": 5, "Promising": 4, "Need Attention": 3,
        "About to Sleep": 2, "At Risk": 1, "Lost": 0
    }
    df["segment_score"] = df["segment"].map(seg_map).fillna(0)

    extended = features + [
        "avg_order_value", "revenue_per_day",
        "recency_x_frequency", "monetary_x_frequency",
        "log_monetary", "log_frequency", "log_recency",
        "segment_score"
    ]

    # Try to merge CLV data if available
    if os.path.exists("clv_analysis_results.csv"):
        clv = pd.read_csv("clv_analysis_results.csv")[
            ["customer_id", "clv_discounted", "lifespan_years", "purchase_count"]
        ].rename(columns={"purchase_count": "clv_purchase_count"})
        df = df.merge(clv, on="customer_id", how="left")
        df["clv_discounted"]   = df["clv_discounted"].fillna(0)
        df["lifespan_years"]   = df["lifespan_years"].fillna(0)
        df["clv_purchase_count"] = df["clv_purchase_count"].fillna(0)
        extended += ["clv_discounted", "lifespan_years", "clv_purchase_count"]

    # Fill any remaining NaN
    df[extended] = df[extended].fillna(0)
    return df, extended


# ── 3. TRAIN MODELS ──────────────────────────────────────────────────────
def train_models(X_train, X_test, y_train, y_test, feature_names):
    """Train & compare multiple models, return the best one."""

    models = {
        "Random Forest": RandomForestClassifier(
            n_estimators=200, max_depth=10, min_samples_leaf=5,
            class_weight="balanced", random_state=42, n_jobs=-1
        ),
        "Gradient Boosting": GradientBoostingClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.05,
            random_state=42
        ),
        "Logistic Regression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(
                class_weight="balanced", max_iter=1000, random_state=42
            ))
        ])
    }

    results = {}
    best_model_name = None
    best_auc = 0

    logger.info("\n📊 Model Comparison:")
    logger.info(f"  {'Model':<25} {'Accuracy':>10} {'AUC-ROC':>10} {'CV Score':>10} {'F1':>8} {'Precision':>10} {'Recall':>8}")
    logger.info("  " + "-"*85)

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred  = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        acc  = accuracy_score(y_test, y_pred)
        auc  = roc_auc_score(y_test, y_proba)
        cv   = cross_val_score(model, X_train, y_train,
                               cv=StratifiedKFold(5), scoring="roc_auc").mean()
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec  = recall_score(y_test, y_pred, zero_division=0)
        f1   = f1_score(y_test, y_pred, zero_division=0)

        results[name] = {
            "model": model, "accuracy": acc, "auc": auc, "cv_score": cv,
            "precision": prec, "recall": rec, "f1": f1,
            "y_pred": y_pred, "y_proba": y_proba
        }
        logger.info(f"  {name:<25} {acc:>10.4f} {auc:>10.4f} {cv:>10.4f} {f1:>8.4f} {prec:>10.4f} {rec:>8.4f}")

        if auc > best_auc:
            best_auc        = auc
            best_model_name = name

    logger.info(f"\n  🏆 Best model: {best_model_name} (AUC: {best_auc:.4f})")
    return results, best_model_name


# ── 4. EVALUATE BEST MODEL ───────────────────────────────────────────────
def evaluate_model(results, best_name, y_test, feature_names, X_test):
    best  = results[best_name]
    model = best["model"]

    logger.info("\n📋 Classification Report:")
    report_str  = classification_report(y_test, best["y_pred"],
                                        target_names=["Active", "Churned"])
    report_dict = classification_report(y_test, best["y_pred"],
                                        target_names=["Active", "Churned"],
                                        output_dict=True)
    for line in report_str.split("\n"):
        if line.strip():
            logger.info(f"  {line}")

    # Confusion matrix
    cm = confusion_matrix(y_test, best["y_pred"])
    logger.info("\n🔢 Confusion Matrix:")
    logger.info(f"  True Negative  (Active   → Active):   {cm[0][0]:>6,}")
    logger.info(f"  False Positive (Active   → Churned):  {cm[0][1]:>6,}")
    logger.info(f"  False Negative (Churned  → Active):   {cm[1][0]:>6,}")
    logger.info(f"  True Positive  (Churned  → Churned):  {cm[1][1]:>6,}")

    # Feature importance
    logger.info("\n🔑 Top 10 Feature Importances:")
    if hasattr(model, "feature_importances_"):
        importances = model.feature_importances_
    else:
        # Logistic Regression pipeline
        importances = np.abs(model.named_steps["clf"].coef_[0])

    fi = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
    for feat, imp in fi[:10]:
        bar = "█" * int(imp * 50)
        logger.info(f"  {feat:<30} {imp:.4f}  {bar}")

    return cm, fi, report_dict


# ── 5. PREDICT CHURN PROBABILITIES ───────────────────────────────────────
def predict_all_customers(df, best_model, feature_names, recency_threshold):
    """Run churn prediction on all customers and save results."""
    X_all = df[feature_names].fillna(0)
    df = df.copy()
    df["churn_probability"] = best_model.predict_proba(X_all)[:, 1]
    df["churn_prediction"]  = best_model.predict(X_all)

    # Risk tier
    def risk_tier(prob):
        if prob >= 0.75: return "Critical Risk"
        if prob >= 0.50: return "High Risk"
        if prob >= 0.25: return "Medium Risk"
        return "Low Risk"

    df["churn_risk_tier"] = df["churn_probability"].apply(risk_tier)

    # Save results
    output_cols = ["customer_id", "customer_name", "state", "city",
                   "recency", "frequency", "monetary", "segment",
                   "churn_probability", "churn_prediction", "churn_risk_tier"]
    output_cols = [c for c in output_cols if c in df.columns]
    df[output_cols].to_csv("churn_predictions.csv", index=False)

    # Log risk tier distribution
    logger.info("\n⚠️  Churn Risk Distribution:")
    for tier in ["Critical Risk", "High Risk", "Medium Risk", "Low Risk"]:
        count = (df["churn_risk_tier"] == tier).sum()
        pct   = count / len(df) * 100
        logger.info(f"  {tier:<20} {count:>6,} customers ({pct:.1f}%)")

    return df


# ── 6. SAVE MODEL & METADATA ─────────────────────────────────────────────
def save_model(best_model, best_name, feature_names, recency_threshold,
               results, cm, fi, report_dict):
    os.makedirs("models", exist_ok=True)

    # Save model
    joblib.dump(best_model, "models/churn_model.pkl")

    # Save feature list
    with open("models/churn_features.json", "w") as f:
        json.dump(feature_names, f)

    # Save metadata
    best = results[best_name]

    # Per-class metrics from classification_report (precision/recall/f1 per class)
    active_row  = report_dict.get("Active",  {})
    churned_row = report_dict.get("Churned", {})
    weighted    = report_dict.get("weighted avg", {})

    metadata = {
        "model_name":        best_name,
        "trained_at":        datetime.now().isoformat(),
        "recency_threshold": recency_threshold,
        "features":          feature_names,
        "metrics": {
            "accuracy"           : round(best["accuracy"],  4),
            "auc_roc"            : round(best["auc"],       4),
            "cv_score"           : round(best["cv_score"],  4),
            # Churned-class metrics (most business-relevant)
            "precision"          : round(best["precision"], 4),
            "recall"             : round(best["recall"],    4),
            "f1"                 : round(best["f1"],        4),
            # Weighted averages across both classes
            "precision_weighted" : round(weighted.get("precision", 0), 4),
            "recall_weighted"    : round(weighted.get("recall",    0), 4),
            "f1_weighted"        : round(weighted.get("f1-score",  0), 4),
        },
        "class_report": {
            "Active": {
                "precision": round(active_row.get("precision", 0), 4),
                "recall"   : round(active_row.get("recall",    0), 4),
                "f1"       : round(active_row.get("f1-score",  0), 4),
                "support"  : int(active_row.get("support",     0)),
            },
            "Churned": {
                "precision": round(churned_row.get("precision", 0), 4),
                "recall"   : round(churned_row.get("recall",    0), 4),
                "f1"       : round(churned_row.get("f1-score",  0), 4),
                "support"  : int(churned_row.get("support",     0)),
            },
        },
        "confusion_matrix": {
            "tn": int(cm[0][0]), "fp": int(cm[0][1]),
            "fn": int(cm[1][0]), "tp": int(cm[1][1])
        },
        "top_features": [{"feature": f, "importance": round(float(i), 4)}
                         for f, i in fi[:10]]
    }
    with open("models/churn_model_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info("\n💾 Saved:")
    logger.info("  models/churn_model.pkl")
    logger.info("  models/churn_features.json")
    logger.info("  models/churn_model_metadata.json")
    logger.info("  churn_predictions.csv")


# ── MAIN ─────────────────────────────────────────────────────────────────
def run_churn_prediction():
    logger.info("="*70)
    logger.info("🤖 ML CHURN PREDICTION PIPELINE")
    logger.info("="*70)

    # Load RFM data
    if not os.path.exists("rfm_analysis_results.csv"):
        logger.error("❌ rfm_analysis_results.csv not found")
        return
    df = pd.read_csv("rfm_analysis_results.csv")
    logger.info(f"✅ Loaded RFM data: {len(df):,} customers")

    # Step 1: Label churn
    logger.info("\n1️⃣  LABELLING CHURN")
    df, recency_threshold = label_churn(df)

    # Step 2: Feature engineering
    logger.info("\n2️⃣  FEATURE ENGINEERING")
    df, feature_names = build_features(df)
    logger.info(f"  Features built: {len(feature_names)}")
    for f in feature_names:
        logger.info(f"    • {f}")

    # Step 3: Train/test split
    logger.info("\n3️⃣  TRAIN/TEST SPLIT (80/20, stratified)")
    X = df[feature_names].fillna(0)
    y = df["churned"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    logger.info(f"  Train: {len(X_train):,}  |  Test: {len(X_test):,}")

    # Step 4: Train models
    logger.info("\n4️⃣  TRAINING MODELS")
    results, best_name = train_models(
        X_train, X_test, y_train, y_test, feature_names
    )

    # Step 5: Evaluate
    logger.info("\n5️⃣  EVALUATING BEST MODEL")
    cm, fi, report_dict = evaluate_model(
        results, best_name, y_test, feature_names, X_test
    )

    # Step 6: Predict all customers
    logger.info("\n6️⃣  PREDICTING ALL CUSTOMERS")
    best_model = results[best_name]["model"]
    df = predict_all_customers(df, best_model, feature_names, recency_threshold)

    # Step 7: Save
    logger.info("\n7️⃣  SAVING MODEL & RESULTS")
    save_model(best_model, best_name, feature_names,
               recency_threshold, results, cm, fi, report_dict)

    logger.info("\n" + "="*70)
    logger.info("✅ CHURN PREDICTION COMPLETE")
    logger.info("="*70)
    best_r = results[best_name]
    logger.info(f"  Best Model:  {best_name}")
    logger.info(f"  AUC-ROC:     {best_r['auc']:.4f}")
    logger.info(f"  Accuracy:    {best_r['accuracy']:.4f}")
    logger.info(f"  Precision:   {best_r['precision']:.4f}  (churned class)")
    logger.info(f"  Recall:      {best_r['recall']:.4f}  (churned class)")
    logger.info(f"  F1 Score:    {best_r['f1']:.4f}  (churned class)")
    logger.info(f"\n  Next step → add churn router to FastAPI:")
    logger.info(f"  GET /api/churn/customer/{{id}}")
    logger.info(f"  GET /api/churn/risk/critical")
    logger.info(f"  GET /api/churn/summary")

    return df


if __name__ == "__main__":
    run_churn_prediction()