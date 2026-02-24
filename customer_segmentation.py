"""
customer_segmentation.py -- K-Means Customer Segmentation

Builds behaviour-based customer clusters using 7 engineered features:
  recency_days      -- days since last purchase (lower = better)
  frequency         -- number of distinct purchase days
  monetary_total    -- lifetime spend ($)
  avg_order_value   -- monetary / frequency
  product_variety   -- number of unique products bought
  purchase_span     -- days between first & last purchase
  avg_monthly_spend -- monetary / active months

Pipeline:
  1. Feature engineering from DWH (fact_sales + dim_time)
  2. Robust scaling (less sensitive to outliers)
  3. Elbow method + Silhouette scores to pick optimal k (2-10)
  4. K-Means fit at optimal k
  5. PCA (2 components) for scatter visualisation
  6. Cluster profiling + automatic label assignment
  7. Per-customer cluster assignment saved to CSV

Outputs:
  customer_segments_km.csv           -- per-customer features + cluster
  models/segmentation_metadata.json  -- cluster profiles, elbow data, PCA

Usage:
  python customer_segmentation.py
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from sklearn.preprocessing import RobustScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("customer_segmentation", "customer_segmentation_logs.txt")

RESULTS_CSV   = "customer_segments_km.csv"
METADATA_JSON = "models/segmentation_metadata.json"

FEATURES = [
    "recency_days",
    "frequency",
    "monetary_total",
    "avg_order_value",
    "product_variety",
    "purchase_span",
    "avg_monthly_spend",
]

# ── helpers ───────────────────────────────────────────────────────────────────

def _safe(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return round(float(v), 4)
    return v

def _r2(x):
    return None if (x is None or (isinstance(x, float) and np.isnan(x))) else round(float(x), 2)


# ── feature engineering ───────────────────────────────────────────────────────

def _build_features(analysis_date: str = "2015-12-31") -> pd.DataFrame:
    """
    Returns one row per customer with the 7 engineered features.
    """
    sql = text(f"""
        WITH base AS (
            SELECT
                dc.customer_key,
                dc.customer_id,
                dc.customer_name,
                dc.state,
                dc.age_group,
                dc.loyalty_tier,
                dc.customer_segment,
                dt.date               AS purchase_date,
                fs.sales_amount,
                fs.product_key
            FROM fact_sales fs
            JOIN dim_time     dt ON fs.time_key     = dt.time_key
            JOIN dim_customer dc ON fs.customer_key = dc.customer_key
            WHERE dc.is_current = TRUE
        ),
        agg AS (
            SELECT
                customer_key,
                customer_id,
                MAX(customer_name)     AS customer_name,
                MAX(state)             AS state,
                MAX(age_group)         AS age_group,
                MAX(loyalty_tier)      AS loyalty_tier,
                MAX(customer_segment)  AS customer_segment,
                DATE '{analysis_date}' - MAX(purchase_date)::date  AS recency_days,
                COUNT(DISTINCT purchase_date)                       AS frequency,
                SUM(sales_amount)                                   AS monetary_total,
                SUM(sales_amount) / NULLIF(COUNT(DISTINCT purchase_date),0) AS avg_order_value,
                COUNT(DISTINCT product_key)                         AS product_variety,
                MAX(purchase_date)::date - MIN(purchase_date)::date AS purchase_span,
                SUM(sales_amount) / NULLIF(
                    EXTRACT(YEAR FROM AGE(MAX(purchase_date), MIN(purchase_date))) * 12 +
                    EXTRACT(MONTH FROM AGE(MAX(purchase_date), MIN(purchase_date))) + 1
                , 0)                                                AS avg_monthly_spend
            FROM base
            GROUP BY customer_key, customer_id
        )
        SELECT * FROM agg
        ORDER BY monetary_total DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    logger.info(f"Loaded {len(df)} customers with features.")
    return df


# ── optimal k selection ───────────────────────────────────────────────────────

def _find_optimal_k(X_scaled: np.ndarray, k_range=range(2, 11)) -> dict:
    """
    Run K-Means for each k, collect inertia and silhouette score.
    Enforces a minimum cluster size of 1% of samples so degenerate
    outlier-only clusters don't dominate the silhouette optimum.
    Returns: {k: int, inertia_curve: [...], silhouette_curve: [...]}
    """
    min_cluster_size = max(5, int(len(X_scaled) * 0.01))  # at least 1% or 5 samples
    inertias   = []
    silhouettes = []

    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        labels = km.fit_predict(X_scaled)
        inertias.append(float(km.inertia_))
        counts = np.bincount(labels)
        # If any cluster is too small, penalise the silhouette score
        if counts.min() < min_cluster_size:
            sil = -1.0
        else:
            sil = silhouette_score(X_scaled, labels)
        silhouettes.append(float(sil))

    k_list = list(k_range)

    # Pick k with best silhouette score (simple, robust criterion)
    best_idx = int(np.argmax(silhouettes))
    best_k   = k_list[best_idx]

    logger.info(f"Elbow results — best k={best_k} (silhouette={silhouettes[best_idx]:.4f})")

    return {
        "best_k"          : best_k,
        "k_values"        : k_list,
        "inertia_curve"   : [_r2(v) for v in inertias],
        "silhouette_curve": [_r2(v) for v in silhouettes],
    }


# ── cluster labelling ─────────────────────────────────────────────────────────

def _assign_labels(profiles: list) -> list:
    """
    Assign human-readable cluster names based on normalised
    monetary + frequency ranks.

    Sort clusters by (monetary_total_mean DESC) and assign names.
    """
    LABELS = [
        "Elite Buyers",
        "Active Buyers",
        "Occasional Buyers",
        "Dormant Buyers",
        "Casual Browsers",
        "New Customers",
        "Bargain Hunters",
        "Loyal Spenders",
        "Lapsed Customers",
        "Core Customers",
    ]
    sorted_profiles = sorted(profiles, key=lambda p: p["monetary_total_mean"], reverse=True)
    for i, p in enumerate(sorted_profiles):
        p["cluster_label"] = LABELS[i] if i < len(LABELS) else f"Cluster {p['cluster_id']}"
    return sorted_profiles


# ── main ──────────────────────────────────────────────────────────────────────

class SegmentationAnalyzer:

    def run(self):
        logger.info("=== Customer Segmentation START ===")

        # 1. Features
        logger.info("Building customer feature matrix...")
        df = _build_features()

        # Drop rows with any NaN in feature columns
        df_clean = df.dropna(subset=FEATURES).copy()
        logger.info(f"  {len(df_clean)}/{len(df)} customers have complete features.")

        # 2. Log-transform skewed monetary/frequency features, then scale
        #    log1p compresses extreme outlier ranges common in retail spend data
        LOG_FEATURES = {"monetary_total", "avg_order_value", "avg_monthly_spend",
                        "frequency", "product_variety"}
        df_model = df_clean[FEATURES].copy().astype(float)
        for col in FEATURES:
            if col in LOG_FEATURES:
                df_model[col] = np.log1p(df_model[col].clip(lower=0))

        scaler   = RobustScaler()
        X_raw    = df_model.values
        X_scaled = scaler.fit_transform(X_raw)

        # 3. Optimal k
        logger.info("Finding optimal number of clusters (k=2..10)...")
        elbow = _find_optimal_k(X_scaled)
        k     = elbow["best_k"]

        # 4. Final K-Means
        logger.info(f"Fitting K-Means with k={k}...")
        km     = KMeans(n_clusters=k, random_state=42, n_init=15, max_iter=500)
        labels = km.fit_predict(X_scaled)
        df_clean["cluster_id"] = labels

        final_sil = silhouette_score(X_scaled, labels)
        logger.info(f"  Silhouette score: {final_sil:.4f}")

        # 5. PCA (2D scatter)
        logger.info("Computing PCA (2 components)...")
        pca = PCA(n_components=2, random_state=42)
        X_pca = pca.fit_transform(X_scaled)
        df_clean["pca_x"] = X_pca[:, 0]
        df_clean["pca_y"] = X_pca[:, 1]
        explained_var = [_r2(v * 100) for v in pca.explained_variance_ratio_]

        # 6. Cluster profiles
        logger.info("Building cluster profiles...")
        profile_cols = FEATURES + ["recency_days"]
        grp = df_clean.groupby("cluster_id")

        profiles = []
        for cid, group in grp:
            size = int(len(group))
            prof = {"cluster_id": int(cid), "size": size,
                    "pct_of_customers": _r2(size / len(df_clean) * 100)}
            for feat in FEATURES:
                prof[f"{feat}_mean"]   = _r2(group[feat].mean())
                prof[f"{feat}_median"] = _r2(group[feat].median())
            profiles.append(prof)

        profiles = _assign_labels(profiles)

        # Build cluster_id -> label map
        label_map = {p["cluster_id"]: p["cluster_label"] for p in profiles}
        df_clean["cluster_label"] = df_clean["cluster_id"].map(label_map)

        # 7. PCA scatter data (sample up to 2000 points)
        scatter_df = df_clean[["customer_id", "customer_name", "pca_x", "pca_y",
                                "cluster_id", "cluster_label",
                                "monetary_total", "frequency", "recency_days"]].copy()
        if len(scatter_df) > 2000:
            scatter_df = scatter_df.sample(2000, random_state=42)
        scatter_data = [
            {k: _safe(v) for k, v in row.items()}
            for row in scatter_df.to_dict("records")
        ]

        # 8. Feature importance from PCA loadings
        feature_importance = []
        loadings = pca.components_          # shape (2, n_features)
        abs_load = np.abs(loadings).sum(axis=0)  # sum across PC1 + PC2
        for feat, imp in sorted(zip(FEATURES, abs_load), key=lambda x: -x[1]):
            feature_importance.append({"feature": feat, "importance": _r2(float(imp))})

        # ── Global KPIs ───────────────────────────────────────────────────────
        metadata = {
            "generated_at"       : datetime.now().isoformat(),
            "total_customers"    : int(len(df_clean)),
            "n_clusters"         : k,
            "silhouette_score"   : _r2(final_sil),
            "pca_explained_var"  : explained_var,
            "pca_total_var_pct"  : _r2(sum(pca.explained_variance_ratio_) * 100),
            "features_used"      : FEATURES,
            "elbow"              : elbow,
            "profiles"           : profiles,
            "scatter_data"       : scatter_data,
            "feature_importance" : feature_importance,
        }

        # Save
        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")

        # Save CSV
        save_cols = ["customer_id", "customer_name", "state", "age_group",
                     "loyalty_tier", "customer_segment",
                     "cluster_id", "cluster_label"] + FEATURES + ["pca_x", "pca_y"]
        df_clean[save_cols].to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved results -> {RESULTS_CSV}  ({len(df_clean)} rows)")

        # Summary log
        logger.info(f"Optimal k={k}  Silhouette={final_sil:.4f}")
        for p in sorted(profiles, key=lambda x: x["cluster_id"]):
            logger.info(
                f"  Cluster {p['cluster_id']} | {p['cluster_label']:20s} | "
                f"n={p['size']:5d} ({p['pct_of_customers']:.1f}%) | "
                f"Monetary=${p['monetary_total_mean']:,.0f} | "
                f"Freq={p['frequency_mean']:.1f}"
            )
        logger.info("=== Customer Segmentation DONE ===")
        return metadata


if __name__ == "__main__":
    try:
        analyzer = SegmentationAnalyzer()
        meta = analyzer.run()
        print(f"\nDone. k={meta['n_clusters']}  "
              f"silhouette={meta['silhouette_score']}  "
              f"PCA variance={meta['pca_total_var_pct']}%")
        print("\nCluster profiles:")
        for p in meta["profiles"]:
            print(f"  [{p['cluster_id']}] {p['cluster_label']:22s}  "
                  f"n={p['size']:5d}  "
                  f"monetary=${p['monetary_total_mean']:>10,.0f}  "
                  f"freq={p['frequency_mean']:5.1f}  "
                  f"recency={p['recency_days_mean']:>6.0f}d")
    except Exception as e:
        logger.error(f"Segmentation failed: {e}", exc_info=True)
        raise
