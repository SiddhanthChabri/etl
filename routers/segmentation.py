"""
routers/segmentation.py -- K-Means Customer Segmentation API

Endpoints:
  GET /api/segmentation/summary      -- KPIs: total customers, k, silhouette, PCA variance
  GET /api/segmentation/clusters     -- All cluster profiles with means/medians
  GET /api/segmentation/scatter      -- PCA scatter data (up to 2000 points)
  GET /api/segmentation/elbow        -- Elbow + silhouette curve for k=2..10
  GET /api/segmentation/customers    -- Per-customer assignments (filterable)
  GET /api/segmentation/customer/{id} -- Single customer's cluster info
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RESULTS_CSV   = "customer_segments_km.csv"
METADATA_JSON = "models/segmentation_metadata.json"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Segmentation metadata not found. Run customer_segmentation.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


def _load_df() -> pd.DataFrame:
    if not os.path.exists(RESULTS_CSV):
        raise HTTPException(
            status_code=404,
            detail=f"{RESULTS_CSV} not found. Run customer_segmentation.py first.",
        )
    return pd.read_csv(RESULTS_CSV)


def _df_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Customer segmentation KPIs")
def get_summary():
    """
    Returns top-level KPIs:
    - total_customers, n_clusters, silhouette_score
    - PCA explained variance (PC1, PC2, total)
    - Features used, generation timestamp
    """
    meta = _meta()
    return {
        "generated_at"      : meta.get("generated_at"),
        "total_customers"   : meta.get("total_customers"),
        "n_clusters"        : meta.get("n_clusters"),
        "silhouette_score"  : meta.get("silhouette_score"),
        "pca_explained_var" : meta.get("pca_explained_var"),
        "pca_total_var_pct" : meta.get("pca_total_var_pct"),
        "features_used"     : meta.get("features_used"),
        "feature_importance": meta.get("feature_importance"),
    }


# ── Cluster profiles ──────────────────────────────────────────────────────────

@router.get("/clusters", summary="All cluster profiles with mean/median feature values")
def get_clusters():
    """
    Returns one entry per cluster:
    - cluster_id, cluster_label, size, pct_of_customers
    - mean & median for all 7 features
    """
    meta = _meta()
    profiles = meta.get("profiles", [])
    return {"n_clusters": len(profiles), "clusters": profiles}


# ── PCA scatter ───────────────────────────────────────────────────────────────

@router.get("/scatter", summary="PCA 2D scatter plot data (≤2000 points)")
def get_scatter(
    cluster_id: int = Query(None, description="Filter to a single cluster ID"),
):
    """
    Returns PCA coordinates for all customers (sampled to ≤2000).
    Each record includes: customer_id, customer_name, pca_x, pca_y,
    cluster_id, cluster_label, monetary_total, frequency, recency_days.
    """
    meta    = _meta()
    scatter = meta.get("scatter_data", [])
    if cluster_id is not None:
        scatter = [r for r in scatter if r.get("cluster_id") == cluster_id]
    return {"total": len(scatter), "data": scatter}


# ── Elbow / silhouette curve ──────────────────────────────────────────────────

@router.get("/elbow", summary="Elbow and silhouette curves for k=2..10")
def get_elbow():
    """
    Returns inertia and silhouette score for each tested k value.
    Use for elbow-method visualisation.
    """
    meta  = _meta()
    elbow = meta.get("elbow", {})
    return {
        "best_k"          : elbow.get("best_k"),
        "k_values"        : elbow.get("k_values"),
        "inertia_curve"   : elbow.get("inertia_curve"),
        "silhouette_curve": elbow.get("silhouette_curve"),
    }


# ── Per-customer list ─────────────────────────────────────────────────────────

@router.get("/customers", summary="Per-customer cluster assignments (filterable, paginated)")
def get_customers(
    cluster_id   : int = Query(None, description="Filter by cluster ID"),
    cluster_label: str = Query(None, description="Filter by cluster label, e.g. 'Elite Buyers'"),
    state        : str = Query(None, description="Filter by state/country"),
    age_group    : str = Query(None, description="Filter by age group"),
    limit        : int = Query(100, description="Max rows (default 100)"),
    offset       : int = Query(0,   description="Pagination offset"),
):
    """
    Returns per-customer segment assignments with all 7 features.
    """
    df = _load_df()
    if cluster_id is not None:
        df = df[df["cluster_id"] == cluster_id]
    if cluster_label:
        df = df[df["cluster_label"].str.lower() == cluster_label.lower()]
    if state:
        df = df[df["state"].str.lower() == state.lower()]
    if age_group:
        df = df[df["age_group"].str.lower() == age_group.lower()]
    total = len(df)
    page  = df.iloc[offset: offset + limit]
    return {"total": total, "limit": limit, "offset": offset,
            "data": _df_records(page)}


# ── Single customer ───────────────────────────────────────────────────────────

@router.get("/customer/{customer_id}", summary="Cluster assignment for a single customer")
def get_customer(customer_id: str):
    """
    Returns the cluster profile for a specific customer by customer_id.
    """
    df = _load_df()
    row = df[df["customer_id"].astype(str) == str(customer_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found.")
    record = _df_records(row)[0]

    # Attach the full cluster profile
    meta     = _meta()
    profiles = {p["cluster_id"]: p for p in meta.get("profiles", [])}
    cid      = record.get("cluster_id")
    record["cluster_profile"] = profiles.get(cid, {})
    return record
