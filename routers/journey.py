"""
routers/journey.py — Customer Journey API Endpoints

Endpoints:
  GET /api/journey/summary        — metadata, retention, tier thresholds
  GET /api/journey/transitions    — full transition matrix
  GET /api/journey/top-transitions — N most common A→B flows
  GET /api/journey/sequences      — most common multi-step paths
  GET /api/journey/funnel         — retention at each purchase step
  GET /api/journey/sankey-data    — pre-formatted Plotly Sankey payload
  GET /api/journey/customer/{id}  — individual customer journey
"""

import json
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

TRANSITIONS_CSV = "journey_transitions.csv"
SEQUENCES_CSV   = "journey_sequences.csv"
FUNNEL_CSV      = "journey_funnel.csv"
METADATA_JSON   = "models/journey_metadata.json"


def _load(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=f"{path} not found. Run customer_journey.py first.",
        )
    return pd.read_csv(path)


def _df_to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-safe records (NaN → null, not float nan)."""
    return json.loads(df.to_json(orient="records"))


def _meta() -> dict:
    if not os.path.exists(METADATA_JSON):
        raise HTTPException(
            status_code=404,
            detail="Journey metadata not found. Run customer_journey.py first.",
        )
    with open(METADATA_JSON) as f:
        return json.load(f)


# ── Summary ───────────────────────────────────────────────────────────────────────────────────────

@router.get("/summary", summary="Customer journey summary & metadata")
def get_summary():
    meta = _meta()
    return {
        "generated_at"             : meta.get("generated_at"),
        "total_customers"          : meta.get("total_customers"),
        "total_transactions"       : meta.get("total_transactions"),
        "avg_journey_length"       : meta.get("avg_journey_length"),
        "retention_after_1st_pct"  : meta.get("retention_after_1st"),
        "customers_1_purchase"     : meta.get("customers_1_purchase"),
        "customers_2plus"          : meta.get("customers_2plus"),
        "customers_3plus"          : meta.get("customers_3plus"),
        "tier_thresholds"          : meta.get("tier_thresholds"),
        "first_purchase_tiers"     : meta.get("first_purchase_tiers"),
        "avg_days_between"         : meta.get("avg_days_between_purchases"),
        "median_days_between"      : meta.get("median_days_between"),
        "top_sequences"            : meta.get("top_sequences"),
    }


# ── Transition matrix ─────────────────────────────────────────────────────────────────────────────

@router.get("/transitions", summary="Full transition probability matrix")
def get_transitions(from_tier: str = Query(None, description="Filter by source tier")):
    df = _load(TRANSITIONS_CSV)
    if from_tier:
        df = df[df["from_tier"].str.lower() == from_tier.lower()]
    return _df_to_records(df)

# ── Top transitions ────────────────────────────────────────────────────────────────────────────────

@router.get("/top-transitions", summary="Most common A→B tier transitions")
def get_top_transitions(
    n              : int  = Query(10),
    exclude_churn  : bool = Query(False, description="Hide 'Churned' as destination"),
):
    df = _load(TRANSITIONS_CSV)
    if exclude_churn:
        df = df[df["to_tier"] != "Churned"]
    top = df.nlargest(n, "count")
    return _df_to_records(top)


# ── Sequences ──────────────────────────────────────────────────────────────────────────────────────

@router.get("/sequences", summary="Most common multi-step purchase sequences")
def get_sequences(
    steps : int = Query(None, description="Filter by number of steps (2 or 3)"),
    limit : int = Query(20),
):
    df = _load(SEQUENCES_CSV)
    if steps:
        df = df[df["steps"] == steps]
    return _df_to_records(df.head(limit))


# ── Funnel ────────────────────────────────────────────────────────────────────────────────────

@router.get("/funnel", summary="Customer retention funnel by purchase step")
def get_funnel():
    df = _load(FUNNEL_CSV)
    return _df_to_records(df)


# ── Sankey data ───────────────────────────────────────────────────────────────────────────────────

@router.get("/sankey-data", summary="Pre-formatted Plotly Sankey diagram data")
def get_sankey_data():
    """
    Returns node and link data for a 3-level Sankey diagram showing
    how customers flow through Low/Medium/High purchase tiers across
    their 1st, 2nd, and 3rd purchases.
    """
    meta = _meta()
    sankey = meta.get("sankey")
    if not sankey:
        raise HTTPException(status_code=404, detail="Sankey data not found in metadata.")
    return sankey

# ── Individual customer journey ─────────────────────────────────────────────────────────────────────

@router.get("/customer/{customer_id}", summary="Individual customer's purchase journey")
def get_customer_journey(customer_id: str):
    """
    Reconstructs a customer's journey from the transitions data.
    Note: requires re-querying — returns what's available from saved data.
    """
    meta = _meta()
    thresholds = meta.get("tier_thresholds", {})

    from db_connection import engine
    from sqlalchemy import text

    query = text("""
        SELECT
            c.customer_id,
            c.customer_name,
            t.date::date               AS order_date,
            SUM(fs.sales_amount)       AS order_total,
            COUNT(DISTINCT fs.product_key) AS unique_products
        FROM fact_sales    fs
        JOIN dim_customer  c  ON fs.customer_key = c.customer_key
        JOIN dim_time      t  ON fs.time_key     = t.time_key
        WHERE c.is_current = TRUE
          AND fs.sales_amount > 0
          AND c.customer_id = :cid
        GROUP BY c.customer_id, c.customer_name, t.date
        ORDER BY t.date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cid": customer_id})

    if df.empty:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found.")

    low_max  = thresholds.get("low_max",  200)
    high_min = thresholds.get("high_min", 400)

    def tier(v):
        if v < low_max:  return "Low"
        if v < high_min: return "Medium"
        return "High"

    df["tier"]       = df["order_total"].apply(tier)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")
    df["purchase_num"] = range(1, len(df) + 1)

    journey_str = " → ".join(df["tier"].tolist())

    return {
        "customer_id"    : customer_id,
        "customer_name"  : df["customer_name"].iloc[0],
        "total_purchases": len(df),
        "total_spent"    : round(float(df["order_total"].sum()), 2),
        "journey_path"   : journey_str,
        "transactions"   : df.drop(columns=["customer_id","customer_name"])
                            .where(pd.notnull(df), None).to_dict("records"),
    }
