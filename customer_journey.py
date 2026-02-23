"""
customer_journey.py — Customer Purchase Journey Analysis

Analyses how customers move through order-value tiers across successive purchases.

Journey States (based on order total tertiles):
  Low    — bottom third of order values
  Medium — middle third
  High   — top third
  Churned — customer did not return for next purchase

Outputs:
  journey_transitions.csv      — transition counts and probabilities
  journey_sequences.csv        — most common 2/3-step purchase paths
  journey_funnel.csv           — retention at each purchase step
  models/journey_metadata.json — thresholds, summary stats, sankey data

Usage:
  python customer_journey.py
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import text

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("customer_journey", "customer_journey_logs.txt")

TRANSITIONS_CSV = "journey_transitions.csv"
SEQUENCES_CSV   = "journey_sequences.csv"
FUNNEL_CSV      = "journey_funnel.csv"
METADATA_JSON   = "models/journey_metadata.json"

TIERS         = ["Low", "Medium", "High"]
CHURN_LABEL   = "Churned"
MAX_STEPS     = 6   # analyse up to 6th purchase


class CustomerJourneyAnalyzer:

    def __init__(self):
        self.engine = engine
        self.thresholds = {}

    # ── 1. Fetch ────────────────────────────────────────────────────────────────────────────

    def fetch_transactions(self) -> pd.DataFrame:
        logger.info("Fetching customer transactions from data warehouse...")
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
            GROUP BY c.customer_id, c.customer_name, t.date
            ORDER BY c.customer_id, t.date
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["order_date"]  = pd.to_datetime(df["order_date"])
        df["customer_name"] = df["customer_name"].fillna("Unknown")
        logger.info(f"  {len(df):,} transactions | {df['customer_id'].nunique():,} customers")
        return df

    # ── 2. Classify tiers ────────────────────────────────────────────────────────────────────────────

    def classify_tiers(self, df: pd.DataFrame) -> pd.DataFrame:
        q33 = df["order_total"].quantile(0.33)
        q67 = df["order_total"].quantile(0.67)
        self.thresholds = {"low_max": round(q33, 2), "high_min": round(q67, 2)}
        logger.info(f"  Tier thresholds — Low < {q33:.2f}, Medium {q33:.2f}–{q67:.2f}, High > {q67:.2f}")

        def _tier(v):
            if v < q33:  return "Low"
            if v < q67:  return "Medium"
            return "High"

        df["tier"] = df["order_total"].apply(_tier)
        return df

    # ── 3. Build per-customer journeys ────────────────────────────────────────────────────────────────

    def build_journeys(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["customer_id", "order_date"]).copy()
        df["purchase_num"] = df.groupby("customer_id").cumcount() + 1

        df["prev_date"] = df.groupby("customer_id")["order_date"].shift(1)
        df["days_since_prev"] = (df["order_date"] - df["prev_date"]).dt.days

        return df
    # ── 4. Transition matrix ────────────────────────────────────────────────────────────────────────────

    def build_transitions(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        max_purch = df.groupby("customer_id")["purchase_num"].max()

        for cid, group in df.groupby("customer_id"):
            group    = group.sort_values("purchase_num")
            max_p    = max_purch[cid]
            tiers_seq = group["tier"].tolist()

            for i in range(len(tiers_seq) - 1):
                rows.append({"from_tier": tiers_seq[i], "to_tier": tiers_seq[i + 1]})

            # Churn: customer stopped purchasing
            rows.append({"from_tier": tiers_seq[-1], "to_tier": CHURN_LABEL})

        trans_df = pd.DataFrame(rows)
        counts = (
            trans_df.groupby(["from_tier", "to_tier"])
            .size()
            .reset_index(name="count")
        )

        # Add probability
        totals = counts.groupby("from_tier")["count"].sum().rename("from_total")
        counts = counts.merge(totals, on="from_tier")
        counts["probability"] = (counts["count"] / counts["from_total"]).round(4)
        counts = counts.drop(columns="from_total")

        return counts.sort_values(["from_tier", "count"], ascending=[True, False])

    # ── 5. Common sequences ────────────────────────────────────────────────────────────────────────────

    def find_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
        seq_counts = {}

        for cid, group in df.groupby("customer_id"):
            tiers = group.sort_values("purchase_num")["tier"].tolist()
            # 2-step
            for i in range(len(tiers) - 1):
                s = " → ".join(tiers[i:i+2])
                seq_counts[s] = seq_counts.get(s, 0) + 1
            # 3-step
            for i in range(len(tiers) - 2):
                s = " → ".join(tiers[i:i+3])
                seq_counts[s] = seq_counts.get(s, 0) + 1

        total_customers = df["customer_id"].nunique()
        rows = [
            {"sequence": k, "steps": k.count("→") + 1,
             "count": v, "pct": round(v / total_customers * 100, 2)}
            for k, v in seq_counts.items()
        ]
        return (
            pd.DataFrame(rows)
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )

    # ── 6. Retention funnel ────────────────────────────────────────────────────────────────────────────

    def compute_funnel(self, df: pd.DataFrame) -> pd.DataFrame:
        total = df["customer_id"].nunique()
        rows  = []
        for step in range(1, MAX_STEPS + 1):
            n = df[df["purchase_num"] == step]["customer_id"].nunique()
            if n == 0:
                break
            avg_val  = df[df["purchase_num"] == step]["order_total"].mean()
            avg_days = df[df["purchase_num"] == step]["days_since_prev"].mean()
            rows.append({
                "purchase_step"    : step,
                "customers"        : n,
                "retention_pct"    : round(n / total * 100, 2),
                "avg_order_value"  : round(avg_val,  2),
                "avg_days_since_prev": round(avg_days, 1) if not pd.isna(avg_days) else None,
            })
        return pd.DataFrame(rows)

    # ── 7. Sankey data ────────────────────────────────────────────────────────────────────────────

    def build_sankey_data(self, df: pd.DataFrame) -> dict:
        """
        Returns node labels + link source/target/value for a 3-level Sankey.
        Levels: 1st purchase → 2nd purchase → 3rd purchase
        Each level has nodes: Low, Medium, High, Churned (except level 1)
        """
        TIER_COLORS = {
            "Low"    : "rgba(91,155,213,0.8)",
            "Medium" : "rgba(237,125,49,0.8)",
            "High"   : "rgba(112,173,71,0.8)",
            "Churned": "rgba(120,120,120,0.5)",
        }

        # Node labels: L1 (3 nodes), L2 (4 nodes), L3 (4 nodes) = 11 total
        l1_nodes = [f"{t} (1st)" for t in TIERS]
        l2_nodes = [f"{t} (2nd)" for t in TIERS] + [f"{CHURN_LABEL} (1→2)"]
        l3_nodes = [f"{t} (3rd)" for t in TIERS] + [f"{CHURN_LABEL} (2→3)"]
        all_labels = l1_nodes + l2_nodes + l3_nodes

        # Node index helpers
        def l1_idx(t): return TIERS.index(t)
        def l2_idx(t): return 3 + (TIERS.index(t) if t in TIERS else 3)
        def l3_idx(t): return 7 + (TIERS.index(t) if t in TIERS else 3)

        node_colors = (
            [TIER_COLORS[t] for t in TIERS] +
            [TIER_COLORS[t] if t in TIERS else TIER_COLORS[CHURN_LABEL]
             for t in TIERS + [CHURN_LABEL]] * 2
        )

        sources, targets, values = [], [], []

        # Level 1 → Level 2
        cust_by_id = {cid: g.sort_values("purchase_num")
                      for cid, g in df.groupby("customer_id")}

        l1l2 = {}
        l2l3 = {}

        for cid, group in cust_by_id.items():
            tiers = group["tier"].tolist()
            t1    = tiers[0]
            t2    = tiers[1] if len(tiers) > 1 else CHURN_LABEL
            t3    = tiers[2] if len(tiers) > 2 else CHURN_LABEL
            l1l2[(t1, t2)] = l1l2.get((t1, t2), 0) + 1
            if t2 != CHURN_LABEL:
                l2l3[(t2, t3)] = l2l3.get((t2, t3), 0) + 1

        for (t1, t2), cnt in l1l2.items():
            sources.append(l1_idx(t1))
            targets.append(l2_idx(t2))
            values.append(cnt)

        for (t2, t3), cnt in l2l3.items():
            sources.append(l2_idx(t2))
            targets.append(l3_idx(t3))
            values.append(cnt)

        return {
            "node"  : {"label": all_labels, "color": node_colors,
                       "pad": 20, "thickness": 25},
            "link"  : {"source": sources, "target": targets, "value": values},
        }

    # ── 8. Run ────────────────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        logger.info("=" * 60)
        logger.info("CUSTOMER JOURNEY ANALYSIS")
        logger.info("=" * 60)

        df      = self.fetch_transactions()
        df      = self.classify_tiers(df)
        df      = self.build_journeys(df)

        trans   = self.build_transitions(df)
        seqs    = self.find_sequences(df)
        funnel  = self.compute_funnel(df)
        sankey  = self.build_sankey_data(df)

        # Save CSVs
        trans.to_csv(TRANSITIONS_CSV,  index=False)
        seqs.to_csv(SEQUENCES_CSV,     index=False)
        funnel.to_csv(FUNNEL_CSV,      index=False)
        logger.info(f"Saved transitions -> {TRANSITIONS_CSV}")
        logger.info(f"Saved sequences   -> {SEQUENCES_CSV}")
        logger.info(f"Saved funnel      -> {FUNNEL_CSV}")

        # Tier distribution per step
        tier_by_step = {}
        for step in range(1, 5):
            sub = df[df["purchase_num"] == step]
            if len(sub):
                tier_by_step[f"step_{step}"] = sub["tier"].value_counts().to_dict()

        # Most common first purchase
        first_tiers = df[df["purchase_num"] == 1]["tier"].value_counts().to_dict()

        # Days between purchases
        gap_stats = df.dropna(subset=["days_since_prev"])
        gap_stats = gap_stats["days_since_prev"]

        os.makedirs("models", exist_ok=True)
        metadata = {
            "generated_at"       : datetime.now().isoformat(),
            "total_customers"    : int(df["customer_id"].nunique()),
            "total_transactions" : int(len(df)),
            "avg_journey_length" : round(float(df.groupby("customer_id").size().mean()), 2),
            "customers_1_purchase"  : int((df.groupby("customer_id").size() == 1).sum()),
            "customers_2plus"    : int((df.groupby("customer_id").size() >= 2).sum()),
            "customers_3plus"    : int((df.groupby("customer_id").size() >= 3).sum()),
            "retention_after_1st": round(
                int((df.groupby("customer_id").size() >= 2).sum()) /
                int(df["customer_id"].nunique()) * 100, 1),
            "tier_thresholds"    : self.thresholds,
            "first_purchase_tiers": first_tiers,
            "tier_by_step"       : tier_by_step,
            "avg_days_between_purchases": round(float(gap_stats.mean()), 1),
            "median_days_between": round(float(gap_stats.median()), 1),
            "top_sequences"      : seqs.head(10).to_dict("records"),
            "sankey"             : sankey,
        }
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata    -> {METADATA_JSON}")
        logger.info("Customer journey analysis complete.")

        return metadata


if __name__ == "__main__":
    analyzer = CustomerJourneyAnalyzer()
    meta = analyzer.run()

    print(f"\nCustomers analysed : {meta['total_customers']:,}")
    print(f"Avg journey length : {meta['avg_journey_length']}")
    print(f"Retention after 1st: {meta['retention_after_1st']}%")
    print(f"Avg days between   : {meta['avg_days_between_purchases']} days")
    print(f"\nTop 5 sequences:")
    for s in meta["top_sequences"][:5]:
        print(f"  {s['sequence']:30s}  n={s['count']:4d}  ({s['pct']}%)")
