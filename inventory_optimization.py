"""
inventory_optimization.py — Inventory Optimisation Analysis

Calculates optimal inventory parameters for every active product
using historical sales demand and (optionally) the demand forecast.

Metrics computed per product:
  avg_daily_demand   — historical mean daily units sold
  std_daily_demand   — standard deviation of daily demand
  cv                 — coefficient of variation (std / mean)
  safety_stock       — buffer stock at 95 % service level
  reorder_point      — restock trigger level (lead-time demand + safety stock)
  eoq                — Economic Order Quantity (optimal order size)
  max_stock          — upper bound = reorder_point + EOQ
  annual_demand      — projected annual units (from forecast if available)
  orders_per_year    — how often to reorder annually
  total_annual_cost  — holding + ordering costs (EOQ model)
  stockout_risk      — Critical / High / Medium / Low

Assumptions (configurable via CLI):
  Lead time          : 7 days
  Service level      : 95 %  (Z = 1.645)
  Ordering cost      : $50 per purchase order
  Holding cost rate  : 20 % of unit price per year

Outputs:
  inventory_optimization_results.csv
  models/inventory_metadata.json

Usage:
  python inventory_optimization.py
  python inventory_optimization.py --lead-time 14 --service-level 99
"""

import os
import json
import warnings
import argparse
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import text

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("inventory_optimization", "inventory_optimization_logs.txt")

RESULTS_CSV   = "inventory_optimization_results.csv"
METADATA_JSON = "models/inventory_metadata.json"
FORECAST_CSV  = "demand_forecast_results.csv"

ORDERING_COST = 50.0   # $ per purchase order
HOLDING_RATE  = 0.20   # fraction of unit price per year

# Service level → Z-score table
Z_SCORES = {90: 1.282, 95: 1.645, 97: 1.881, 99: 2.326}


class InventoryOptimizer:

    def __init__(self, lead_time_days: int = 7, service_level: int = 95):
        self.engine       = engine
        self.lead_time    = lead_time_days
        self.service_level = service_level
        self.z            = Z_SCORES.get(service_level, 1.645)

    # ── 1. Fetch historical daily sales ───────────────────────────────────────

    def fetch_daily_sales(self) -> pd.DataFrame:
        logger.info("Fetching historical daily sales per product ...")
        query = text("""
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                t.date::date                                        AS sale_date,
                SUM(fs.quantity_sold)                               AS daily_qty,
                SUM(fs.sales_amount)                                AS daily_revenue,
                SUM(fs.sales_amount) / NULLIF(SUM(fs.quantity_sold), 0) AS avg_unit_price
            FROM fact_sales   fs
            JOIN dim_product  p  ON fs.product_key = p.product_key
            JOIN dim_time     t  ON fs.time_key     = t.time_key
            WHERE fs.quantity_sold > 0
              AND fs.sales_amount  > 0
            GROUP BY p.product_id, p.product_name, p.category, t.date
            ORDER BY p.product_id, t.date
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["sale_date"] = pd.to_datetime(df["sale_date"])
        df["product_name"] = df["product_name"].fillna("Unknown")
        df["category"]     = df["category"].fillna("General")
        logger.info(f"  {len(df):,} product-day records | {df['product_id'].nunique():,} products")
        return df

    # ── 2. Compute per-product demand stats ───────────────────────────────────

    def compute_demand_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Computing demand statistics per product ...")
        rows = []

        for pid, grp in df.groupby("product_id"):
            name  = str(grp["product_name"].iloc[0])[:60]
            cat   = grp["category"].iloc[0]
            price = float(grp["avg_unit_price"].mean())

            qty       = grp["daily_qty"].values
            avg_d     = float(np.mean(qty))
            std_d     = float(np.std(qty, ddof=1)) if len(qty) > 1 else avg_d * 0.3
            if std_d == 0 or np.isnan(std_d):
                std_d = max(avg_d * 0.2, 0.01)

            cv = round(std_d / avg_d, 3) if avg_d > 0 else 0

            # Safety Stock: Z × σ × √(L)
            ss  = self.z * std_d * (self.lead_time ** 0.5)
            # Reorder Point: d̄ × L + SS
            rop = avg_d * self.lead_time + ss
            # Annual demand (historical rate)
            ann = avg_d * 365
            # EOQ: √(2DS/H)
            H = price * HOLDING_RATE
            eoq = ((2 * ann * ORDERING_COST) / H) ** 0.5 if H > 0 and ann > 0 else avg_d * 30
            # Max stock
            max_s = rop + eoq
            # Orders per year
            opy = ann / eoq if eoq > 0 else 0
            # Annual costs
            hold_cost  = (eoq / 2) * H
            order_cost = opy * ORDERING_COST
            total_cost = hold_cost + order_cost

            rows.append({
                "product_id"           : pid,
                "product_name"         : name,
                "category"             : cat,
                "avg_unit_price"       : round(price, 2),
                "trading_days"         : len(grp),
                "avg_daily_demand"     : round(avg_d, 3),
                "std_daily_demand"     : round(std_d, 3),
                "cv"                   : cv,
                "safety_stock"         : round(ss, 1),
                "reorder_point"        : round(rop, 1),
                "eoq"                  : round(eoq, 1),
                "max_stock"            : round(max_s, 1),
                "annual_demand"        : round(ann, 1),
                "orders_per_year"      : round(opy, 1),
                "annual_holding_cost"  : round(hold_cost, 2),
                "annual_ordering_cost" : round(order_cost, 2),
                "total_annual_cost"    : round(total_cost, 2),
            })

        return pd.DataFrame(rows)

    # ── 3. Merge forecast, recalculate with forward-looking demand ────────────

    def merge_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        if not os.path.exists(FORECAST_CSV):
            logger.info("  No forecast CSV found — using historical demand only.")
            df["avg_daily_forecast"]  = None
            df["forecast_annual"]     = None
            df["forecast_rop"]        = df["reorder_point"]
            df["forecast_eoq"]        = df["eoq"]
            return df

        logger.info("  Merging demand forecast data ...")
        fc = pd.read_csv(FORECAST_CSV)
        future = fc[fc["is_forecast"] == True]
        agg = (
            future.groupby("product_id")["predicted_quantity"]
            .mean()
            .reset_index()
            .rename(columns={"predicted_quantity": "avg_daily_forecast"})
        )
        agg["product_id"] = agg["product_id"].astype(str)
        df["product_id"]  = df["product_id"].astype(str)
        df = df.merge(agg, on="product_id", how="left")

        # Where forecast exists, recalculate ROP and EOQ with forecast demand
        fc_mask = df["avg_daily_forecast"].notna()
        df["forecast_annual"] = None
        df.loc[fc_mask, "forecast_annual"] = (df.loc[fc_mask, "avg_daily_forecast"] * 365).round(1)

        def fc_rop(row):
            d = row["avg_daily_forecast"] if pd.notna(row["avg_daily_forecast"]) else row["avg_daily_demand"]
            return round(d * self.lead_time + row["safety_stock"], 1)

        def fc_eoq(row):
            ann = row["forecast_annual"] if pd.notna(row["forecast_annual"]) else row["annual_demand"]
            H   = row["avg_unit_price"] * HOLDING_RATE
            if H > 0 and ann > 0:
                return round(((2 * ann * ORDERING_COST) / H) ** 0.5, 1)
            return row["eoq"]

        df["forecast_rop"] = df.apply(fc_rop, axis=1)
        df["forecast_eoq"] = df.apply(fc_eoq, axis=1)
        logger.info(f"  Forecast merged for {fc_mask.sum()} products.")
        return df

    # ── 4. Classify stockout risk ─────────────────────────────────────────────

    def classify_risk(self, df: pd.DataFrame) -> pd.DataFrame:
        def label(cv):
            if cv > 1.5:  return "Critical"
            if cv > 1.0:  return "High"
            if cv > 0.5:  return "Medium"
            return "Low"

        df["stockout_risk"] = df["cv"].apply(label)
        return df

    # ── 5. Run ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        logger.info("=" * 60)
        logger.info("INVENTORY OPTIMISATION ANALYSIS")
        logger.info(f"  Lead time: {self.lead_time}d  |  Service level: {self.service_level}%  |  Z={self.z}")
        logger.info("=" * 60)

        daily  = self.fetch_daily_sales()
        stats  = self.compute_demand_stats(daily)
        stats  = self.merge_forecast(stats)
        stats  = self.classify_risk(stats)

        stats = stats.sort_values("annual_demand", ascending=False).reset_index(drop=True)
        stats.to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved results  -> {RESULTS_CSV}  ({len(stats):,} products)")

        # Category-level summary
        cat_agg = (
            stats.groupby("category")
            .agg(
                product_count    = ("product_id",       "count"),
                avg_eoq          = ("eoq",               "mean"),
                avg_safety_stock = ("safety_stock",      "mean"),
                avg_rop          = ("reorder_point",     "mean"),
                total_annual_cost= ("total_annual_cost", "sum"),
                critical_count   = ("stockout_risk",     lambda x: (x == "Critical").sum()),
                high_count       = ("stockout_risk",     lambda x: (x == "High").sum()),
            )
            .reset_index()
            .round(2)
        )

        risk_dist = stats["stockout_risk"].value_counts().to_dict()

        os.makedirs("models", exist_ok=True)
        top_critical = (
            stats[stats["stockout_risk"].isin(["Critical", "High"])]
            .head(10)
            [["product_id", "product_name", "category", "stockout_risk",
              "avg_daily_demand", "cv", "safety_stock", "reorder_point", "eoq"]]
            .to_dict("records")
        )

        metadata = {
            "generated_at"              : datetime.now().isoformat(),
            "lead_time_days"            : self.lead_time,
            "service_level_pct"         : self.service_level,
            "z_score"                   : self.z,
            "ordering_cost"             : ORDERING_COST,
            "holding_cost_rate"         : HOLDING_RATE,
            "products_analysed"         : int(len(stats)),
            "risk_distribution"         : risk_dist,
            "avg_eoq"                   : round(float(stats["eoq"].mean()), 1),
            "avg_safety_stock"          : round(float(stats["safety_stock"].mean()), 1),
            "avg_reorder_point"         : round(float(stats["reorder_point"].mean()), 1),
            "total_annual_holding_cost" : round(float(stats["annual_holding_cost"].sum()), 2),
            "total_annual_ordering_cost": round(float(stats["annual_ordering_cost"].sum()), 2),
            "total_annual_inventory_cost": round(float(stats["total_annual_cost"].sum()), 2),
            "category_summary"          : json.loads(cat_agg.to_json(orient="records")),
            "top_alerts"                : top_critical,
        }

        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")
        logger.info("Inventory optimisation complete.")
        return metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inventory Optimisation")
    parser.add_argument("--lead-time",      type=int, default=7,  help="Lead time days (default: 7)")
    parser.add_argument("--service-level",  type=int, default=95, choices=[90, 95, 97, 99],
                        help="Service level %% (default: 95)")
    args = parser.parse_args()

    opt  = InventoryOptimizer(lead_time_days=args.lead_time, service_level=args.service_level)
    meta = opt.run()

    print(f"\nProducts analysed      : {meta['products_analysed']:,}")
    print(f"Avg EOQ                : {meta['avg_eoq']:,.1f} units")
    print(f"Avg Safety Stock       : {meta['avg_safety_stock']:,.1f} units")
    print(f"Avg Reorder Point      : {meta['avg_reorder_point']:,.1f} units")
    print(f"\nRisk Distribution:")
    for risk, count in meta["risk_distribution"].items():
        print(f"  {risk:10s}: {count:,}")
    print(f"\nTotal Annual Inventory Cost : ${meta['total_annual_inventory_cost']:,.2f}")
