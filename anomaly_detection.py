"""
anomaly_detection.py — Sales Anomaly Detection

Detects unusual spikes and drops in sales using two complementary methods:
  1. Z-Score    : (value - rolling_mean) / rolling_std
  2. IQR Fence  : value outside Q1 - k*IQR  or  Q3 + k*IQR

Three anomaly types computed:
  a. Overall daily revenue anomalies  (all products combined)
  b. Per-product daily revenue anomalies  (top 50 products by volume)
  c. Daily price anomalies per product  (unusual unit price)

Anomaly severity:
  Critical : |z_score| > 4  or  IQR fence multiplier > 3
  High     : |z_score| > 3
  Medium   : |z_score| > 2

Outputs:
  anomaly_results.csv           — overall + per-product anomalies
  models/anomaly_metadata.json  — summary statistics & top anomalies

Usage:
  python anomaly_detection.py
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

logger = setup_logger("anomaly_detection", "anomaly_detection_logs.txt")

RESULTS_CSV   = "anomaly_results.csv"
METADATA_JSON = "models/anomaly_metadata.json"

ROLLING_WINDOW  = 30    # days for rolling mean/std
Z_MEDIUM        = 2.0
Z_HIGH          = 3.0
Z_CRITICAL      = 4.0
IQR_MULTIPLIER  = 1.5   # standard fence
TOP_PRODUCTS    = 50    # products to analyse individually


class AnomalyDetector:

    def __init__(self):
        self.engine = engine

    # ── 1. Fetch overall daily sales ──────────────────────────────────────────

    def fetch_daily_overall(self) -> pd.DataFrame:
        logger.info("Fetching overall daily sales ...")
        query = text("""
            SELECT
                t.date,
                t.year,
                t.month,
                t.day_of_week,
                SUM(fs.sales_amount)                        AS revenue,
                SUM(fs.quantity_sold)                       AS units,
                COUNT(*)                                    AS transactions,
                COUNT(DISTINCT fs.customer_key)             AS unique_customers
            FROM fact_sales fs
            JOIN dim_time t ON fs.time_key = t.time_key
            GROUP BY t.date, t.year, t.month, t.day_of_week
            ORDER BY t.date
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        df["date"] = pd.to_datetime(df["date"])
        logger.info(f"  {len(df):,} trading days loaded")
        return df

    # ── 2. Fetch per-product daily sales (top N) ──────────────────────────────

    def fetch_daily_product(self, top_n: int = TOP_PRODUCTS) -> pd.DataFrame:
        logger.info(f"Fetching daily sales for top {top_n} products ...")

        # Get top products by total revenue
        with self.engine.connect() as conn:
            top = pd.read_sql(text("""
                SELECT p.product_id, p.product_name, SUM(fs.sales_amount) AS total_rev
                FROM fact_sales fs
                JOIN dim_product p ON fs.product_key = p.product_key
                GROUP BY p.product_id, p.product_name
                ORDER BY total_rev DESC
                LIMIT :n
            """), conn, params={"n": top_n})

        top_ids = tuple(top["product_id"].astype(str).tolist())

        query = text(f"""
            SELECT
                p.product_id,
                p.product_name,
                t.date,
                SUM(fs.sales_amount)                                        AS revenue,
                SUM(fs.quantity_sold)                                       AS units,
                SUM(fs.sales_amount) / NULLIF(SUM(fs.quantity_sold), 0)    AS avg_price
            FROM fact_sales fs
            JOIN dim_product p ON fs.product_key = p.product_key
            JOIN dim_time    t ON fs.time_key    = t.time_key
            WHERE p.product_id = ANY(:ids)
            GROUP BY p.product_id, p.product_name, t.date
            ORDER BY p.product_id, t.date
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"ids": list(top_ids)})

        df["date"] = pd.to_datetime(df["date"])
        logger.info(f"  {len(df):,} product-day records for {df['product_id'].nunique()} products")
        return df

    # ── 3. Detect anomalies in a numeric series ────────────────────────────────

    @staticmethod
    def detect(series: pd.Series, window: int = ROLLING_WINDOW) -> pd.DataFrame:
        """
        Returns DataFrame with z_score, is_anomaly, severity, direction columns.
        Uses rolling Z-score + global IQR fence.
        """
        s = series.copy().reset_index(drop=True)

        # Rolling mean & std (min_periods=7 so early rows still get scored)
        roll_mean = s.rolling(window=window, min_periods=7, center=False).mean()
        roll_std  = s.rolling(window=window, min_periods=7, center=False).std()
        roll_std  = roll_std.replace(0, np.nan)

        z = (s - roll_mean) / roll_std

        # Global IQR fence
        Q1, Q3 = s.quantile(0.25), s.quantile(0.75)
        IQR    = Q3 - Q1
        iqr_lo = Q1 - IQR_MULTIPLIER * IQR
        iqr_hi = Q3 + IQR_MULTIPLIER * IQR
        iqr_flag = (s < iqr_lo) | (s > iqr_hi)

        abs_z = z.abs()
        severity = pd.cut(
            abs_z,
            bins=[-np.inf, Z_MEDIUM, Z_HIGH, Z_CRITICAL, np.inf],
            labels=["Normal", "Medium", "High", "Critical"],
        )

        is_anomaly = (abs_z >= Z_MEDIUM) | iqr_flag
        direction  = np.where(z > 0, "Spike", "Drop")

        return pd.DataFrame({
            "z_score"    : z.round(3),
            "roll_mean"  : roll_mean.round(2),
            "is_anomaly" : is_anomaly,
            "severity"   : severity.astype(str),
            "direction"  : direction,
            "iqr_flag"   : iqr_flag,
        })

    # ── 4. Overall anomalies ──────────────────────────────────────────────────

    def overall_anomalies(self, daily: pd.DataFrame) -> pd.DataFrame:
        logger.info("Detecting overall daily revenue anomalies ...")
        stats = self.detect(daily["revenue"])
        result = pd.concat([
            daily.reset_index(drop=True),
            stats
        ], axis=1)
        result["scope"] = "overall"
        result["product_id"]   = None
        result["product_name"] = None
        result["metric"]       = "revenue"
        n = result["is_anomaly"].sum()
        logger.info(f"  {n} anomalous days detected out of {len(result)}")
        return result

    # ── 5. Per-product anomalies ──────────────────────────────────────────────

    def product_anomalies(self, prod_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Detecting per-product revenue anomalies ...")
        rows = []
        for pid, grp in prod_df.groupby("product_id"):
            grp = grp.sort_values("date").reset_index(drop=True)
            if len(grp) < 10:
                continue
            stats = self.detect(grp["revenue"])
            tmp = pd.concat([grp.reset_index(drop=True), stats], axis=1)
            tmp["scope"]  = "product"
            tmp["metric"] = "revenue"
            rows.append(tmp)

        if not rows:
            return pd.DataFrame()

        result = pd.concat(rows, ignore_index=True)
        n = result["is_anomaly"].sum()
        logger.info(f"  {n} anomalous product-days detected")
        return result

    # ── 6. Price anomalies ────────────────────────────────────────────────────

    def price_anomalies(self, prod_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Detecting per-product price anomalies ...")
        rows = []
        for pid, grp in prod_df.groupby("product_id"):
            grp = grp.sort_values("date").dropna(subset=["avg_price"]).reset_index(drop=True)
            if len(grp) < 10:
                continue
            stats = self.detect(grp["avg_price"])
            tmp = pd.concat([grp.reset_index(drop=True), stats], axis=1)
            tmp["scope"]   = "product"
            tmp["metric"]  = "price"
            tmp["revenue"] = tmp["avg_price"]   # reuse revenue column for unified schema
            rows.append(tmp)

        if not rows:
            return pd.DataFrame()

        result = pd.concat(rows, ignore_index=True)
        n = result["is_anomaly"].sum()
        logger.info(f"  {n} anomalous price-days detected")
        return result

    # ── 7. Run ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        logger.info("=" * 60)
        logger.info("ANOMALY DETECTION ANALYSIS")
        logger.info(f"  Rolling window  : {ROLLING_WINDOW} days")
        logger.info(f"  Z thresholds    : Medium>{Z_MEDIUM}  High>{Z_HIGH}  Critical>{Z_CRITICAL}")
        logger.info("=" * 60)

        daily    = self.fetch_daily_overall()
        prod_df  = self.fetch_daily_product()

        overall  = self.overall_anomalies(daily)
        product  = self.product_anomalies(prod_df)
        price    = self.price_anomalies(prod_df)

        # Combine all anomalies
        cols = ["date", "product_id", "product_name", "scope", "metric",
                "revenue", "units", "transactions",
                "z_score", "roll_mean", "is_anomaly", "severity", "direction", "iqr_flag"]

        all_parts = [overall]
        if not product.empty:
            all_parts.append(product)
        if not price.empty:
            all_parts.append(price)

        combined = pd.concat(all_parts, ignore_index=True)

        # Only keep anomaly rows in CSV (reduce file size)
        anomalies = combined[combined["is_anomaly"] == True].copy()
        anomalies["date"] = anomalies["date"].astype(str)

        # Keep relevant columns
        save_cols = [c for c in cols if c in anomalies.columns]
        anomalies = anomalies[save_cols].sort_values(
            ["severity", "z_score"], ascending=[True, True],
            key=lambda s: s if s.name != "severity" else
                s.map({"Critical": 0, "High": 1, "Medium": 2, "Normal": 3})
        ).reset_index(drop=True)

        anomalies.to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved {len(anomalies):,} anomaly rows -> {RESULTS_CSV}")

        # ── Summary stats ──
        overall_anom = overall[overall["is_anomaly"] == True]
        sev_dist = overall_anom["severity"].value_counts().to_dict()
        prod_anom_count = 0 if product.empty else product["is_anomaly"].sum()

        # Top 10 overall anomalies by z_score magnitude
        top_overall = (
            overall_anom.assign(abs_z=overall_anom["z_score"].abs())
            .nlargest(10, "abs_z")
            [["date", "revenue", "units", "transactions", "z_score", "severity", "direction"]]
        )
        top_overall["date"] = top_overall["date"].astype(str)
        top_overall["revenue"] = top_overall["revenue"].round(2)

        # Monthly anomaly counts
        overall["year_month"] = overall["date"].dt.to_period("M").astype(str)
        monthly_counts = (
            overall[overall["is_anomaly"]]
            .groupby("year_month")["is_anomaly"].count()
            .reset_index()
            .rename(columns={"is_anomaly": "anomaly_count"})
            .sort_values("year_month")
        )

        # Overall daily series for sparkline
        daily_series = overall[["date", "revenue", "roll_mean", "z_score", "is_anomaly", "severity", "direction"]].copy()
        daily_series["date"]     = daily_series["date"].astype(str)
        daily_series["revenue"]  = daily_series["revenue"].round(2)
        daily_series["roll_mean"]= daily_series["roll_mean"].round(2)

        metadata = {
            "generated_at"          : datetime.now().isoformat(),
            "rolling_window_days"   : ROLLING_WINDOW,
            "z_thresholds"          : {"medium": Z_MEDIUM, "high": Z_HIGH, "critical": Z_CRITICAL},
            "total_trading_days"    : int(len(overall)),
            "overall_anomaly_count" : int(len(overall_anom)),
            "overall_anomaly_rate"  : round(len(overall_anom) / len(overall) * 100, 1),
            "severity_distribution" : sev_dist,
            "product_anomaly_count" : int(prod_anom_count),
            "products_analysed"     : int(prod_df["product_id"].nunique()),
            "top_anomalies"         : json.loads(top_overall.to_json(orient="records")),
            "monthly_anomaly_counts": json.loads(monthly_counts.to_json(orient="records")),
            "daily_series"          : json.loads(daily_series.to_json(orient="records")),
        }

        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")
        logger.info("Anomaly detection complete.")
        return metadata


if __name__ == "__main__":
    ad   = AnomalyDetector()
    meta = ad.run()

    print(f"\nTrading days analysed  : {meta['total_trading_days']:,}")
    print(f"Overall anomalies      : {meta['overall_anomaly_count']} ({meta['overall_anomaly_rate']}%)")
    print(f"Severity distribution  :")
    for sev, cnt in meta["severity_distribution"].items():
        print(f"  {sev:10s}: {cnt}")
    print(f"\nTop 5 anomalous days:")
    for r in meta["top_anomalies"][:5]:
        print(f"  {r['date']}  ${r['revenue']:>10,.2f}  z={r['z_score']:+.2f}  {r['severity']:8s}  {r['direction']}")
