"""
price_elasticity.py — Price Elasticity of Demand Analysis

For each product, estimates how sensitive demand is to price changes using
a log-log OLS regression: log(quantity) ~ log(price).

The slope coefficient is the price elasticity:
  E < -1.5  → Highly Elastic   (demand drops sharply with price increase)
  -1.5 to -1 → Elastic
  -1 to -0.5 → Inelastic
  -0.5 to 0  → Highly Inelastic
  E > 0      → Positive (luxury / Giffen goods pattern)

Outputs:
  - price_elasticity_results.csv
  - models/price_elasticity_metadata.json

Usage:
  python price_elasticity.py
  python price_elasticity.py --min-days 20 --min-cv 0.03
"""

import os
import json
import argparse
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from scipy import stats
from sqlalchemy import text

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("price_elasticity", "price_elasticity_logs.txt")

ELASTICITY_CSV  = "price_elasticity_results.csv"
METADATA_JSON   = "models/price_elasticity_metadata.json"

# Minimum requirements for a product to be analysed
DEFAULT_MIN_DAYS = 15    # at least N distinct trading days
DEFAULT_MIN_CV   = 0.02  # minimum price coefficient of variation


# ── Classification helpers ─────────────────────────────────────────────────────

def classify_elasticity(e: float) -> str:
    if e is None or np.isnan(e):
        return "Unknown"
    if e >= 0:
        return "Positive"
    if e < -1.5:
        return "Highly Elastic"
    if e < -1.0:
        return "Elastic"
    if e < -0.5:
        return "Inelastic"
    return "Highly Inelastic"


def interpret_elasticity(e: float, pname: str) -> str:
    if e is None or np.isnan(e):
        return "Insufficient data for interpretation."
    pname = pname or "This product"
    if e >= 0:
        return f"{pname}: demand increases with price — possible luxury/prestige effect."
    direction = "decrease" if e < 0 else "increase"
    pct = abs(round(e, 2))
    return (
        f"A 1% price increase leads to a {pct}% {direction} in demand. "
        f"{'Highly price-sensitive.' if pct > 1.5 else 'Moderately price-sensitive.' if pct > 1 else 'Low price sensitivity.'}"
    )


# ── Elasticity Engine ──────────────────────────────────────────────────────────

class PriceElasticityAnalyzer:

    def __init__(self, min_days: int = DEFAULT_MIN_DAYS, min_cv: float = DEFAULT_MIN_CV):
        self.engine   = engine
        self.min_days = min_days
        self.min_cv   = min_cv

    # ── 1. Fetch data ─────────────────────────────────────────────────────────

    def fetch_data(self) -> pd.DataFrame:
        logger.info("Fetching daily price and quantity data from data warehouse...")

        query = text("""
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                t.date::date                                             AS sale_date,
                SUM(fs.sales_amount) / NULLIF(SUM(fs.quantity_sold), 0) AS avg_unit_price,
                SUM(fs.quantity_sold)                                    AS daily_qty
            FROM fact_sales    fs
            JOIN dim_product   p  ON fs.product_key = p.product_key
            JOIN dim_time      t  ON fs.time_key    = t.time_key
            WHERE fs.quantity_sold > 0
              AND fs.sales_amount  > 0
            GROUP BY p.product_id, p.product_name, p.category, t.date
            ORDER BY p.product_id, t.date
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["sale_date"]     = pd.to_datetime(df["sale_date"])
        df["avg_unit_price"] = pd.to_numeric(df["avg_unit_price"], errors="coerce")
        df["daily_qty"]      = pd.to_numeric(df["daily_qty"],      errors="coerce")
        df = df.dropna(subset=["avg_unit_price", "daily_qty"])
        df = df[(df["avg_unit_price"] > 0) & (df["daily_qty"] > 0)]

        logger.info(
            f"  Loaded {len(df):,} daily records for "
            f"{df['product_id'].nunique():,} products"
        )
        return df

    # ── 2. Filter eligible products ───────────────────────────────────────────

    def filter_products(self, df: pd.DataFrame) -> list:
        stats_df = (
            df.groupby("product_id")["avg_unit_price"]
            .agg(
                trading_days="count",
                price_std="std",
                price_mean="mean",
            )
            .reset_index()
        )
        stats_df["price_cv"] = stats_df["price_std"] / stats_df["price_mean"]
        stats_df = stats_df.fillna(0)

        eligible = stats_df[
            (stats_df["trading_days"] >= self.min_days) &
            (stats_df["price_cv"]     >= self.min_cv)
        ]["product_id"].tolist()

        logger.info(
            f"  {len(eligible):,} products eligible "
            f"(>= {self.min_days} days, price CV >= {self.min_cv})"
        )
        return eligible

    # ── 3. Log-log OLS regression per product ─────────────────────────────────

    def _compute_elasticity(self, product_df: pd.DataFrame) -> dict:
        """Run log-log OLS: log(qty) ~ log(price). Returns elasticity + stats."""
        log_price = np.log(product_df["avg_unit_price"].values)
        log_qty   = np.log(product_df["daily_qty"].values)

        # Need variance in both X and Y
        if np.std(log_price) < 1e-6:
            return None

        slope, intercept, r, p_value, std_err = stats.linregress(log_price, log_qty)

        return {
            "price_elasticity": round(float(slope), 4),
            "r_squared"       : round(float(r ** 2), 4),
            "p_value"         : round(float(p_value), 4),
            "std_err"         : round(float(std_err), 4),
            "is_significant"  : bool(p_value < 0.05),
        }

    # ── 4. Run analysis ───────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        logger.info("=" * 60)
        logger.info("PRICE ELASTICITY ANALYSIS")
        logger.info(f"  Min trading days : {self.min_days}")
        logger.info(f"  Min price CV     : {self.min_cv}")
        logger.info("=" * 60)

        raw_df       = self.fetch_data()
        eligible_ids = self.filter_products(raw_df)

        rows = []
        skipped = 0

        for i, pid in enumerate(eligible_ids, 1):
            subset = raw_df[raw_df["product_id"] == pid].copy()
            name   = subset["product_name"].iloc[0] or "Unknown"
            cat    = subset["category"].iloc[0]    or "Unknown"

            result = self._compute_elasticity(subset)
            if result is None:
                skipped += 1
                continue

            e = result["price_elasticity"]

            rows.append({
                "product_id"       : pid,
                "product_name"     : name,
                "category"         : cat,
                "price_elasticity" : e,
                "elasticity_type"  : classify_elasticity(e),
                "r_squared"        : result["r_squared"],
                "p_value"          : result["p_value"],
                "std_err"          : result["std_err"],
                "is_significant"   : result["is_significant"],
                "avg_price"        : round(float(subset["avg_unit_price"].mean()), 4),
                "min_price"        : round(float(subset["avg_unit_price"].min()),  4),
                "max_price"        : round(float(subset["avg_unit_price"].max()),  4),
                "price_cv"         : round(float(subset["avg_unit_price"].std() /
                                                  subset["avg_unit_price"].mean()), 4),
                "avg_daily_qty"    : round(float(subset["daily_qty"].mean()), 2),
                "trading_days"     : int(len(subset)),
                "interpretation"   : interpret_elasticity(e, name),
            })

            if i % 100 == 0:
                logger.info(f"  Processed {i:,}/{len(eligible_ids):,} products...")

        logger.info(
            f"  Done — {len(rows):,} products analysed, {skipped} skipped (no price variance)"
        )

        if not rows:
            logger.error("No results generated.")
            return pd.DataFrame()

        results = pd.DataFrame(rows).sort_values("price_elasticity")

        # ── Save CSV ──────────────────────────────────────────────────────────
        results.to_csv(ELASTICITY_CSV, index=False)
        logger.info(f"Saved {len(results):,} rows -> {ELASTICITY_CSV}")

        # ── Save metadata ─────────────────────────────────────────────────────
        type_counts = results["elasticity_type"].value_counts().to_dict()
        sig_count   = int(results["is_significant"].sum())

        os.makedirs("models", exist_ok=True)
        metadata = {
            "generated_at"       : datetime.now().isoformat(),
            "method"             : "Log-Log OLS Regression",
            "min_trading_days"   : self.min_days,
            "min_price_cv"       : self.min_cv,
            "products_analysed"  : len(results),
            "significant_results": sig_count,
            "significance_pct"   : round(sig_count / len(results) * 100, 1),
            "elasticity_distribution": type_counts,
            "avg_elasticity"     : round(float(results["price_elasticity"].mean()), 4),
            "median_elasticity"  : round(float(results["price_elasticity"].median()), 4),
            "most_elastic_product": results.nsmallest(1, "price_elasticity")[
                ["product_id","product_name","price_elasticity"]
            ].to_dict("records")[0],
            "most_inelastic_product": results[results["price_elasticity"] < 0].nlargest(
                1, "price_elasticity"
            )[["product_id","product_name","price_elasticity"]].to_dict("records")[0]
            if len(results[results["price_elasticity"] < 0]) else None,
        }
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata    -> {METADATA_JSON}")
        logger.info("Price elasticity analysis complete.")

        return results


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Price Elasticity Analyzer")
    parser.add_argument(
        "--min-days", type=int, default=DEFAULT_MIN_DAYS,
        help=f"Minimum trading days required per product (default: {DEFAULT_MIN_DAYS})"
    )
    parser.add_argument(
        "--min-cv", type=float, default=DEFAULT_MIN_CV,
        help=f"Minimum price coefficient of variation (default: {DEFAULT_MIN_CV})"
    )
    args = parser.parse_args()

    analyzer = PriceElasticityAnalyzer(min_days=args.min_days, min_cv=args.min_cv)
    df = analyzer.run()

    if not df.empty:
        print(f"\nAnalysed {len(df):,} products.\n")
        print("Elasticity type distribution:")
        print(df["elasticity_type"].value_counts().to_string())
        print("\nTop 10 most elastic products (demand most sensitive to price):")
        cols = ["product_id","product_name","price_elasticity","elasticity_type","r_squared","is_significant"]
        print(df.nsmallest(10, "price_elasticity")[cols].to_string(index=False))
        print("\nTop 10 most inelastic products (demand least sensitive to price):")
        print(df[df["price_elasticity"] < 0].nlargest(10, "price_elasticity")[cols].to_string(index=False))
