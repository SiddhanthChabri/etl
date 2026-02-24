"""
geographic_analysis.py — Geographic Sales Analysis

Computes per-region sales metrics across all years in the DWH.

Metrics per region:
  total_revenue       — lifetime sales amount
  total_transactions  — number of fact_sales rows
  unique_customers    — distinct customer_key count
  avg_order_value     — revenue / transactions
  revenue_per_customer— revenue / customers
  revenue_share_pct   — % of global total revenue
  yoy_growth_pct      — latest year-over-year revenue growth
  country_code        — ISO-3 code for Plotly choropleth

Outputs:
  geographic_results.csv
  models/geographic_metadata.json

Usage:
  python geographic_analysis.py
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

logger = setup_logger("geographic_analysis", "geographic_analysis_logs.txt")

RESULTS_CSV   = "geographic_results.csv"
METADATA_JSON = "models/geographic_metadata.json"

# ISO-3 country code mapping for Plotly choropleth
ISO3 = {
    "United Kingdom"      : "GBR",
    "France"              : "FRA",
    "Germany"             : "DEU",
    "EIRE"                : "IRL",
    "Netherlands"         : "NLD",
    "Belgium"             : "BEL",
    "Spain"               : "ESP",
    "Switzerland"         : "CHE",
    "Australia"           : "AUS",
    "Portugal"            : "PRT",
    "Norway"              : "NOR",
    "Finland"             : "FIN",
    "Cyprus"              : "CYP",
    "Channel Islands"     : "GBR",
    "Japan"               : "JPN",
    "Poland"              : "POL",
    "Sweden"              : "SWE",
    "Italy"               : "ITA",
    "Denmark"             : "DNK",
    "Austria"             : "AUT",
    "USA"                 : "USA",
    "Iceland"             : "ISL",
    "Greece"              : "GRC",
    "United Arab Emirates": "ARE",
    "Singapore"           : "SGP",
    "Canada"              : "CAN",
    "Malta"               : "MLT",
    "Lithuania"           : "LTU",
    "Israel"              : "ISR",
    "Czech Republic"      : "CZE",
    "Bahrain"             : "BHR",
    "Unspecified"         : None,
    "European Community"  : None,
}


class GeographicAnalyzer:

    def __init__(self):
        self.engine = engine

    # ── 1. Region summary ─────────────────────────────────────────────────────

    def fetch_region_summary(self) -> pd.DataFrame:
        logger.info("Fetching region-level sales summary ...")
        query = text("""
            SELECT
                s.region,
                COUNT(*)                                    AS total_transactions,
                COUNT(DISTINCT fs.customer_key)             AS unique_customers,
                SUM(fs.sales_amount)                        AS total_revenue,
                AVG(fs.sales_amount)                        AS avg_order_value,
                SUM(fs.quantity_sold)                       AS total_units
            FROM fact_sales fs
            JOIN dim_store s ON fs.store_key = s.store_key
            GROUP BY s.region
            ORDER BY total_revenue DESC
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        total_rev = df["total_revenue"].sum()
        df["revenue_share_pct"]    = (df["total_revenue"] / total_rev * 100).round(2)
        df["revenue_per_customer"] = (df["total_revenue"] / df["unique_customers"]).round(2)
        df["avg_order_value"]      = df["avg_order_value"].round(2)
        df["total_revenue"]        = df["total_revenue"].round(2)
        df["country_code"]         = df["region"].map(ISO3)

        logger.info(f"  {len(df)} regions loaded")
        return df

    # ── 2. Year-over-year revenue per region ──────────────────────────────────

    def fetch_yoy(self) -> pd.DataFrame:
        logger.info("Fetching year-over-year revenue by region ...")
        query = text("""
            SELECT
                s.region,
                t.year,
                SUM(fs.sales_amount) AS revenue
            FROM fact_sales fs
            JOIN dim_store s ON fs.store_key = s.store_key
            JOIN dim_time  t ON fs.time_key  = t.time_key
            GROUP BY s.region, t.year
            ORDER BY s.region, t.year
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        df["revenue"] = df["revenue"].round(2)
        return df

    # ── 3. Monthly trend (top 6 regions) ─────────────────────────────────────

    def fetch_monthly_trend(self, top_n: int = 6) -> pd.DataFrame:
        logger.info(f"Fetching monthly trend for top {top_n} regions ...")
        query = text("""
            SELECT
                s.region,
                t.year,
                t.month,
                SUM(fs.sales_amount) AS revenue
            FROM fact_sales fs
            JOIN dim_store s ON fs.store_key = s.store_key
            JOIN dim_time  t ON fs.time_key  = t.time_key
            GROUP BY s.region, t.year, t.month
            ORDER BY s.region, t.year, t.month
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        # Keep only top N regions by total revenue
        top_regions = (
            df.groupby("region")["revenue"].sum()
            .nlargest(top_n).index.tolist()
        )
        df = df[df["region"].isin(top_regions)].copy()
        df["revenue"] = df["revenue"].round(2)
        df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
        return df

    # ── 4. Top products per region ────────────────────────────────────────────

    def fetch_top_products(self, top_n: int = 5) -> pd.DataFrame:
        logger.info("Fetching top products per region ...")
        query = text("""
            SELECT
                s.region,
                p.product_id,
                p.product_name,
                SUM(fs.sales_amount)  AS revenue,
                SUM(fs.quantity_sold) AS units
            FROM fact_sales fs
            JOIN dim_store   s ON fs.store_key   = s.store_key
            JOIN dim_product p ON fs.product_key = p.product_key
            GROUP BY s.region, p.product_id, p.product_name
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["revenue"] = df["revenue"].round(2)
        # Rank within each region
        df["rank"] = df.groupby("region")["revenue"].rank(
            method="dense", ascending=False
        ).astype(int)
        df = df[df["rank"] <= top_n].sort_values(["region", "rank"])
        return df

    # ── 5. Compute YoY growth for summary ─────────────────────────────────────

    def compute_yoy_growth(self, summary: pd.DataFrame, yoy: pd.DataFrame) -> pd.DataFrame:
        years = sorted(yoy["year"].unique())
        if len(years) < 2:
            summary["yoy_growth_pct"] = None
            return summary

        latest, prev = years[-1], years[-2]
        rev_latest = yoy[yoy["year"] == latest].set_index("region")["revenue"]
        rev_prev   = yoy[yoy["year"] == prev  ].set_index("region")["revenue"]

        growth = ((rev_latest - rev_prev) / rev_prev.replace(0, np.nan) * 100).round(1)
        summary["yoy_growth_pct"] = summary["region"].map(growth)
        return summary

    # ── 6. Run ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        logger.info("=" * 60)
        logger.info("GEOGRAPHIC SALES ANALYSIS")
        logger.info("=" * 60)

        summary  = self.fetch_region_summary()
        yoy      = self.fetch_yoy()
        monthly  = self.fetch_monthly_trend()
        products = self.fetch_top_products()
        summary  = self.compute_yoy_growth(summary, yoy)

        # Save main CSV
        summary.to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved results  -> {RESULTS_CSV}  ({len(summary)} regions)")

        # YoY pivot for metadata
        yoy_pivot = (
            yoy.pivot(index="region", columns="year", values="revenue")
            .fillna(0).round(2)
        )
        yoy_records = yoy_pivot.reset_index().to_dict("records")

        # Top products per region dict
        top_prod_dict = {}
        for region, grp in products.groupby("region"):
            top_prod_dict[region] = json.loads(
                grp[["product_id", "product_name", "revenue", "units", "rank"]]
                .to_json(orient="records")
            )

        # Global stats
        years = sorted(yoy["year"].unique())

        metadata = {
            "generated_at"         : datetime.now().isoformat(),
            "total_regions"        : int(len(summary)),
            "total_revenue"        : round(float(summary["total_revenue"].sum()), 2),
            "total_transactions"   : int(summary["total_transactions"].sum()),
            "total_customers"      : int(summary["unique_customers"].sum()),
            "years_covered"        : [int(y) for y in years],
            "top_region"           : str(summary.iloc[0]["region"]),
            "top_region_revenue"   : float(summary.iloc[0]["total_revenue"]),
            "top_region_share_pct" : float(summary.iloc[0]["revenue_share_pct"]),
            "regions"              : json.loads(
                summary.to_json(orient="records")
            ),
            "yoy_by_region"        : yoy_records,
            "monthly_trend"        : json.loads(
                monthly.to_json(orient="records")
            ),
            "top_products_by_region": top_prod_dict,
        }

        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")
        logger.info("Geographic analysis complete.")
        return metadata


if __name__ == "__main__":
    ga   = GeographicAnalyzer()
    meta = ga.run()

    print(f"\nRegions analysed   : {meta['total_regions']}")
    print(f"Total revenue      : ${meta['total_revenue']:,.2f}")
    print(f"Total transactions : {meta['total_transactions']:,}")
    print(f"Years covered      : {meta['years_covered']}")
    print(f"\nTop region         : {meta['top_region']}")
    print(f"  Revenue          : ${meta['top_region_revenue']:,.2f}")
    print(f"  Share            : {meta['top_region_share_pct']:.1f}%")
    print(f"\nTop 5 regions by revenue:")
    for r in meta["regions"][:5]:
        print(f"  {r['region']:25} ${r['total_revenue']:>12,.2f}  ({r['revenue_share_pct']:.1f}%)")
