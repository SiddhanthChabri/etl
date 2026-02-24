"""
store_performance.py -- Store Performance Analysis

Analyses all 33+ stores across the 6-year trading period:
  1. Per-store KPIs: revenue, transactions, unique customers, AOV, units sold
  2. Tier classification (A/B/C) by cumulative revenue contribution
  3. YoY revenue growth per store (year-over-year)
  4. Monthly revenue series per store (for sparklines / trend lines)
  5. Market share of each store (% of total revenue)
  6. Top-10 products per store by revenue
  7. Best month per store

Outputs:
  store_performance_results.csv             -- one row per store, all KPIs
  models/store_performance_metadata.json   -- monthly trends, top products,
                                              YoY data, global KPIs

Usage:
  python store_performance.py
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

logger = setup_logger("store_performance", "store_performance_logs.txt")

RESULTS_CSV   = "store_performance_results.csv"
METADATA_JSON = "models/store_performance_metadata.json"


# ── helpers ───────────────────────────────────────────────────────────────────

def _j(v):
    """JSON-safe conversion of numpy types."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return round(float(v), 4)
    return v

def _r2(v):
    return None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), 2)

def _safe_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


# ── data loading ──────────────────────────────────────────────────────────────

def _load_base() -> pd.DataFrame:
    """
    Load all sales joined to dim_store and dim_time.
    Returns one row per sales transaction with store info and date parts.
    """
    sql = text("""
        SELECT
            fs.sales_key,
            fs.sales_amount,
            fs.quantity_sold,
            fs.customer_key,
            fs.product_key,
            ds.store_key,
            ds.store_id,
            ds.store_name,
            ds.region      AS country,
            dt.date        AS sale_date,
            dt.year,
            dt.month,
            dt.quarter,
            dp.product_name
        FROM fact_sales fs
        JOIN dim_store   ds ON fs.store_key   = ds.store_key
        JOIN dim_time    dt ON fs.time_key    = dt.time_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    logger.info(f"Loaded {len(df):,} sales rows across {df['store_key'].nunique()} stores.")
    return df


# ── per-store KPIs ────────────────────────────────────────────────────────────

def _store_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate to one row per store with all primary KPIs.
    """
    grp = df.groupby(["store_key", "store_id", "store_name", "country"])

    kpi = grp.agg(
        total_revenue    =("sales_amount",  "sum"),
        total_units      =("quantity_sold", "sum"),
        total_txns       =("sales_key",     "count"),
        unique_customers =("customer_key",  "nunique"),
        unique_products  =("product_key",   "nunique"),
        first_sale       =("sale_date",     "min"),
        last_sale        =("sale_date",     "max"),
    ).reset_index()

    kpi["avg_order_value"]    = kpi["total_revenue"] / kpi["total_txns"].replace(0, np.nan)
    kpi["revenue_per_unit"]   = kpi["total_revenue"] / kpi["total_units"].replace(0, np.nan)
    kpi["active_days"]        = (kpi["last_sale"] - kpi["first_sale"]).dt.days + 1
    kpi["revenue_per_day"]    = kpi["total_revenue"] / kpi["active_days"].replace(0, np.nan)

    # Market share
    total_rev = kpi["total_revenue"].sum()
    kpi["market_share_pct"] = kpi["total_revenue"] / total_rev * 100

    # Cumulative share → ABC tier
    # A store belongs to the tier it contributes to (before its own addition crosses the threshold)
    kpi = kpi.sort_values("total_revenue", ascending=False).reset_index(drop=True)
    kpi["cumulative_share"] = kpi["market_share_pct"].cumsum()
    cum_before = kpi["market_share_pct"].cumsum().shift(1).fillna(0)
    kpi["tier"] = cum_before.apply(
        lambda x: "A" if x < 80 else ("B" if x < 95 else "C")
    )
    kpi["rank"] = range(1, len(kpi) + 1)

    logger.info(f"  Tier A: {(kpi.tier=='A').sum()} stores  "
                f"B: {(kpi.tier=='B').sum()}  C: {(kpi.tier=='C').sum()}")
    return kpi


# ── YoY growth ────────────────────────────────────────────────────────────────

def _yoy_growth(df: pd.DataFrame) -> dict:
    """
    Year-over-year revenue per store. Returns:
    {store_key: {year: revenue, ...yoy_pct: ..., ...}, ...}
    Also returns a list structure suitable for charting.
    """
    yearly = df.groupby(["store_key", "store_name", "year"])["sales_amount"].sum().reset_index()
    yearly.columns = ["store_key", "store_name", "year", "revenue"]
    yearly = yearly.sort_values(["store_key", "year"])

    # YoY pct change per store
    yearly["yoy_pct"] = yearly.groupby("store_key")["revenue"].pct_change() * 100

    pivot = yearly.pivot_table(index=["store_key", "store_name"],
                                columns="year",
                                values="revenue",
                                aggfunc="sum").reset_index()
    pivot.columns = [str(c) for c in pivot.columns]

    years = sorted(df["year"].unique().tolist())
    return {
        "years":   years,
        "records": _safe_records(yearly.fillna(0)),
        "pivot":   _safe_records(pivot.fillna(0)),
    }


# ── monthly trends ────────────────────────────────────────────────────────────

def _monthly_trends(df: pd.DataFrame) -> list:
    """
    Monthly revenue per store. Returns list of {store_name, year, month, revenue}.
    Capped at top-10 stores by revenue to keep JSON manageable.
    """
    top10 = df.groupby("store_key")["sales_amount"].sum().nlargest(10).index.tolist()
    sub   = df[df["store_key"].isin(top10)].copy()

    monthly = sub.groupby(["store_key", "store_name", "year", "month"])["sales_amount"]\
                 .sum().reset_index()
    monthly.columns = ["store_key", "store_name", "year", "month", "revenue"]
    monthly["period"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
    monthly = monthly.sort_values(["store_key", "year", "month"])
    return _safe_records(monthly)


# ── top products per store ────────────────────────────────────────────────────

def _top_products_per_store(df: pd.DataFrame, n: int = 10) -> dict:
    """
    Returns top-n products by revenue for each store.
    {store_key: [{product_name, revenue, units, txns}, ...]}
    """
    grp = df.groupby(["store_key", "store_name", "product_name"]).agg(
        revenue=("sales_amount",  "sum"),
        units  =("quantity_sold", "sum"),
        txns   =("sales_key",     "count"),
    ).reset_index()

    result = {}
    for (sk, sn), sub in grp.groupby(["store_key", "store_name"]):
        top = sub.nlargest(n, "revenue")[["product_name", "revenue", "units", "txns"]]
        result[int(sk)] = {
            "store_name": sn,
            "products":   _safe_records(top),
        }
    return result


# ── best month per store ──────────────────────────────────────────────────────

def _best_month_per_store(df: pd.DataFrame) -> dict:
    """
    Returns {store_key: {year, month, revenue}} for the peak trading month.
    """
    monthly = df.groupby(["store_key", "year", "month"])["sales_amount"].sum().reset_index()
    idx = monthly.groupby("store_key")["sales_amount"].idxmax()
    best = monthly.loc[idx].set_index("store_key")
    return {
        int(k): {
            "year":    int(row["year"]),
            "month":   int(row["month"]),
            "revenue": _r2(row["sales_amount"]),
        }
        for k, row in best.iterrows()
    }


# ── quarterly breakdown ───────────────────────────────────────────────────────

def _quarterly(df: pd.DataFrame) -> list:
    """Average revenue per quarter across all years."""
    q = df.groupby("quarter")["sales_amount"].agg(
        total_revenue="sum",
        avg_revenue="mean",
        txns="count"
    ).reset_index()
    q.columns = ["quarter", "total_revenue", "avg_revenue", "txns"]
    return _safe_records(q)


# ── main ──────────────────────────────────────────────────────────────────────

class StorePerformanceAnalyzer:

    def run(self):
        logger.info("=== Store Performance Analysis START ===")

        # 1. Load raw data
        logger.info("Loading sales data from DWH...")
        df = _load_base()

        # 2. Per-store KPIs
        logger.info("Computing per-store KPIs...")
        kpi_df = _store_kpis(df)

        # 3. YoY growth
        logger.info("Computing year-over-year growth...")
        yoy = _yoy_growth(df)

        # 4. Monthly trends (top-10 stores)
        logger.info("Building monthly trend series...")
        monthly = _monthly_trends(df)

        # 5. Top products per store
        logger.info("Finding top products per store...")
        top_products = _top_products_per_store(df)

        # 6. Best month per store
        logger.info("Computing best month per store...")
        best_months = _best_month_per_store(df)

        # 7. Quarterly pattern
        logger.info("Computing quarterly pattern...")
        quarterly = _quarterly(df)

        # ── Global KPIs ───────────────────────────────────────────────────────
        total_revenue    = float(df["sales_amount"].sum())
        total_txns       = int(len(df))
        total_customers  = int(df["customer_key"].nunique())
        total_stores     = int(kpi_df["store_key"].nunique())
        top_store_row    = kpi_df.iloc[0]
        overall_aov      = total_revenue / total_txns if total_txns else 0

        # Attach best month to kpi_df
        kpi_df["best_month_year"]    = kpi_df["store_key"].map(
            lambda k: best_months.get(int(k), {}).get("year"))
        kpi_df["best_month_month"]   = kpi_df["store_key"].map(
            lambda k: best_months.get(int(k), {}).get("month"))
        kpi_df["best_month_revenue"] = kpi_df["store_key"].map(
            lambda k: best_months.get(int(k), {}).get("revenue"))

        # Round floats for CSV
        for col in ["total_revenue", "avg_order_value", "revenue_per_unit",
                    "revenue_per_day", "market_share_pct", "cumulative_share"]:
            kpi_df[col] = kpi_df[col].round(2)

        # Save CSV
        save_cols = [
            "store_key", "store_id", "store_name", "country", "tier", "rank",
            "total_revenue", "total_txns", "total_units", "unique_customers", "unique_products",
            "avg_order_value", "revenue_per_unit", "revenue_per_day",
            "market_share_pct", "cumulative_share",
            "first_sale", "last_sale", "active_days",
            "best_month_year", "best_month_month", "best_month_revenue",
        ]
        kpi_df[save_cols].to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved results -> {RESULTS_CSV}  ({len(kpi_df)} stores)")

        # Store profiles as records (JSON-safe)
        store_records = []
        for _, row in kpi_df.iterrows():
            rec = {}
            for col in save_cols:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                if isinstance(v, float) and np.isnan(v):
                    v = None
                if isinstance(v, pd.Timestamp):
                    v = str(v.date())
                rec[col] = round(float(v), 2) if isinstance(v, float) else v
            store_records.append(rec)

        # Donut data: top-5 + Others
        top5     = kpi_df.head(5)
        others_rev = kpi_df.iloc[5:]["total_revenue"].sum()
        donut_labels = top5["store_name"].tolist() + (["Others"] if len(kpi_df) > 5 else [])
        donut_values = top5["total_revenue"].round(2).tolist() + ([round(others_rev, 2)] if len(kpi_df) > 5 else [])

        # Build metadata
        metadata = {
            "generated_at"    : datetime.now().isoformat(),
            "total_stores"    : total_stores,
            "total_revenue"   : _r2(total_revenue),
            "total_txns"      : total_txns,
            "total_customers" : total_customers,
            "overall_aov"     : _r2(overall_aov),
            "top_store_name"  : top_store_row["store_name"],
            "top_store_share" : _r2(float(top_store_row["market_share_pct"])),
            "tier_counts"     : {
                "A": int((kpi_df.tier=="A").sum()),
                "B": int((kpi_df.tier=="B").sum()),
                "C": int((kpi_df.tier=="C").sum()),
            },
            "stores"          : store_records,
            "yoy"             : yoy,
            "monthly_trends"  : monthly,
            "top_products"    : top_products,
            "quarterly"       : quarterly,
            "donut"           : {"labels": donut_labels, "values": donut_values},
        }

        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")

        # Summary log
        logger.info(f"Total stores: {total_stores}  Total revenue: ${total_revenue:,.0f}")
        for _, row in kpi_df.head(10).iterrows():
            logger.info(
                f"  [{row['tier']}] #{int(row['rank']):2d} {row['store_name']:30s}  "
                f"Rev=${float(row['total_revenue']):>12,.0f}  "
                f"Share={float(row['market_share_pct']):.1f}%  "
                f"AOV=${float(row['avg_order_value']):.2f}"
            )
        logger.info("=== Store Performance Analysis DONE ===")
        return metadata


if __name__ == "__main__":
    try:
        analyzer = StorePerformanceAnalyzer()
        meta = analyzer.run()
        print(f"\nDone. {meta['total_stores']} stores  "
              f"Total revenue: ${meta['total_revenue']:,.0f}  "
              f"Top store: {meta['top_store_name']} ({meta['top_store_share']:.1f}%)")
        print("\nStore ranking (top 10):")
        for s in meta["stores"][:10]:
            print(f"  [{s['tier']}] #{s['rank']:2d} {s['store_name']:30s}  "
                  f"${s['total_revenue']:>12,.0f}  {s['market_share_pct']:.1f}%  "
                  f"AOV=${s['avg_order_value']:.2f}")
    except Exception as e:
        logger.error(f"Store performance analysis failed: {e}", exc_info=True)
        raise
