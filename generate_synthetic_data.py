"""
generate_synthetic_data.py — Synthetic Transaction Generator

Extends the DWH from 2012-01-01 to 2015-12-31 using patterns
learned from the existing 2010-2011 Online Retail data.

Pattern-learned (not random):
  - Product popularity  : frequency-weighted sampling (Pareto-like)
  - Customer loyalty    : frequency-weighted sampling (repeat buyers)
  - Store distribution  : weighted by historical traffic
  - Seasonality         : monthly multipliers from historical data
  - Basket size         : log-normal matching empirical distribution
  - Quantity per item   : log-normal matching P25=2, P50=6, P75=12
  - Discounts           : none (matching original dataset)
  - Price               : sampled from each product's historical avg price

Usage:
  python generate_synthetic_data.py
  python generate_synthetic_data.py --start 2012-01-01 --end 2015-12-31 --invoices-per-day 25
"""

import argparse
import numpy as np
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("synthetic_data", "logs/synthetic_data_logs.txt")

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_START         = date(2012, 1, 1)
DEFAULT_END           = date(2015, 12, 31)
BASE_INVOICES_PER_DAY = 25      # base number of invoices per trading day
BATCH_SIZE            = 5_000   # rows per DB insert batch
SEED                  = 42

# Monthly multipliers learned from 2010-2011 data (normalised).
# December is boosted to account for truncated Dec 2011 data.
MONTHLY_MULT = {
    1: 0.97,  2: 0.85,  3: 0.78,
    4: 1.06,  5: 1.16,  6: 1.31,
    7: 1.10,  8: 1.13,  9: 0.93,
    10: 1.01, 11: 1.20, 12: 1.35,
}


# ── Load reference data ────────────────────────────────────────────────────────

def load_reference_data():
    """Load product/customer/store lookup tables with frequency weights."""
    logger.info("Loading reference data from DWH ...")
    with engine.connect() as conn:

        # Products: key, avg price, frequency weight
        prod_df = pd.read_sql(text("""
            SELECT
                p.product_key,
                AVG(fs.sales_amount / NULLIF(fs.quantity_sold, 0)) AS avg_price,
                COUNT(*) AS freq
            FROM fact_sales fs
            JOIN dim_product p ON fs.product_key = p.product_key
            WHERE fs.quantity_sold > 0
            GROUP BY p.product_key
        """), conn)

        # Customers: key, frequency weight
        cust_df = pd.read_sql(text("""
            SELECT customer_key, COUNT(*) AS freq
            FROM fact_sales
            GROUP BY customer_key
        """), conn)

        # Stores: key, frequency weight
        store_df = pd.read_sql(text("""
            SELECT store_key, COUNT(*) AS freq
            FROM fact_sales
            GROUP BY store_key
        """), conn)

        # Existing dim_time dates (to avoid duplicates)
        existing_dates = set(
            r[0] for r in conn.execute(text("SELECT date FROM dim_time")).fetchall()
        )

        # Next available keys
        max_time_key  = conn.execute(text("SELECT COALESCE(MAX(time_key), 0)  FROM dim_time")).scalar()
        max_sales_key = conn.execute(text("SELECT COALESCE(MAX(sales_key), 0) FROM fact_sales")).scalar()

    # Normalise weights
    prod_df["weight"]  = prod_df["freq"]  / prod_df["freq"].sum()
    cust_df["weight"]  = cust_df["freq"]  / cust_df["freq"].sum()
    store_df["weight"] = store_df["freq"] / store_df["freq"].sum()

    logger.info(f"  Products : {len(prod_df):,}")
    logger.info(f"  Customers: {len(cust_df):,}")
    logger.info(f"  Stores   : {len(store_df):,}")
    logger.info(f"  Existing dim_time dates : {len(existing_dates):,}")

    return prod_df, cust_df, store_df, existing_dates, max_time_key, max_sales_key


# ── Trading day generator ──────────────────────────────────────────────────────

def trading_days(start: date, end: date, existing_dates: set):
    """Yield Mon-Sat dates in [start, end] that aren't already in dim_time."""
    d = start
    while d <= end:
        if d.weekday() < 6 and d not in existing_dates:  # 0=Mon … 5=Sat
            yield d
        d += timedelta(days=1)


# ── Quantity sampler (log-normal, clipped to 1-100) ───────────────────────────

def sample_qty(rng: np.random.Generator, n: int) -> np.ndarray:
    """
    Log-normal matching empirical distribution:
      median ≈ 6, mean ≈ 10, P75 ≈ 12
    mu=1.8, sigma=0.9 gives: median=exp(1.8)≈6.05, mean≈exp(2.205)≈9.07
    """
    raw = rng.lognormal(mean=1.8, sigma=0.9, size=n)
    return np.clip(np.round(raw).astype(int), 1, 100)


# ── Basket size sampler ───────────────────────────────────────────────────────

def sample_basket_size(rng: np.random.Generator) -> int:
    """Empirical: median basket = 16 items, avg = 22.7. Log-normal."""
    return max(1, int(np.round(rng.lognormal(mean=2.8, sigma=0.8))))


# ── Main generator ────────────────────────────────────────────────────────────

def generate(start: date, end: date, invoices_per_day: int):
    rng = np.random.default_rng(SEED)

    prod_df, cust_df, store_df, existing_dates, max_tk, max_sk = load_reference_data()

    prod_keys   = prod_df["product_key"].values
    prod_prices = prod_df["avg_price"].values.astype(float)
    prod_w      = prod_df["weight"].values

    cust_keys   = cust_df["customer_key"].values
    cust_w      = cust_df["weight"].values

    store_keys  = store_df["store_key"].values
    store_w     = store_df["weight"].values

    days = list(trading_days(start, end, existing_dates))
    logger.info(f"Trading days to generate: {len(days):,}  ({start} → {end})")

    if not days:
        logger.warning("No new trading days found — all dates already in dim_time.")
        return

    time_rows  = []   # for dim_time
    fact_rows  = []   # for fact_sales
    time_key   = max_tk
    sales_key  = max_sk

    total_invoices = 0

    for d in days:
        mult  = MONTHLY_MULT[d.month]
        n_inv = max(1, int(rng.poisson(lam=invoices_per_day * mult)))

        # --- dim_time row ---
        time_key += 1
        time_rows.append({
            "time_key"   : time_key,
            "date"       : d,
            "day"        : d.day,
            "month"      : d.month,
            "quarter"    : (d.month - 1) // 3 + 1,
            "year"       : d.year,
            "day_of_week": d.strftime("%A"),
        })

        tk = time_key   # capture for this day's fact rows

        # --- invoices for this day ---
        # Pick customers for all invoices at once
        chosen_custs  = rng.choice(cust_keys, size=n_inv, p=cust_w)
        chosen_stores = rng.choice(store_keys, size=n_inv, p=store_w)

        for inv_idx in range(n_inv):
            basket_size = sample_basket_size(rng)
            ck = int(chosen_custs[inv_idx])
            sk = int(chosen_stores[inv_idx])

            # Pick products for this invoice (no replacement within invoice)
            n_pick = min(basket_size, len(prod_keys))
            prod_idx = rng.choice(len(prod_keys), size=n_pick, replace=False, p=prod_w)

            qtys = sample_qty(rng, n_pick)

            for i, pidx in enumerate(prod_idx):
                sales_key += 1
                qty   = int(qtys[i])
                price = float(prod_prices[pidx])
                # Small price noise ±5 %
                price = round(price * rng.uniform(0.95, 1.05), 2)
                amount = round(qty * price, 2)

                fact_rows.append((
                    sales_key,
                    ck,
                    int(prod_keys[pidx]),
                    sk,
                    tk,
                    qty,
                    amount,
                    0.0,        # discount_amount — always 0 in original data
                ))

        total_invoices += n_inv

        # Flush in batches
        if len(fact_rows) >= BATCH_SIZE:
            _flush(time_rows, fact_rows)
            logger.info(f"  Flushed batch | day={d} | invoices so far={total_invoices:,} | fact rows so far={sales_key - max_sk:,}")
            time_rows = []
            fact_rows = []

    # Final flush
    if time_rows or fact_rows:
        _flush(time_rows, fact_rows)

    logger.info(f"Generation complete.")
    logger.info(f"  New dim_time rows : {len(days):,}")
    logger.info(f"  New fact_sales rows: {sales_key - max_sk:,}")
    logger.info(f"  New invoices       : {total_invoices:,}")

    # Update ETL watermark
    _update_watermark(end, sales_key - max_sk)

    print(f"\nDone!")
    print(f"  New trading days : {len(days):,}")
    print(f"  New fact rows    : {sales_key - max_sk:,}")
    print(f"  New invoices     : {total_invoices:,}")


# ── DB flush ──────────────────────────────────────────────────────────────────

def _flush(time_rows: list, fact_rows: list):
    with engine.begin() as conn:
        if time_rows:
            conn.execute(text("""
                INSERT INTO dim_time (time_key, date, day, month, quarter, year, day_of_week)
                VALUES (:time_key, :date, :day, :month, :quarter, :year, :day_of_week)
                ON CONFLICT (time_key) DO NOTHING
            """), time_rows)

        if fact_rows:
            conn.execute(text("""
                INSERT INTO fact_sales
                    (sales_key, customer_key, product_key, store_key, time_key,
                     quantity_sold, sales_amount, discount_amount)
                VALUES
                    (:sk, :ck, :pk, :stk, :tk, :qty, :amt, :disc)
            """), [
                {"sk": r[0], "ck": r[1], "pk": r[2], "stk": r[3], "tk": r[4],
                 "qty": r[5], "amt": r[6], "disc": r[7]}
                for r in fact_rows
            ])


def _update_watermark(end_date, records_added: int):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_watermark
               SET last_loaded_date    = :d,
                   records_processed   = records_processed + :n,
                   updated_at          = NOW()
             WHERE table_name = 'fact_sales'
               AND source_system = 'RETAIL_OLTP'
        """), {"d": end_date, "n": records_added})
    logger.info(f"Watermark updated to {end_date}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic retail data generator")
    parser.add_argument("--start",            default="2012-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",              default="2015-12-31", help="End date   (YYYY-MM-DD)")
    parser.add_argument("--invoices-per-day", type=int, default=BASE_INVOICES_PER_DAY,
                        help=f"Base invoices per trading day (default: {BASE_INVOICES_PER_DAY})")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    logger.info("=" * 60)
    logger.info("SYNTHETIC DATA GENERATOR")
    logger.info(f"  Range             : {start} -> {end}")
    logger.info(f"  Base invoices/day : {args.invoices_per_day}")
    logger.info("=" * 60)

    generate(start, end, args.invoices_per_day)
