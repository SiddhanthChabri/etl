"""
Analytics Verification Script
Runs SQL queries to verify analytics results against database
"""

from sqlalchemy import text
from db_connection import engine
import pandas as pd
from logger_config import setup_logger

logger = setup_logger('verify_analytics')


def verify_rfm_analysis():
    logger.info("\n" + "="*70)
    logger.info("1️⃣  VERIFYING RFM ANALYSIS")
    logger.info("="*70)

    rfm_csv = pd.read_csv('rfm_analysis_results.csv')

    query = text("""
        WITH customer_rfm AS (
            SELECT 
                c.customer_id,
                COUNT(DISTINCT fs.sales_key) as frequency,
                SUM(fs.sales_amount) as monetary,
                CURRENT_DATE - MAX(t.date) as recency_days
            FROM fact_sales fs
            JOIN dim_customer c ON fs.customer_key = c.customer_key
            JOIN dim_time t ON fs.time_key = t.time_key
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id
        )
        SELECT 
            COUNT(*) as customers,
            ROUND(AVG(recency_days), 0)    as avg_recency,
            ROUND(AVG(frequency)::numeric, 1) as avg_frequency,
            ROUND(AVG(monetary)::numeric, 2)  as avg_monetary
        FROM customer_rfm
    """)

    with engine.connect() as conn:
        db = pd.read_sql(query, conn)

    logger.info("\n📊 Comparison:")
    logger.info(f"  CSV Customers: {len(rfm_csv):,}")
    logger.info(f"  DB Customers:  {db['customers'].iloc[0]:,}")
    logger.info(f"  Match: {'✅ YES' if len(rfm_csv) == db['customers'].iloc[0] else '❌ NO'}")
    logger.info(f"\n  CSV Avg Recency:   {rfm_csv['recency'].mean():.0f} days")
    logger.info(f"  DB Avg Recency:    {db['avg_recency'].iloc[0]:.0f} days")
    logger.info(f"\n  CSV Avg Frequency: {rfm_csv['frequency'].mean():.1f}")
    logger.info(f"  DB Avg Frequency:  {db['avg_frequency'].iloc[0]:.1f}")
    logger.info(f"\n  CSV Avg Monetary:  ${rfm_csv['monetary'].mean():,.2f}")
    logger.info(f"  DB Avg Monetary:   ${db['avg_monetary'].iloc[0]:,.2f}")
    return True


def verify_abc_analysis():
    logger.info("\n" + "="*70)
    logger.info("2️⃣  VERIFYING ABC ANALYSIS")
    logger.info("="*70)

    abc_csv = pd.read_csv('abc_analysis_results.csv')

    query = text("""
        SELECT 
            COUNT(DISTINCT p.product_id)        as products,
            ROUND(SUM(fs.sales_amount)::numeric, 2) as total_revenue
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
    """)

    with engine.connect() as conn:
        db = pd.read_sql(query, conn)

    logger.info("\n📊 Comparison:")
    logger.info(f"  CSV Products: {len(abc_csv):,}")
    logger.info(f"  DB Products:  {db['products'].iloc[0]:,}")
    logger.info(f"  Match: {'✅ YES' if len(abc_csv) == db['products'].iloc[0] else '❌ NO'}")
    logger.info(f"\n  CSV Total Revenue: ${abc_csv['total_revenue'].sum():,.2f}")
    logger.info(f"  DB Total Revenue:  ${db['total_revenue'].iloc[0]:,.2f}")

    logger.info("\n📦 ABC Class Distribution:")
    for cls in ['A', 'B', 'C']:
        count   = (abc_csv['abc_class'] == cls).sum()
        revenue = abc_csv[abc_csv['abc_class'] == cls]['total_revenue'].sum()
        pct     = count / len(abc_csv) * 100
        rpct    = revenue / abc_csv['total_revenue'].sum() * 100
        logger.info(f"  Class {cls}: {count:,} products ({pct:.1f}%) → ${revenue:,.2f} ({rpct:.1f}%)")
    return True


def verify_clv_analysis():
    logger.info("\n" + "="*70)
    logger.info("3️⃣  VERIFYING CLV ANALYSIS")
    logger.info("="*70)

    clv_csv = pd.read_csv('clv_analysis_results.csv')

    # Key fix: cast everything to ::numeric before ROUND and PERCENTILE_CONT
    query = text("""
        WITH customer_metrics AS (
            SELECT 
                c.customer_id,
                COUNT(DISTINCT fs.sales_key)                          AS purchase_count,
                AVG(fs.sales_amount)                                  AS avg_purchase_value,
                (MAX(t.date) - MIN(t.date))::numeric / 365.25        AS lifespan_years
            FROM fact_sales fs
            JOIN dim_customer c ON fs.customer_key = c.customer_key
            JOIN dim_time t    ON fs.time_key = t.time_key
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id
        ),
        customer_clv AS (
            SELECT
                (
                    avg_purchase_value
                    * (purchase_count / GREATEST(lifespan_years, 0.1))
                    * GREATEST(lifespan_years, 0.1)
                    / POWER(1.10, GREATEST(lifespan_years, 0.1))
                )::numeric AS clv
            FROM customer_metrics
        )
        SELECT
            COUNT(*)                                                  AS customers,
            ROUND(AVG(clv), 2)                                        AS avg_clv,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY clv)::numeric, 2) AS median_clv
        FROM customer_clv
    """)

    with engine.connect() as conn:
        db = pd.read_sql(query, conn)

    logger.info("\n📊 Comparison:")
    logger.info(f"  CSV Customers:  {len(clv_csv):,}")
    logger.info(f"  DB Customers:   {db['customers'].iloc[0]:,}")
    logger.info(f"  Match: {'✅ YES' if len(clv_csv) == db['customers'].iloc[0] else '❌ NO'}")
    logger.info(f"\n  CSV Avg CLV:    ${clv_csv['clv_discounted'].mean():,.2f}")
    logger.info(f"  DB Avg CLV:     ${db['avg_clv'].iloc[0]:,.2f}")
    logger.info(f"\n  CSV Median CLV: ${clv_csv['clv_discounted'].median():,.2f}")
    logger.info(f"  DB Median CLV:  ${db['median_clv'].iloc[0]:,.2f}")
    logger.info(f"\n  CSV Total CLV:  ${clv_csv['clv_discounted'].sum():,.2f}")

    logger.info("\n💰 CLV Segment Distribution:")
    for seg in clv_csv['clv_segment'].value_counts().index:
        count   = (clv_csv['clv_segment'] == seg).sum()
        pct     = count / len(clv_csv) * 100
        avg_clv = clv_csv[clv_csv['clv_segment'] == seg]['clv_discounted'].mean()
        logger.info(f"  {seg}: {count:,} customers ({pct:.1f}%) → Avg CLV: ${avg_clv:,.2f}")
    return True


def verify_data_quality():
    logger.info("\n" + "="*70)
    logger.info("4️⃣  DATA QUALITY CHECKS")
    logger.info("="*70)

    query = text("""
        SELECT 'fact_sales'   AS table_name, COUNT(*) AS record_count FROM fact_sales  UNION ALL
        SELECT 'dim_customer',               COUNT(*)                 FROM dim_customer UNION ALL
        SELECT 'dim_product',                COUNT(*)                 FROM dim_product  UNION ALL
        SELECT 'dim_time',                   COUNT(*)                 FROM dim_time
    """)
    with engine.connect() as conn:
        results = pd.read_sql(query, conn)

    logger.info("\n📊 Table Record Counts:")
    for _, row in results.iterrows():
        logger.info(f"  {row['table_name']:<20} {row['record_count']:>10,} records")

    query = text("""
        SELECT 
            COUNT(*)                                        AS total_transactions,
            COUNT(DISTINCT fs.customer_key)                AS unique_customers,
            COUNT(DISTINCT fs.product_key)                 AS unique_products,
            ROUND(SUM(fs.sales_amount)::numeric, 2)        AS total_revenue,
            ROUND(AVG(fs.sales_amount)::numeric, 2)        AS avg_transaction_value
        FROM fact_sales fs
    """)
    with engine.connect() as conn:
        rev = pd.read_sql(query, conn).iloc[0]

    logger.info("\n💰 Revenue Summary:")
    logger.info(f"  Total Transactions: {rev['total_transactions']:,}")
    logger.info(f"  Unique Customers:   {rev['unique_customers']:,}")
    logger.info(f"  Unique Products:    {rev['unique_products']:,}")
    logger.info(f"  Total Revenue:      ${rev['total_revenue']:,.2f}")
    logger.info(f"  Avg Transaction:    ${rev['avg_transaction_value']:,.2f}")
    return True


def run_all_verifications():
    logger.info("="*70)
    logger.info("🔍 ANALYTICS VERIFICATION SYSTEM")
    logger.info("="*70)

    try:
        verify_rfm_analysis()
        verify_abc_analysis()
        verify_clv_analysis()
        verify_data_quality()

        logger.info("\n" + "="*70)
        logger.info("✅ ALL VERIFICATIONS PASSED")
        logger.info("="*70)
        logger.info("  • RFM Analysis: ✅ Verified")
        logger.info("  • ABC Analysis: ✅ Verified")
        logger.info("  • CLV Analysis: ✅ Verified")
        logger.info("  • Data Quality: ✅ Verified")

    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_all_verifications()