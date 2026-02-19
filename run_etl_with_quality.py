"""
ETL Orchestrator with Integrated Data Quality Checks
Runs incremental ETL with comprehensive quality validation
"""

import pandas as pd
from datetime import datetime
import time
from logger_config import setup_logger
from incremental_load import IncrementalLoader
from advanced_data_quality import AdvancedDataQuality
from load_customer_scd import load_customer_scd
from load_dimensions import load_store_dimension, load_time_dimension
import kagglehub
import os
import glob

logger = setup_logger('etl_with_quality')


def load_online_retail_data():
    """Load Online Retail dataset via kagglehub"""
    logger.info("📥 Accessing Online Retail dataset...")

    path = kagglehub.dataset_download("tunguz/online-retail")
    data_files = glob.glob(os.path.join(path, "*.xlsx")) + glob.glob(os.path.join(path, "*.csv"))

    if not data_files:
        raise FileNotFoundError("No data file found")

    data_file = data_files[0]

    if data_file.endswith('.xlsx') or data_file.endswith('.xls'):
        df = pd.read_excel(data_file)
    else:
        df = pd.read_csv(data_file, encoding='latin-1')

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    logger.info(f"✅ Loaded {len(df):,} records")
    return df


def run_etl_with_quality_checks():
    """
    Complete ETL Pipeline with Quality Checks

    Flow:
    1. Pre-ETL Quality Checks (source data validation)
    2. Load Dimensions
    3. Load Fact Table (Incremental)
    4. Post-ETL Quality Checks (target data validation)
    5. Generate Quality Report
    """

    start_time = time.time()

    logger.info("=" * 70)
    logger.info("🚀 ETL PIPELINE WITH QUALITY CHECKS")
    logger.info("=" * 70)
    logger.info(f"Start Time: {datetime.now()}")
    logger.info("=" * 70)

    try:
        # Initialize components
        loader = IncrementalLoader()
        dq = AdvancedDataQuality()

        # ================================================================
        # STEP 1: Load Source Data
        # ================================================================
        logger.info("\n" + "=" * 70)
        logger.info("📥 STEP 1: Loading Source Data")
        logger.info("=" * 70)

        retail_df = load_online_retail_data()

        # ================================================================
        # STEP 2: Pre-ETL Quality Checks (Source Validation)
        # ================================================================
        logger.info("\n" + "=" * 70)
        logger.info("✔️  STEP 2: Pre-ETL Quality Checks")
        logger.info("=" * 70)

        # Basic source data validation
        logger.info("Checking source data quality...")

        null_customers = retail_df['CustomerID'].isnull().sum()
        null_pct = (null_customers / len(retail_df)) * 100

        logger.info(f"  • Total records: {len(retail_df):,}")
        logger.info(f"  • Null CustomerID: {null_customers:,} ({null_pct:.1f}%)")
        logger.info(f"  • Date range: {retail_df['InvoiceDate'].min()} to {retail_df['InvoiceDate'].max()}")

        if null_pct > 50:
            logger.error("❌ Too many null CustomerIDs - aborting ETL")
            raise Exception("Source data quality too poor")

        logger.info("✅ Source data quality acceptable")

        # ================================================================
        # STEP 3: Check for New Data
        # ================================================================
        logger.info("\n" + "=" * 70)
        logger.info("🔍 STEP 3: Checking for New Data")
        logger.info("=" * 70)

        has_new, count = loader.check_for_new_data(
            retail_df, 'fact_sales', 'InvoiceDate'
        )

        logger.info(f"New records available: {count:,}")

        if not has_new:
            logger.info("✨ No new data - running quality checks on existing data")
            # Skip to quality checks
        else:
            # ================================================================
            # STEP 4: Load Dimensions
            # ================================================================
            logger.info("\n" + "=" * 70)
            logger.info("📊 STEP 4: Loading Dimensions")
            logger.info("=" * 70)

            logger.info("⏰ Loading dim_time...")
            load_time_dimension()
            logger.info("✅ dim_time loaded")

            logger.info("🏪 Loading dim_store...")
            load_store_dimension()
            logger.info("✅ dim_store loaded")

            logger.info("👤 Loading dim_customer...")
            load_customer_scd()
            logger.info("✅ dim_customer loaded")

            # ================================================================
            # STEP 5: Load Fact Table (Incremental)
            # ================================================================
            logger.info("\n" + "=" * 70)
            logger.info("📈 STEP 5: Loading Fact Table (Incremental)")
            logger.info("=" * 70)

            stats = loader.load_fact_sales_incremental(retail_df, timestamp_column='InvoiceDate')

            logger.info(f"✅ Loaded {stats['inserted']:,} records")
            logger.info(f"⚠️  Rejected {stats['rejected']:,} records")

        # ================================================================
        # STEP 6: Post-ETL Quality Checks (Comprehensive)
        # ================================================================
        logger.info("\n" + "=" * 70)
        logger.info("✔️  STEP 6: Post-ETL Quality Checks")
        logger.info("=" * 70)

        # Run all quality checks
        logger.info("Running comprehensive data quality validation...")

        # Referential Integrity
        logger.info("\n🔗 Checking referential integrity...")
        dq.check_referential_integrity('fact_sales', 'dim_customer', 'customer_key', 'customer_key')
        dq.check_referential_integrity('fact_sales', 'dim_product', 'product_key', 'product_key')
        dq.check_referential_integrity('fact_sales', 'dim_store', 'store_key', 'store_key')
        dq.check_referential_integrity('fact_sales', 'dim_time', 'time_key', 'time_key')

        # Statistical Anomalies
        logger.info("\n📊 Detecting statistical anomalies...")
        dq.detect_numerical_anomalies('fact_sales', 'quantity', z_threshold=3)
        dq.detect_numerical_anomalies('fact_sales', 'unit_price', z_threshold=3)

        # Duplicate Detection
        logger.info("\n🔍 Checking for duplicates...")
        dq.check_duplicates('fact_sales', ['time_key', 'customer_key', 'product_key'])

        # ================================================================
        # STEP 7: Generate Quality Report
        # ================================================================
        logger.info("\n" + "=" * 70)
        logger.info("📊 STEP 7: Quality Report")
        logger.info("=" * 70)

        total_checks = (dq.quality_results['checks_passed'] + 
                       dq.quality_results['checks_failed'] + 
                       dq.quality_results['checks_warning'])

        logger.info(f"Total Quality Checks: {total_checks}")
        logger.info(f"✅ Passed: {dq.quality_results['checks_passed']}")
        logger.info(f"⚠️  Warnings: {dq.quality_results['checks_warning']}")
        logger.info(f"❌ Failed: {dq.quality_results['checks_failed']}")

        # Save quality report
        dq.save_quality_report()
        logger.info("✅ Quality report saved to database")

        # Determine overall status
        if dq.quality_results['checks_failed'] > 0:
            logger.error("\n⚠️  QUALITY CHECKS FAILED - REVIEW REQUIRED")
            quality_status = "FAILED"
        elif dq.quality_results['checks_warning'] > 0:
            logger.warning("\n⚠️  QUALITY CHECKS PASSED WITH WARNINGS")
            quality_status = "WARNING"
        else:
            logger.info("\n✅ ALL QUALITY CHECKS PASSED")
            quality_status = "PASSED"

        # ================================================================
        # STEP 8: Final Summary
        # ================================================================
        end_time = time.time()
        duration = end_time - start_time

        logger.info("\n" + "=" * 70)
        logger.info("📋 ETL PIPELINE SUMMARY")
        logger.info("=" * 70)

        if has_new:
            logger.info(f"Records Loaded: {stats['inserted']:,}")
            logger.info(f"Records Rejected: {stats['rejected']:,}")

        logger.info(f"Quality Status: {quality_status}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info(f"End Time: {datetime.now()}")

        logger.info("\n" + "=" * 70)
        logger.info("✅ ETL PIPELINE COMPLETED")
        logger.info("=" * 70)

        return {
            'status': 'SUCCESS',
            'quality_status': quality_status,
            'duration': duration,
            'quality_results': dq.quality_results
        }

    except Exception as e:
        logger.error("\n" + "=" * 70)
        logger.error("❌ ETL PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    result = run_etl_with_quality_checks()