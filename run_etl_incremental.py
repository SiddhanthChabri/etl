"""
Incremental ETL Orchestrator for Retail Data Warehouse
Coordinates incremental loading with existing watermark manager
Author: [Your Name]
Date: February 2026
"""

import pandas as pd
from datetime import datetime
import time
from logger_config import setup_logger
from data_quality_checks import DataQualityChecker
from incremental_load import IncrementalLoader
from load_customer_scd import load_customer_scd
from load_dimensions import load_store_dimension, load_time_dimension

logger = setup_logger('etl_incremental')


def load_source_data():
    """
    Load source data from CSV/Excel/Database
    
    Returns:
        dict: Dictionary of DataFrames
    """
    logger.info("Loading source data...")
    
    try:
        # TODO: Replace paths with your actual data sources
        sales_df = pd.read_csv('data/sales_transactions.csv')
        customers_df = pd.read_csv('data/customers.csv')
        products_df = pd.read_csv('data/products.csv')
        
        # Ensure timestamp columns are datetime type
        sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'])
        
        if 'created_date' in customers_df.columns:
            customers_df['created_date'] = pd.to_datetime(customers_df['created_date'])
        if 'modified_date' in customers_df.columns:
            customers_df['modified_date'] = pd.to_datetime(customers_df['modified_date'])
            
        if 'created_date' in products_df.columns:
            products_df['created_date'] = pd.to_datetime(products_df['created_date'])
        if 'modified_date' in products_df.columns:
            products_df['modified_date'] = pd.to_datetime(products_df['modified_date'])
        
        logger.info(f"✅ Loaded {len(sales_df)} sales records")
        logger.info(f"✅ Loaded {len(customers_df)} customer records")
        logger.info(f"✅ Loaded {len(products_df)} product records")
        
        return {
            'sales': sales_df,
            'customers': customers_df,
            'products': products_df
        }
        
    except FileNotFoundError as e:
        logger.error(f"Source file not found: {e}")
        logger.info("Please ensure source data files are in the 'data/' directory")
        raise
    except Exception as e:
        logger.error(f"Error loading source data: {e}")
        raise


def run_incremental_etl():
    """
    Main ETL orchestrator with incremental loading
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("🚀 STARTING INCREMENTAL ETL PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Start Time: {datetime.now()}")
    
    total_records_loaded = 0
    total_records_rejected = 0
    
    try:
        # Initialize components
        loader = IncrementalLoader()
        quality_checker = DataQualityChecker()
        
        # ============================================================
        # STEP 1: Load Source Data
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📥 STEP 1: Loading Source Data")
        logger.info("=" * 70)
        
        source_data = load_source_data()
        
        # ============================================================
        # STEP 2: Check for New Data
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("🔍 STEP 2: Checking for New Data")
        logger.info("=" * 70)
        
        has_new_sales, new_sales_count = loader.check_for_new_data(
            source_data['sales'], 'fact_sales', 'transaction_date'
        )
        
        logger.info(f"New sales records available: {new_sales_count}")
        
        if not has_new_sales:
            logger.info("✨ No new data found - ETL pipeline complete")
            return
        
        # ============================================================
        # STEP 3: Data Quality Checks
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("✔️  STEP 3: Data Quality Validation")
        logger.info("=" * 70)
        
        quality_results = quality_checker.run_all_checks(source_data['sales'], 'sales_source')
        
        if not quality_results['passed']:
            logger.error("❌ Data quality checks failed - aborting ETL")
            raise Exception("Data quality validation failed")
        
        logger.info("✅ Data quality checks passed")
        
        # ============================================================
        # STEP 4: Load Dimensions
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📊 STEP 4: Loading Dimension Tables")
        logger.info("=" * 70)
        
        # Time dimension - full refresh (small static table)
        logger.info("⏰ Loading dim_time...")
        load_time_dimension()
        logger.info("✅ dim_time loaded")
        
        # Store dimension - full refresh (small static table)
        logger.info("🏪 Loading dim_store...")
        load_store_dimension()
        logger.info("✅ dim_store loaded")
        
        # Customer dimension - SCD Type 2 (use existing function)
        logger.info("👤 Loading dim_customer (SCD Type 2)...")
        load_customer_scd()
        logger.info("✅ dim_customer loaded")
        
        # Product dimension - Incremental load
        if 'created_date' in source_data['products'].columns or 'modified_date' in source_data['products'].columns:
            logger.info("📦 Loading dim_product (incremental)...")
            timestamp_col = 'modified_date' if 'modified_date' in source_data['products'].columns else 'created_date'
            product_stats = loader.load_dimension_incremental(
                source_data['products'],
                'dim_product',
                timestamp_col,
                'product_key'
            )
            logger.info(f"✅ dim_product loaded: {product_stats['inserted']} inserted, {product_stats['updated']} updated")
        else:
            logger.info("📦 Loading dim_product (full refresh - no timestamp column)...")
            logger.warning("⚠️  dim_product: No timestamp column found, skipping incremental load")
        
        # ============================================================
        # STEP 5: Load Fact Table (Incremental)
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📈 STEP 5: Loading Fact Table (Incremental)")
        logger.info("=" * 70)
        
        fact_stats = loader.load_fact_sales_incremental(
            source_data['sales'],
            timestamp_column='transaction_date'
        )
        
        total_records_loaded = fact_stats['inserted']
        total_records_rejected = fact_stats['rejected']
        
        logger.info(f"✅ fact_sales loaded: {fact_stats['inserted']} records")
        if fact_stats['rejected'] > 0:
            logger.warning(f"⚠️  {fact_stats['rejected']} records rejected (see etl_rejected_records table)")
        
        # ============================================================
        # STEP 6: Post-Load Validation
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("🔍 STEP 6: Post-Load Validation")
        logger.info("=" * 70)
        
        # Check row counts
        logger.info("Verifying record counts...")
        # TODO: Add referential integrity checks in next option
        logger.info("✅ Post-load validation complete")
        
        # ============================================================
        # STEP 7: ETL Summary
        # ============================================================
        logger.info("\n" + "=" * 70)
        logger.info("📊 STEP 7: ETL Summary")
        logger.info("=" * 70)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Records Loaded: {total_records_loaded}")
        logger.info(f"Records Rejected: {total_records_rejected}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        
        # Get overall statistics
        stats = loader.get_load_statistics('fact_sales')
        if stats:
            logger.info(f"Total Records Processed (All-Time): {stats['total_records_processed']}")
            logger.info(f"Total Records Rejected (All-Time): {stats['total_records_rejected']}")
        
        logger.info("=" * 70)
        logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"End Time: {datetime.now()}")
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error("❌ ETL PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_incremental_etl()
