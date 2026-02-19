"""
Simple Incremental ETL Test for Online Retail Dataset
Uses kagglehub to access dataset directly without manual download
"""

import pandas as pd
from datetime import datetime
import time
import os
import glob
import kagglehub
from logger_config import setup_logger
from incremental_load import IncrementalLoader

logger = setup_logger('etl_incremental_test')


def get_kaggle_dataset_path():
    """
    Download/access Online Retail dataset via kagglehub

    Returns:
        str: Path to the dataset directory
    """
    logger.info("📥 Accessing Online Retail dataset via kagglehub...")

    try:
        # Download dataset (kagglehub caches it, so subsequent calls are fast)
        path = kagglehub.dataset_download("tunguz/online-retail")
        logger.info(f"✅ Dataset path: {path}")

        # List files in the dataset
        files = os.listdir(path)
        logger.info(f"📂 Available files:")
        for f in files:
            file_path = os.path.join(path, f)
            if os.path.isfile(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"  - {f} ({size_mb:.2f} MB)")

        return path

    except Exception as e:
        logger.error(f"❌ Error accessing dataset: {e}")
        raise


def find_data_file(base_path):
    """
    Find the main data file in the dataset directory

    Args:
        base_path (str): Base path from kagglehub

    Returns:
        str: Full path to the data file
    """
    # Search for Excel files first
    excel_files = glob.glob(os.path.join(base_path, "*.xlsx")) + \
                  glob.glob(os.path.join(base_path, "*.xls"))

    if excel_files:
        return excel_files[0]

    # Then search for CSV files
    csv_files = glob.glob(os.path.join(base_path, "*.csv"))

    if csv_files:
        return csv_files[0]

    raise FileNotFoundError(f"No data file found in {base_path}")


def load_online_retail_data():
    """
    Load Online Retail dataset from Kaggle via kagglehub

    Returns:
        DataFrame: Loaded and cleaned data
    """
    # Get dataset path from kagglehub
    dataset_path = get_kaggle_dataset_path()

    # Find the data file
    data_file = find_data_file(dataset_path)

    logger.info(f"📖 Loading data from: {os.path.basename(data_file)}")

    try:
        # Load based on file extension
        if data_file.endswith('.xlsx') or data_file.endswith('.xls'):
            df = pd.read_excel(data_file)
        else:
            # Load as CSV with proper encoding
            df = pd.read_csv(data_file, encoding='latin-1')

        logger.info(f"✅ Loaded {len(df):,} records")

        # Show column names
        logger.info(f"📋 Columns: {list(df.columns)}")

        # Convert InvoiceDate to datetime
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

        # Display basic statistics
        logger.info(f"📊 Dataset Statistics:")
        logger.info(f"  • Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
        logger.info(f"  • Unique customers: {df['CustomerID'].nunique():,}")
        logger.info(f"  • Unique products: {df['StockCode'].nunique():,}")
        logger.info(f"  • Unique invoices: {df['InvoiceNo'].nunique():,}")
        if 'Country' in df.columns:
            logger.info(f"  • Countries: {df['Country'].nunique()}")

        # Show data quality issues
        null_customers = df['CustomerID'].isnull().sum()
        cancellations = df['InvoiceNo'].astype(str).str.startswith('C').sum()
        negative_qty = (df['Quantity'] < 0).sum()

        logger.info(f"📉 Data Quality Preview:")
        logger.info(f"  • Null CustomerID: {null_customers:,} ({null_customers/len(df)*100:.1f}%)")
        logger.info(f"  • Cancellations (InvoiceNo starts with C): {cancellations:,}")
        logger.info(f"  • Negative Quantity: {negative_qty:,}")

        return df

    except Exception as e:
        logger.error(f"❌ Error loading data: {e}")
        raise


def run_simple_incremental_etl():
    """Test incremental loading on fact_sales with Online Retail data"""

    logger.info("=" * 70)
    logger.info("🚀 TESTING INCREMENTAL ETL - ONLINE RETAIL DATASET")
    logger.info("=" * 70)
    logger.info("Using kagglehub for data access (no manual download needed)")
    logger.info("=" * 70)

    try:
        # Initialize loader
        loader = IncrementalLoader()

        # Load Online Retail data via kagglehub
        retail_df = load_online_retail_data()

        # Show sample data
        logger.info("\n📊 Sample Data (First 5 rows):")
        print(retail_df.head())
        print("\n" + "=" * 70)

        # Check for new data
        logger.info("\n🔍 Checking for new data...")
        has_new, count = loader.check_for_new_data(
            retail_df, 'fact_sales', 'InvoiceDate'
        )
        logger.info(f"New records available: {count:,}")

        if not has_new:
            logger.info("✨ No new data to load - all records already processed")
            logger.info("\nℹ️  To test again with fresh data:")
            logger.info("  1. Truncate fact_sales table, OR")
            logger.info("  2. Delete watermark: DELETE FROM etl_watermark WHERE table_name = 'fact_sales'")
            return

        # Ask for confirmation if loading large dataset
        if count > 100000:
            logger.warning(f"\n⚠️  About to load {count:,} records. This may take a few minutes.")
            logger.info("Press Ctrl+C to cancel, or wait 5 seconds to continue...")
            time.sleep(5)

        # Load incrementally
        logger.info("\n📈 Starting incremental load...")
        logger.info("This will:")
        logger.info("  1. Validate all records")
        logger.info("  2. Filter out cancellations and invalid data")
        logger.info("  3. Load valid records to fact_sales")
        logger.info("  4. Update watermark for future runs")
        logger.info("")

        start_time = time.time()

        stats = loader.load_fact_sales_incremental(
            retail_df,
            timestamp_column='InvoiceDate'
        )

        end_time = time.time()
        duration = end_time - start_time

        # Display results
        logger.info("\n" + "=" * 70)
        logger.info("✅ TEST COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"📊 Load Statistics:")
        logger.info(f"  • Records Inserted: {stats['inserted']:,}")
        logger.info(f"  • Records Rejected: {stats['rejected']:,}")
        logger.info(f"  • Total New Records: {stats['total']:,}")
        logger.info(f"  • Acceptance Rate: {stats['inserted']/stats['total']*100:.1f}%")
        logger.info("")
        logger.info(f"⏱️  Performance:")
        logger.info(f"  • Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")

        # Calculate processing speed
        if duration > 0 and stats['inserted'] > 0:
            records_per_sec = stats['inserted'] / duration
            logger.info(f"  • Processing Speed: {records_per_sec:,.0f} records/second")

        # Get cumulative statistics
        logger.info("\n" + "=" * 70)
        logger.info("📊 Cumulative Statistics (All-Time)")
        logger.info("=" * 70)

        cumulative_stats = loader.get_load_statistics('fact_sales')
        if cumulative_stats:
            logger.info(f"Last Loaded: {cumulative_stats['last_loaded_timestamp']}")
            if cumulative_stats.get('last_invoice_number'):
                logger.info(f"Last Invoice: {cumulative_stats['last_invoice_number']}")
            logger.info(f"Total Processed: {cumulative_stats['total_records_processed']:,}")
            logger.info(f"Total Rejected: {cumulative_stats['total_records_rejected']:,}")

        # Show rejection reasons if any
        if stats['rejected'] > 0:
            logger.info("\n" + "=" * 70)
            logger.info("⚠️  Rejected Records Summary")
            logger.info("=" * 70)
            logger.info(f"Total Rejected: {stats['rejected']:,}")
            logger.info("\nCommon rejection reasons:")
            logger.info("  1. Cancellations (InvoiceNo starting with 'C')")
            logger.info("  2. Null CustomerID (incomplete transactions)")
            logger.info("  3. Invalid Quantity (<=0)")
            logger.info("  4. Invalid UnitPrice (<=0)")
            logger.info("\n💡 Check 'etl_rejected_records' table for full details:")
            logger.info("   SELECT * FROM etl_rejected_records ORDER BY rejection_timestamp DESC LIMIT 10;")

        logger.info("\n" + "=" * 70)
        logger.info("🎉 INCREMENTAL LOADING WORKS!")
        logger.info("=" * 70)
        logger.info("\n✅ Next run will only load NEW records (incremental)")
        logger.info("✅ Watermark tracking is active")
        logger.info("✅ Ready for scheduled ETL runs")

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Operation cancelled by user")
    except Exception as e:
        logger.error("\n" + "=" * 70)
        logger.error("❌ TEST FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_simple_incremental_etl()