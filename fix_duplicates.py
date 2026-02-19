"""
Fix Duplicate Records in fact_sales
Removes duplicate transactions based on business rules
"""

from sqlalchemy import text
from db_connection import engine
from logger_config import setup_logger

logger = setup_logger('fix_duplicates')


def analyze_duplicates():
    """Analyze duplicate records to understand the issue"""

    logger.info("="*70)
    logger.info("🔍 ANALYZING DUPLICATE RECORDS")
    logger.info("="*70)

    # Find duplicate groups
    query = text("""
        SELECT 
            time_key,
            customer_key,
            product_key,
            COUNT(*) as duplicate_count,
            MIN(sales_key) as first_key,
            MAX(sales_key) as last_key,
            MIN(created_at) as first_created,
            MAX(created_at) as last_created
        FROM fact_sales
        GROUP BY time_key, customer_key, product_key
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)

    with engine.connect() as conn:
        results = conn.execute(query).fetchall()

    logger.info(f"\nFound {len(results)} duplicate groups\n")

    for row in results:
        logger.info(f"Duplicate Group:")
        logger.info(f"  • Time Key: {row[0]}")
        logger.info(f"  • Customer Key: {row[1]}")
        logger.info(f"  • Product Key: {row[2]}")
        logger.info(f"  • Count: {row[3]}")
        logger.info(f"  • Sales Key Range: {row[4]} to {row[5]}")
        logger.info(f"  • Created: {row[6]} to {row[7]}")
        logger.info("")

    return results


def fix_duplicates_keep_first():
    """
    Remove duplicates by keeping only the first record (oldest sales_key)
    for each unique combination of time_key, customer_key, product_key
    """

    logger.info("="*70)
    logger.info("🔧 FIXING DUPLICATES - KEEPING FIRST RECORD")
    logger.info("="*70)

    # Count total duplicates before
    count_query = text("""
        SELECT COUNT(*) 
        FROM (
            SELECT time_key, customer_key, product_key
            FROM fact_sales
            GROUP BY time_key, customer_key, product_key
            HAVING COUNT(*) > 1
        ) dup
    """)

    with engine.connect() as conn:
        before_count = conn.execute(count_query).scalar()

    logger.info(f"\nDuplicate groups before: {before_count}")

    # Delete duplicates, keeping only the row with minimum sales_key
    delete_query = text("""
        DELETE FROM fact_sales
        WHERE sales_key IN (
            SELECT sales_key
            FROM (
                SELECT 
                    sales_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY time_key, customer_key, product_key 
                        ORDER BY sales_key ASC
                    ) as rn
                FROM fact_sales
            ) ranked
            WHERE rn > 1
        )
    """)

    try:
        with engine.begin() as conn:
            result = conn.execute(delete_query)
            deleted_count = result.rowcount

        logger.info(f"✅ Deleted {deleted_count} duplicate records")

        # Verify
        with engine.connect() as conn:
            after_count = conn.execute(count_query).scalar()

        logger.info(f"Duplicate groups after: {after_count}")

        if after_count == 0:
            logger.info("\n🎉 All duplicates removed successfully!")
        else:
            logger.warning(f"⚠️  Still {after_count} duplicate groups remaining")

        return deleted_count

    except Exception as e:
        logger.error(f"❌ Error fixing duplicates: {e}")
        raise


def fix_duplicates_aggregate():
    """
    Alternative: Aggregate duplicate records (sum quantities)
    Use this if duplicates represent multiple line items
    """

    logger.info("="*70)
    logger.info("🔧 FIXING DUPLICATES - AGGREGATING VALUES")
    logger.info("="*70)

    # Create a temporary table with aggregated data
    aggregate_query = text("""
        -- Create temp table with aggregated duplicates
        CREATE TEMP TABLE fact_sales_deduped AS
        SELECT 
            MIN(sales_key) as sales_key,
            time_key,
            customer_key,
            product_key,
            store_key,
            SUM(quantity_sold) as quantity_sold,
            AVG(sales_amount) as sales_amount,
            AVG(discount_amount) as discount_amount,
            MIN(created_at) as created_at
        FROM fact_sales
        GROUP BY time_key, customer_key, product_key, store_key;

        -- Backup original table
        ALTER TABLE fact_sales RENAME TO fact_sales_backup;

        -- Replace with deduplicated data
        ALTER TABLE fact_sales_deduped RENAME TO fact_sales;
    """)

    logger.warning("⚠️  This will replace the fact_sales table with aggregated data")
    logger.warning("⚠️  Original table will be backed up as fact_sales_backup")

    try:
        with engine.begin() as conn:
            conn.execute(aggregate_query)

        logger.info("✅ Duplicates aggregated successfully")
        logger.info("✅ Original table backed up as fact_sales_backup")

        return True

    except Exception as e:
        logger.error(f"❌ Error aggregating duplicates: {e}")
        raise


def main():
    """Main function with options"""

    logger.info("="*70)
    logger.info("🔧 DUPLICATE FIX UTILITY")
    logger.info("="*70)

    # First, analyze
    analyze_duplicates()

    # Ask user which method to use
    print("\n" + "="*70)
    print("DUPLICATE FIX OPTIONS")
    print("="*70)
    print("\n1. Keep First Record (delete others)")
    print("   • Fastest method")
    print("   • Keeps oldest record by sales_key")
    print("   • Recommended for accidental duplicates")
    print("\n2. Aggregate Records (sum quantities)")
    print("   • Combines duplicate values")
    print("   • Use if duplicates are valid line items")
    print("   • Creates backup table")
    print("\n3. Cancel (analyze only)")

    choice = input("\nEnter choice (1/2/3): ").strip()

    if choice == '1':
        logger.info("\nSelected: Keep First Record")
        fix_duplicates_keep_first()
    elif choice == '2':
        logger.info("\nSelected: Aggregate Records")
        confirm = input("This will modify your table. Type 'YES' to confirm: ")
        if confirm == 'YES':
            fix_duplicates_aggregate()
        else:
            logger.info("Cancelled")
    else:
        logger.info("Cancelled - analysis only")


if __name__ == "__main__":
    main()