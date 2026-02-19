"""
Verification Script - Check if Quality Issues are Fixed
"""

from sqlalchemy import text
from db_connection import engine
from logger_config import setup_logger

logger = setup_logger('verify_fixes')


def verify_all_fixes():
    """Verify both issues are resolved"""

    logger.info("="*70)
    logger.info("✅ VERIFYING QUALITY FIXES")
    logger.info("="*70)

    issues_fixed = []
    issues_remaining = []

    # ========================================
    # Check 1: Duplicates Fixed?
    # ========================================
    logger.info("\n🔍 Checking for duplicate records...")

    dup_query = text("""
        SELECT COUNT(*) as duplicate_groups
        FROM (
            SELECT time_key, customer_key, product_key
            FROM fact_sales
            GROUP BY time_key, customer_key, product_key
            HAVING COUNT(*) > 1
        ) dup
    """)

    with engine.connect() as conn:
        dup_count = conn.execute(dup_query).scalar()

    if dup_count == 0:
        logger.info("✅ PASS: No duplicate records found!")
        issues_fixed.append("Duplicate Detection")
    else:
        logger.error(f"❌ FAIL: Still {dup_count} duplicate groups found")
        issues_remaining.append(f"Duplicates ({dup_count} groups)")

    # ========================================
    # Check 2: Schema Correct?
    # ========================================
    logger.info("\n🔍 Checking schema structure...")

    from sqlalchemy import inspect
    inspector = inspect(engine)

    actual_columns = {col['name'] for col in inspector.get_columns('fact_sales')}
    expected_columns = {
        'sales_key', 'time_key', 'customer_key', 'product_key', 
        'store_key', 'quantity_sold', 'sales_amount', 
        'discount_amount', 'created_at'
    }

    if expected_columns.issubset(actual_columns):
        logger.info("✅ PASS: Schema contains all expected columns!")
        issues_fixed.append("Schema Validation")
    else:
        missing = expected_columns - actual_columns
        logger.error(f"❌ FAIL: Missing columns: {missing}")
        issues_remaining.append(f"Schema (missing: {missing})")

    # ========================================
    # Check 3: Record Counts
    # ========================================
    logger.info("\n📊 Checking record counts...")

    count_query = text("SELECT COUNT(*) FROM fact_sales")

    with engine.connect() as conn:
        total_records = conn.execute(count_query).scalar()

    logger.info(f"Total records in fact_sales: {total_records:,}")

    # ========================================
    # Summary
    # ========================================
    logger.info("\n" + "="*70)
    logger.info("📊 VERIFICATION SUMMARY")
    logger.info("="*70)

    if issues_remaining:
        logger.error(f"\n❌ Issues Remaining: {len(issues_remaining)}")
        for issue in issues_remaining:
            logger.error(f"  • {issue}")
        logger.info("\n💡 Run these commands:")
        if "Duplicates" in str(issues_remaining):
            logger.info("  python fix_duplicates.py")
    else:
        logger.info("\n🎉 ALL ISSUES FIXED!")
        logger.info(f"✅ Fixed: {', '.join(issues_fixed)}")

    logger.info("\n" + "="*70)

    # ========================================
    # Next Steps
    # ========================================
    if not issues_remaining:
        logger.info("\n🚀 NEXT STEPS:")
        logger.info("="*70)
        logger.info("\n1. Re-run quality checks:")
        logger.info("   python test_advanced_dq.py")
        logger.info("\n2. Generate HTML dashboard:")
        logger.info("   python generate_quality_dashboard.py")
        logger.info("\n3. Run ETL with quality checks:")
        logger.info("   python run_etl_with_quality.py")
        logger.info("\n4. Move to Option 3:")
        logger.info("   Configuration Management (config.yaml)")

    return len(issues_remaining) == 0


if __name__ == "__main__":
    success = verify_all_fixes()

    if success:
        print("\n✅ Ready to proceed to Option 3!")
    else:
        print("\n⚠️  Please fix remaining issues first")