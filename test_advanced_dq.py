"""
Test Script for Advanced Data Quality Checks
UPDATED with correct fact_sales schema
"""

from advanced_data_quality import AdvancedDataQuality
from logger_config import setup_logger
from datetime import datetime

logger = setup_logger('test_advanced_dq')


def run_advanced_quality_tests():
    """Run comprehensive data quality tests with correct schema"""

    logger.info("=" * 70)
    logger.info("🧪 TESTING ADVANCED DATA QUALITY CHECKS")
    logger.info("=" * 70)
    logger.info(f"Start Time: {datetime.now()}")
    logger.info("=" * 70)

    try:
        # Initialize quality checker
        dq = AdvancedDataQuality()

        # ========================================
        # TEST 1: Referential Integrity
        # ========================================
        logger.info("\n📋 TEST 1: Referential Integrity Checks")
        logger.info("-" * 70)

        dq.check_referential_integrity('fact_sales', 'dim_customer', 'customer_key', 'customer_key')
        dq.check_referential_integrity('fact_sales', 'dim_product', 'product_key', 'product_key')
        dq.check_referential_integrity('fact_sales', 'dim_store', 'store_key', 'store_key')
        dq.check_referential_integrity('fact_sales', 'dim_time', 'time_key', 'time_key')

        # ========================================
        # TEST 2: Statistical Anomaly Detection
        # ========================================
        logger.info("\n📋 TEST 2: Statistical Anomaly Detection")
        logger.info("-" * 70)

        # Use actual column names from your schema
        dq.detect_numerical_anomalies('fact_sales', 'quantity_sold', z_threshold=3)
        dq.detect_numerical_anomalies('fact_sales', 'sales_amount', z_threshold=3)
        dq.detect_numerical_anomalies('fact_sales', 'discount_amount', z_threshold=3)

        # ========================================
        # TEST 3: Schema Validation
        # ========================================
        logger.info("\n📋 TEST 3: Schema Validation")
        logger.info("-" * 70)

        # CORRECTED schema based on your actual table structure
        fact_sales_schema = {
            'sales_key': 'INTEGER',
            'time_key': 'INTEGER',
            'customer_key': 'INTEGER', 
            'product_key': 'INTEGER',
            'store_key': 'INTEGER',
            'quantity_sold': 'INTEGER',
            'sales_amount': 'NUMERIC',
            'discount_amount': 'NUMERIC',
            'created_at': 'TIMESTAMP'
        }

        dq.validate_schema('fact_sales', fact_sales_schema)

        # ========================================
        # TEST 4: Duplicate Detection
        # ========================================
        logger.info("\n📋 TEST 4: Duplicate Detection")
        logger.info("-" * 70)

        # Check for duplicate transactions
        # Note: This will likely fail if you haven't run fix_duplicates.py yet
        dq.check_duplicates('fact_sales', ['time_key', 'customer_key', 'product_key'])

        # ========================================
        # FINAL REPORT
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("📊 FINAL DATA QUALITY REPORT")
        logger.info("=" * 70)

        total = dq.quality_results['checks_passed'] + dq.quality_results['checks_failed'] + dq.quality_results['checks_warning']

        logger.info(f"Total Checks Run: {total}")
        logger.info(f"✅ Passed: {dq.quality_results['checks_passed']} ({dq.quality_results['checks_passed']/total*100:.1f}%)")
        logger.info(f"⚠️  Warnings: {dq.quality_results['checks_warning']} ({dq.quality_results['checks_warning']/total*100:.1f}%)")
        logger.info(f"❌ Failed: {dq.quality_results['checks_failed']} ({dq.quality_results['checks_failed']/total*100:.1f}%)")

        # Show details of failed checks
        if dq.quality_results['checks_failed'] > 0:
            logger.info("\n" + "=" * 70)
            logger.info("❌ FAILED CHECKS DETAILS")
            logger.info("=" * 70)

            for detail in dq.quality_results['details']:
                if detail.get('status') == 'FAIL':
                    logger.error(f"\n{detail['check_name']}:")

                    # Schema validation - show what's wrong
                    if detail['check_name'] == 'Schema Validation':
                        if detail.get('missing_columns'):
                            logger.error(f"  Missing Columns: {detail['missing_columns']}")
                        if detail.get('extra_columns'):
                            logger.error(f"  Extra Columns: {detail['extra_columns']}")
                        if detail.get('type_mismatches'):
                            logger.error(f"  Type Mismatches: {detail['type_mismatches']}")

                    # Duplicate detection - show samples
                    elif detail['check_name'] == 'Duplicate Detection':
                        logger.error(f"  Duplicate Groups: {detail.get('duplicate_groups')}")
                        if detail.get('sample_duplicates'):
                            logger.error(f"  Sample Duplicates:")
                            for dup in detail['sample_duplicates'][:3]:
                                logger.error(f"    • {dup}")
                            logger.error("\n  💡 To fix duplicates, run:")
                            logger.error("     python fix_duplicates.py")

                    # Other checks
                    else:
                        for key, value in detail.items():
                            if key not in ['check_name', 'status']:
                                logger.error(f"  {key}: {value}")

        # Show warnings
        if dq.quality_results['checks_warning'] > 0:
            logger.info("\n" + "=" * 70)
            logger.info("⚠️  WARNING CHECKS DETAILS")
            logger.info("=" * 70)

            for detail in dq.quality_results['details']:
                if detail.get('status') == 'WARNING':
                    logger.warning(f"\n{detail['check_name']}:")
                    for key, value in detail.items():
                        if key not in ['check_name', 'status']:
                            logger.warning(f"  {key}: {value}")

        logger.info("\n" + "=" * 70)
        logger.info("✅ ADVANCED DATA QUALITY TESTS COMPLETE")
        logger.info("=" * 70)
        logger.info(f"End Time: {datetime.now()}")

        # Recommendations
        if dq.quality_results['checks_failed'] > 0:
            logger.info("\n" + "=" * 70)
            logger.info("💡 RECOMMENDATIONS")
            logger.info("=" * 70)
            logger.info("\n1. To inspect your actual schema:")
            logger.info("   python inspect_schema.py")
            logger.info("\n2. To fix duplicate records:")
            logger.info("   python fix_duplicates.py")
            logger.info("\n3. After fixes, re-run quality checks:")
            logger.info("   python test_advanced_dq.py")

        # Save report
        dq.save_quality_report()
        logger.info("\n💾 Quality report saved to etl_quality_reports table")

    except Exception as e:
        logger.error(f"\n❌ Test failed: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_advanced_quality_tests()