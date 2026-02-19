"""
Test Performance Monitoring System
Demonstrates all monitoring features
"""

import time
from performance_monitor import monitor
from logger_config import setup_logger

logger = setup_logger('test_performance')


# Example 1: Manual session management
def test_manual_monitoring():
    """Test manual session start/end"""
    logger.info("\n" + "="*70)
    logger.info("TEST 1: Manual Session Management")
    logger.info("="*70)

    # Start monitoring
    session_id = monitor.start_session("Manual ETL Test", metadata={'source': 'test'})

    # Simulate ETL work
    logger.info("Processing data...")
    time.sleep(2)

    # End monitoring
    monitor.end_session(
        status='SUCCESS',
        records_processed=5000,
        records_rejected=100
    )

    logger.info("✅ Manual monitoring test complete")


# Example 2: Using decorator for automatic monitoring
@monitor.monitor(process_name="Automated ETL Test")
def test_decorator_monitoring():
    """Test automatic monitoring with decorator"""
    logger.info("\n" + "="*70)
    logger.info("TEST 2: Decorator-Based Monitoring")
    logger.info("="*70)

    # Simulate ETL work
    logger.info("Processing with decorator...")
    time.sleep(1.5)

    # Return results (auto-tracked)
    return {
        'inserted': 3000,
        'updated': 500,
        'rejected': 50
    }


# Example 3: Query performance tracking
def test_query_tracking():
    """Test query performance tracking"""
    logger.info("\n" + "="*70)
    logger.info("TEST 3: Query Performance Tracking")
    logger.info("="*70)

    session_id = monitor.start_session("Query Tracking Test")

    # Simulate queries
    queries = [
        ("Load Customer Dimension", "INSERT INTO dim_customer...", 450, 1000),
        ("Load Fact Sales", "INSERT INTO fact_sales...", 1200, 5000),
        ("Update Watermark", "UPDATE etl_watermarks...", 150, 1)
    ]

    for query_name, query_text, exec_time, rows in queries:
        monitor.track_query(query_name, query_text, exec_time, rows)
        logger.info(f"  Tracked: {query_name} ({exec_time}ms, {rows} rows)")
        time.sleep(0.5)

    monitor.end_session(status='SUCCESS', records_processed=6001)
    logger.info("✅ Query tracking test complete")


# Example 4: Performance reports
def test_performance_reports():
    """Test performance reporting"""
    logger.info("\n" + "="*70)
    logger.info("TEST 4: Performance Reports")
    logger.info("="*70)

    # Execution summary
    logger.info("\n📊 Execution Summary (Last 7 Days):")
    summary = monitor.get_execution_summary(days=7)
    print(summary)

    # Slow queries
    logger.info("\n⚠️  Slow Queries:")
    slow_queries = monitor.get_slow_queries(threshold_ms=500, limit=5)
    print(slow_queries)

    # Performance trend
    logger.info("\n📈 Performance Trend (Last 30 Days):")
    trend = monitor.get_performance_trend(days=30)
    print(trend.head(10))

    # Generate full report
    monitor.generate_performance_report()


def run_all_tests():
    """Run all performance monitoring tests"""
    logger.info("="*70)
    logger.info("🚀 TESTING PERFORMANCE MONITORING SYSTEM")
    logger.info("="*70)

    try:
        # Run tests
        test_manual_monitoring()
        time.sleep(1)

        result = test_decorator_monitoring()
        logger.info(f"Decorator result: {result}")
        time.sleep(1)

        test_query_tracking()
        time.sleep(1)

        test_performance_reports()

        logger.info("\n" + "="*70)
        logger.info("✅ ALL TESTS PASSED")
        logger.info("="*70)

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()