"""
Fix Performance Monitoring Table Schema
Repairs query_performance_log table structure
"""

from sqlalchemy import text
from db_connection import engine
from logger_config import setup_logger

logger = setup_logger('fix_perf_schema')


def fix_query_performance_table():
    """Drop and recreate query_performance_log with correct schema"""

    logger.info("="*70)
    logger.info("🔧 FIXING QUERY PERFORMANCE TABLE SCHEMA")
    logger.info("="*70)

    try:
        # Drop existing table
        drop_query = text("DROP TABLE IF EXISTS query_performance_log CASCADE")

        # Create with correct schema
        create_query = text("""
            CREATE TABLE query_performance_log (
                query_id SERIAL PRIMARY KEY,
                execution_id INTEGER REFERENCES etl_execution_log(execution_id),
                query_name VARCHAR(200),
                query_text TEXT,
                execution_time_ms NUMERIC,
                rows_affected INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                query_plan TEXT
            )
        """)

        with engine.begin() as conn:
            logger.info("Dropping old table...")
            conn.execute(drop_query)

            logger.info("Creating table with correct schema...")
            conn.execute(create_query)

        logger.info("✅ Table schema fixed successfully!")

        # Verify schema
        verify_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'query_performance_log'
            ORDER BY ordinal_position
        """)

        with engine.connect() as conn:
            result = conn.execute(verify_query)

            logger.info("\n📋 Verified Schema:")
            for row in result:
                logger.info(f"  • {row[0]:<25} {row[1]}")

        logger.info("\n" + "="*70)
        logger.info("✅ SCHEMA FIX COMPLETE")
        logger.info("="*70)
        logger.info("\nYou can now run:")
        logger.info("  python test_performance_monitor.py")

        return True

    except Exception as e:
        logger.error(f"❌ Error fixing schema: {e}")
        return False


if __name__ == "__main__":
    success = fix_query_performance_table()

    if success:
        print("\n✅ Schema fixed! Run the test again.")
    else:
        print("\n❌ Failed to fix schema. Check logs for details.")