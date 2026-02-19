"""
Performance Monitoring Module for Retail Data Warehouse
Tracks ETL execution times, resource usage, and query performance
Author: [Your Name]
Date: February 2026
"""

import time
import psutil
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from db_connection import engine
from logger_config import setup_logger
from functools import wraps
import json

logger = setup_logger('performance_monitor')


class PerformanceMonitor:
    """
    Performance monitoring and tracking system
    Tracks execution times, resource usage, and query performance
    """

    def __init__(self):
        """Initialize performance monitor"""
        self.engine = engine
        self.current_session = {
            'session_id': None,
            'start_time': None,
            'process_name': None,
            'metrics': {}
        }

        # Initialize tracking tables
        self._create_tracking_tables()

    def _create_tracking_tables(self):
        """Create performance tracking tables if they don't exist"""

        # ETL execution tracking
        create_execution_table = text("""
            CREATE TABLE IF NOT EXISTS etl_execution_log (
                execution_id SERIAL PRIMARY KEY,
                process_name VARCHAR(100) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                duration_seconds NUMERIC,
                status VARCHAR(20),
                records_processed INTEGER,
                records_rejected INTEGER,
                error_message TEXT,
                cpu_percent NUMERIC,
                memory_mb NUMERIC,
                metadata JSONB
            )
        """)

        # Query performance tracking
        create_query_table = text("""
            CREATE TABLE IF NOT EXISTS query_performance_log (
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

        # Daily performance summary
        create_summary_table = text("""
            CREATE TABLE IF NOT EXISTS performance_daily_summary (
                summary_id SERIAL PRIMARY KEY,
                summary_date DATE NOT NULL UNIQUE,
                total_executions INTEGER,
                successful_executions INTEGER,
                failed_executions INTEGER,
                total_records_processed BIGINT,
                avg_duration_seconds NUMERIC,
                max_duration_seconds NUMERIC,
                avg_cpu_percent NUMERIC,
                avg_memory_mb NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(create_execution_table)
                conn.execute(create_query_table)
                conn.execute(create_summary_table)

            logger.info("✅ Performance tracking tables initialized")

        except Exception as e:
            logger.error(f"❌ Error creating tracking tables: {e}")

    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================

    def start_session(self, process_name: str, metadata: dict = None):
        """
        Start a new monitoring session

        Args:
            process_name (str): Name of the ETL process
            metadata (dict): Additional metadata to store

        Returns:
            int: Session/Execution ID
        """
        self.current_session['process_name'] = process_name
        self.current_session['start_time'] = datetime.now()
        self.current_session['metrics'] = {
            'cpu_start': psutil.cpu_percent(interval=1),
            'memory_start': psutil.Process().memory_info().rss / (1024 * 1024)  # MB
        }

        # Insert into database
        insert_query = text("""
            INSERT INTO etl_execution_log 
            (process_name, start_time, status, metadata)
            VALUES (:process_name, :start_time, 'RUNNING', :metadata)
            RETURNING execution_id
        """)

        try:
            with self.engine.begin() as conn:
                result = conn.execute(insert_query, {
                    'process_name': process_name,
                    'start_time': self.current_session['start_time'],
                    'metadata': json.dumps(metadata) if metadata else None
                })
                self.current_session['session_id'] = result.fetchone()[0]

            logger.info(f"📊 Started monitoring session: {process_name} (ID: {self.current_session['session_id']})")
            return self.current_session['session_id']

        except Exception as e:
            logger.error(f"❌ Error starting session: {e}")
            return None

    def end_session(self, status: str = 'SUCCESS', 
                   records_processed: int = 0, 
                   records_rejected: int = 0,
                   error_message: str = None):
        """
        End the current monitoring session

        Args:
            status (str): SUCCESS, FAILED, or WARNING
            records_processed (int): Number of records processed
            records_rejected (int): Number of records rejected
            error_message (str): Error message if failed
        """
        if not self.current_session['session_id']:
            logger.warning("No active session to end")
            return

        end_time = datetime.now()
        duration = (end_time - self.current_session['start_time']).total_seconds()

        # Get final resource metrics
        cpu_end = psutil.cpu_percent(interval=1)
        memory_end = psutil.Process().memory_info().rss / (1024 * 1024)

        avg_cpu = (self.current_session['metrics']['cpu_start'] + cpu_end) / 2
        avg_memory = (self.current_session['metrics']['memory_start'] + memory_end) / 2

        # Update database
        update_query = text("""
            UPDATE etl_execution_log
            SET end_time = :end_time,
                duration_seconds = :duration,
                status = :status,
                records_processed = :records_processed,
                records_rejected = :records_rejected,
                error_message = :error_message,
                cpu_percent = :cpu_percent,
                memory_mb = :memory_mb
            WHERE execution_id = :execution_id
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(update_query, {
                    'execution_id': self.current_session['session_id'],
                    'end_time': end_time,
                    'duration': duration,
                    'status': status,
                    'records_processed': records_processed,
                    'records_rejected': records_rejected,
                    'error_message': error_message,
                    'cpu_percent': avg_cpu,
                    'memory_mb': avg_memory
                })

            logger.info(f"✅ Session ended: {status} | Duration: {duration:.2f}s | Records: {records_processed:,}")

            # Generate daily summary
            self._update_daily_summary()

        except Exception as e:
            logger.error(f"❌ Error ending session: {e}")

        finally:
            # Reset session
            self.current_session = {
                'session_id': None,
                'start_time': None,
                'process_name': None,
                'metrics': {}
            }

    # ========================================================================
    # QUERY PERFORMANCE TRACKING
    # ========================================================================

    def track_query(self, query_name: str, query_text: str, 
                   execution_time_ms: float, rows_affected: int = 0):
        """
        Track individual query performance

        Args:
            query_name (str): Name/description of query
            query_text (str): SQL query text
            execution_time_ms (float): Execution time in milliseconds
            rows_affected (int): Number of rows affected
        """
        if not self.current_session['session_id']:
            logger.warning("No active session for query tracking")
            return

        insert_query = text("""
            INSERT INTO query_performance_log
            (execution_id, query_name, query_text, execution_time_ms, rows_affected)
            VALUES (:execution_id, :query_name, :query_text, :execution_time_ms, :rows_affected)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(insert_query, {
                    'execution_id': self.current_session['session_id'],
                    'query_name': query_name,
                    'query_text': query_text[:1000],  # Truncate long queries
                    'execution_time_ms': execution_time_ms,
                    'rows_affected': rows_affected
                })

            if execution_time_ms > 1000:  # Alert if > 1 second
                logger.warning(f"⚠️  Slow query detected: {query_name} ({execution_time_ms:.0f}ms)")

        except Exception as e:
            logger.error(f"❌ Error tracking query: {e}")

    # ========================================================================
    # DECORATOR FOR AUTOMATIC TRACKING
    # ========================================================================

    def monitor(self, process_name: str = None):
        """
        Decorator to automatically monitor function execution

        Usage:
            @monitor.monitor(process_name="Load Fact Sales")
            def load_fact_sales():
                # Your ETL logic
                pass
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Use function name if process_name not provided
                p_name = process_name or func.__name__

                # Start monitoring
                self.start_session(p_name)

                status = 'SUCCESS'
                error_msg = None
                result = None

                try:
                    # Execute function
                    result = func(*args, **kwargs)

                    # Extract metrics from result if it's a dict
                    if isinstance(result, dict):
                        records_processed = result.get('inserted', 0) + result.get('updated', 0)
                        records_rejected = result.get('rejected', 0)
                    else:
                        records_processed = 0
                        records_rejected = 0

                except Exception as e:
                    status = 'FAILED'
                    error_msg = str(e)
                    records_processed = 0
                    records_rejected = 0
                    logger.error(f"❌ Function {p_name} failed: {e}")
                    raise

                finally:
                    # End monitoring
                    self.end_session(
                        status=status,
                        records_processed=records_processed,
                        records_rejected=records_rejected,
                        error_message=error_msg
                    )

                return result

            return wrapper
        return decorator

    # ========================================================================
    # REPORTING AND ANALYTICS
    # ========================================================================

    def get_execution_summary(self, days: int = 7):
        """
        Get execution summary for last N days

        Args:
            days (int): Number of days to look back

        Returns:
            DataFrame: Execution summary
        """
        query = text("""
            SELECT 
                process_name,
                COUNT(*) as total_executions,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                AVG(duration_seconds) as avg_duration_sec,
                MAX(duration_seconds) as max_duration_sec,
                SUM(records_processed) as total_records,
                AVG(cpu_percent) as avg_cpu,
                AVG(memory_mb) as avg_memory_mb
            FROM etl_execution_log
            WHERE start_time >= CURRENT_DATE - INTERVAL ':days days'
            GROUP BY process_name
            ORDER BY total_executions DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'days': days})

        return df

    def get_slow_queries(self, threshold_ms: float = 1000, limit: int = 10):
        """
        Get slowest queries

        Args:
            threshold_ms (float): Minimum execution time in milliseconds
            limit (int): Number of queries to return

        Returns:
            DataFrame: Slow queries
        """
        query = text("""
            SELECT 
                query_name,
                AVG(execution_time_ms) as avg_time_ms,
                MAX(execution_time_ms) as max_time_ms,
                COUNT(*) as execution_count,
                MAX(timestamp) as last_executed
            FROM query_performance_log
            WHERE execution_time_ms > :threshold
            GROUP BY query_name
            ORDER BY avg_time_ms DESC
            LIMIT :limit
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'threshold': threshold_ms, 'limit': limit})

        return df

    def get_performance_trend(self, days: int = 30):
        """
        Get performance trend over time

        Args:
            days (int): Number of days to analyze

        Returns:
            DataFrame: Daily performance metrics
        """
        query = text("""
            SELECT 
                DATE(start_time) as date,
                COUNT(*) as executions,
                AVG(duration_seconds) as avg_duration,
                SUM(records_processed) as total_records,
                AVG(cpu_percent) as avg_cpu,
                AVG(memory_mb) as avg_memory
            FROM etl_execution_log
            WHERE start_time >= CURRENT_DATE - INTERVAL ':days days'
            GROUP BY DATE(start_time)
            ORDER BY date DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'days': days})

        return df

    def _update_daily_summary(self):
        """Update daily performance summary"""
        query = text("""
            INSERT INTO performance_daily_summary (
                summary_date, total_executions, successful_executions, failed_executions,
                total_records_processed, avg_duration_seconds, max_duration_seconds,
                avg_cpu_percent, avg_memory_mb
            )
            SELECT 
                CURRENT_DATE,
                COUNT(*),
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END),
                SUM(COALESCE(records_processed, 0)),
                AVG(duration_seconds),
                MAX(duration_seconds),
                AVG(cpu_percent),
                AVG(memory_mb)
            FROM etl_execution_log
            WHERE DATE(start_time) = CURRENT_DATE
            ON CONFLICT (summary_date) 
            DO UPDATE SET
                total_executions = EXCLUDED.total_executions,
                successful_executions = EXCLUDED.successful_executions,
                failed_executions = EXCLUDED.failed_executions,
                total_records_processed = EXCLUDED.total_records_processed,
                avg_duration_seconds = EXCLUDED.avg_duration_seconds,
                max_duration_seconds = EXCLUDED.max_duration_seconds,
                avg_cpu_percent = EXCLUDED.avg_cpu_percent,
                avg_memory_mb = EXCLUDED.avg_memory_mb
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query)
        except Exception as e:
            logger.error(f"Error updating daily summary: {e}")

    def generate_performance_report(self):
        """Generate comprehensive performance report"""
        logger.info("="*70)
        logger.info("📊 PERFORMANCE REPORT")
        logger.info("="*70)

        # Get summary for last 7 days
        summary = self.get_execution_summary(days=7)

        if len(summary) > 0:
            logger.info("\n📈 Execution Summary (Last 7 Days):")
            for _, row in summary.iterrows():
                logger.info(f"\n  Process: {row['process_name']}")
                logger.info(f"    Total Runs: {row['total_executions']}")
                logger.info(f"    Success Rate: {row['successful']/row['total_executions']*100:.1f}%")
                logger.info(f"    Avg Duration: {row['avg_duration_sec']:.2f}s")
                logger.info(f"    Total Records: {row['total_records']:,.0f}")
                logger.info(f"    Avg CPU: {row['avg_cpu']:.1f}%")
                logger.info(f"    Avg Memory: {row['avg_memory_mb']:.1f} MB")

        # Get slow queries
        slow_queries = self.get_slow_queries(threshold_ms=500, limit=5)

        if len(slow_queries) > 0:
            logger.info("\n⚠️  Slow Queries (>500ms):")
            for _, row in slow_queries.iterrows():
                logger.info(f"\n  Query: {row['query_name']}")
                logger.info(f"    Avg Time: {row['avg_time_ms']:.0f}ms")
                logger.info(f"    Max Time: {row['max_time_ms']:.0f}ms")
                logger.info(f"    Executions: {row['execution_count']}")

        logger.info("\n" + "="*70)


# Global monitor instance
monitor = PerformanceMonitor()


if __name__ == "__main__":
    # Test performance monitoring
    print("🧪 Testing Performance Monitor...")

    session_id = monitor.start_session("Test ETL Process")

    # Simulate some work
    time.sleep(2)

    monitor.end_session(status='SUCCESS', records_processed=1000, records_rejected=10)

    print("\n✅ Performance monitoring test complete!")