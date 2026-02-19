"""
Advanced Data Quality Checks for Retail Data Warehouse
Implements comprehensive data quality framework with:
- Referential integrity validation
- Statistical anomaly detection
- Data freshness monitoring
- Schema validation
- Historical comparison
Author: [Your Name]
Date: February 2026
"""

import pandas as pd
import numpy as np
from sqlalchemy import text, inspect
from datetime import datetime, timedelta
from db_connection import engine
from logger_config import setup_logger
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

logger = setup_logger('advanced_dq')


class AdvancedDataQuality:
    """
    Advanced Data Quality Framework
    Implements industry best practices for data validation
    """
    
    def __init__(self):
        """Initialize advanced data quality checker"""
        self.engine = engine
        self.quality_results = {
            'timestamp': datetime.now(),
            'checks_passed': 0,
            'checks_failed': 0,
            'checks_warning': 0,
            'details': []
        }
    
    # ========================================================================
    # 1. REFERENTIAL INTEGRITY CHECKS
    # ========================================================================
    
    def check_referential_integrity(self, fact_table, dim_table, fact_fk, dim_pk):
        """
        Check referential integrity between fact and dimension tables
        
        Args:
            fact_table (str): Fact table name
            dim_table (str): Dimension table name
            fact_fk (str): Foreign key column in fact table
            dim_pk (str): Primary key column in dimension table
            
        Returns:
            dict: Check results with orphaned records count
        """
        logger.info(f"🔗 Checking referential integrity: {fact_table}.{fact_fk} -> {dim_table}.{dim_pk}")
        
        try:
            # Find orphaned records (FK values not in dimension table)
            query = text(f"""
                SELECT COUNT(*) as orphaned_count
                FROM {fact_table} f
                LEFT JOIN {dim_table} d ON f.{fact_fk} = d.{dim_pk}
                WHERE d.{dim_pk} IS NULL
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                orphaned_count = result[0]
            
            # Get sample orphaned records for investigation
            if orphaned_count > 0:
                sample_query = text(f"""
                    SELECT DISTINCT f.{fact_fk}
                    FROM {fact_table} f
                    LEFT JOIN {dim_table} d ON f.{fact_fk} = d.{dim_pk}
                    WHERE d.{dim_pk} IS NULL
                    LIMIT 10
                """)
                
                with self.engine.connect() as conn:
                    samples = [row[0] for row in conn.execute(sample_query)]
            else:
                samples = []
            
            result = {
                'check_name': 'Referential Integrity',
                'relationship': f"{fact_table}.{fact_fk} -> {dim_table}.{dim_pk}",
                'orphaned_records': orphaned_count,
                'sample_orphaned_keys': samples,
                'status': 'PASS' if orphaned_count == 0 else 'FAIL'
            }
            
            if orphaned_count == 0:
                logger.info(f"✅ PASS: No orphaned records found")
                self.quality_results['checks_passed'] += 1
            else:
                logger.error(f"❌ FAIL: {orphaned_count} orphaned records found")
                logger.error(f"Sample keys: {samples[:5]}")
                self.quality_results['checks_failed'] += 1
            
            self.quality_results['details'].append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error checking referential integrity: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def check_all_referential_integrity(self):
        """
        Check referential integrity for all fact-dimension relationships
        
        Returns:
            list: Results for all relationships
        """
        logger.info("\n" + "="*70)
        logger.info("🔗 REFERENTIAL INTEGRITY CHECKS")
        logger.info("="*70)
        
        # Define all FK relationships
        relationships = [
            ('fact_sales', 'dim_customer', 'customer_key', 'customer_key'),
            ('fact_sales', 'dim_product', 'product_key', 'product_key'),
            ('fact_sales', 'dim_store', 'store_key', 'store_key'),
            ('fact_sales', 'dim_time', 'time_key', 'time_key'),
        ]
        
        results = []
        for fact, dim, fk, pk in relationships:
            result = self.check_referential_integrity(fact, dim, fk, pk)
            results.append(result)
        
        return results
    
    # ========================================================================
    # 2. STATISTICAL ANOMALY DETECTION
    # ========================================================================
    
    def detect_numerical_anomalies(self, table_name, column_name, z_threshold=3):
        """
        Detect anomalies in numerical columns using Z-score method
        
        Args:
            table_name (str): Table name
            column_name (str): Numerical column to check
            z_threshold (float): Z-score threshold (default: 3 = 99.7% confidence)
            
        Returns:
            dict: Anomaly detection results
        """
        logger.info(f"📊 Detecting anomalies in {table_name}.{column_name} (Z-score threshold: {z_threshold})")
        
        try:
            # Load column data
            query = text(f"SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL")
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            if len(df) == 0:
                return {'status': 'SKIP', 'reason': 'No data'}
            
            values = df[column_name].values
            
            # Calculate Z-scores
            mean_val = np.mean(values)
            std_val = np.std(values)
            
            if std_val == 0:
                return {'status': 'SKIP', 'reason': 'No variation in data'}
            
            z_scores = np.abs((values - mean_val) / std_val)
            anomalies = z_scores > z_threshold
            anomaly_count = np.sum(anomalies)
            anomaly_percentage = (anomaly_count / len(values)) * 100
            
            # Get anomaly statistics
            if anomaly_count > 0:
                anomaly_values = values[anomalies]
                anomaly_stats = {
                    'min': float(np.min(anomaly_values)),
                    'max': float(np.max(anomaly_values)),
                    'mean': float(np.mean(anomaly_values))
                }
            else:
                anomaly_stats = {}
            
            result = {
                'check_name': 'Statistical Anomaly Detection',
                'table': table_name,
                'column': column_name,
                'total_records': len(values),
                'anomaly_count': int(anomaly_count),
                'anomaly_percentage': round(anomaly_percentage, 2),
                'data_mean': round(mean_val, 2),
                'data_std': round(std_val, 2),
                'anomaly_stats': anomaly_stats,
                'status': 'PASS' if anomaly_percentage < 1 else 'WARNING' if anomaly_percentage < 5 else 'FAIL'
            }
            
            if result['status'] == 'PASS':
                logger.info(f"✅ PASS: {anomaly_percentage:.2f}% anomalies (within acceptable range)")
                self.quality_results['checks_passed'] += 1
            elif result['status'] == 'WARNING':
                logger.warning(f"⚠️  WARNING: {anomaly_percentage:.2f}% anomalies detected")
                self.quality_results['checks_warning'] += 1
            else:
                logger.error(f"❌ FAIL: {anomaly_percentage:.2f}% anomalies (exceeds threshold)")
                self.quality_results['checks_failed'] += 1
            
            self.quality_results['details'].append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error detecting anomalies: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def detect_all_numerical_anomalies(self):
        """
        Detect anomalies in all numerical columns
        
        Returns:
            list: Results for all numerical columns
        """
        logger.info("\n" + "="*70)
        logger.info("📊 STATISTICAL ANOMALY DETECTION")
        logger.info("="*70)
        
        # Define columns to check
        columns_to_check = [
            ('fact_sales', 'quantity'),
            ('fact_sales', 'unit_price'),
            ('fact_sales', 'total_amount'),
        ]
        
        results = []
        for table, column in columns_to_check:
            result = self.detect_numerical_anomalies(table, column)
            results.append(result)
        
        return results
    
    # ========================================================================
    # 3. DATA FRESHNESS MONITORING
    # ========================================================================
    
    def check_data_freshness(self, table_name, timestamp_column, max_age_hours=24):
        """
        Check if data is fresh (recently updated)
        
        Args:
            table_name (str): Table name
            timestamp_column (str): Timestamp column to check
            max_age_hours (int): Maximum acceptable age in hours
            
        Returns:
            dict: Freshness check results
        """
        logger.info(f"⏰ Checking data freshness: {table_name}.{timestamp_column} (max age: {max_age_hours}h)")
        
        try:
            query = text(f"""
                SELECT 
                    MAX({timestamp_column}) as latest_timestamp,
                    COUNT(*) as total_records
                FROM {table_name}
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                latest_timestamp = result[0]
                total_records = result[1]
            
            if latest_timestamp is None:
                return {'status': 'FAIL', 'reason': 'No data in table'}
            
            # Calculate age
            current_time = datetime.now()
            data_age = current_time - latest_timestamp
            age_hours = data_age.total_seconds() / 3600
            
            result = {
                'check_name': 'Data Freshness',
                'table': table_name,
                'timestamp_column': timestamp_column,
                'latest_timestamp': latest_timestamp,
                'age_hours': round(age_hours, 2),
                'max_age_hours': max_age_hours,
                'total_records': total_records,
                'status': 'PASS' if age_hours <= max_age_hours else 'WARNING' if age_hours <= max_age_hours * 2 else 'FAIL'
            }
            
            if result['status'] == 'PASS':
                logger.info(f"✅ PASS: Data is fresh ({age_hours:.1f}h old)")
                self.quality_results['checks_passed'] += 1
            elif result['status'] == 'WARNING':
                logger.warning(f"⚠️  WARNING: Data is getting stale ({age_hours:.1f}h old)")
                self.quality_results['checks_warning'] += 1
            else:
                logger.error(f"❌ FAIL: Data is stale ({age_hours:.1f}h old)")
                self.quality_results['checks_failed'] += 1
            
            self.quality_results['details'].append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error checking data freshness: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    # ========================================================================
    # 4. SCHEMA VALIDATION
    # ========================================================================
    
    def validate_schema(self, table_name, expected_columns):
        """
        Validate table schema matches expected structure
        
        Args:
            table_name (str): Table name
            expected_columns (dict): Expected columns with data types
                                    Example: {'column_name': 'data_type'}
            
        Returns:
            dict: Schema validation results
        """
        logger.info(f"📋 Validating schema for {table_name}")
        
        try:
            inspector = inspect(self.engine)
            
            if not inspector.has_table(table_name):
                logger.error(f"❌ FAIL: Table {table_name} does not exist")
                self.quality_results['checks_failed'] += 1
                return {'status': 'FAIL', 'reason': 'Table not found'}
            
            # Get actual columns
            actual_columns = inspector.get_columns(table_name)
            actual_col_dict = {col['name']: str(col['type']) for col in actual_columns}
            
            # Compare with expected
            missing_columns = set(expected_columns.keys()) - set(actual_col_dict.keys())
            extra_columns = set(actual_col_dict.keys()) - set(expected_columns.keys())
            type_mismatches = []
            
            for col_name in set(expected_columns.keys()) & set(actual_col_dict.keys()):
                expected_type = expected_columns[col_name].upper()
                actual_type = actual_col_dict[col_name].upper()
                
                # Fuzzy match for types (e.g., VARCHAR matches VARCHAR(255))
                if expected_type not in actual_type and actual_type not in expected_type:
                    type_mismatches.append({
                        'column': col_name,
                        'expected': expected_type,
                        'actual': actual_type
                    })
            
            result = {
                'check_name': 'Schema Validation',
                'table': table_name,
                'expected_columns': len(expected_columns),
                'actual_columns': len(actual_col_dict),
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns),
                'type_mismatches': type_mismatches,
                'status': 'PASS' if not (missing_columns or type_mismatches) else 'FAIL'
            }
            
            if result['status'] == 'PASS':
                logger.info(f"✅ PASS: Schema matches expected structure")
                self.quality_results['checks_passed'] += 1
            else:
                logger.error(f"❌ FAIL: Schema validation failed")
                if missing_columns:
                    logger.error(f"  Missing columns: {missing_columns}")
                if extra_columns:
                    logger.warning(f"  Extra columns: {extra_columns}")
                if type_mismatches:
                    logger.error(f"  Type mismatches: {type_mismatches}")
                self.quality_results['checks_failed'] += 1
            
            self.quality_results['details'].append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error validating schema: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    # ========================================================================
    # 5. HISTORICAL COMPARISON
    # ========================================================================
    
    def compare_with_historical(self, table_name, metric_column, tolerance_percentage=20):
        """
        Compare current data volume/metrics with historical averages
        
        Args:
            table_name (str): Table name
            metric_column (str): Column to aggregate (e.g., 'quantity', 'total_amount')
            tolerance_percentage (float): Acceptable deviation percentage
            
        Returns:
            dict: Historical comparison results
        """
        logger.info(f"📈 Comparing {table_name}.{metric_column} with historical data")
        
        try:
            # Get current day's metrics
            query_current = text(f"""
                SELECT 
                    COUNT(*) as record_count,
                    AVG({metric_column}) as avg_value,
                    SUM({metric_column}) as total_value
                FROM {table_name}
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            
            # Get historical average (last 30 days, excluding today)
            query_historical = text(f"""
                SELECT 
                    AVG(daily_count) as avg_record_count,
                    AVG(daily_avg) as avg_value,
                    AVG(daily_total) as avg_total
                FROM (
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as daily_count,
                        AVG({metric_column}) as daily_avg,
                        SUM({metric_column}) as daily_total
                    FROM {table_name}
                    WHERE DATE(created_at) BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY DATE(created_at)
                ) daily_stats
            """)
            
            with self.engine.connect() as conn:
                current = conn.execute(query_current).fetchone()
                historical = conn.execute(query_historical).fetchone()
            
            if historical[0] is None:
                return {'status': 'SKIP', 'reason': 'Insufficient historical data'}
            
            # Calculate deviations
            record_deviation = ((current[0] - historical[0]) / historical[0]) * 100 if historical[0] > 0 else 0
            value_deviation = ((current[1] - historical[1]) / historical[1]) * 100 if historical[1] > 0 else 0
            
            result = {
                'check_name': 'Historical Comparison',
                'table': table_name,
                'metric': metric_column,
                'current_records': int(current[0]),
                'historical_avg_records': round(historical[0], 2),
                'record_deviation_pct': round(record_deviation, 2),
                'current_avg_value': round(current[1], 2) if current[1] else 0,
                'historical_avg_value': round(historical[1], 2) if historical[1] else 0,
                'value_deviation_pct': round(value_deviation, 2),
                'tolerance_pct': tolerance_percentage,
                'status': 'PASS' if abs(record_deviation) <= tolerance_percentage else 'WARNING'
            }
            
            if result['status'] == 'PASS':
                logger.info(f"✅ PASS: Metrics within {tolerance_percentage}% of historical average")
                self.quality_results['checks_passed'] += 1
            else:
                logger.warning(f"⚠️  WARNING: Deviation of {record_deviation:.1f}% exceeds tolerance")
                self.quality_results['checks_warning'] += 1
            
            self.quality_results['details'].append(result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in historical comparison: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    # ========================================================================
    # 6. DUPLICATE DETECTION
    # ========================================================================
    
    def check_duplicates(self, table_name, unique_columns):
        """
        Check for duplicate records based on unique column combination
        
        Args:
            table_name (str): Table name
            unique_columns (list): Columns that should form a unique combination
            
        Returns:
            dict: Duplicate check results
        """
        logger.info(f"🔍 Checking for duplicates in {table_name} on {unique_columns}")
        
        try:
            columns_str = ", ".join(unique_columns)
            
            query = text(f"""
                SELECT 
                    {columns_str},
                    COUNT(*) as duplicate_count
                FROM {table_name}
                GROUP BY {columns_str}
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                duplicates = result.fetchall()
            
            duplicate_count = len(duplicates)
            
            result_dict = {
                'check_name': 'Duplicate Detection',
                'table': table_name,
                'unique_columns': unique_columns,
                'duplicate_groups': duplicate_count,
                'sample_duplicates': [dict(zip(unique_columns + ['count'], dup)) for dup in duplicates[:5]],
                'status': 'PASS' if duplicate_count == 0 else 'FAIL'
            }
            
            if result_dict['status'] == 'PASS':
                logger.info(f"✅ PASS: No duplicates found")
                self.quality_results['checks_passed'] += 1
            else:
                logger.error(f"❌ FAIL: {duplicate_count} duplicate groups found")
                self.quality_results['checks_failed'] += 1
            
            self.quality_results['details'].append(result_dict)
            return result_dict
            
        except Exception as e:
            logger.error(f"❌ Error checking duplicates: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    # ========================================================================
    # 7. RUN ALL CHECKS
    # ========================================================================
    
    def run_all_checks(self):
        """
        Run complete data quality suite
        
        Returns:
            dict: Comprehensive quality report
        """
        logger.info("\n" + "="*70)
        logger.info("🚀 RUNNING ADVANCED DATA QUALITY CHECKS")
        logger.info("="*70)
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info("="*70)
        
        # 1. Referential Integrity
        self.check_all_referential_integrity()
        
        # 2. Statistical Anomalies
        self.detect_all_numerical_anomalies()
        
        # 3. Data Freshness (skip for historical datasets)
        # self.check_data_freshness('fact_sales', 'created_at', max_age_hours=24)
        
        # 4. Schema Validation
        expected_fact_sales_schema = {
            'sale_id': 'INTEGER',
            'time_key': 'INTEGER',
            'customer_key': 'INTEGER',
            'product_key': 'INTEGER',
            'store_key': 'INTEGER',
            'quantity': 'INTEGER',
            'unit_price': 'NUMERIC',
            'total_amount': 'NUMERIC'
        }
        self.validate_schema('fact_sales', expected_fact_sales_schema)
        
        # 5. Duplicate Detection
        self.check_duplicates('fact_sales', ['time_key', 'customer_key', 'product_key', 'store_key'])
        
        # Generate summary report
        self.generate_quality_report()
        
        return self.quality_results
    
    def generate_quality_report(self):
        """Generate and display quality report summary"""
        logger.info("\n" + "="*70)
        logger.info("📊 DATA QUALITY REPORT SUMMARY")
        logger.info("="*70)
        
        total_checks = (self.quality_results['checks_passed'] + 
                       self.quality_results['checks_failed'] + 
                       self.quality_results['checks_warning'])
        
        logger.info(f"Total Checks: {total_checks}")
        logger.info(f"✅ Passed: {self.quality_results['checks_passed']}")
        logger.info(f"⚠️  Warnings: {self.quality_results['checks_warning']}")
        logger.info(f"❌ Failed: {self.quality_results['checks_failed']}")
        
        if self.quality_results['checks_failed'] == 0:
            logger.info("\n🎉 ALL CRITICAL CHECKS PASSED!")
        else:
            logger.error("\n⚠️  SOME CHECKS FAILED - REVIEW REQUIRED")
        
        logger.info("="*70)
        
        # Save report to database
        self.save_quality_report()
    
    def save_quality_report(self):
        """Save quality report to database for tracking"""
        try:
            # Create quality reports table if not exists
            create_table_sql = text("""
                CREATE TABLE IF NOT EXISTS etl_quality_reports (
                    report_id SERIAL PRIMARY KEY,
                    report_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_checks INTEGER,
                    checks_passed INTEGER,
                    checks_failed INTEGER,
                    checks_warning INTEGER,
                    report_details JSONB
                )
            """)
            
            with self.engine.begin() as conn:
                conn.execute(create_table_sql)
            
            # Insert report
            import json
            insert_sql = text("""
                INSERT INTO etl_quality_reports 
                (report_timestamp, total_checks, checks_passed, checks_failed, checks_warning, report_details)
                VALUES (:ts, :total, :passed, :failed, :warning, :details)
            """)
            
            total = (self.quality_results['checks_passed'] + 
                    self.quality_results['checks_failed'] + 
                    self.quality_results['checks_warning'])
            
            with self.engine.begin() as conn:
                conn.execute(insert_sql, {
                    'ts': self.quality_results['timestamp'],
                    'total': total,
                    'passed': self.quality_results['checks_passed'],
                    'failed': self.quality_results['checks_failed'],
                    'warning': self.quality_results['checks_warning'],
                    'details': json.dumps(self.quality_results['details'])
                })
            
            logger.info("✅ Quality report saved to etl_quality_reports table")
            
        except Exception as e:
            logger.error(f"⚠️  Could not save quality report: {e}")


# Test function
if __name__ == "__main__":
    dq = AdvancedDataQuality()
    results = dq.run_all_checks()
