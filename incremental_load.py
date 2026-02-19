"""
Incremental Loading Module for Retail Data Warehouse
Implements timestamp-based incremental loading with existing watermark tracking
Author: [Your Name]
Date: February 2026
"""

import pandas as pd
from sqlalchemy import text
from datetime import datetime
from db_connection import engine
from watermark_manager import WatermarkManager
from logger_config import setup_logger

logger = setup_logger('incremental_load')


class IncrementalLoader:
    """
    Handles incremental loading for fact and dimension tables
    Uses timestamp-based CDC (Change Data Capture) approach
    Integrates with existing WatermarkManager
    """
    
    def __init__(self):
        """Initialize incremental loader"""
        self.engine = engine
        self.watermark_mgr = WatermarkManager()
    
    def load_fact_sales_incremental(self, source_df, source_system=None, timestamp_column='transaction_date'):
        """
        Load fact_sales incrementally based on transaction_date
        
        Args:
            source_df (DataFrame): Source sales data with timestamp column
            source_system (str): Optional source system identifier
            timestamp_column (str): Column name containing timestamp
            
        Returns:
            dict: Load statistics (inserted, rejected)
        """
        table_name = 'fact_sales'
        
        try:
            logger.info(f"Starting incremental load for {table_name}" + 
                       (f" from {source_system}" if source_system else ""))
            
            # Get last watermark
            watermark = self.watermark_mgr.get_last_watermark(table_name, source_system)
            
            if watermark and watermark['timestamp']:
                # Filter only new records since last watermark
                last_timestamp = watermark['timestamp']
                new_records = source_df[source_df[timestamp_column] > last_timestamp].copy()
                logger.info(f"Found {len(new_records)} new records after {last_timestamp}")
                logger.info(f"Previous load: {watermark['records_processed']} processed, "
                           f"{watermark['records_rejected']} rejected")
            elif watermark and watermark['date']:
                # Fallback to date-based filtering
                last_date = watermark['date']
                new_records = source_df[source_df[timestamp_column].dt.date > last_date].copy()
                logger.info(f"Found {len(new_records)} new records after {last_date}")
            else:
                # First run - load all data
                new_records = source_df.copy()
                logger.info(f"First run - loading all {len(new_records)} records")
            
            if len(new_records) == 0:
                logger.info("No new records to load")
                return {'inserted': 0, 'rejected': 0, 'total': 0}
            
            # Sort by timestamp to ensure proper watermark
            new_records = new_records.sort_values(timestamp_column)
            
            # Validate data before loading (basic checks)
            valid_records, rejected_records = self._validate_records(new_records, table_name)
            
            records_inserted = 0
            records_rejected = len(rejected_records)
            
            # Load valid records to database
            if len(valid_records) > 0:
                valid_records.to_sql(
                    table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=5000
                )
                records_inserted = len(valid_records)
                logger.info(f"Successfully loaded {records_inserted} records to {table_name}")
            
            # Log rejected records
            if records_rejected > 0:
                logger.warning(f"Rejected {records_rejected} invalid records")
                self._log_rejected_records(rejected_records, table_name, source_system)
            
            # Update watermark
            new_timestamp = source_df[timestamp_column].max()
            new_date = new_timestamp.date() if hasattr(new_timestamp, 'date') else None
            
            self.watermark_mgr.update_watermark(
                table_name=table_name,
                source_system=source_system,
                new_timestamp=new_timestamp,
                new_date=new_date,
                records_processed=records_inserted,
                records_rejected=records_rejected
            )
            
            return {
                'inserted': records_inserted,
                'rejected': records_rejected,
                'total': len(new_records)
            }
            
        except Exception as e:
            logger.error(f"Error in incremental load for {table_name}: {e}")
            raise
    
    def load_dimension_incremental(self, source_df, table_name, timestamp_column, 
                                   unique_key, source_system=None):
        """
        Load dimension table incrementally with UPSERT logic
        
        Args:
            source_df (DataFrame): Source dimension data
            table_name (str): Target dimension table name
            timestamp_column (str): Column to use for watermark
            unique_key (str): Primary key column for upsert logic
            source_system (str): Optional source system identifier
            
        Returns:
            dict: Load statistics
        """
        try:
            logger.info(f"Starting incremental load for {table_name}" +
                       (f" from {source_system}" if source_system else ""))
            
            # Ensure timestamp column exists
            if timestamp_column not in source_df.columns:
                logger.error(f"Timestamp column '{timestamp_column}' not found in source data")
                raise ValueError(f"Column {timestamp_column} not found")
            
            # Get last watermark
            watermark = self.watermark_mgr.get_last_watermark(table_name, source_system)
            
            if watermark and watermark['timestamp']:
                # Filter only new/modified records
                last_timestamp = watermark['timestamp']
                new_records = source_df[source_df[timestamp_column] > last_timestamp].copy()
                logger.info(f"Found {len(new_records)} new/modified records after {last_timestamp}")
            else:
                # First run - load all data
                new_records = source_df.copy()
                logger.info(f"First run - loading all {len(new_records)} records")
            
            if len(new_records) == 0:
                logger.info("No new records to load")
                return {'inserted': 0, 'updated': 0, 'rejected': 0}
            
            # Sort by timestamp
            new_records = new_records.sort_values(timestamp_column)
            
            # Validate records
            valid_records, rejected_records = self._validate_records(new_records, table_name)
            
            # Perform UPSERT
            inserted, updated = self._upsert_dimension(valid_records, table_name, unique_key)
            records_rejected = len(rejected_records)
            
            # Log rejected records
            if records_rejected > 0:
                logger.warning(f"Rejected {records_rejected} invalid records")
                self._log_rejected_records(rejected_records, table_name, source_system)
            
            # Update watermark
            new_timestamp = source_df[timestamp_column].max()
            new_date = new_timestamp.date() if hasattr(new_timestamp, 'date') else None
            
            self.watermark_mgr.update_watermark(
                table_name=table_name,
                source_system=source_system,
                new_timestamp=new_timestamp,
                new_date=new_date,
                records_processed=(inserted + updated),
                records_rejected=records_rejected
            )
            
            logger.info(f"Successfully processed {inserted + updated} records for {table_name}")
            logger.info(f"Inserted: {inserted}, Updated: {updated}, Rejected: {records_rejected}")
            
            return {
                'inserted': inserted,
                'updated': updated,
                'rejected': records_rejected
            }
            
        except Exception as e:
            logger.error(f"Error in incremental load for {table_name}: {e}")
            raise
    
    def _validate_records(self, df, table_name):
        """
        Validate records before loading
        
        Args:
            df (DataFrame): Records to validate
            table_name (str): Table name for context
            
        Returns:
            tuple: (valid_df, rejected_df)
        """
        # Start with all records as valid
        valid_mask = pd.Series([True] * len(df), index=df.index)
        
        # Check for null values in critical columns
        critical_columns = self._get_critical_columns(table_name)
        for col in critical_columns:
            if col in df.columns:
                null_mask = df[col].isnull()
                if null_mask.any():
                    logger.warning(f"Found {null_mask.sum()} records with null {col}")
                    valid_mask &= ~null_mask
        
        # Check for negative values in amount/quantity columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            if 'amount' in col.lower() or 'quantity' in col.lower() or 'price' in col.lower():
                negative_mask = df[col] < 0
                if negative_mask.any():
                    logger.warning(f"Found {negative_mask.sum()} records with negative {col}")
                    valid_mask &= ~negative_mask
        
        valid_df = df[valid_mask].copy()
        rejected_df = df[~valid_mask].copy()
        
        return valid_df, rejected_df
    
    def _get_critical_columns(self, table_name):
        """
        Get list of critical columns that cannot be null
        
        Args:
            table_name (str): Table name
            
        Returns:
            list: Critical column names
        """
        critical_cols = {
            'fact_sales': ['time_key', 'customer_key', 'product_key', 'store_key', 'quantity', 'unit_price'],
            'dim_product': ['product_key', 'product_name'],
            'dim_customer': ['customer_key', 'customer_name'],
            'dim_store': ['store_key', 'store_name'],
        }
        return critical_cols.get(table_name, [])
    
    def _log_rejected_records(self, rejected_df, table_name, source_system):
        """
        Log rejected records to a rejection table for analysis
        
        Args:
            rejected_df (DataFrame): Rejected records
            table_name (str): Source table name
            source_system (str): Source system
        """
        if len(rejected_df) == 0:
            return
        
        try:
            # Add metadata columns
            rejected_df = rejected_df.copy()
            rejected_df['rejection_timestamp'] = datetime.now()
            rejected_df['source_table'] = table_name
            rejected_df['source_system'] = source_system
            rejected_df['rejection_reason'] = 'Validation failed'
            
            # Log to rejection table
            rejected_df.to_sql(
                'etl_rejected_records',
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Logged {len(rejected_df)} rejected records to etl_rejected_records")
            
        except Exception as e:
            logger.error(f"Error logging rejected records: {e}")
    
    def _upsert_dimension(self, df, table_name, unique_key):
        """
        Perform UPSERT operation for dimension table
        
        Args:
            df (DataFrame): Data to upsert
            table_name (str): Target table
            unique_key (str): Primary key column
            
        Returns:
            tuple: (inserted_count, updated_count)
        """
        inserted_count = 0
        updated_count = 0
        
        try:
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    # Check if record exists
                    check_sql = text(f"""
                        SELECT COUNT(*) FROM {table_name}
                        WHERE {unique_key} = :{unique_key}
                    """)
                    
                    exists = conn.execute(check_sql, {unique_key: row[unique_key]}).scalar()
                    
                    if exists > 0:
                        # UPDATE existing record
                        set_clause = ", ".join([f"{col} = :{col}" for col in row.index if col != unique_key])
                        update_sql = text(f"""
                            UPDATE {table_name}
                            SET {set_clause}
                            WHERE {unique_key} = :{unique_key}
                        """)
                        conn.execute(update_sql, row.to_dict())
                        updated_count += 1
                    else:
                        # INSERT new record
                        columns = ", ".join(row.index)
                        placeholders = ", ".join([f":{col}" for col in row.index])
                        insert_sql = text(f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                        """)
                        conn.execute(insert_sql, row.to_dict())
                        inserted_count += 1
                
                conn.commit()
            
            return inserted_count, updated_count
            
        except Exception as e:
            logger.error(f"Error in upsert operation: {e}")
            raise
    
    def check_for_new_data(self, source_df, table_name, timestamp_column, source_system=None):
        """
        Check if source has new data since last watermark (without loading)
        
        Args:
            source_df (DataFrame): Source data
            table_name (str): Table name
            timestamp_column (str): Timestamp column
            source_system (str): Optional source system
            
        Returns:
            tuple: (has_new_data: bool, count: int)
        """
        try:
            watermark = self.watermark_mgr.get_last_watermark(table_name, source_system)
            
            if watermark and watermark['timestamp']:
                new_records = source_df[source_df[timestamp_column] > watermark['timestamp']]
                return (len(new_records) > 0, len(new_records))
            else:
                return (True, len(source_df))
                
        except Exception as e:
            logger.error(f"Error checking for new data: {e}")
            return (False, 0)
    
    def get_load_statistics(self, table_name=None, source_system=None):
        """
        Get loading statistics for a table
        
        Args:
            table_name (str): Optional table name filter
            source_system (str): Optional source system filter
            
        Returns:
            dict: Statistics
        """
        try:
            watermark = self.watermark_mgr.get_last_watermark(table_name, source_system)
            if watermark:
                return {
                    'table_name': table_name,
                    'source_system': source_system,
                    'last_loaded_timestamp': watermark['timestamp'],
                    'last_loaded_date': watermark['date'],
                    'total_records_processed': watermark['records_processed'],
                    'total_records_rejected': watermark['records_rejected']
                }
            return None
        except Exception as e:
            logger.error(f"Error getting load statistics: {e}")
            return None


# Test/Demo function
if __name__ == "__main__":
    loader = IncrementalLoader()
    
    # Display statistics for fact_sales
    print("\n=== fact_sales Load Statistics ===")
    stats = loader.get_load_statistics('fact_sales')
    if stats:
        print(f"Last Loaded: {stats['last_loaded_timestamp']}")
        print(f"Records Processed: {stats['total_records_processed']}")
        print(f"Records Rejected: {stats['total_records_rejected']}")
    else:
        print("No watermark data found")
