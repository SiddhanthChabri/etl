import pandas as pd
from logger_config import setup_logger
from datetime import datetime

logger = setup_logger('DataQuality')

class DataQualityChecker:
    """
    Data Quality validation class for ETL processes
    """
    
    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name
        self.issues = []
        logger.info(f"Initializing Data Quality Checks for {table_name}")
        logger.info(f"Dataset shape: {df.shape[0]} rows x {df.shape[1]} columns")
    
    def check_nulls(self, critical_columns):
        """Check for null values in critical columns"""
        logger.info("="*60)
        logger.info("CHECK 1: Null Value Check")
        logger.info("="*60)
        
        has_issues = False
        for col in critical_columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found in dataset")
                continue
                
            null_count = self.df[col].isnull().sum()
            null_percentage = (null_count / len(self.df)) * 100
            
            if null_count > 0:
                has_issues = True
                logger.warning(f"[!] {col}: {null_count:,} null values ({null_percentage:.2f}%)")
                self.issues.append({
                    'check': 'null_check',
                    'column': col,
                    'issue_count': null_count,
                    'percentage': null_percentage
                })
            else:
                logger.info(f"[OK] {col}: No null values")
        
        if not has_issues:
            logger.info("[PASS] All critical columns have no null values")
        
        return not has_issues
    
    def check_duplicates(self, key_columns):
        """Check for duplicate records"""
        logger.info("="*60)
        logger.info("CHECK 2: Duplicate Records Check")
        logger.info("="*60)
        
        duplicate_count = self.df.duplicated(subset=key_columns).sum()
        
        if duplicate_count > 0:
            logger.warning(f"[!] Found {duplicate_count:,} duplicate records")
            self.issues.append({
                'check': 'duplicate_check',
                'issue_count': duplicate_count
            })
            
            # Show sample duplicates
            duplicates = self.df[self.df.duplicated(subset=key_columns, keep=False)]
            logger.warning(f"Sample duplicate records:")
            logger.warning(f"\n{duplicates.head()}")
            return False
        else:
            logger.info(f"[PASS] No duplicate records found")
            return True
    
    def check_negative_values(self, numeric_columns):
        """Check for negative values in columns that should be positive"""
        logger.info("="*60)
        logger.info("CHECK 3: Negative Values Check")
        logger.info("="*60)
        
        has_issues = False
        for col in numeric_columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found in dataset")
                continue
            
            negative_count = (self.df[col] < 0).sum()
            
            if negative_count > 0:
                has_issues = True
                logger.warning(f"[!] {col}: {negative_count:,} negative values found")
                logger.warning(f"    Min value: {self.df[col].min()}")
                self.issues.append({
                    'check': 'negative_values',
                    'column': col,
                    'issue_count': negative_count
                })
            else:
                logger.info(f"[OK] {col}: No negative values")
        
        if not has_issues:
            logger.info("[PASS] All numeric columns have valid positive values")
        
        return not has_issues
    
    def check_data_ranges(self, column, min_val=None, max_val=None):
        """Check if data is within expected range"""
        logger.info("="*60)
        logger.info(f"CHECK 4: Data Range Check for {column}")
        logger.info("="*60)
        
        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found in dataset")
            return False
        
        out_of_range = 0
        
        if min_val is not None:
            below_min = (self.df[column] < min_val).sum()
            if below_min > 0:
                logger.warning(f"[!] {below_min:,} records below minimum value {min_val}")
                out_of_range += below_min
        
        if max_val is not None:
            above_max = (self.df[column] > max_val).sum()
            if above_max > 0:
                logger.warning(f"[!] {above_max:,} records above maximum value {max_val}")
                out_of_range += above_max
        
        if out_of_range > 0:
            self.issues.append({
                'check': 'range_check',
                'column': column,
                'issue_count': out_of_range
            })
            return False
        else:
            logger.info(f"[PASS] All values in {column} are within expected range")
            return True
    
    def check_future_dates(self, date_column, date_format='%Y%m%d'):
        """Check for future dates"""
        logger.info("="*60)
        logger.info(f"CHECK 5: Future Dates Check for {date_column}")
        logger.info("="*60)
        
        if date_column not in self.df.columns:
            logger.warning(f"Column '{date_column}' not found in dataset")
            return False
        
        try:
            today = int(datetime.now().strftime(date_format))
            future_dates = (self.df[date_column] > today).sum()
            
            if future_dates > 0:
                logger.warning(f"[!] Found {future_dates:,} records with future dates")
                logger.warning(f"    Today: {today}")
                logger.warning(f"    Max date in data: {self.df[date_column].max()}")
                self.issues.append({
                    'check': 'future_dates',
                    'column': date_column,
                    'issue_count': future_dates
                })
                return False
            else:
                logger.info(f"[PASS] No future dates found")
                return True
        except Exception as e:
            logger.error(f"Error checking future dates: {e}")
            return False
    
    def check_referential_integrity(self, foreign_key_col, reference_values):
        """Check if foreign keys exist in reference table"""
        logger.info("="*60)
        logger.info(f"CHECK 6: Referential Integrity for {foreign_key_col}")
        logger.info("="*60)
        
        if foreign_key_col not in self.df.columns:
            logger.warning(f"Column '{foreign_key_col}' not found in dataset")
            return False
        
        # Find foreign keys that don't exist in reference
        invalid_keys = ~self.df[foreign_key_col].isin(reference_values)
        invalid_count = invalid_keys.sum()
        
        if invalid_count > 0:
            logger.warning(f"[!] Found {invalid_count:,} records with invalid {foreign_key_col}")
            logger.warning(f"Sample invalid keys: {self.df[invalid_keys][foreign_key_col].unique()[:5]}")
            self.issues.append({
                'check': 'referential_integrity',
                'column': foreign_key_col,
                'issue_count': invalid_count
            })
            return False
        else:
            logger.info(f"[PASS] All {foreign_key_col} values are valid")
            return True
    
    def generate_summary(self):
        """Generate summary report of all checks"""
        logger.info("="*60)
        logger.info("DATA QUALITY CHECK SUMMARY")
        logger.info("="*60)
        logger.info(f"Table: {self.table_name}")
        logger.info(f"Total Records: {len(self.df):,}")
        logger.info(f"Total Issues Found: {len(self.issues)}")
        
        if len(self.issues) == 0:
            logger.info("[SUCCESS] All data quality checks passed!")
        else:
            logger.warning("[WARNING] Data quality issues detected:")
            for i, issue in enumerate(self.issues, 1):
                logger.warning(f"  {i}. {issue['check']} - {issue.get('column', 'N/A')}: {issue['issue_count']:,} issues")
        
        logger.info("="*60)
        
        return len(self.issues) == 0


def validate_fact_sales(df):
    """
    Run all data quality checks for fact_sales
    """
    logger.info("\n" + "="*60)
    logger.info("STARTING DATA QUALITY VALIDATION FOR FACT_SALES")
    logger.info("="*60 + "\n")
    
    checker = DataQualityChecker(df, 'fact_sales')
    
    # Check 1: Null values in critical columns
    checker.check_nulls([
        'sales_key',
        'customer_key',
        'product_key',
        'store_key',
        'time_key',
        'sales_amount',
        'quantity_sold'
    ])
    
    # Check 2: Duplicates
    checker.check_duplicates(['sales_key'])
    
    # Check 3: Negative values
    checker.check_negative_values([
        'sales_amount',
        'quantity_sold'
    ])
    
    # Check 4: Data ranges
    checker.check_data_ranges('quantity_sold', min_val=1, max_val=100)
    
    # Check 5: Future dates
    checker.check_future_dates('time_key')
    
    # Generate final summary
    all_passed = checker.generate_summary()
    
    return all_passed
