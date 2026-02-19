import pandas as pd
import kagglehub
import os
from sqlalchemy import text
from db_connection import engine
from watermark_manager import WatermarkManager

def load_time_dimension():
    """Load only new dates to dim_time (incremental)"""
    
    print("\n" + "="*50)
    print("INCREMENTAL TIME DIMENSION LOAD")
    print("="*50)
    
    # Get watermark
    watermark = WatermarkManager.get_last_watermark('dim_time', 'RETAIL_OLTP')
    last_date = watermark['date'] if watermark else None
    print(f"📅 Last loaded date: {last_date}")
    
    # Download dataset
    dataset_path = kagglehub.dataset_download("tunguz/online-retail")
    csv_file = None
    for f in os.listdir(dataset_path):
        if f.lower().endswith('.csv'):
            csv_file = os.path.join(dataset_path, f)
            break
    
    # Load data
    df = pd.read_csv(csv_file, encoding='ISO-8859-1')
    
    # Parse dates
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%d/%m/%y %H:%M', errors='coerce')
    df = df[df['InvoiceDate'].notna()]
    
    # Filter only new dates if incremental
    if last_date and last_date.year > 1900:
        df = df[df['InvoiceDate'].dt.date > last_date]
        print(f"🔍 Filtering dates after {last_date}")
    
    if len(df) == 0:
        print("✅ No new dates to load")
        return
    
    # Build time dimension
    time_df = pd.DataFrame()
    time_df['date'] = df['InvoiceDate'].dt.date.drop_duplicates()
    time_df['time_key'] = time_df['date'].apply(lambda d: int(d.strftime('%Y%m%d')))
    time_df['day'] = time_df['date'].apply(lambda d: d.day)
    time_df['month'] = time_df['date'].apply(lambda d: d.month)
    time_df['quarter'] = time_df['date'].apply(lambda d: (d.month - 1) // 3 + 1)
    time_df['year'] = time_df['date'].apply(lambda d: d.year)
    time_df['day_of_week'] = time_df['date'].apply(lambda d: d.strftime('%A'))
    
    # Get existing time_keys from database
    with engine.connect() as conn:
        existing_keys = pd.read_sql(
            "SELECT time_key FROM dim_time",
            conn
        )
    
    # Filter only truly new dates
    new_dates = time_df[~time_df['time_key'].isin(existing_keys['time_key'])]
    
    if len(new_dates) > 0:
        # Insert only new dates
        new_dates.to_sql('dim_time', engine, if_exists='append', index=False)
        print(f"✅ Inserted {len(new_dates)} new dates into dim_time")
        
        # Update watermark
        max_date = time_df['date'].max()
        WatermarkManager.update_watermark(
            'dim_time',
            source_system='RETAIL_OLTP',
            new_date=max_date,
            records_processed=len(new_dates)
        )
    else:
        print("✅ No new dates to insert (all already exist)")

if __name__ == "__main__":
    load_time_dimension()
