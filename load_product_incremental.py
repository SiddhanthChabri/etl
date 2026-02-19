import pandas as pd
import kagglehub
import os
from sqlalchemy import text
from db_connection import engine
from watermark_manager import WatermarkManager

def load_product_incremental():
    """Load only new products since last run"""
    
    print("\n" + "="*50)
    print("INCREMENTAL PRODUCT DIMENSION LOAD")
    print("="*50)
    
    watermark = WatermarkManager.get_last_watermark('dim_product', 'RETAIL_OLTP')
    last_date = watermark['date'] if watermark else None
    print(f"📅 Last loaded date: {last_date}")
    
    dataset_path = kagglehub.dataset_download("tunguz/online-retail")
    csv_file = None
    for file in os.listdir(dataset_path):
        if file.lower().endswith('.csv'):
            csv_file = os.path.join(dataset_path, file)
            break
    
    df = pd.read_csv(csv_file, encoding='ISO-8859-1')
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%d/%m/%y %H:%M', errors='coerce')
    df = df[df['InvoiceDate'].notna()]
    
    if last_date and last_date.year > 1900:
        df = df[df['InvoiceDate'].dt.date > last_date]
        print(f"🔍 Filtering records after {last_date}")
    
    if len(df) == 0:
        print("✅ No new products to load")
        return
    
    product_df = df[['StockCode', 'Description']].drop_duplicates()
    product_df.columns = ['product_id', 'product_name']
    product_df['product_id'] = product_df['product_id'].astype(str).str.strip()
    product_df['category'] = 'General'
    product_df['subcategory'] = 'General'
    
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT product_id FROM dim_product", conn)
    
    new_products = product_df[~product_df['product_id'].isin(existing['product_id'])]
    
    if len(new_products) > 0:
        new_products.to_sql('dim_product', engine, if_exists='append', index=False)
        print(f"✅ Inserted {len(new_products)} new products")
        
        max_date = df['InvoiceDate'].max().date()
        WatermarkManager.update_watermark(
            'dim_product', 
            source_system='RETAIL_OLTP',
            new_date=max_date,
            records_processed=len(new_products)
        )
    else:
        print("✅ No new products found")

if __name__ == "__main__":
    load_product_incremental()
