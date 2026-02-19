import pandas as pd
import kagglehub
import os
from sqlalchemy import text
from db_connection import engine
from watermark_manager import WatermarkManager

def load_fact_sales_incremental():
    """Incremental fact table load - only new invoices"""
    
    print("\n" + "="*50)
    print("INCREMENTAL FACT SALES LOAD")
    print("="*50)
    
    watermark = WatermarkManager.get_last_watermark('fact_sales', 'RETAIL_OLTP')
    last_date = watermark['date'] if watermark else None
    
    print(f"📅 Last loaded date: {last_date}")
    
    dataset_path = kagglehub.dataset_download("tunguz/online-retail")
    csv_file = None
    for f in os.listdir(dataset_path):
        if f.lower().endswith('.csv'):
            csv_file = os.path.join(dataset_path, f)
            break
    
    df = pd.read_csv(csv_file, encoding='ISO-8859-1')
    df['StockCode'] = df['StockCode'].astype(str).str.strip()
    df['Country'] = df['Country'].astype(str).str.strip()
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%d/%m/%y %H:%M', errors='coerce')
    
    df = df[df['CustomerID'].notna()]
    df = df[df['Quantity'] > 0]
    df = df[df['InvoiceDate'].notna()]
    
    if last_date and last_date.year > 1900:
        df = df[df['InvoiceDate'].dt.date > last_date]
        print(f"🔍 Loading only records after {last_date}")
    
    if len(df) == 0:
        print("✅ No new sales records to load")
        return
    
    print(f"📦 New records to process: {len(df)}")
    
    with engine.begin() as conn:
        product_map = dict(conn.execute(text("SELECT product_id, product_key FROM dim_product")).fetchall())
        customer_map = dict(conn.execute(text(
            "SELECT customer_id, customer_key FROM dim_customer WHERE is_current = TRUE"
        )).fetchall())
        store_map = dict(conn.execute(text("SELECT region, store_key FROM dim_store")).fetchall())
    
    print(f"🔑 Loaded keys - Products: {len(product_map)}, Customers: {len(customer_map)}, Stores: {len(store_map)}")
    
    fact_rows = []
    for _, row in df.iterrows():
        pid = row['StockCode']
        cid = int(row['CustomerID'])
        region = row['Country']
        
        if pid not in product_map or cid not in customer_map or region not in store_map:
            continue
        
        fact_rows.append({
            'customer_key': customer_map[cid],
            'product_key': product_map[pid],
            'store_key': store_map[region],
            'time_key': int(row['InvoiceDate'].strftime('%Y%m%d')),
            'quantity_sold': int(row['Quantity']),
            'sales_amount': float(row['Quantity'] * row['UnitPrice']),
            'discount_amount': 0.0
        })
    
    print(f"✅ Prepared {len(fact_rows)} fact rows")
    
    if fact_rows:
        pd.DataFrame(fact_rows).to_sql('fact_sales', engine, if_exists='append', index=False)
        print(f"✅ fact_sales loaded successfully. Rows inserted: {len(fact_rows)}")
        
        max_date = df['InvoiceDate'].max().date()
        max_invoice = df['InvoiceNo'].max()
        WatermarkManager.update_watermark(
            'fact_sales',
            source_system='RETAIL_OLTP',
            new_date=max_date,
            invoice_number=max_invoice,
            records_processed=len(fact_rows)
        )
        print(f"📊 Watermark updated: {max_date}, Invoice: {max_invoice}")

if __name__ == "__main__":
    load_fact_sales_incremental()
