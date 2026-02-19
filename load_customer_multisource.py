import pandas as pd
import kagglehub
import os
from datetime import date
from sqlalchemy import text
from db_connection import engine
from watermark_manager import WatermarkManager
from config import CUSTOMER_DEMOGRAPHICS_FILE  # ADD THIS LINE


def reconcile_customer_data(oltp_data, demographics_data):
    """Merge and reconcile customer data from two sources"""
    reconciliation_log = []
    
    merged = oltp_data.merge(
        demographics_data, 
        on='customer_id', 
        how='left',
        suffixes=('_oltp', '_demo')
    )
    
    for idx, row in merged.iterrows():
        cid = row['customer_id']
        
        if pd.notna(row.get('Country')) and pd.notna(row.get('city')):
            if row['Country'] != row.get('state_oltp', row['Country']):
                reconciliation_log.append({
                    'customer_id': cid,
                    'field_name': 'country',
                    'source1_value': row.get('state_oltp'),
                    'source2_value': row['Country'],
                    'resolved_value': row['Country'],
                    'resolution_rule': 'OLTP_PRIORITY_RECENT_TRANSACTION'
                })
    
    merged['state'] = merged.apply(
        lambda x: x['Country'] if pd.notna(x.get('Country')) else x.get('state_oltp'),
        axis=1
    )
    
    merged['customer_name'] = merged['customer_name'].fillna('Unknown')
    merged['source_system'] = merged.apply(
        lambda x: 'MULTI_SOURCE' if pd.notna(x.get('email')) else 'RETAIL_OLTP',
        axis=1
    )
    
    return merged, reconciliation_log


def load_customer_multisource_incremental():
    """Multi-source incremental customer dimension load with SCD Type-2"""
    
    print("\n" + "="*60)
    print("🔗 MULTI-SOURCE CUSTOMER DIMENSION LOAD (SCD-2)")
    print("="*60)
    
    print("\n📊 Loading Source 1: RETAIL_OLTP (Kaggle)")
    watermark_oltp = WatermarkManager.get_last_watermark('dim_customer', 'RETAIL_OLTP')
    last_date_oltp = watermark_oltp['date'] if watermark_oltp else None
    print(f"   Last loaded: {last_date_oltp}")
    
    dataset_path = kagglehub.dataset_download("tunguz/online-retail")
    csv_file = None
    for file in os.listdir(dataset_path):
        if file.lower().endswith('.csv'):
            csv_file = os.path.join(dataset_path, file)
            break
    
    df_oltp = pd.read_csv(csv_file, encoding='ISO-8859-1')
    df_oltp = df_oltp[df_oltp['CustomerID'].notna()]
    df_oltp['InvoiceDate'] = pd.to_datetime(df_oltp['InvoiceDate'], format='%d/%m/%y %H:%M', errors='coerce')
    df_oltp = df_oltp[df_oltp['InvoiceDate'].notna()]
    
    if last_date_oltp and last_date_oltp.year > 1900:
        df_oltp = df_oltp[df_oltp['InvoiceDate'].dt.date > last_date_oltp]
    
    customers_oltp = df_oltp[['CustomerID', 'Country']].drop_duplicates()
    customers_oltp.columns = ['customer_id', 'Country']
    customers_oltp['customer_id'] = customers_oltp['customer_id'].astype(int)
    
    print(f"   ✅ OLTP Records: {len(customers_oltp)}")
    
    print("\n📊 Loading Source 2: CUSTOMER_DEMOGRAPHICS (CSV)")
    demographics_file = CUSTOMER_DEMOGRAPHICS_FILE
    print(f"   Looking for file: {demographics_file}")

    
    if os.path.exists(demographics_file):
        df_demo = pd.read_csv(demographics_file)
        df_demo['registration_date'] = pd.to_datetime(df_demo['registration_date'], errors='coerce')
        print(f"   ✅ Demographics Records: {len(df_demo)}")
    else:
        print("   ⚠️  Demographics file not found - using OLTP only")
        df_demo = pd.DataFrame()
    
    print("\n🔄 Reconciling data from multiple sources...")
    
    if not df_demo.empty:
        merged_customers, recon_log = reconcile_customer_data(customers_oltp, df_demo)
        
        if recon_log:
            pd.DataFrame(recon_log).to_sql(
                'data_reconciliation_log', 
                engine, 
                if_exists='append', 
                index=False
            )
            print(f"   ⚠️  Reconciled {len(recon_log)} conflicts")
    else:
        merged_customers = customers_oltp
        merged_customers['customer_name'] = 'Unknown'
        merged_customers['state'] = merged_customers['Country']
        merged_customers['source_system'] = 'RETAIL_OLTP'
    
    print("\n💾 Loading to data warehouse with SCD Type-2 logic...")
    
    today = date.today()
    rows_inserted = 0
    rows_updated = 0
    rows_skipped = 0
    
    with engine.begin() as conn:
        for _, row in merged_customers.iterrows():
            customer_id = int(row['customer_id'])
            country = row['state']
            source = row.get('source_system', 'RETAIL_OLTP')
            
            customer_name = row.get('customer_name', 'Unknown')
            email = row.get('email')
            phone = row.get('phone')
            city = row.get('city')
            postal_code = row.get('postal_code')
            age_group = row.get('age_group')
            segment = row.get('customer_segment')
            loyalty = row.get('loyalty_tier')
            reg_date = row.get('registration_date')
            
            result = conn.execute(
                text("""
                    SELECT customer_key, state, email, customer_segment
                    FROM dim_customer 
                    WHERE customer_id = :cid AND is_current = TRUE
                """),
                {"cid": customer_id}
            ).fetchone()
            
            if result is None:
                conn.execute(
                    text("""
                        INSERT INTO dim_customer 
                        (customer_id, customer_name, city, state, email, phone, 
                         postal_code, age_group, customer_segment, loyalty_tier, 
                         registration_date, effective_date, expiry_date, is_current, 
                         source_system, last_updated_source)
                        VALUES (:cid, :name, :city, :state, :email, :phone, 
                                :postal, :age, :segment, :loyalty, :reg_date,
                                :eff, '9999-12-31', TRUE, :source, :source)
                    """),
                    {
                        "cid": customer_id, "name": customer_name, "city": city,
                        "state": country, "email": email, "phone": phone,
                        "postal": postal_code, "age": age_group, "segment": segment,
                        "loyalty": loyalty, "reg_date": reg_date, "eff": today,
                        "source": source
                    }
                )
                rows_inserted += 1
                
            else:
                changed = (result[1] != country or 
                          result[2] != email or 
                          result[3] != segment)
                
                if changed:
                    conn.execute(
                        text("""
                            UPDATE dim_customer 
                            SET expiry_date = :exp, is_current = FALSE 
                            WHERE customer_key = :ck
                        """),
                        {"exp": today, "ck": result[0]}
                    )
                    
                    conn.execute(
                        text("""
                            INSERT INTO dim_customer 
                            (customer_id, customer_name, city, state, email, phone,
                             postal_code, age_group, customer_segment, loyalty_tier,
                             registration_date, effective_date, expiry_date, is_current,
                             source_system, last_updated_source)
                            VALUES (:cid, :name, :city, :state, :email, :phone,
                                    :postal, :age, :segment, :loyalty, :reg_date,
                                    :eff, '9999-12-31', TRUE, :source, :source)
                        """),
                        {
                            "cid": customer_id, "name": customer_name, "city": city,
                            "state": country, "email": email, "phone": phone,
                            "postal": postal_code, "age": age_group, "segment": segment,
                            "loyalty": loyalty, "reg_date": reg_date, "eff": today,
                            "source": source
                        }
                    )
                    rows_updated += 1
                else:
                    rows_skipped += 1
    
    print(f"✅ Customers - Inserted: {rows_inserted}, Updated (SCD): {rows_updated}, Skipped: {rows_skipped}")
    
    if not df_oltp.empty:
        max_date_oltp = df_oltp['InvoiceDate'].max().date()
        WatermarkManager.update_watermark(
            'dim_customer', 
            source_system='RETAIL_OLTP',
            new_date=max_date_oltp,
            records_processed=rows_inserted + rows_updated
        )
    
    if not df_demo.empty:
        WatermarkManager.update_watermark(
            'dim_customer',
            source_system='CUSTOMER_DEMOGRAPHICS',
            new_date=date.today(),
            records_processed=len(df_demo)
        )
    
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE data_source_registry 
                SET last_successful_load = CURRENT_TIMESTAMP
                WHERE source_name IN ('RETAIL_OLTP', 'CUSTOMER_DEMOGRAPHICS')
            """)
        )

if __name__ == "__main__":
    load_customer_multisource_incremental()
