from datetime import datetime
from load_product_incremental import load_product_incremental
from load_customer_multisource import load_customer_multisource_incremental
from load_fact_sales_incremental import load_fact_sales_incremental
from sqlalchemy import text
from db_connection import engine

def load_time_dimension_safe():
    """Wrapper that only loads time dimension if needed"""
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dim_time")).scalar()
    
    if count == 0:
        print("\n" + "="*50)
        print("TIME DIMENSION LOAD (Initial)")
        print("="*50)
        from load_dimensions import load_time_dimension
        load_time_dimension()
    else:
        print("\n" + "="*50)
        print("TIME DIMENSION - Skipping (already loaded)")
        print("="*50)
        print(f"✅ dim_time already has {count} records")

def run_multisource_incremental_etl():
    """Master ETL pipeline with multi-source integration and incremental loading"""
    
    print("\n" + "="*70)
    print("🚀 MULTI-SOURCE INCREMENTAL ETL PIPELINE")
    print(f"⏰ Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    try:
        print("\n📂 PHASE 1: DIMENSION LOADING")
        print("-" * 70)
        
        load_product_incremental()
        load_customer_multisource_incremental()
        load_time_dimension_safe()  # Use safe wrapper
        
        print("\n📊 PHASE 2: FACT TABLE LOADING")
        print("-" * 70)
        load_fact_sales_incremental()
        
        print("\n📈 PHASE 3: ETL SUMMARY")
        print("-" * 70)
        
        with engine.connect() as conn:
            watermarks = conn.execute(
                text("""
                    SELECT table_name, source_system, last_loaded_date, 
                           records_processed, records_rejected
                    FROM etl_watermark
                    ORDER BY table_name, source_system
                """)
            ).fetchall()
            
            print("\n🏷️  Watermark Status:")
            for w in watermarks:
                print(f"   {w[0]:20} [{w[1]:25}] → {w[2]} | Processed: {w[3]:6} | Rejected: {w[4]}")
            
            sources = conn.execute(
                text("""
                    SELECT source_name, source_type, last_successful_load, is_active
                    FROM data_source_registry
                """)
            ).fetchall()
            
            print("\n🔌 Data Source Health:")
            for s in sources:
                status = "🟢 ACTIVE" if s[3] else "🔴 INACTIVE"
                print(f"   {status} {s[0]:30} ({s[1]}) → Last Load: {s[2]}")
        
        print("\n" + "="*70)
        print("✅ MULTI-SOURCE INCREMENTAL ETL COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ETL FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_multisource_incremental_etl()
