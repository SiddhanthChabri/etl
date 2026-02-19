from db_connection import engine
import pandas as pd

print("\n" + "="*70)
print("📊 EXPORTING DATA WAREHOUSE TO EXCEL")
print("="*70)

# Read tables
print("\n📥 Reading data from PostgreSQL...")
fact_sales = pd.read_sql_table('fact_sales', engine)
dim_customer = pd.read_sql_table('dim_customer', engine)
dim_product = pd.read_sql_table('dim_product', engine)
dim_store = pd.read_sql_table('dim_store', engine)
dim_time = pd.read_sql_table('dim_time', engine)

print("✅ Data loaded successfully")

# Export to Excel
output_file = '../Retail_DW_Data.xlsx'

print(f"\n💾 Exporting to {output_file}...")

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    fact_sales.to_excel(writer, sheet_name='fact_sales', index=False)
    dim_customer.to_excel(writer, sheet_name='dim_customer', index=False)
    dim_product.to_excel(writer, sheet_name='dim_product', index=False)
    dim_store.to_excel(writer, sheet_name='dim_store', index=False)
    dim_time.to_excel(writer, sheet_name='dim_time', index=False)

print("\n" + "="*70)
print("✅ EXPORT COMPLETE!")
print("="*70)
print(f"\n📊 File created: {output_file}")
print(f"\n📈 Row counts:")
print(f"   - fact_sales:    {len(fact_sales):,} rows")
print(f"   - dim_customer:  {len(dim_customer):,} rows")
print(f"   - dim_product:   {len(dim_product):,} rows")
print(f"   - dim_store:     {len(dim_store):,} rows")
print(f"   - dim_time:      {len(dim_time):,} rows")
print("\n" + "="*70)
print("🎯 Next Steps:")
print("   1. Open Retail_DW_Data.xlsx in Excel")
print("   2. Use Power Pivot to create relationships")
print("   3. Build dashboard with PivotTables & Charts")
print("="*70 + "\n")
