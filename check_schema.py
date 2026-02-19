"""
Check Actual Database Schema
"""

from sqlalchemy import text
from db_connection import engine

def check_table_schema(table_name):
    """Check actual columns in a table"""
    query = text(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = :table_name
        ORDER BY ordinal_position
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {'table_name': table_name})
        columns = result.fetchall()

    return columns

print("="*70)
print("🔍 CHECKING DATABASE SCHEMA")
print("="*70)

# Check dim_customer
print("\n📋 dim_customer columns:")
customer_cols = check_table_schema('dim_customer')
for col in customer_cols:
    print(f"  • {col[0]:<30} {col[1]}")

# Check dim_product
print("\n📋 dim_product columns:")
product_cols = check_table_schema('dim_product')
for col in product_cols:
    print(f"  • {col[0]:<30} {col[1]}")

# Check fact_sales
print("\n📋 fact_sales columns:")
sales_cols = check_table_schema('fact_sales')
for col in sales_cols:
    print(f"  • {col[0]:<30} {col[1]}")

print("\n" + "="*70)