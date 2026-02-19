"""
Inspect Actual Database Schema
Shows the real structure of your tables
"""

from sqlalchemy import inspect
from db_connection import engine
import json

def inspect_table_schema(table_name):
    """Get detailed schema information for a table"""

    inspector = inspect(engine)

    if not inspector.has_table(table_name):
        print(f"❌ Table {table_name} does not exist")
        return None

    print(f"\n{'='*70}")
    print(f"📋 SCHEMA FOR: {table_name}")
    print('='*70)

    columns = inspector.get_columns(table_name)

    print(f"\nTotal Columns: {len(columns)}\n")

    print(f"{'Column Name':<25} {'Data Type':<20} {'Nullable':<10}")
    print('-'*70)

    schema_dict = {}

    for col in columns:
        col_name = col['name']
        col_type = str(col['type'])
        nullable = 'YES' if col['nullable'] else 'NO'

        print(f"{col_name:<25} {col_type:<20} {nullable:<10}")

        # Store in dictionary for code generation
        schema_dict[col_name] = col_type

    # Get primary keys
    pk_constraint = inspector.get_pk_constraint(table_name)
    if pk_constraint and pk_constraint['constrained_columns']:
        print(f"\n🔑 Primary Key(s): {', '.join(pk_constraint['constrained_columns'])}")

    # Get foreign keys
    fk_constraints = inspector.get_foreign_keys(table_name)
    if fk_constraints:
        print(f"\n🔗 Foreign Keys:")
        for fk in fk_constraints:
            print(f"  • {fk['constrained_columns']} → {fk['referred_table']}.{fk['referred_columns']}")

    print('='*70)

    return schema_dict


def generate_schema_code(table_name, schema_dict):
    """Generate Python code for schema validation"""

    print(f"\n📝 GENERATED CODE FOR SCHEMA VALIDATION:")
    print('='*70)
    print(f"\n# Expected schema for {table_name}")
    print(f"expected_{table_name}_schema = {{")

    for col_name, col_type in schema_dict.items():
        # Simplify type names
        simple_type = col_type.upper().split('(')[0]
        print(f"    '{col_name}': '{simple_type}',")

    print("}")
    print()


if __name__ == "__main__":
    print("="*70)
    print("🔍 DATABASE SCHEMA INSPECTOR")
    print("="*70)

    # Inspect all main tables
    tables = ['fact_sales', 'dim_customer', 'dim_product', 'dim_store', 'dim_time']

    all_schemas = {}

    for table in tables:
        schema = inspect_table_schema(table)
        if schema:
            all_schemas[table] = schema
            generate_schema_code(table, schema)

    print("\n" + "="*70)
    print("✅ SCHEMA INSPECTION COMPLETE")
    print("="*70)