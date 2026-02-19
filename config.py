import os

# Project root directory (one level up from etl folder)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data paths
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
CUSTOMER_DEMOGRAPHICS_FILE = os.path.join(DATA_DIR, 'customer_demographics.csv')
ONLINE_RETAIL_FILE = os.path.join(DATA_DIR, 'online_retail.csv')  # For future use

# Database configuration
DB_CONFIG = {
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': '5432',
    'database': 'retail_dw'
}

# Print paths for verification (optional)
if __name__ == "__main__":
    print(f"📁 Project Root: {PROJECT_ROOT}")
    print(f"📁 Data Directory: {DATA_DIR}")
    print(f"📄 Demographics File: {CUSTOMER_DEMOGRAPHICS_FILE}")
    print(f"✅ Config loaded successfully")
