"""
Quick verification script to test kagglehub connection
Run this first to ensure everything works
"""

import kagglehub
import os
import pandas as pd

print("=" * 70)
print("🔍 VERIFYING KAGGLEHUB SETUP")
print("=" * 70)

try:
    # Download/access dataset
    print("\n📥 Accessing dataset from Kaggle...")
    path = kagglehub.dataset_download("tunguz/online-retail")

    print(f"✅ Success! Dataset path: {path}")

    # List files
    print("\n📂 Files in dataset:")
    files = os.listdir(path)
    for f in files:
        file_path = os.path.join(path, f)
        if os.path.isfile(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  • {f} ({size_mb:.2f} MB)")

    # Try loading first file
    data_file = None
    for f in files:
        if f.endswith(('.xlsx', '.xls', '.csv')):
            data_file = os.path.join(path, f)
            break

    if data_file:
        print(f"\n📖 Testing data load: {os.path.basename(data_file)}")

        if data_file.endswith('.xlsx') or data_file.endswith('.xls'):
            df = pd.read_excel(data_file, nrows=5)
        else:
            df = pd.read_csv(data_file, encoding='latin-1', nrows=5)

        print(f"✅ Successfully loaded sample data")
        print(f"\n📋 Columns: {list(df.columns)}")
        print(f"\n📊 Sample (first 5 rows):")
        print(df)

        print("\n" + "=" * 70)
        print("✅ VERIFICATION SUCCESSFUL!")
        print("=" * 70)
        print("\nYou're ready to run:")
        print("  python test_incremental_online_retail.py")
    else:
        print("\n⚠️  No data file found in dataset")

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\n" + "=" * 70)
    print("TROUBLESHOOTING")
    print("=" * 70)
    print("\n1. Upgrade kagglehub:")
    print("   pip install --upgrade kagglehub")
    print("\n2. Check internet connection")
    print("\n3. Verify Kaggle account access")
