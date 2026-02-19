import pandas as pd
import kagglehub
import os
from db_connection import engine

# Download dataset
dataset_path = kagglehub.dataset_download("tunguz/online-retail")

# Locate CSV
csv_file = None
for file in os.listdir(dataset_path):
    if file.lower().endswith(".csv"):
        csv_file = os.path.join(dataset_path, file)
        break

df = pd.read_csv(csv_file, encoding="ISO-8859-1")

# Select product-related columns
product_df = df[["StockCode", "Description"]].drop_duplicates()

product_df.columns = ["product_id", "product_name"]

# Optional derived attributes
product_df["category"] = "General"
product_df["sub_category"] = "General"

# Load into dim_product
product_df.to_sql("dim_product", engine, if_exists="append", index=False)

print("dim_product loaded successfully")
