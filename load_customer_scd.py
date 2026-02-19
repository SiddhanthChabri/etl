import pandas as pd
import kagglehub
import os
from datetime import date
from sqlalchemy import text
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

# Clean data
df = df[df["CustomerID"].notna()]

customers = df[["CustomerID", "Country"]].drop_duplicates()

today = date.today()

with engine.begin() as conn:
    for _, row in customers.iterrows():
        customer_id = int(row["CustomerID"])
        country = row["Country"]

        # Check existing current record
        result = conn.execute(text("""
            SELECT customer_key, state
            FROM dim_customer
            WHERE customer_id = :cid AND is_current = TRUE
        """), {"cid": customer_id}).fetchone()

        if result is None:
            # Insert new customer
            conn.execute(text("""
                INSERT INTO dim_customer
                (customer_id, customer_name, city, state,
                 effective_date, expiry_date, is_current)
                VALUES
                (:cid, 'Unknown', NULL, :state,
                 :eff, '9999-12-31', TRUE)
            """), {
                "cid": customer_id,
                "state": country,
                "eff": today
            })
        else:
            # Simulate change (SCD Type-2 trigger)
            if result.state != country:
                # Expire old record
                conn.execute(text("""
                    UPDATE dim_customer
                    SET expiry_date = :exp, is_current = FALSE
                    WHERE customer_key = :ck
                """), {
                    "exp": today,
                    "ck": result.customer_key
                })

                # Insert new record
                conn.execute(text("""
                    INSERT INTO dim_customer
                    (customer_id, customer_name, city, state,
                     effective_date, expiry_date, is_current)
                    VALUES
                    (:cid, 'Unknown', NULL, :state,
                     :eff, '9999-12-31', TRUE)
                """), {
                    "cid": customer_id,
                    "state": country,
                    "eff": today
                })

print("dim_customer loaded with SCD Type-2 logic")
