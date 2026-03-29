"""
load_store_dimension.py — Populate dim_store from the Kaggle dataset.

Each unique Country in the online-retail dataset becomes a store/region row.
Safe to run multiple times (skips already-loaded regions).

Usage:
    python load_store_dimension.py
"""

import os
import kagglehub
import pandas as pd
from sqlalchemy import text
from db_connection import engine


def load_store_dimension():
    print("\n" + "=" * 50)
    print("STORE DIMENSION LOAD")
    print("=" * 50)

    dataset_path = kagglehub.dataset_download("tunguz/online-retail")
    csv_file = next(
        (os.path.join(dataset_path, f) for f in os.listdir(dataset_path)
         if f.lower().endswith(".csv")),
        None,
    )
    if csv_file is None:
        raise FileNotFoundError("No CSV found in Kaggle dataset download.")

    df = pd.read_csv(csv_file, encoding="ISO-8859-1")
    countries = df["Country"].dropna().unique()

    with engine.begin() as conn:
        existing = {
            row[0]
            for row in conn.execute(
                text("SELECT region FROM dim_store")
            ).fetchall()
        }

        inserted = 0
        for country in countries:
            if country in existing:
                continue
            conn.execute(
                text("""
                    INSERT INTO dim_store (store_id, store_name, region, country)
                    VALUES (:sid, :name, :region, :country)
                """),
                {
                    "sid": country[:50],
                    "name": country[:200],
                    "region": country,
                    "country": country,
                },
            )
            inserted += 1

    print(f"dim_store loaded: {inserted} new regions inserted "
          f"({len(countries)} total countries in dataset).")


if __name__ == "__main__":
    load_store_dimension()
