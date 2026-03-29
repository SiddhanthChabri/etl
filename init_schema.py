"""
init_schema.py — Idempotent schema initialisation.

Creates all dimension/fact tables and the ETL watermark table if they don't
already exist.  Safe to run multiple times.

Usage:
    python init_schema.py
"""

from sqlalchemy import text
from db_connection import engine

SCHEMA_SQL = """
-- ── ETL housekeeping ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_watermark (
    id                   SERIAL PRIMARY KEY,
    table_name           VARCHAR(100) NOT NULL,
    source_system        VARCHAR(50),
    last_loaded_timestamp TIMESTAMP,
    last_loaded_date     DATE,
    last_invoice_number  VARCHAR(50),
    records_processed    INTEGER DEFAULT 0,
    records_rejected     INTEGER DEFAULT 0,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_name, source_system)
);

CREATE TABLE IF NOT EXISTS data_source_registry (
    id                   SERIAL PRIMARY KEY,
    source_name          VARCHAR(100) UNIQUE NOT NULL,
    source_type          VARCHAR(50) DEFAULT 'BATCH',
    last_successful_load TIMESTAMP,
    is_active            BOOLEAN DEFAULT TRUE,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE data_source_registry ADD COLUMN IF NOT EXISTS source_type VARCHAR(50) DEFAULT 'BATCH';
ALTER TABLE data_source_registry ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

INSERT INTO data_source_registry (source_name, source_type, is_active)
VALUES ('RETAIL_OLTP', 'BATCH', TRUE), ('CUSTOMER_DEMOGRAPHICS', 'BATCH', TRUE)
ON CONFLICT (source_name) DO NOTHING;

-- ── Dimensions ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key         SERIAL PRIMARY KEY,
    customer_id          INTEGER NOT NULL,
    customer_name        VARCHAR(200),
    city                 VARCHAR(100),
    state                VARCHAR(100),
    email                VARCHAR(200),
    phone                VARCHAR(50),
    postal_code          VARCHAR(20),
    age_group            VARCHAR(50),
    customer_segment     VARCHAR(100),
    loyalty_tier         VARCHAR(50),
    registration_date    DATE,
    effective_date       DATE,
    expiry_date          DATE,
    is_current           BOOLEAN DEFAULT TRUE,
    source_system        VARCHAR(50),
    last_updated_source  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_id    VARCHAR(50),
    product_name  VARCHAR(500),
    category      VARCHAR(100),
    subcategory   VARCHAR(100),
    sub_category  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key    INTEGER PRIMARY KEY,
    date        DATE,
    day         INTEGER,
    month       INTEGER,
    quarter     INTEGER,
    year        INTEGER,
    day_of_week VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key  SERIAL PRIMARY KEY,
    store_id   VARCHAR(50),
    store_name VARCHAR(200),
    region     VARCHAR(100),
    country    VARCHAR(100)
);

-- ── Fact table ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key       SERIAL PRIMARY KEY,
    customer_key    INTEGER REFERENCES dim_customer(customer_key),
    product_key     INTEGER REFERENCES dim_product(product_key),
    store_key       INTEGER REFERENCES dim_store(store_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    quantity_sold   INTEGER,
    sales_amount    NUMERIC(12, 2),
    discount_amount NUMERIC(12, 2) DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def init_schema():
    print("Initialising database schema...")
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
    print("Schema ready.")


if __name__ == "__main__":
    init_schema()
