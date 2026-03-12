# Retail Analytics Data Warehouse

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green?logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-orange?logo=scikit-learn)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-yellow)

**9-Credit Major Project | B.Tech 8th Semester**

A complete end-to-end retail analytics platform — incremental ETL pipeline, PostgreSQL star-schema data warehouse, 20+ analytics & ML modules, 115+ REST API endpoints, and a live multi-tab web dashboard.

</div>

---

## System Architecture

```
Raw Data (CSV / Excel)
        │
        ▼
┌──────────────────────────────────────────┐
│             ETL PIPELINE                 │
│  Extract → Validate → Transform → Load   │
│  Data Quality Checks · Performance Mon.  │
│  Incremental Loading (watermark-based)   │
│  SCD Type 2 · Audit Columns              │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│        POSTGRESQL DATA WAREHOUSE         │
│  Star Schema: fact_sales + dim tables    │
│  dim_customer · dim_product · dim_store  │
│  dim_time                                │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│           ANALYTICS ENGINE               │
│  20+ modules  ·  CSV result files        │
│  Trained ML models (.pkl + metadata)     │
│  APScheduler: nightly refresh at 02:00   │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│           FASTAPI REST API               │
│  115+ endpoints  ·  20 routers           │
│  Swagger UI  ·  Structured JSON logging  │
│  In-memory CSV cache (mtime-keyed)       │
└───────────────┬──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│         LIVE WEB DASHBOARD               │
│  Multi-tab Chart.js + Plotly dashboard   │
│  Drill-down charts  ·  CSV export        │
│  SHAP explainability viewer              │
└──────────────────────────────────────────┘
```

---

## Analytics Modules

| # | Module | Technique | Output File |
|---|--------|-----------|-------------|
| 1 | **RFM Analysis** | Recency / Frequency / Monetary scoring | `rfm_analysis_results.csv` |
| 2 | **ABC Analysis** | Revenue-contribution product classification | `abc_analysis_results.csv` |
| 3 | **CLV Analysis** | Customer Lifetime Value scoring & tiers | `clv_analysis_results.csv` |
| 4 | **CLV Prediction** | BG/NBD + Gamma-Gamma probabilistic model | `clv_predictions.csv` |
| 5 | **Cohort Analysis** | Retention heatmap by acquisition cohort | `cohort_retention_matrix.csv` |
| 6 | **Market Basket** | Apriori association rule mining | `market_basket_results.csv` |
| 7 | **ML Churn Prediction** | RandomForest · AUC-ROC · 4 risk tiers · SHAP | `churn_predictions.csv` |
| 8 | **Demand Forecasting** | Per-product time-series (Prophet/ARIMA) | `demand_forecast_results.csv` |
| 9 | **Price Elasticity** | Log-log OLS regression per product | `price_elasticity_results.csv` |
| 10 | **Pricing Optimizer** | Lerner-condition revenue-maximising prices | `pricing_optimizer_results.csv` |
| 11 | **Customer Journey** | Markov-chain tier transition analysis | `journey_transitions.csv` |
| 12 | **Geographic Analysis** | Revenue & growth by region / country | `geographic_results.csv` |
| 13 | **Anomaly Detection** | IQR + rolling Z-score on daily revenue | `anomaly_results.csv` |
| 14 | **Seasonality Analysis** | STL decomposition · YoY · heatmap | `seasonality_results.csv` |
| 15 | **Customer Segmentation** | K-Means + PCA (7 features, k=2) | `customer_segments_km.csv` |
| 16 | **Store Performance** | Tier-based multi-metric store ranking | `store_performance_results.csv` |
| 17 | **Product Recommendations** | Item-item collaborative filtering (cosine) | `product_recommendations.csv` |
| 18 | **Inventory Optimization** | EOQ + safety-stock + reorder point | `inventory_optimization_results.csv` |
| 19 | **Model Drift Monitor** | PSI-based feature drift detection | `drift_results.csv` |
| 20 | **SHAP Explainability** | TreeExplainer waterfall + summary plots | `static/shap/` |

---

## Quick Start

### Option A — Docker (recommended)

```bash
cp .env.example .env          # fill in DB credentials
docker compose up --build     # starts app + postgres:15
```

Dashboard: `http://localhost:8000/dashboard`

### Option B — Local

#### 1. Install dependencies
```bash
pip install -r requirements.txt
```

#### 2. Configure database
Create `db_connection.py` with your PostgreSQL credentials:
```python
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "retail_dwh"
DB_USER     = "your_user"
DB_PASSWORD = "your_password"
```

#### 3. Run ETL pipeline
```bash
python run_etl.py
```

#### 4. Run all analytics
```bash
python run_all.py             # full run (all phases)
python run_all.py --skip-phase1  # reuse existing CSVs, re-run ML only
```

#### 5. Start the API server
```bash
uvicorn main:app --reload --port 8000
```

#### 6. Run tests
```bash
pytest tests/
```

---

## API Endpoints (115+)

### Core
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Landing page & API explorer |
| GET | `/dashboard` | Live analytics dashboard |
| GET | `/docs` | Interactive Swagger UI |
| GET | `/health` | Health check — all data files + scheduler status |

### RFM Analysis `/api/rfm/`
| Endpoint | Description |
|----------|-------------|
| `GET /customers` | All customers with RFM scores & segments |
| `GET /customer/{id}` | Single customer RFM detail |
| `GET /segments/summary` | Segment distribution & revenue stats |
| `GET /segments/list` | All unique segment labels |
| `GET /top` | Top N customers by monetary value |

### ABC Analysis `/api/abc/`
| Endpoint | Description |
|----------|-------------|
| `GET /products` | All products with ABC class |
| `GET /product/{id}` | Single product ABC detail |
| `GET /classes/summary` | Class distribution & cumulative revenue |
| `GET /top` | Top N products by revenue |
| `GET /categories` | Revenue by product category |

### CLV Analysis `/api/clv/`
| Endpoint | Description |
|----------|-------------|
| `GET /customers` | All CLV scores |
| `GET /customer/{id}` | Single customer CLV |
| `GET /segments/summary` | CLV segment breakdown |
| `GET /top` | Top N customers by CLV |

### CLV Prediction `/api/clv-predict/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Model summary & segment distribution |
| `GET /customers` | All predicted CLVs (paginated) |
| `GET /customer/{id}` | Forward CLV for one customer |
| `GET /top` | Top N by predicted CLV |
| `GET /segment/{segment}` | Customers in a specific CLV tier |

### Churn Prediction `/api/churn/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Model metrics (AUC, accuracy) + risk distribution |
| `GET /customer/{id}` | Churn probability & risk tier |
| `GET /risk/{tier}` | All customers in a risk tier |
| `GET /top-at-risk` | Top N highest-risk customers |
| `POST /predict` | Predict churn for a custom input payload |

### SHAP Explainability `/api/explain/`
| Endpoint | Description |
|----------|-------------|
| `GET /importance` | Global feature importance rankings |
| `GET /bar-data` | Bar chart data for feature importance |
| `GET /summary-data` | Beeswarm summary data |
| `GET /customer/{id}/waterfall` | Per-customer SHAP waterfall values |
| `GET /customer/{id}` | SHAP explanation for one customer |

### Demand Forecasting `/api/forecast/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Forecast summary & model metadata |
| `GET /products` | All product forecasts (paginated) |
| `GET /product/{id}` | Full time series for one product |
| `GET /top` | Top N by expected demand |
| `GET /category/{cat}` | Aggregate forecast for a category |
| `GET /categories` | All forecastable categories |
| `GET /accuracy` | Model accuracy metrics |

### Price Elasticity `/api/elasticity/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Elasticity distribution summary |
| `GET /products` | All products with elasticity (paginated) |
| `GET /product/{id}` | Elasticity for one product |
| `GET /type/{type}` | Products by elasticity type |
| `GET /top/elastic` | Most price-sensitive products |
| `GET /top/inelastic` | Least price-sensitive products |
| `GET /categories` | Avg elasticity by category |
| `GET /types` | All elasticity type labels |

### Pricing Optimizer `/api/pricing/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Optimisation summary & top opportunities |
| `GET /products` | All pricing recommendations (paginated) |
| `GET /product/{id}` | Recommendation for one product |
| `GET /action/{action}` | Products by pricing action (raise/lower/hold) |
| `GET /top` | Top N by expected revenue lift |

### Customer Journey `/api/journey/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Journey summary & metadata |
| `GET /transitions` | Full tier transition probability matrix |
| `GET /top-transitions` | Most common A→B transitions |
| `GET /sequences` | Most common multi-step purchase sequences |
| `GET /funnel` | Customer retention funnel by step |
| `GET /sankey-data` | Plotly Sankey diagram data |
| `GET /customer/{id}` | Individual customer's journey |

### Geographic Analysis `/api/geographic/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Global KPIs |
| `GET /regions` | All regions with sales metrics |
| `GET /region/{region}` | Full profile for one region |
| `GET /trends` | Year-over-year revenue by region |
| `GET /monthly` | Monthly revenue trend for top regions |
| `GET /top-products/{region}` | Top products in a region |

### Anomaly Detection `/api/anomaly/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Top anomalies & detection summary |
| `GET /daily` | Full daily series with anomaly flags |
| `GET /alerts` | Anomalous days sorted by severity |
| `GET /monthly` | Anomaly count by month |
| `GET /product/{id}` | Anomalies for a specific product |

### Seasonality Analysis `/api/seasonality/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | KPIs & top-level metrics |
| `GET /decomposition` | STL: trend + seasonal + residual |
| `GET /patterns` | Day-of-week, month, quarter patterns |
| `GET /yoy` | Year-over-year monthly revenue matrix |
| `GET /heatmap` | Year × week heatmap data |
| `GET /peaks` | Peak and trough trading days |
| `GET /daily` | Full daily series (filterable) |

### Customer Segmentation `/api/segmentation/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Segmentation KPIs |
| `GET /clusters` | Cluster profiles (mean/median features) |
| `GET /scatter` | PCA 2D scatter data |
| `GET /elbow` | Elbow + silhouette curves (k=2..10) |
| `GET /customers` | Per-customer assignments (paginated) |
| `GET /customer/{id}` | Cluster for one customer |

### Store Performance `/api/store/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Global KPIs |
| `GET /stores` | All stores with KPIs (filterable) |
| `GET /top` | Top N stores by revenue |
| `GET /{store_id}` | Single store detail + top products |
| `GET /trends/monthly` | Monthly revenue for top-10 stores |
| `GET /products/{store_id}` | Top products for one store |
| `GET /yoy/all` | Year-over-year revenue per store |

### Product Recommendations `/api/recommendations/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Model stats (coverage, avg similarity) |
| `GET /popular` | Top products by buyer count (cold-start) |
| `GET /customer/{id}` | Top-10 recs for one customer |
| `GET /customers` | All customers with rec counts (paginated) |
| `GET /product/{id}` | Top-10 similar products |
| `GET /top` | Most-recommended products |

### Inventory Optimization `/api/inventory/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Inventory summary & metadata |
| `GET /products` | All products with EOQ/ROP params (paginated) |
| `GET /product/{id}` | Inventory parameters for one product |
| `GET /alerts` | Products at critical or high stockout risk |
| `GET /categories` | Inventory metrics by category |

### Model Drift Monitor `/api/drift/`
| Endpoint | Description |
|----------|-------------|
| `GET /summary` | Overall drift health summary |
| `GET /features` | All drift scores per feature (paginated) |
| `GET /module/{module}` | Drift results for one module |
| `GET /alerts` | Features with status != STABLE |

### ETL Operations `/api/etl/`
| Endpoint | Description |
|----------|-------------|
| `GET /status` | Status of all ETL jobs |
| `POST /run` | Trigger incremental ETL pipeline |
| `POST /analytics/refresh` | Re-run all analytics modules |
| `POST /quality/check` | Run data quality checks |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.9+ |
| API framework | FastAPI + Uvicorn |
| Database | PostgreSQL 15 (star schema) |
| ORM / queries | psycopg2 (raw SQL) |
| ML | scikit-learn, RandomForest, KMeans, SHAP |
| Probabilistic CLV | lifetimes (BG/NBD + Gamma-Gamma) |
| Time-series | Prophet / statsmodels ARIMA |
| Data | pandas, NumPy, scipy |
| Charts (frontend) | Chart.js, Plotly |
| Scheduling | APScheduler (cron, 02:00 UTC) |
| Containerisation | Docker + docker-compose |
| Testing | pytest |
| Logging | structlog (JSON file + human console) |

---

## Project Structure

```
etl/
├── main.py                    # FastAPI app + APScheduler lifespan
├── run_all.py                 # Orchestrator (3 phases, --skip-phase1 flag)
├── run_etl.py                 # Full ETL pipeline runner
│
├── routers/                   # 20 FastAPI routers (115+ endpoints)
│   ├── rfm.py, abc.py, clv.py, clv_prediction.py
│   ├── churn.py, explain.py, forecast.py, elasticity.py
│   ├── pricing.py, journey.py, geographic.py, anomaly.py
│   ├── seasonality.py, segmentation.py, store_performance.py
│   ├── recommendations.py, inventory.py, drift.py
│   ├── basket.py, etl.py, dashboard.py
│
├── models/                    # Trained models + JSON metadata
│   ├── churn_model.pkl
│   └── *_metadata.json
│
├── static/                    # Frontend assets
│   ├── dashboard.html         # Multi-tab live dashboard
│   ├── index.html             # API explorer
│   └── shap/                  # SHAP plot assets
│
├── tests/                     # pytest suite
│   ├── conftest.py
│   ├── test_health.py
│   ├── test_routers.py
│   └── test_csvs.py
│
├── cache.py                   # Singleton CSV cache (mtime-keyed)
├── logger_config.py           # Structured logging (JSON + console)
├── config.py                  # DB and app configuration
│
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── requirements.txt
```

---

## Data

Dataset: Online Retail II (UCI Machine Learning Repository)
~500,000 transactions · 4,000+ customers · 3,500+ products · 6 years

The ETL pipeline loads raw data into a PostgreSQL star schema:

```
fact_sales
  ├── dim_customer  (SCD Type 2, loyalty tier, age group)
  ├── dim_product   (category, description)
  ├── dim_store     (region / country)
  └── dim_time      (date, week, month, quarter, year)
```
