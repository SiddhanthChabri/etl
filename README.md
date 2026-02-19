# 🛒 Retail Analytics Data Warehouse

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green?logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-orange?logo=scikit-learn)
![License](https://img.shields.io/badge/License-MIT-yellow)

**9-Credit Major Project | B.Tech 8th Semester**

A complete end-to-end retail analytics system featuring an incremental ETL pipeline,
PostgreSQL star-schema data warehouse, 5 analytics modules, ML churn prediction, and a FastAPI REST API with live web dashboards.

</div>

---

## 🏗️ System Architecture

```
Raw CSV Data
     │
     ▼
┌─────────────────────────────────────────┐
│           ETL PIPELINE                  │
│  Extract → Validate → Transform → Load  │
│  Data Quality Checks                    │
│  Performance Monitoring                 │
│  Incremental Loading (watermark-based)  │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│       POSTGRESQL DATA WAREHOUSE         │
│  Star Schema · Fact + Dimension Tables  │
│  SCD Type 2 · Audit Columns             │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│         ANALYTICS ENGINE                │
│  RFM · ABC · CLV · Cohort · Basket      │
│  ML Churn Prediction (RandomForest)     │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│         FASTAPI REST API                │
│  25+ Endpoints · Swagger UI             │
│  HTML Dashboard · Excel Reports         │
│  Live Churn Predictor                   │
└─────────────────────────────────────────┘
```

---

## 📦 Modules

| Module | Description | Output |
|--------|-------------|--------|
| ⚙️ **ETL Pipeline** | Incremental CSV → PostgreSQL with quality checks & monitoring | Star schema DWH |
| 🎯 **RFM Analysis** | Customer segmentation (Champions, Loyal, At Risk, etc.) | `rfm_analysis_results.csv` |
| 📦 **ABC Analysis** | Product classification by revenue contribution (A/B/C) | `abc_analysis_results.csv` |
| 💎 **CLV Analysis** | Customer Lifetime Value scoring & segmentation | `clv_analysis_results.csv` |
| 📅 **Cohort Analysis** | Retention heatmap by acquisition cohort | `cohort_retention_matrix.csv` |
| 🛒 **Market Basket** | Association rule mining — products bought together | `market_basket_results.csv` |
| 🤖 **ML Churn Prediction** | RandomForest model · AUC-ROC scored · 4 risk tiers | `churn_predictions.csv` |
| 📊 **Excel Dashboard** | Multi-sheet formatted Excel report | `analytics_dashboard_*.xlsx` |
| 🌐 **FastAPI REST API** | 25+ REST endpoints for all analytics modules | `http://localhost:8000` |
| 🖥️ **Web Dashboards** | HTML live dashboard + API explorer with live churn predictor | `static/` |

---

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure database
Create `db_config.py` (not included — contains credentials):
```python
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "retail_dwh"
DB_USER     = "your_user"
DB_PASSWORD = "your_password"
```

### 3. Run ETL pipeline
```bash
python incremental_etl.py
```

### 4. Run analytics
```bash
python test_advanced_analytics.py
```

### 5. Train churn model
```bash
python ml_churn_prediction.py
```

### 6. Generate Excel report
```bash
python generate_excel_dashboard.py
```

### 7. Start the API
```bash
uvicorn main:app --reload --port 8000
```

---

## 🌐 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Landing page & API explorer |
| GET | `/dashboard` | Live analytics dashboard |
| GET | `/docs` | Interactive Swagger UI |
| GET | `/api/rfm/customers` | All customers with RFM segments |
| GET | `/api/rfm/segments/summary` | Segment distribution & stats |
| GET | `/api/rfm/top` | Top customers by revenue |
| GET | `/api/abc/products` | All products with ABC class |
| GET | `/api/abc/classes/summary` | Class distribution & revenue |
| GET | `/api/clv/customers` | All CLV scores |
| GET | `/api/clv/top` | Top customers by CLV |
| GET | `/api/cohort` | Cohort retention matrix |
| GET | `/api/basket/rules` | All association rules |
| GET | `/api/basket/recommendations/{product}` | Products bought together |
| GET | `/api/churn/summary` | Model metrics + risk distribution |
| GET | `/api/churn/customer/{id}` | Churn probability for a custom
