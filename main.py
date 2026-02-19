"""
main.py — FastAPI Entry Point
Retail Analytics API
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routers import rfm, abc, clv, basket, etl, dashboard, churn
import uvicorn

app = FastAPI(
    title="🛒 Retail Analytics API",
    description="""
## Retail Data Warehouse — Analytics API

A complete REST API for retail analytics powered by a PostgreSQL Data Warehouse.

### Modules
| Module | Description |
|--------|-------------|
| 🎯 **RFM Analysis** | Customer segmentation by Recency, Frequency & Monetary value |
| 📦 **ABC Analysis** | Product classification by revenue contribution |
| 💎 **CLV Analysis** | Customer Lifetime Value scoring |
| 📅 **Cohort Analysis** | Retention tracking by customer acquisition cohort |
| 🛒 **Market Basket** | Association rules — products bought together |
| 🤖 **Churn Prediction** | ML model to identify customers likely to leave |
| ⚙️  **ETL Pipeline** | Trigger & monitor incremental data loads |
| 📊 **Dashboard** | Serve HTML dashboard & download Excel reports |

### Quick Start
1. Visit `/dashboard` for the visual dashboard
2. Visit `/docs` for interactive API documentation
3. Use `/api/summary` for a full analytics overview
    """,
    version="1.0.0",
    contact={"name": "Retail Analytics Project"},
    license_info={"name": "MIT"},
    openapi_tags=[
        {"name": "Health",              "description": "Health check & root endpoints"},
        {"name": "RFM Analysis",        "description": "Customer segmentation using RFM scoring"},
        {"name": "ABC Analysis",        "description": "Product classification by revenue contribution"},
        {"name": "CLV Analysis",        "description": "Customer Lifetime Value calculation & segmentation"},
        {"name": "Market Basket",       "description": "Association rule mining — frequently bought together"},
        {"name": "Churn Prediction 🤖", "description": "ML-based churn probability scoring per customer"},
        {"name": "ETL Pipeline",        "description": "Trigger & monitor ETL pipeline jobs"},
        {"name": "Dashboard",           "description": "HTML dashboard, Excel download & cohort data"},
    ]
)

# ── CORS ──────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── STATIC FILES ──────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── ROUTERS ───────────────────────────────────────────────────────────────
app.include_router(rfm.router,       prefix="/api/rfm",       tags=["RFM Analysis"])
app.include_router(abc.router,       prefix="/api/abc",       tags=["ABC Analysis"])
app.include_router(clv.router,       prefix="/api/clv",       tags=["CLV Analysis"])
app.include_router(basket.router,    prefix="/api/basket",    tags=["Market Basket"])
app.include_router(churn.router,     prefix="/api/churn",     tags=["Churn Prediction 🤖"])
app.include_router(etl.router,       prefix="/etl",           tags=["ETL Pipeline"])
app.include_router(dashboard.router, prefix="",               tags=["Dashboard"])


# ── ROOT ──────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"], summary="API root — links to all modules")
def root():
    return {
        "status":  "✅ running",
        "app":     "Retail Analytics API",
        "version": "1.0.0",
        "links": {
            "docs":           "/docs",
            "redoc":          "/redoc",
            "dashboard":      "/dashboard",
            "download_excel": "/download/excel",
            "summary":        "/api/summary",
            "cohort":         "/api/cohort",
        },
        "endpoints": {
            "rfm":    {"all_customers": "/api/rfm/customers", "single_customer": "/api/rfm/customer/{id}",
                       "segment_summary": "/api/rfm/segments/summary", "top_customers": "/api/rfm/top"},
            "abc":    {"all_products": "/api/abc/products", "single_product": "/api/abc/product/{id}",
                       "class_summary": "/api/abc/classes/summary", "top_products": "/api/abc/top"},
            "clv":    {"all_customers": "/api/clv/customers", "single_customer": "/api/clv/customer/{id}",
                       "segment_summary": "/api/clv/segments/summary", "top_customers": "/api/clv/top"},
            "basket": {"all_rules": "/api/basket/rules", "recommendations": "/api/basket/recommendations/{product}",
                       "top_rules": "/api/basket/top"},
            "churn":  {"summary": "/api/churn/summary", "single_customer": "/api/churn/customer/{id}",
                       "by_risk_tier": "/api/churn/risk/{tier}", "top_at_risk": "/api/churn/top-at-risk",
                       "predict_custom": "POST /api/churn/predict"},
            "etl":    {"status": "/etl/status", "run_etl": "POST /etl/run",
                       "refresh_analytics": "POST /etl/analytics/refresh",
                       "quality_check": "POST /etl/quality/check"}
        }
    }


# ── HEALTH CHECK ──────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"], summary="Health check — shows which data files exist")
def health():
    import os
    return {
        "status": "ok",
        "files": {
            "rfm_csv":     os.path.exists("rfm_analysis_results.csv"),
            "abc_csv":     os.path.exists("abc_analysis_results.csv"),
            "clv_csv":     os.path.exists("clv_analysis_results.csv"),
            "cohort_csv":  os.path.exists("cohort_retention_matrix.csv"),
            "basket_csv":  os.path.exists("market_basket_results.csv"),
            "churn_csv":   os.path.exists("churn_predictions.csv"),
            "churn_model": os.path.exists("models/churn_model.pkl"),
        }
    }


# ── RUN ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
