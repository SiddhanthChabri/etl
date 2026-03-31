"""
main.py — FastAPI Entry Point
Retail Analytics API
"""

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from routers import (
    abc, anomaly, basket, churn, clv, clv_prediction, dashboard,
    elasticity, etl, forecast, geographic, inventory, journey,
    recommendations, rfm, seasonality, segmentation, store_performance,
)
from routers import drift, pricing
from routers.explain import router as explain_router

# ── APScheduler (ETL auto-refresh) ────────────────────────────────────────────
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    _HAS_SCHEDULER = True
except ImportError:
    _HAS_SCHEDULER = False

_scheduler = None


async def _refresh_analytics():
    """Nightly analytics refresh triggered by APScheduler."""
    import subprocess, sys
    print(f"[Scheduler] Starting nightly analytics refresh at {datetime.utcnow().isoformat()}")
    subprocess.Popen([sys.executable, "run_all.py", "--no-server"])


# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    if _HAS_SCHEDULER:
        _scheduler = AsyncIOScheduler()
        # Refresh analytics every night at 02:00 UTC
        _scheduler.add_job(
            _refresh_analytics,
            trigger=CronTrigger(hour=2, minute=0),
            id="nightly_refresh",
            replace_existing=True,
        )
        _scheduler.start()
        print("[Scheduler] APScheduler started — nightly refresh at 02:00 UTC")
    yield
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


# ── App ───────────────────────────────────────────────────────────────────────
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
| 🔮 **CLV Prediction** | Forward-looking CLV via BG/NBD model |
| 📅 **Cohort Analysis** | Retention tracking by customer acquisition cohort |
| 🛒 **Market Basket** | Association rules — products bought together |
| 🤖 **Churn Prediction** | ML model to identify customers likely to leave |
| 📈 **Demand Forecasting** | Time-series forecast of product demand |
| 💹 **Price Elasticity** | Log-log OLS regression — demand sensitivity to price |
| 💰 **Pricing Optimizer** | Revenue-maximising price recommendations |
| 📊 **Drift Monitor** | Feature distribution drift detection (PSI) |
| ⚙️  **ETL Pipeline** | Trigger & monitor incremental data loads |
| 📊 **Dashboard** | Serve HTML dashboard & download Excel reports |

### Quick Start
1. Visit `/dashboard` for the visual dashboard
2. Visit `/docs` for interactive API documentation
3. Use `/api/summary` for a full analytics overview
    """,
    version="2.0.0",
    contact={"name": "Retail Analytics Project"},
    license_info={"name": "MIT"},
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Health",              "description": "Health check & root endpoints"},
        {"name": "RFM Analysis",        "description": "Customer segmentation using RFM scoring"},
        {"name": "ABC Analysis",        "description": "Product classification by revenue contribution"},
        {"name": "CLV Analysis",        "description": "Customer Lifetime Value calculation & segmentation"},
        {"name": "CLV Prediction",      "description": "Forward-looking CLV via BG/NBD + Gamma-Gamma"},
        {"name": "Market Basket",       "description": "Association rule mining — frequently bought together"},
        {"name": "Churn Prediction 🤖", "description": "ML-based churn probability scoring per customer"},
        {"name": "Pricing Optimizer",   "description": "Revenue-maximising price adjustment recommendations"},
        {"name": "Drift Monitor",       "description": "PSI-based feature distribution drift detection"},
        {"name": "ETL Pipeline",        "description": "Trigger & monitor ETL pipeline jobs"},
        {"name": "Dashboard",           "description": "HTML dashboard, Excel download & cohort data"},
        {"name": "Demand Forecasting",  "description": "Time-series demand forecasting per product & category"},
        {"name": "Price Elasticity",    "description": "Log-log OLS elasticity of demand per product & category"},
        {"name": "Customer Journey",    "description": "Purchase sequence analysis — tier transitions & Sankey flow"},
        {"name": "Inventory Optimisation", "description": "EOQ, safety stock, reorder points & stockout risk per product"},
        {"name": "Geographic Analysis",    "description": "Revenue, growth & top products by country/region"},
        {"name": "Anomaly Detection",      "description": "Z-score & IQR detection of sales spikes and drops"},
        {"name": "Seasonality Analysis",   "description": "Trend decomposition, day-of-week/month/quarter patterns, YoY growth"},
        {"name": "Customer Segmentation",  "description": "K-Means clustering with PCA scatter, elbow curves & cluster profiles"},
        {"name": "Store Performance",      "description": "Per-store KPIs, tier classification, YoY growth & top products"},
        {"name": "Recommendations",        "description": "Item-item collaborative filtering — per-customer & similar-product recommendations"},
    ]
)


# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── STATIC FILES ──────────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── ROUTERS ───────────────────────────────────────────────────────────────────
app.include_router(rfm.router,              prefix="/api/rfm",            tags=["RFM Analysis"])
app.include_router(abc.router,              prefix="/api/abc",            tags=["ABC Analysis"])
app.include_router(clv.router,              prefix="/api/clv",            tags=["CLV Analysis"])
app.include_router(clv_prediction.router,   prefix="/api/clv-predict",    tags=["CLV Prediction"])
app.include_router(basket.router,           prefix="/api/basket",         tags=["Market Basket"])
app.include_router(churn.router,            prefix="/api/churn",          tags=["Churn Prediction 🤖"])
app.include_router(pricing.router,          prefix="/api/pricing",        tags=["Pricing Optimizer"])
app.include_router(drift.router,            prefix="/api/drift",          tags=["Drift Monitor"])
app.include_router(etl.router,              prefix="/etl",                tags=["ETL Pipeline"])
app.include_router(dashboard.router,        prefix="",                    tags=["Dashboard"])
app.include_router(forecast.router,         prefix="/api/forecast",       tags=["Demand Forecasting"])
app.include_router(elasticity.router,       prefix="/api/elasticity",     tags=["Price Elasticity"])
app.include_router(journey.router,          prefix="/api/journey",        tags=["Customer Journey"])
app.include_router(inventory.router,        prefix="/api/inventory",      tags=["Inventory Optimisation"])
app.include_router(geographic.router,       prefix="/api/geo",            tags=["Geographic Analysis"])
app.include_router(anomaly.router,          prefix="/api/anomaly",        tags=["Anomaly Detection"])
app.include_router(seasonality.router,      prefix="/api/seasonality",    tags=["Seasonality Analysis"])
app.include_router(segmentation.router,     prefix="/api/segmentation",   tags=["Customer Segmentation"])
app.include_router(store_performance.router, prefix="/api/store",         tags=["Store Performance"])
app.include_router(recommendations.router,  prefix="/api/recommendations", tags=["Recommendations"])
app.include_router(explain_router)


# ── ROOT ──────────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"], summary="API root — serve landing page",
         include_in_schema=False)
def root():
    return FileResponse("static/index.html")


# ── API INFO ──────────────────────────────────────────────────────────────────
@app.get("/api", tags=["Health"], summary="API info JSON")
def api_info():
    return {
        "status" : "✅ running",
        "app"    : "Retail Analytics API",
        "version": "2.0.0",
        "links"  : {
            "docs"          : "/docs",
            "redoc"         : "/redoc",
            "dashboard"     : "/dashboard",
            "download_excel": "/download/excel",
            "summary"         : "/api/summary",
            "cohort"          : "/api/cohort",
            "pipeline_status" : "/api/pipeline/status",
        },
        "endpoints": {
            "rfm"        : {"customers": "/api/rfm/customers", "summary": "/api/rfm/segments/summary"},
            "abc"        : {"products": "/api/abc/products",   "summary": "/api/abc/classes/summary"},
            "clv"        : {"customers": "/api/clv/customers", "summary": "/api/clv/segments/summary"},
            "clv_predict": {"customers": "/api/clv-predict/customers", "summary": "/api/clv-predict/summary", "top": "/api/clv-predict/top"},
            "churn"      : {"summary": "/api/churn/summary", "top_at_risk": "/api/churn/top-at-risk"},
            "pricing"    : {"summary": "/api/pricing/summary", "top": "/api/pricing/top"},
            "drift"      : {"summary": "/api/drift/summary", "alerts": "/api/drift/alerts"},
            "forecast"   : {"summary": "/api/forecast/summary", "accuracy": "/api/forecast/accuracy"},
            "elasticity" : {"summary": "/api/elasticity/summary", "top_elastic": "/api/elasticity/top/elastic"},
            "inventory"  : {"summary": "/api/inventory/summary", "alerts": "/api/inventory/alerts"},
            "geo"        : {"summary": "/api/geo/summary"},
            "anomaly"    : {"summary": "/api/anomaly/summary"},
            "seasonality": {"summary": "/api/seasonality/summary"},
            "segmentation": {"summary": "/api/segmentation/summary"},
            "stores"     : {"summary": "/api/store/summary", "top": "/api/store/top"},
            "recommendations": {"summary": "/api/recommendations/summary", "popular": "/api/recommendations/popular"},
        }
    }


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"],
         summary="Health check — data file status with freshness timestamps")
def health():
    base = os.path.dirname(os.path.abspath(__file__))

    def file_info(filename: str) -> dict:
        path = os.path.join(base, filename)
        exists = os.path.exists(path)
        if not exists:
            return {"exists": False, "size_kb": None, "last_modified": None}
        stat = os.stat(path)
        return {
            "exists"       : True,
            "size_kb"      : round(stat.st_size / 1024, 1),
            "last_modified": datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
        }

    files = {
        "rfm_csv"              : file_info("rfm_analysis_results.csv"),
        "abc_csv"              : file_info("abc_analysis_results.csv"),
        "clv_csv"              : file_info("clv_analysis_results.csv"),
        "clv_predictions_csv"  : file_info("clv_predictions.csv"),
        "cohort_csv"           : file_info("cohort_retention_matrix.csv"),
        "basket_csv"           : file_info("market_basket_results.csv"),
        "churn_csv"            : file_info("churn_predictions.csv"),
        "churn_model"          : file_info("models/churn_model.pkl"),
        "forecast_csv"         : file_info("demand_forecast_results.csv"),
        "elasticity_csv"       : file_info("price_elasticity_results.csv"),
        "pricing_csv"          : file_info("pricing_optimizer_results.csv"),
        "drift_csv"            : file_info("drift_results.csv"),
        "journey_csv"          : file_info("journey_transitions.csv"),
        "inventory_csv"        : file_info("inventory_optimization_results.csv"),
        "geographic_csv"       : file_info("geographic_results.csv"),
        "anomaly_csv"          : file_info("anomaly_results.csv"),
        "seasonality_csv"      : file_info("seasonality_results.csv"),
        "segmentation_csv"     : file_info("customer_segments_km.csv"),
        "store_csv"            : file_info("store_performance_results.csv"),
        "recommendations_csv"  : file_info("product_recommendations.csv"),
    }

    all_exist = all(v["exists"] for v in files.values())
    missing   = [k for k, v in files.items() if not v["exists"]]

    scheduler_status = "running" if (_scheduler and _scheduler.running) else "not_started"
    if not _HAS_SCHEDULER:
        scheduler_status = "apscheduler_not_installed"

    return {
        "status"          : "ok" if all_exist else "partial",
        "all_files_ready" : all_exist,
        "missing_files"   : missing,
        "scheduler"       : scheduler_status,
        "checked_at"      : datetime.utcnow().isoformat() + "Z",
        "files"           : files,
    }


# ── PIPELINE STATUS ───────────────────────────────────────────────────────────
_PIPELINE_STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_status.json")


@app.get("/api/pipeline/status", tags=["Health"], summary="ETL + analytics pipeline run status")
def pipeline_status():
    if not os.path.exists(_PIPELINE_STATUS_FILE):
        return {"status": "unknown", "detail": "No pipeline run recorded yet"}
    try:
        with open(_PIPELINE_STATUS_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "detail": str(exc)}


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
