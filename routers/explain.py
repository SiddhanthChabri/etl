"""
routers/explain.py — SHAP Explainability Endpoints
"""

import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from shap_explainability import (
    get_global_feature_importance,
    get_customer_shap_breakdown,
    get_bar_plot_data,
    get_summary_plot_data,
    generate_waterfall_plot,
)

router = APIRouter(prefix="/api/churn/explain", tags=["SHAP Explainability"])


# ── Global feature importance (JSON) ─────────────────────────────────────────
@router.get("/importance",
            summary="SHAP global feature importance ranked list")
def shap_feature_importance():
    try:
        return JSONResponse(content=get_global_feature_importance())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SHAP error: {str(e)}")


# ── Bar chart JSON for Plotly ─────────────────────────────────────────────────
@router.get("/bar-data",
            summary="SHAP bar chart data for Plotly (JSON)")
def shap_bar_data():
    try:
        return JSONResponse(content=get_bar_plot_data())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SHAP error: {str(e)}")


# ── Beeswarm summary JSON for Plotly ─────────────────────────────────────────
@router.get("/summary-data",
            summary="SHAP beeswarm summary data for Plotly (JSON)")
def shap_summary_data():
    try:
        return JSONResponse(content=get_summary_plot_data())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SHAP error: {str(e)}")


# ── Per-customer SHAP breakdown (JSON) ───────────────────────────────────────
# NOTE: This route MUST come before /customer/{customer_id}/waterfall
#       otherwise FastAPI matches "waterfall" as the customer_id
@router.get("/customer/{customer_id}/waterfall",
            summary="Waterfall plot PNG for a single customer")
def customer_waterfall(customer_id: str):
    try:
        path = generate_waterfall_plot(customer_id)
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="Waterfall plot not generated.")
        return FileResponse(path, media_type="image/png")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SHAP error: {str(e)}")


# ── Per-customer breakdown JSON ───────────────────────────────────────────────
@router.get("/customer/{customer_id}",
            summary="SHAP breakdown for a single customer")
def customer_shap_breakdown(customer_id: str):
    try:
        return JSONResponse(content=get_customer_shap_breakdown(customer_id))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SHAP error: {str(e)}")
