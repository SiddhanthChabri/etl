"""
tests/test_health.py — Health check and root endpoint tests
"""

import pytest


def test_root_serves_html(client):
    """GET / should return the landing page (HTML)."""
    r = client.get("/")
    assert r.status_code == 200


def test_health_returns_ok(client):
    """GET /health should return status=ok."""
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "files" in body
    assert isinstance(body["files"], dict)


def test_health_has_all_expected_keys(client):
    """Health check should list every expected CSV."""
    r    = client.get("/health")
    keys = set(r.json()["files"].keys())
    expected = {
        "rfm_csv", "abc_csv", "clv_csv", "churn_csv",
        "forecast_csv", "elasticity_csv", "journey_csv",
        "inventory_csv", "geographic_csv", "anomaly_csv",
        "seasonality_csv", "segmentation_csv", "store_csv",
        "recommendations_csv",
    }
    assert expected.issubset(keys), f"Missing keys: {expected - keys}"


def test_api_info(client):
    """GET /api should return version info."""
    r = client.get("/api")
    assert r.status_code == 200
    body = r.json()
    assert "status" in body
    assert "endpoints" in body
