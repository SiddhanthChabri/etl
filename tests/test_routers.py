"""
tests/test_routers.py — Endpoint smoke tests for every analytics router

Each test:
  1. Hits the endpoint
  2. Asserts 200 (or 404 if CSV not yet generated)
  3. Validates the response schema has expected keys
"""

import os
import pytest


# Helper: assert a list endpoint returns the right envelope
def _list_ok(r, data_key="data"):
    assert r.status_code in (200, 404), f"Unexpected {r.status_code}: {r.text[:200]}"
    if r.status_code == 200:
        body = r.json()
        assert data_key in body or isinstance(body, list), \
            f"Expected '{data_key}' key or list, got: {list(body.keys())}"


# ── RFM ───────────────────────────────────────────────────────────────────────

class TestRFM:
    def test_segments_summary(self, client):
        r = client.get("/api/rfm/segments/summary")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            data = r.json()
            assert isinstance(data, list)
            if data:
                assert "segment" in data[0]
                assert "customer_count" in data[0]

    def test_customers_paginated(self, client):
        r = client.get("/api/rfm/customers?limit=5&offset=0")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            body = r.json()
            assert "total" in body
            assert "data" in body
            assert len(body["data"]) <= 5

    def test_top_customers(self, client):
        r = client.get("/api/rfm/top?n=3")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            assert isinstance(r.json(), list)

    def test_unknown_customer_404(self, client):
        r = client.get("/api/rfm/customer/DOESNOTEXIST99999")
        assert r.status_code == 404


# ── ABC ───────────────────────────────────────────────────────────────────────

class TestABC:
    def test_classes_summary(self, client):
        r = client.get("/api/abc/classes/summary")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            data = r.json()
            assert isinstance(data, list)

    def test_products_paginated(self, client):
        r = client.get("/api/abc/products?limit=5")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            assert "data" in r.json()


# ── CLV ───────────────────────────────────────────────────────────────────────

class TestCLV:
    def test_segments_summary(self, client):
        r = client.get("/api/clv/segments/summary")
        assert r.status_code in (200, 404)

    def test_top_customers(self, client):
        r = client.get("/api/clv/top?n=5")
        assert r.status_code in (200, 404)


# ── Churn ─────────────────────────────────────────────────────────────────────

class TestChurn:
    def test_summary(self, client):
        r = client.get("/api/churn/summary")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            body = r.json()
            assert "risk_distribution" in body or "total_customers" in body

    def test_top_at_risk(self, client):
        r = client.get("/api/churn/top-at-risk?n=5")
        assert r.status_code in (200, 404)


# ── Forecast ──────────────────────────────────────────────────────────────────

class TestForecast:
    def test_summary(self, client):
        r = client.get("/api/forecast/summary")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            body = r.json()
            assert "method" in body or "generated_at" in body

    def test_accuracy(self, client):
        r = client.get("/api/forecast/accuracy")
        assert r.status_code in (200, 404)

    def test_categories_list(self, client):
        r = client.get("/api/forecast/categories")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            assert "categories" in r.json()


# ── Price Elasticity ──────────────────────────────────────────────────────────

class TestElasticity:
    def test_summary(self, client):
        r = client.get("/api/elasticity/summary")
        assert r.status_code in (200, 404)

    def test_top_elastic(self, client):
        r = client.get("/api/elasticity/top/elastic?n=5")
        assert r.status_code in (200, 404)

    def test_top_inelastic(self, client):
        r = client.get("/api/elasticity/top/inelastic?n=5")
        assert r.status_code in (200, 404)


# ── Inventory ─────────────────────────────────────────────────────────────────

class TestInventory:
    def test_summary(self, client):
        r = client.get("/api/inventory/summary")
        assert r.status_code in (200, 404)

    def test_alerts(self, client):
        r = client.get("/api/inventory/alerts?limit=5")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            assert "data" in r.json()


# ── Geographic ────────────────────────────────────────────────────────────────

class TestGeographic:
    def test_summary(self, client):
        r = client.get("/api/geo/summary")
        assert r.status_code in (200, 404)


# ── Anomaly ───────────────────────────────────────────────────────────────────

class TestAnomaly:
    def test_summary(self, client):
        r = client.get("/api/anomaly/summary")
        assert r.status_code in (200, 404)


# ── Seasonality ───────────────────────────────────────────────────────────────

class TestSeasonality:
    def test_summary(self, client):
        r = client.get("/api/seasonality/summary")
        assert r.status_code in (200, 404)


# ── Segmentation ──────────────────────────────────────────────────────────────

class TestSegmentation:
    def test_summary(self, client):
        r = client.get("/api/segmentation/summary")
        assert r.status_code in (200, 404)

    def test_scatter(self, client):
        r = client.get("/api/segmentation/scatter")
        assert r.status_code in (200, 404)


# ── Store Performance ─────────────────────────────────────────────────────────

class TestStores:
    def test_summary(self, client):
        r = client.get("/api/store/summary")
        assert r.status_code in (200, 404)

    def test_top_stores(self, client):
        r = client.get("/api/store/top?n=5")
        assert r.status_code in (200, 404)


# ── Recommendations ───────────────────────────────────────────────────────────

class TestRecommendations:
    def test_summary(self, client):
        r = client.get("/api/recommendations/summary")
        assert r.status_code in (200, 404)

    def test_popular(self, client):
        r = client.get("/api/recommendations/popular?n=5")
        assert r.status_code in (200, 404)


# ── Pricing Optimizer ─────────────────────────────────────────────────────────

class TestPricing:
    def test_summary(self, client):
        r = client.get("/api/pricing/summary")
        assert r.status_code in (200, 404)

    def test_top_opportunities(self, client):
        r = client.get("/api/pricing/top?n=5")
        assert r.status_code in (200, 404)

    def test_invalid_action_400(self, client):
        r = client.get("/api/pricing/action/INVALID_ACTION")
        # Either 400 (if CSV exists) or 404 (if CSV not yet generated)
        assert r.status_code in (400, 404)


# ── CLV Prediction ────────────────────────────────────────────────────────────

class TestCLVPrediction:
    def test_summary(self, client):
        r = client.get("/api/clv-predict/summary")
        assert r.status_code in (200, 404)

    def test_top(self, client):
        r = client.get("/api/clv-predict/top?n=5")
        assert r.status_code in (200, 404)


# ── Drift Monitor ─────────────────────────────────────────────────────────────

class TestDrift:
    def test_summary(self, client):
        r = client.get("/api/drift/summary")
        assert r.status_code in (200, 404)

    def test_alerts(self, client):
        r = client.get("/api/drift/alerts")
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            body = r.json()
            assert "alerts" in body
            assert "alert_count" in body
