"""
tests/conftest.py — Shared pytest fixtures for the Retail Analytics API
"""

import os
import sys

import pytest
from fastapi.testclient import TestClient

# Add project root to path so imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    """Return a TestClient bound to the FastAPI app."""
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def csv_dir():
    """Return the project root directory where CSV files live."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
