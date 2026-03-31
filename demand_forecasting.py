"""
demand_forecasting.py — Product Demand Forecasting

Queries the retail data warehouse for historical daily sales,
then forecasts future demand for the top N products.

Forecasting method priority:
  1. Facebook Prophet  (pip install prophet)
  2. Statsmodels ETS   (pip install statsmodels)
  3. Moving Average    (pure numpy/pandas — always available)

Outputs:
  - demand_forecast_results.csv       : historical + future forecast rows
  - models/demand_forecast_metadata.json : model info & per-product accuracy

Usage:
  python demand_forecasting.py
  python demand_forecasting.py --horizon 60 --top-n 30
"""

import os
import json
import warnings
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

warnings.filterwarnings("ignore")

# ── Optional dependency detection ─────────────────────────────────────────────
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    ETS_AVAILABLE = True
except ImportError:
    ETS_AVAILABLE = False

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("demand_forecasting", "demand_forecasting_logs.txt")

# ── Constants ─────────────────────────────────────────────────────────────────
FORECAST_CSV  = "demand_forecast_results.csv"
METADATA_JSON = "models/demand_forecast_metadata.json"
DEFAULT_HORIZON  = 30   # days ahead to forecast
DEFAULT_TOP_N    = 50   # top products by volume to forecast


# ── Forecaster Class ──────────────────────────────────────────────────────────
class DemandForecaster:
    """
    Forecasts daily product demand for the top N products
    using historical sales from the data warehouse.
    """

    def __init__(self, horizon: int = DEFAULT_HORIZON, top_n: int = DEFAULT_TOP_N):
        self.engine  = engine
        self.horizon = horizon
        self.top_n   = top_n

        if PROPHET_AVAILABLE:
            self.method = "prophet"
        elif ETS_AVAILABLE:
            self.method = "ets"
        else:
            self.method = "moving_avg"

        logger.info(f"Forecasting method: {self.method.upper()}")

    # ── 1. Data Fetching ──────────────────────────────────────────────────────

    def fetch_sales_data(self) -> pd.DataFrame:
        """Query daily sales per product from the data warehouse."""
        logger.info("Fetching historical daily sales from data warehouse...")

        query = text("""
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                t.date::date          AS sale_date,
                SUM(fs.quantity_sold) AS daily_quantity,
                SUM(fs.sales_amount)  AS daily_revenue
            FROM fact_sales    fs
            JOIN dim_product   p  ON fs.product_key = p.product_key
            JOIN dim_time      t  ON fs.time_key     = t.time_key
            GROUP BY p.product_id, p.product_name, p.category, t.date
            ORDER BY p.product_id, t.date
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["sale_date"] = pd.to_datetime(df["sale_date"])
        logger.info(
            f"  Loaded {len(df):,} daily records "
            f"for {df['product_id'].nunique():,} unique products"
        )
        return df

    # ── 2. Product Selection ──────────────────────────────────────────────────

    def select_top_products(self, df: pd.DataFrame) -> list:
        """Select top N products by total quantity sold."""
        top_ids = (
            df.groupby("product_id")["daily_quantity"]
            .sum()
            .nlargest(self.top_n)
            .index.tolist()
        )
        logger.info(f"  Selected top {len(top_ids)} products by total volume")
        return top_ids

    # ── 3. Forecasting Methods ────────────────────────────────────────────────

    def _build_daily_series(self, product_df: pd.DataFrame) -> pd.Series:
        """Convert product data to a daily time series (fill missing dates with 0)."""
        ts = (
            product_df
            .set_index("sale_date")["daily_quantity"]
            .asfreq("D", fill_value=0)
        )
        return ts

    def _forecast_prophet(self, series: pd.Series, horizon: int) -> pd.DataFrame:
        """Forecast using Facebook Prophet with yearly + weekly seasonality."""
        prophet_df = (
            series.reset_index()
            .rename(columns={"sale_date": "ds", "daily_quantity": "y"})
        )

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            interval_width=0.95,
            seasonality_mode="multiplicative",
        )
        model.fit(prophet_df)

        future   = model.make_future_dataframe(periods=horizon)
        forecast = model.predict(future)

        result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        result.columns = ["forecast_date", "predicted_quantity", "lower_bound", "upper_bound"]
        result["predicted_quantity"] = result["predicted_quantity"].clip(lower=0).round(2)
        result["lower_bound"]        = result["lower_bound"].clip(lower=0).round(2)
        result["upper_bound"]        = result["upper_bound"].clip(lower=0).round(2)
        return result

    def _forecast_ets(self, series: pd.Series, horizon: int) -> pd.DataFrame:
        """Forecast using Holt-Winters Exponential Smoothing."""
        try:
            model  = ExponentialSmoothing(
                series, trend="add", seasonal="add", seasonal_periods=7
            )
            fitted = model.fit(optimized=True)
        except Exception:
            # Fall back to non-seasonal if additive seasonal fails
            model  = ExponentialSmoothing(series, trend="add", seasonal=None)
            fitted = model.fit(optimized=True)

        last_date   = series.index[-1]
        future_vals = fitted.forecast(horizon)
        fut_dates   = pd.date_range(last_date + timedelta(days=1), periods=horizon)

        all_dates = list(series.index) + list(fut_dates)
        all_vals  = (
            fitted.fittedvalues.clip(lower=0).round(2).tolist()
            + future_vals.clip(lower=0).round(2).tolist()
        )

        std   = float(np.std(fitted.fittedvalues))
        lower = [round(max(0.0, v - 1.96 * std), 2) for v in all_vals]
        upper = [round(v + 1.96 * std, 2)             for v in all_vals]

        return pd.DataFrame({
            "forecast_date"     : all_dates,
            "predicted_quantity": all_vals,
            "lower_bound"       : lower,
            "upper_bound"       : upper,
        })

    def _forecast_moving_avg(self, series: pd.Series, horizon: int) -> pd.DataFrame:
        """Fallback: 14-day rolling average repeated for horizon days."""
        window    = min(14, len(series))
        ma        = float(series.tail(window).mean())
        std       = float(series.tail(window).std()) if len(series) > 1 else 0.0
        last_date = series.index[-1]
        fut_dates = pd.date_range(last_date + timedelta(days=1), periods=horizon)

        all_dates = list(series.index) + list(fut_dates)
        hist_vals = series.values.tolist()
        fut_vals  = [round(max(0.0, ma), 2)] * horizon
        all_vals  = hist_vals + fut_vals

        lower = [round(max(0.0, v - 1.96 * std), 2) for v in all_vals]
        upper = [round(v + 1.96 * std, 2)             for v in all_vals]

        return pd.DataFrame({
            "forecast_date"     : all_dates,
            "predicted_quantity": all_vals,
            "lower_bound"       : lower,
            "upper_bound"       : upper,
        })

    def _forecast_product(self, series: pd.Series) -> pd.DataFrame:
        """Dispatch to the appropriate forecasting method with runtime fallback."""
        if self.method == "prophet":
            try:
                return self._forecast_prophet(series, self.horizon)
            except Exception as exc:
                logger.warning(f"    Prophet failed ({exc}); falling back to ETS")
        if self.method in ("prophet", "ets"):
            try:
                return self._forecast_ets(series, self.horizon)
            except Exception as exc:
                logger.warning(f"    ETS failed ({exc}); falling back to moving average")
        return self._forecast_moving_avg(series, self.horizon)

    # ── 4. Accuracy Metrics ───────────────────────────────────────────────────

    def _compute_accuracy(
        self, actual: pd.Series, predicted: pd.Series
    ) -> dict:
        """Compute MAE, RMSE, MAPE on historical fitted values."""
        mask = actual > 0
        if mask.sum() < 2:
            return {"mae": None, "rmse": None, "mape": None}

        a = actual[mask].values
        p = predicted.reindex(actual[mask].index).fillna(0).values

        mae  = float(np.mean(np.abs(a - p)))
        rmse = float(np.sqrt(np.mean((a - p) ** 2)))
        mape = float(np.mean(np.abs((a - p) / a)) * 100)

        return {
            "mae" : round(mae,  3),
            "rmse": round(rmse, 3),
            "mape": round(mape, 2),
        }

    # ── 5. Main Run ───────────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """Run forecasting for all top products and save results."""
        logger.info("=" * 60)
        logger.info("DEMAND FORECASTING ENGINE")
        logger.info(f"  Method  : {self.method.upper()}")
        logger.info(f"  Horizon : {self.horizon} days")
        logger.info(f"  Top N   : {self.top_n} products")
        logger.info("=" * 60)

        raw_df   = self.fetch_sales_data()
        top_ids  = self.select_top_products(raw_df)
        last_date = raw_df["sale_date"].max()

        all_rows     = []
        product_meta = []

        for i, pid in enumerate(top_ids, 1):
            subset = raw_df[raw_df["product_id"] == pid].copy()
            name   = subset["product_name"].iloc[0] or "Unknown"
            cat    = subset["category"].iloc[0]    or "Unknown"

            logger.info(f"  [{i:02d}/{len(top_ids)}] {pid} — {str(name)[:45]}")

            try:
                series      = self._build_daily_series(subset)
                forecast_df = self._forecast_product(series)
            except Exception as exc:
                logger.warning(f"    Skipped — {exc}")
                continue

            forecast_df["product_id"]   = pid
            forecast_df["product_name"] = name
            forecast_df["category"]     = cat
            forecast_df["is_forecast"]  = (
                pd.to_datetime(forecast_df["forecast_date"]) > last_date
            )

            # Accuracy: compare historical fitted vs actual
            actual     = series
            fitted_ser = (
                forecast_df[~forecast_df["is_forecast"]]
                .set_index(pd.to_datetime(forecast_df.loc[
                    ~forecast_df["is_forecast"], "forecast_date"
                ]))["predicted_quantity"]
            )
            metrics = self._compute_accuracy(actual, fitted_ser)

            all_rows.append(forecast_df)
            product_meta.append({
                "product_id"          : pid,
                "product_name"        : name,
                "category"            : cat,
                "total_historical_qty": int(subset["daily_quantity"].sum()),
                "avg_daily_qty"       : round(float(subset["daily_quantity"].mean()), 2),
                "forecast_horizon"    : self.horizon,
                "accuracy"            : metrics,
            })

        if not all_rows:
            logger.error("No forecasts generated — check data warehouse connectivity.")
            return pd.DataFrame()

        results = pd.concat(all_rows, ignore_index=True)
        results["forecast_date"] = pd.to_datetime(results["forecast_date"]).dt.strftime("%Y-%m-%d")

        # ── Save CSV ──────────────────────────────────────────────────────────
        results.to_csv(FORECAST_CSV, index=False)
        logger.info(f"\nSaved {len(results):,} rows -> {FORECAST_CSV}")

        # ── Save metadata ─────────────────────────────────────────────────────
        os.makedirs("models", exist_ok=True)
        metadata = {
            "generated_at"     : datetime.now().isoformat(),
            "method"           : self.method,
            "horizon_days"     : self.horizon,
            "products_forecast": len(product_meta),
            "data_through"     : last_date.strftime("%Y-%m-%d"),
            "product_details"  : product_meta,
        }
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata    -> {METADATA_JSON}")
        logger.info("Demand forecasting complete.")

        return results


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Product Demand Forecaster")
    parser.add_argument(
        "--horizon", type=int, default=DEFAULT_HORIZON,
        help=f"Days ahead to forecast (default: {DEFAULT_HORIZON})"
    )
    parser.add_argument(
        "--top-n", type=int, default=DEFAULT_TOP_N,
        help=f"Top N products by volume to forecast (default: {DEFAULT_TOP_N})"
    )
    args = parser.parse_args()

    forecaster = DemandForecaster(horizon=args.horizon, top_n=args.top_n)
    try:
        df = forecaster.run()
    except Exception as exc:
        import traceback
        logger.error(f"❌ Demand forecasting pipeline crashed: {exc}")
        logger.error(traceback.format_exc())
        raise

    if not df.empty:
        future = df[df["is_forecast"] == True]
        print(f"\nForecasted {df['product_id'].nunique()} products.")
        print(f"Future rows: {len(future):,}")
        print("\nTop 5 products by expected demand:")
        top5 = (
            future.groupby(["product_id", "product_name"])["predicted_quantity"]
            .sum().nlargest(5).reset_index()
        )
        print(top5.to_string(index=False))
