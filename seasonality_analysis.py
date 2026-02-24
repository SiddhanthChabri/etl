"""
seasonality_analysis.py -- Sales Seasonality & Trend Analysis

Analyses the 6-year daily revenue series to extract:
  - Classical additive decomposition (trend + seasonal + residual)
  - Day-of-week revenue patterns
  - Month-of-year revenue patterns
  - Quarter patterns (Q1-Q4)
  - Year-over-year monthly growth matrix
  - Week-of-year x year heatmap data
  - Top peak and trough trading days
  - Trend direction & strength metrics

All without statsmodels -- uses only pandas / numpy / scipy.

Outputs:
  seasonality_results.csv          -- daily series with decomposition columns
  models/seasonality_metadata.json -- all aggregated patterns + KPIs

Usage:
  python seasonality_analysis.py
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from scipy import stats
from sqlalchemy import text

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("seasonality_analysis", "seasonality_analysis_logs.txt")

RESULTS_CSV   = "seasonality_results.csv"
METADATA_JSON = "models/seasonality_metadata.json"

DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe(v):
    """Convert numpy scalars / NaN to JSON-safe Python types."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return round(float(v), 4)
    return v


def _round2(x):
    return None if (x is None or (isinstance(x, float) and np.isnan(x))) else round(float(x), 2)


# ── main analyser ─────────────────────────────────────────────────────────────

class SeasonalityAnalyzer:

    def __init__(self):
        self.engine = engine

    # ── 1. Load daily revenue ─────────────────────────────────────────────────

    def _load_daily(self) -> pd.DataFrame:
        """Load daily total revenue with calendar attributes from DWH."""
        sql = text("""
            SELECT
                dt.date,
                dt.day_of_week,
                dt.day        AS day_of_month,
                dt.month,
                dt.quarter,
                dt.year,
                SUM(fs.sales_amount)  AS revenue,
                COUNT(fs.sales_key)   AS transactions,
                SUM(fs.quantity_sold) AS units
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            GROUP BY dt.date, dt.day_of_week, dt.day, dt.month, dt.quarter, dt.year
            ORDER BY dt.date
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        # Reindex to fill any missing calendar days with 0 revenue
        full_range = pd.date_range(df.index.min(), df.index.max(), freq="D")
        df = df.reindex(full_range)
        df.index.name = "date"
        df["revenue"]      = df["revenue"].fillna(0)
        df["transactions"] = df["transactions"].fillna(0)
        df["units"]        = df["units"].fillna(0)

        # Fill calendar columns for padded rows
        df["day_of_week"]  = df.index.day_name()
        df["month"]        = df.index.month
        df["quarter"]      = df.index.quarter
        df["year"]         = df.index.year
        df["week_of_year"] = df.index.isocalendar().week.astype(int)
        df["day_of_month"] = df.index.day

        logger.info(f"Loaded {len(df)} daily records ({df.index.min().date()} - {df.index.max().date()})")
        return df

    # ── 2. Decomposition ──────────────────────────────────────────────────────

    def _decompose(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classical additive decomposition:
          trend    = 365-day centered rolling mean (long-term)
          seasonal = 7-day day-of-week index (normalised so mean = 1)
          residual = revenue - trend - seasonal_component
        """
        # Long-term trend: 365-day centered rolling mean
        df["trend"] = (
            df["revenue"]
            .rolling(365, center=True, min_periods=30)
            .mean()
        )

        # Day-of-week seasonal index (use only days where trend is valid)
        valid = df.dropna(subset=["trend"])
        dow_index = (
            valid.groupby("day_of_week")["revenue"].mean()
            / valid["revenue"].mean()
        )
        df["seasonal_index"] = df["day_of_week"].map(dow_index)

        # Seasonal component = trend * (seasonal_index - 1)  [additive framing]
        df["seasonal_component"] = df["trend"] * (df["seasonal_index"] - 1)

        # Residual
        df["residual"] = df["revenue"] - df["trend"] - df["seasonal_component"]

        # 30-day short trend for direction calculation
        df["trend_30d"] = (
            df["revenue"]
            .rolling(30, center=True, min_periods=7)
            .mean()
        )

        return df

    # ── 3. Trend strength & direction ─────────────────────────────────────────

    def _trend_metrics(self, df: pd.DataFrame) -> dict:
        valid = df.dropna(subset=["trend", "residual"])
        if valid.empty:
            return {}

        # Trend strength = 1 - var(residual) / var(trend)
        var_res   = float(np.var(valid["residual"]))
        var_trend = float(np.var(valid["trend"]))
        trend_strength = max(0.0, 1.0 - var_res / (var_trend + 1e-9))

        # Seasonal strength using day-of-week CoV
        dow_avgs = valid.groupby("day_of_week")["revenue"].mean()
        seasonal_strength = float(dow_avgs.std() / (dow_avgs.mean() + 1e-9))

        # Trend direction: linear regression slope on 365d trend
        t_valid = valid["trend"].dropna()
        if len(t_valid) > 30:
            x = np.arange(len(t_valid))
            slope, intercept, r, p, se = stats.linregress(x, t_valid.values)
            trend_direction = "Upward" if slope > 0 else "Downward"
            trend_slope_pct = float(slope / (intercept + 1e-9) * 100)  # % per day
        else:
            trend_direction = "Flat"
            trend_slope_pct = 0.0

        return {
            "trend_strength"    : _round2(trend_strength),
            "seasonal_strength" : _round2(seasonal_strength),
            "trend_direction"   : trend_direction,
            "trend_slope_pct_per_day": _round2(trend_slope_pct),
        }

    # ── 4. Day-of-week patterns ───────────────────────────────────────────────

    def _dow_patterns(self, df: pd.DataFrame) -> list:
        """Average revenue, transactions, and units per day-of-week."""
        grp = df.groupby("day_of_week").agg(
            avg_revenue     = ("revenue",      "mean"),
            avg_transactions= ("transactions", "mean"),
            avg_units       = ("units",        "mean"),
            day_count       = ("revenue",      "count"),
        ).reset_index()

        # Sort in calendar order
        grp["_order"] = grp["day_of_week"].map(
            {d: i for i, d in enumerate(DOW_ORDER)}
        )
        grp.sort_values("_order", inplace=True)
        grp.drop(columns="_order", inplace=True)

        overall_avg = float(df["revenue"].mean())
        grp["index_vs_avg"] = grp["avg_revenue"] / (overall_avg + 1e-9)

        return [
            {k: _safe(v) for k, v in row.items()}
            for row in grp.to_dict("records")
        ]

    # ── 5. Month-of-year patterns ─────────────────────────────────────────────

    def _month_patterns(self, df: pd.DataFrame) -> list:
        grp = df.groupby("month").agg(
            avg_revenue     = ("revenue",      "mean"),
            total_revenue   = ("revenue",      "sum"),
            avg_transactions= ("transactions", "mean"),
            day_count       = ("revenue",      "count"),
        ).reset_index()

        grp["month_name"] = grp["month"].apply(lambda m: MONTH_NAMES[m - 1])
        overall_avg = float(df["revenue"].mean())
        grp["index_vs_avg"] = grp["avg_revenue"] / (overall_avg + 1e-9)

        return [
            {k: _safe(v) for k, v in row.items()}
            for row in grp.to_dict("records")
        ]

    # ── 6. Quarter patterns ───────────────────────────────────────────────────

    def _quarter_patterns(self, df: pd.DataFrame) -> list:
        grp = df.groupby("quarter").agg(
            avg_revenue     = ("revenue",      "mean"),
            total_revenue   = ("revenue",      "sum"),
            avg_transactions= ("transactions", "mean"),
        ).reset_index()

        grp["quarter_label"] = grp["quarter"].apply(lambda q: f"Q{q}")
        overall_avg = float(df["revenue"].mean())
        grp["index_vs_avg"] = grp["avg_revenue"] / (overall_avg + 1e-9)

        return [
            {k: _safe(v) for k, v in row.items()}
            for row in grp.to_dict("records")
        ]

    # ── 7. Year-over-Year monthly matrix ─────────────────────────────────────

    def _yoy_matrix(self, df: pd.DataFrame) -> dict:
        """Returns {years: [...], months: [...], data: {year: {month: revenue}}}"""
        pivot = df.groupby(["year", "month"])["revenue"].sum().reset_index()
        years  = sorted(pivot["year"].unique().tolist())
        months = list(range(1, 13))

        matrix = {}
        for yr in years:
            row = pivot[pivot["year"] == yr].set_index("month")["revenue"]
            matrix[int(yr)] = {
                int(m): _round2(float(row.get(m, 0))) for m in months
            }

        # YoY growth per month
        yoy_growth = {}
        for i in range(1, len(years)):
            prev, curr = years[i - 1], years[i]
            for m in months:
                prev_val = matrix[prev].get(m, 0) or 0
                curr_val = matrix[curr].get(m, 0) or 0
                pct = None
                if prev_val and prev_val > 0:
                    pct = _round2((curr_val - prev_val) / prev_val * 100)
                yoy_growth.setdefault(int(curr), {})[int(m)] = pct

        return {
            "years"      : [int(y) for y in years],
            "month_names": MONTH_NAMES,
            "revenue"    : matrix,
            "yoy_growth" : yoy_growth,
        }

    # ── 8. Week-of-year heatmap ───────────────────────────────────────────────

    def _week_heatmap(self, df: pd.DataFrame) -> dict:
        """Year x week-of-year total revenue matrix for heatmap."""
        grp = df.groupby(["year", "week_of_year"])["revenue"].sum().reset_index()
        years = sorted(grp["year"].unique().tolist())
        weeks = list(range(1, 53))

        matrix = {}
        for yr in years:
            row = grp[grp["year"] == yr].set_index("week_of_year")["revenue"]
            matrix[int(yr)] = {
                int(w): _round2(float(row.get(w, 0))) for w in weeks
            }

        return {
            "years" : [int(y) for y in years],
            "weeks" : weeks,
            "matrix": matrix,
        }

    # ── 9. Peak / trough days ─────────────────────────────────────────────────

    def _peak_trough(self, df: pd.DataFrame, n: int = 10) -> dict:
        """Top N peak and trough trading days (non-zero revenue)."""
        active = df[df["revenue"] > 0].copy()
        active["date_str"] = active.index.astype(str)

        peaks = (
            active.nlargest(n, "revenue")[["revenue", "transactions", "day_of_week", "date_str"]]
            .reset_index(drop=True)
            .to_dict("records")
        )
        troughs = (
            active.nsmallest(n, "revenue")[["revenue", "transactions", "day_of_week", "date_str"]]
            .reset_index(drop=True)
            .to_dict("records")
        )

        def _clean(lst):
            return [{k: _safe(v) for k, v in d.items()} for d in lst]

        return {"peaks": _clean(peaks), "troughs": _clean(troughs)}

    # ── 10. Best / worst months by year ──────────────────────────────────────

    def _best_worst_months(self, df: pd.DataFrame) -> dict:
        """For each year: best and worst month by total revenue."""
        grp = df.groupby(["year", "month"])["revenue"].sum().reset_index()
        result = {}
        for yr in sorted(grp["year"].unique()):
            row = grp[grp["year"] == yr]
            best   = row.loc[row["revenue"].idxmax()]
            worst  = row.loc[row["revenue"].idxmin()]
            result[int(yr)] = {
                "best_month"        : MONTH_NAMES[int(best["month"]) - 1],
                "best_month_revenue": _round2(best["revenue"]),
                "worst_month"       : MONTH_NAMES[int(worst["month"]) - 1],
                "worst_month_revenue": _round2(worst["revenue"]),
            }
        return result

    # ── 11. Weekend vs weekday ────────────────────────────────────────────────

    def _weekend_split(self, df: pd.DataFrame) -> dict:
        weekend_days = {"Saturday", "Sunday"}
        df["is_weekend"] = df["day_of_week"].isin(weekend_days)
        grp = df.groupby("is_weekend").agg(
            avg_revenue = ("revenue", "mean"),
            total_revenue = ("revenue", "sum"),
            day_count   = ("revenue", "count"),
        ).reset_index()
        grp["label"] = grp["is_weekend"].map({True: "Weekend", False: "Weekday"})
        result = {}
        for _, row in grp.iterrows():
            result[row["label"]] = {
                "avg_revenue"  : _round2(row["avg_revenue"]),
                "total_revenue": _round2(row["total_revenue"]),
                "day_count"    : int(row["day_count"]),
            }
        return result

    # ── Main run ─────────────────────────────────────────────────────────────

    def run(self):
        logger.info("=== Seasonality Analysis START ===")

        # 1. Load
        logger.info("Loading daily revenue from DWH...")
        df = self._load_daily()

        # 2. Decompose
        logger.info("Computing trend decomposition...")
        df = self._decompose(df)

        # 3. Trend metrics
        trend_metrics = self._trend_metrics(df)

        # 4. Patterns
        logger.info("Computing seasonal patterns...")
        dow_patterns     = self._dow_patterns(df)
        month_patterns   = self._month_patterns(df)
        quarter_patterns = self._quarter_patterns(df)

        # 5. YoY
        logger.info("Computing year-over-year matrix...")
        yoy_matrix = self._yoy_matrix(df)

        # 6. Heatmap
        week_heatmap = self._week_heatmap(df)

        # 7. Peaks/troughs
        peak_trough = self._peak_trough(df)

        # 8. Best/worst months
        best_worst = self._best_worst_months(df)

        # 9. Weekend split
        weekend_split = self._weekend_split(df)

        # ── Global KPIs ───────────────────────────────────────────────────────
        active = df[df["revenue"] > 0]
        best_dow = max(dow_patterns, key=lambda r: r["avg_revenue"])
        best_month = max(month_patterns, key=lambda r: r["avg_revenue"])

        # Daily series for decomposition chart (subsample to avoid huge JSON)
        df_chart = df.reset_index()[["date", "revenue", "trend", "seasonal_component", "residual", "trend_30d"]]
        df_chart["date"] = df_chart["date"].astype(str)
        df_chart = df_chart.replace({np.nan: None})
        # Keep every 3rd day for the chart (still ~800 points)
        daily_series = [
            {
                "date"               : row["date"],
                "revenue"            : _round2(row["revenue"]),
                "trend"              : _round2(row["trend"]),
                "seasonal_component" : _round2(row["seasonal_component"]),
                "residual"           : _round2(row["residual"]),
                "trend_30d"          : _round2(row["trend_30d"]),
            }
            for _, row in df_chart.iloc[::3].iterrows()
        ]

        metadata = {
            "generated_at"     : datetime.now().isoformat(),
            "date_range_start" : str(df.index.min().date()),
            "date_range_end"   : str(df.index.max().date()),
            "total_days"       : int(len(df)),
            "active_days"      : int(len(active)),
            "total_revenue"    : _round2(float(active["revenue"].sum())),
            "avg_daily_revenue": _round2(float(active["revenue"].mean())),
            "max_daily_revenue": _round2(float(active["revenue"].max())),
            "min_daily_revenue": _round2(float(active[active["revenue"] > 0]["revenue"].min())),
            "best_day_of_week" : best_dow["day_of_week"],
            "best_month"       : best_month["month_name"],
            **trend_metrics,
            "dow_patterns"     : dow_patterns,
            "month_patterns"   : month_patterns,
            "quarter_patterns" : quarter_patterns,
            "yoy_matrix"       : yoy_matrix,
            "week_heatmap"     : week_heatmap,
            "peak_trough"      : peak_trough,
            "best_worst_months": best_worst,
            "weekend_split"    : weekend_split,
            "daily_series"     : daily_series,
        }

        # Save metadata
        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata -> {METADATA_JSON}")

        # Save CSV (full daily series for per-day lookups)
        df_save = df.reset_index()[
            ["date", "day_of_week", "month", "quarter", "year",
             "week_of_year", "revenue", "transactions", "units",
             "trend", "seasonal_component", "residual", "seasonal_index"]
        ]
        df_save["date"] = df_save["date"].astype(str)
        df_save.to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved results -> {RESULTS_CSV}  ({len(df_save)} rows)")

        logger.info(f"Trend direction : {trend_metrics.get('trend_direction')}")
        logger.info(f"Best DOW        : {best_dow['day_of_week']}  avg ${best_dow['avg_revenue']:,.0f}")
        logger.info(f"Best month      : {best_month['month_name']}  avg ${best_month['avg_revenue']:,.0f}")
        logger.info("=== Seasonality Analysis DONE ===")

        return metadata


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        analyzer = SeasonalityAnalyzer()
        meta = analyzer.run()
        print(f"\nDone. Trend: {meta['trend_direction']} | "
              f"Best DOW: {meta['best_day_of_week']} | "
              f"Best Month: {meta['best_month']}")
    except Exception as e:
        logger.error(f"Seasonality analysis failed: {e}", exc_info=True)
        raise
