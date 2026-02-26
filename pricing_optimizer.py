"""
pricing_optimizer.py — Dynamic Pricing Recommendations

Reads price_elasticity_results.csv and applies revenue-maximisation
economics to recommend optimal price adjustments per product.

Revenue maximisation logic:
  - For elastic products (e < -1): revenue increases by lowering price
  - For inelastic products (-1 < e < 0): revenue increases by raising price
  - Optimal monopoly price: p* = p0 * e / (1 + e)  [Lerner condition]
  - We cap adjustments at ±25% to reflect real-world constraints.

Output: pricing_optimizer_results.csv
        models/pricing_optimizer_metadata.json
"""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ELASTICITY_CSV = os.path.join(BASE_DIR, "price_elasticity_results.csv")
OUTPUT_CSV     = os.path.join(BASE_DIR, "pricing_optimizer_results.csv")
META_PATH      = os.path.join(BASE_DIR, "models", "pricing_optimizer_metadata.json")
MAX_ADJ        = 0.25   # cap price adjustment at ±25 %
MARGIN         = 0.40   # assumed gross margin (40 %) for profit calc


def revenue_optimal_adjustment(elasticity: float) -> float:
    """
    Lerner condition: optimal markup = -1/e  -> p* = MC / (1 + 1/e)
    Simplified to price-change recommendation given current price.
    Returns the recommended % change in price (−1 to +1 scale).
    """
    if elasticity >= 0:
        return 0.0                      # positive elasticity — model unreliable
    if elasticity < -50:
        return 0.0                      # extreme outlier — skip

    # Elastic (e < -1): lower price increases revenue
    # Inelastic (-1 < e < 0): raise price increases revenue
    if elasticity < -1:
        # Recommend modest price decrease proportional to elasticity
        adj = max(1 / elasticity, -MAX_ADJ)   # negative -> price cut
    else:
        # Inelastic — recommend modest increase
        adj = min(-1 / elasticity - 1, MAX_ADJ)  # positive -> price rise

    return round(float(adj), 4)


def estimate_revenue_lift(row: pd.Series, adj: float) -> float:
    """
    Estimate % revenue change from the recommended price adjustment.
    ΔRevenue% ≈ Δp% + e × Δp%  = Δp% × (1 + e)
    """
    return round(adj * (1 + row["price_elasticity"]) * 100, 2)   # in %


def main():
    os.makedirs(os.path.join(BASE_DIR, "models"), exist_ok=True)

    if not os.path.exists(ELASTICITY_CSV):
        raise FileNotFoundError(
            f"{ELASTICITY_CSV} not found. Run price_elasticity.py first."
        )

    df = pd.read_csv(ELASTICITY_CSV)
    print(f"[PricingOptimizer] Loaded {len(df)} products.")

    # Keep only significant or near-significant results
    df_sig = df[df["price_elasticity"].notna()].copy()

    recs = []
    for _, row in df_sig.iterrows():
        e   = row["price_elasticity"]
        adj = revenue_optimal_adjustment(e)

        if adj == 0.0:
            action = "HOLD"
        elif adj < 0:
            action = "LOWER_PRICE"
        else:
            action = "RAISE_PRICE"

        new_price = round(row["avg_price"] * (1 + adj), 4)
        rev_lift  = estimate_revenue_lift(row, adj)

        # Simple profit sensitivity: margin × revenue lift (rough approximation)
        profit_impact = round(rev_lift * MARGIN, 2)

        # Confidence based on R² and significance
        if row.get("is_significant") and row.get("r_squared", 0) >= 0.3:
            confidence = "HIGH"
        elif row.get("r_squared", 0) >= 0.15:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        recs.append({
            "product_id"          : row["product_id"],
            "product_name"        : row["product_name"],
            "category"            : row["category"],
            "price_elasticity"    : round(e, 4),
            "elasticity_type"     : row.get("elasticity_type", ""),
            "current_price"       : round(row["avg_price"], 4),
            "recommended_price"   : new_price,
            "price_adjustment_pct": round(adj * 100, 2),
            "action"              : action,
            "expected_revenue_lift_pct": rev_lift,
            "estimated_profit_impact_pct": profit_impact,
            "confidence"          : confidence,
            "r_squared"           : round(row.get("r_squared", 0), 4),
            "is_significant"      : bool(row.get("is_significant", False)),
            "avg_daily_qty"       : round(row.get("avg_daily_qty", 0), 2),
            "interpretation"      : row.get("interpretation", ""),
        })

    result = pd.DataFrame(recs)
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"[PricingOptimizer] Saved {len(result)} recommendations -> {OUTPUT_CSV}")

    # Summary stats
    actions = result["action"].value_counts().to_dict()
    top_lift = (
        result[result["action"].isin(["RAISE_PRICE", "LOWER_PRICE"])]
        .nlargest(10, "expected_revenue_lift_pct")[
            ["product_id", "product_name", "action",
             "price_adjustment_pct", "expected_revenue_lift_pct", "confidence"]
        ]
        .to_dict("records")
    )

    meta = {
        "generated_at"     : datetime.utcnow().isoformat(),
        "total_products"   : len(result),
        "action_breakdown" : actions,
        "assumed_margin_pct": MARGIN * 100,
        "max_adjustment_pct": MAX_ADJ * 100,
        "top_opportunities": top_lift,
        "confidence_breakdown": result["confidence"].value_counts().to_dict(),
    }

    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[PricingOptimizer] Metadata saved -> {META_PATH}")
    print(f"[PricingOptimizer] Actions: {actions}")


if __name__ == "__main__":
    main()
