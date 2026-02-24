"""
product_recommendations.py -- Item-Item Collaborative Filtering

Builds a product recommendation engine from purchase history:

  1. Construct a sparse customer × product purchase matrix
     (values = log1p(quantity_sold) to down-weight bulk buyers)
  2. Compute item-item cosine similarity across all 3 507 products
  3. For each product: store top-10 most similar items
  4. For each customer: score un-purchased products by
       Σ (similarity[bought, candidate]) weighted by customer's purchase strength
     → return top-10 recommendations per customer
  5. Identify globally popular products as cold-start fallback

Outputs:
  product_recommendations.csv            -- per-customer top-10 recommended products
  models/recommendations_metadata.json  -- item similarity, popular products,
                                           model stats, per-customer recs index

Usage:
  python product_recommendations.py
"""

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import text

warnings.filterwarnings("ignore")

from db_connection import engine
from logger_config import setup_logger

logger = setup_logger("product_recommendations", "recommendations_logs.txt")

RESULTS_CSV   = "product_recommendations.csv"
METADATA_JSON = "models/recommendations_metadata.json"

TOP_SIMILAR   = 10   # similar products stored per product
TOP_RECS      = 10   # recommendations per customer
MAX_CUSTOMERS = 5000 # cap for per-customer storage in JSON


# ── helpers ───────────────────────────────────────────────────────────────────

def _j(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return round(float(v), 4)
    return v

def _r2(v):
    return None if (v is None or (isinstance(v, float) and np.isnan(v))) else round(float(v), 2)


# ── data loading ──────────────────────────────────────────────────────────────

def _load_purchases() -> pd.DataFrame:
    """
    Load aggregated customer-product purchase data.
    Returns one row per (customer, product) with total qty and amount.
    """
    sql = text("""
        SELECT
            fs.customer_key,
            dc.customer_id,
            dc.customer_name,
            fs.product_key,
            dp.product_id,
            dp.product_name,
            SUM(fs.quantity_sold) AS total_qty,
            SUM(fs.sales_amount)  AS total_amount,
            COUNT(fs.sales_key)   AS purchase_count
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key AND dc.is_current = TRUE
        JOIN dim_product  dp ON fs.product_key  = dp.product_key
        GROUP BY fs.customer_key, dc.customer_id, dc.customer_name,
                 fs.product_key, dp.product_id, dp.product_name
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    logger.info(f"Loaded {len(df):,} customer-product pairs  "
                f"({df.customer_key.nunique()} customers, {df.product_key.nunique()} products)")
    return df


# ── sparse matrix ─────────────────────────────────────────────────────────────

def _build_matrix(df: pd.DataFrame):
    """
    Build a sparse customer × product matrix.
    Values = log1p(total_qty) to compress range from bulk buyers.
    Returns: matrix (csr), customer_index, product_index, reverse maps.
    """
    customers = sorted(df["customer_key"].unique())
    products  = sorted(df["product_key"].unique())

    cust_idx = {c: i for i, c in enumerate(customers)}
    prod_idx = {p: i for i, p in enumerate(products)}

    rows = df["customer_key"].map(cust_idx).values
    cols = df["product_key"].map(prod_idx).values
    vals = np.log1p(df["total_qty"].values).astype(np.float32)

    mat = csr_matrix((vals, (rows, cols)),
                     shape=(len(customers), len(products)),
                     dtype=np.float32)
    logger.info(f"Sparse matrix: {mat.shape}  nnz={mat.nnz:,}  "
                f"density={mat.nnz / (mat.shape[0]*mat.shape[1])*100:.2f}%")

    return mat, customers, products, cust_idx, prod_idx


# ── item-item similarity ──────────────────────────────────────────────────────

def _item_similarity(mat: csr_matrix):
    """
    Compute item-item cosine similarity.
    mat is (customers × products); transpose to (products × customers) first.
    Returns: ndarray (n_products × n_products)
    """
    item_mat = mat.T   # (products × customers)
    logger.info(f"Computing {item_mat.shape[0]}×{item_mat.shape[0]} cosine similarity…")
    sim = cosine_similarity(item_mat, dense_output=True)
    np.fill_diagonal(sim, 0.0)   # exclude self-similarity
    return sim.astype(np.float32)


# ── top-N similar products per product ───────────────────────────────────────

def _top_similar(sim: np.ndarray, products: list, prod_info: pd.DataFrame,
                 n: int = TOP_SIMILAR) -> dict:
    """
    For each product, return its top-n similar products with similarity scores.
    """
    prod_info_idx = prod_info.set_index("product_key")
    result = {}
    for i, pk in enumerate(products):
        top_idx   = np.argpartition(sim[i], -n)[-n:]
        top_idx   = top_idx[np.argsort(sim[i][top_idx])[::-1]]
        similar   = []
        for j in top_idx:
            score = float(sim[i][j])
            if score < 0.001:
                continue
            tpk  = products[j]
            row  = prod_info_idx.loc[tpk] if tpk in prod_info_idx.index else None
            similar.append({
                "product_key" : int(tpk),
                "product_id"  : str(row["product_id"])  if row is not None else str(tpk),
                "product_name": str(row["product_name"]) if row is not None else "",
                "similarity"  : round(score, 4),
            })
        result[int(pk)] = similar
    return result


# ── per-customer recommendations ─────────────────────────────────────────────

def _customer_recs(mat: csr_matrix, sim: np.ndarray,
                   customers: list, products: list,
                   cust_info: pd.DataFrame, prod_info: pd.DataFrame,
                   n: int = TOP_RECS) -> pd.DataFrame:
    """
    Score each un-purchased product for each customer:
        score(u, p) = Σ_{i ∈ purchased(u)} sim[i, p] × weight(u, i)
    where weight = log1p(qty) (already in the matrix).

    Returns a DataFrame with one row per (customer, recommended product).
    """
    logger.info("Scoring recommendations for all customers…")
    ci_map  = cust_info.groupby("customer_key").first()
    pi_map  = prod_info.set_index("product_key")

    records = []
    mat_dense = mat.toarray()   # (customers × products)  float32

    for ci, ck in enumerate(customers):
        user_vec     = mat_dense[ci]              # (n_products,)
        purchased    = user_vec > 0               # boolean mask
        if not purchased.any():
            continue

        # score = user_weights @ sim  (dot product)
        scores = user_vec @ sim                   # (n_products,)
        scores[purchased] = 0.0                   # zero out already-purchased
        top_idx = np.argpartition(scores, -n)[-n:]
        top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

        cust_row = ci_map.loc[ck] if ck in ci_map.index else None

        for rank, pi in enumerate(top_idx):
            score = float(scores[pi])
            if score < 0.001:
                break
            pk  = products[pi]
            pr  = pi_map.loc[pk] if pk in pi_map.index else None
            records.append({
                "customer_key"  : int(ck),
                "customer_id"   : str(cust_row["customer_id"])   if cust_row is not None else "",
                "customer_name" : str(cust_row["customer_name"]) if cust_row is not None else "",
                "rank"          : rank + 1,
                "product_key"   : int(pk),
                "product_id"    : str(pr["product_id"])   if pr is not None else str(pk),
                "product_name"  : str(pr["product_name"]) if pr is not None else "",
                "rec_score"     : round(score, 4),
            })

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df):,} recommendations for {df['customer_key'].nunique()} customers")
    return df


# ── popular products (cold-start fallback) ────────────────────────────────────

def _popular_products(df_purchases: pd.DataFrame, n: int = 20) -> list:
    """Top-n products by number of unique buyers."""
    pop = (df_purchases.groupby(["product_key", "product_id", "product_name"])
                       .agg(unique_buyers=("customer_key", "nunique"),
                            total_qty    =("total_qty",    "sum"),
                            total_revenue=("total_amount", "sum"))
                       .reset_index()
                       .nlargest(n, "unique_buyers"))
    records = []
    for _, row in pop.iterrows():
        records.append({
            "product_key"   : int(row.product_key),
            "product_id"    : str(row.product_id),
            "product_name"  : str(row.product_name),
            "unique_buyers" : int(row.unique_buyers),
            "total_qty"     : int(row.total_qty),
            "total_revenue" : _r2(row.total_revenue),
        })
    return records


# ── main ──────────────────────────────────────────────────────────────────────

class RecommendationEngine:

    def run(self):
        logger.info("=== Product Recommendation Engine START ===")

        # 1. Load purchases
        logger.info("Loading customer-product purchases…")
        df = _load_purchases()

        prod_info = df[["product_key", "product_id", "product_name"]].drop_duplicates()
        cust_info = df[["customer_key", "customer_id", "customer_name"]].drop_duplicates()

        # 2. Sparse matrix
        logger.info("Building sparse customer-product matrix…")
        mat, customers, products, cust_idx, prod_idx = _build_matrix(df)

        n_customers  = len(customers)
        n_products   = len(products)
        sparsity_pct = (1 - mat.nnz / (n_customers * n_products)) * 100

        # 3. Item-item similarity
        sim = _item_similarity(mat)

        avg_sim = float(np.mean(sim[sim > 0])) if (sim > 0).any() else 0.0
        logger.info(f"  Avg non-zero similarity: {avg_sim:.4f}")

        # 4. Top-similar per product
        logger.info("Computing top similar products per item…")
        item_sim_map = _top_similar(sim, products, prod_info)

        # 5. Per-customer recommendations
        rec_df = _customer_recs(mat, sim, customers, products, cust_info, prod_info)

        # 6. Popular products
        popular = _popular_products(df)

        # 7. Save CSV
        rec_df.to_csv(RESULTS_CSV, index=False)
        logger.info(f"Saved CSV → {RESULTS_CSV}  ({len(rec_df):,} rows)")

        # 8. Per-customer recs index for JSON (top-10 per customer, capped at MAX_CUSTOMERS)
        cust_recs_json = {}
        for ck, grp in rec_df.groupby("customer_key"):
            top = grp.nsmallest(TOP_RECS, "rank")
            cust_recs_json[str(int(ck))] = {
                "customer_id"  : str(grp.iloc[0]["customer_id"]),
                "customer_name": str(grp.iloc[0]["customer_name"]),
                "recommendations": [
                    {
                        "rank"        : int(r["rank"]),
                        "product_key" : int(r["product_key"]),
                        "product_id"  : str(r["product_id"]),
                        "product_name": str(r["product_name"]),
                        "rec_score"   : float(r["rec_score"]),
                    }
                    for _, r in top.iterrows()
                ]
            }
            if len(cust_recs_json) >= MAX_CUSTOMERS:
                break

        # 9. Top recommended products overall
        top_rec_products = (rec_df.groupby(["product_key", "product_id", "product_name"])
                                  .agg(recommendation_count=("customer_key", "count"),
                                       avg_score=("rec_score", "mean"))
                                  .reset_index()
                                  .nlargest(20, "recommendation_count"))
        top_rec_list = [
            {
                "product_key"          : int(r.product_key),
                "product_id"           : str(r.product_id),
                "product_name"         : str(r.product_name),
                "recommendation_count" : int(r.recommendation_count),
                "avg_score"            : _r2(r.avg_score),
            }
            for _, r in top_rec_products.iterrows()
        ]

        # 10. Save metadata
        metadata = {
            "generated_at"        : datetime.now().isoformat(),
            "n_customers"         : n_customers,
            "n_products"          : n_products,
            "n_pairs"             : int(mat.nnz),
            "sparsity_pct"        : _r2(sparsity_pct),
            "avg_similarity"      : _r2(avg_sim),
            "avg_recs_per_customer": _r2(len(rec_df) / rec_df["customer_key"].nunique()
                                        if rec_df["customer_key"].nunique() > 0 else 0),
            "customers_served"    : int(rec_df["customer_key"].nunique()),
            "coverage_pct"        : _r2(rec_df["customer_key"].nunique() / n_customers * 100),
            "popular_products"    : popular,
            "top_recommended"     : top_rec_list,
            "item_similarity"     : {str(k): v for k, v in item_sim_map.items()},
            "customer_recs"       : cust_recs_json,
        }

        os.makedirs("models", exist_ok=True)
        with open(METADATA_JSON, "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata → {METADATA_JSON}")

        # Summary
        logger.info(f"Customers served: {metadata['customers_served']} "
                    f"({metadata['coverage_pct']:.1f}% coverage)")
        logger.info(f"Sparsity: {sparsity_pct:.2f}%  "
                    f"Avg similarity: {avg_sim:.4f}  "
                    f"Avg recs/customer: {metadata['avg_recs_per_customer']:.1f}")
        logger.info("Top 5 most-recommended products:")
        for p in top_rec_list[:5]:
            logger.info(f"  {p['product_name'][:50]:50s}  "
                        f"recommended to {p['recommendation_count']} customers")
        logger.info("=== Product Recommendation Engine DONE ===")
        return metadata


if __name__ == "__main__":
    try:
        engine_obj = RecommendationEngine()
        meta = engine_obj.run()
        print(f"\nDone.")
        print(f"  Products: {meta['n_products']}  Customers: {meta['n_customers']}")
        print(f"  Pairs: {meta['n_pairs']:,}  Sparsity: {meta['sparsity_pct']:.1f}%")
        print(f"  Avg similarity: {meta['avg_similarity']}  "
              f"Coverage: {meta['coverage_pct']:.1f}%")
        print(f"\nTop recommended products:")
        for p in meta["top_recommended"][:5]:
            print(f"  [{p['recommendation_count']:4d} customers]  {p['product_name'][:55]}")
    except Exception as e:
        logger.error(f"Recommendation engine failed: {e}", exc_info=True)
        raise
