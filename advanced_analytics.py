"""
Advanced Analytics Module for Retail Data Warehouse
Implements RFM Analysis, ABC Analysis, Cohort Analysis, and more
Author: [Your Name]
Date: February 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text
from db_connection import engine
from logger_config import setup_logger
import warnings
warnings.filterwarnings('ignore')

logger = setup_logger('advanced_analytics')


class AdvancedAnalytics:
    """
    Advanced Analytics for Retail Business Intelligence

    Includes:
    - RFM Analysis (Customer Segmentation)
    - ABC Analysis (Product Classification)
    - Cohort Analysis (Customer Retention)
    - Customer Lifetime Value (CLV)
    - Market Basket Analysis
    """

    def __init__(self):
        """Initialize analytics engine"""
        self.engine = engine
        self.results = {}

    # ========================================================================
    # 1. RFM ANALYSIS - Customer Segmentation
    # ========================================================================

    def rfm_analysis(self, reference_date=None):
        """
        Perform RFM (Recency, Frequency, Monetary) Analysis

        Segments customers based on:
        - Recency: Days since last purchase
        - Frequency: Number of purchases
        - Monetary: Total spend

        Args:
            reference_date: Reference date for recency calculation (default: today)

        Returns:
            DataFrame: Customer segments with RFM scores
        """
        logger.info("="*70)
        logger.info("🎯 RFM ANALYSIS - Customer Segmentation")
        logger.info("="*70)

        # Set reference date
        if reference_date is None:
            reference_date = datetime.now()

        logger.info(f"Reference Date: {reference_date.date()}")

        # Fetch customer transaction data (using actual schema)
        query = text("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.state,
                c.city,
                MAX(t.date) as last_purchase_date,
                COUNT(DISTINCT fs.sales_key) as frequency,
                SUM(fs.sales_amount) as monetary
            FROM fact_sales fs
            JOIN dim_customer c ON fs.customer_key = c.customer_key
            JOIN dim_time t ON fs.time_key = t.time_key
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id, c.customer_name, c.state, c.city
            HAVING COUNT(DISTINCT fs.sales_key) > 0
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logger.info(f"Analyzing {len(df):,} customers")

        # Calculate Recency (days since last purchase)
        df['recency'] = (reference_date - pd.to_datetime(df['last_purchase_date'])).dt.days

        # Calculate RFM Scores (1-5 scale, 5 is best)
        df['r_score'] = pd.qcut(df['recency'], q=5, labels=[5,4,3,2,1], duplicates='drop')
        df['f_score'] = pd.qcut(df['frequency'], q=5, labels=[1,2,3,4,5], duplicates='drop')
        df['m_score'] = pd.qcut(df['monetary'], q=5, labels=[1,2,3,4,5], duplicates='drop')

        # Combine into RFM Score
        df['rfm_score'] = (df['r_score'].astype(str) + 
                          df['f_score'].astype(str) + 
                          df['m_score'].astype(str))

        # Calculate overall RFM value (average)
        df['rfm_value'] = (df['r_score'].astype(int) + 
                          df['f_score'].astype(int) + 
                          df['m_score'].astype(int)) / 3

        # Segment customers
        df['segment'] = df['rfm_value'].apply(self._categorize_rfm)

        # Summary statistics
        logger.info("\n📊 RFM Distribution:")
        logger.info(f"  • Recency: {df['recency'].mean():.0f} days (avg)")
        logger.info(f"  • Frequency: {df['frequency'].mean():.1f} purchases (avg)")
        logger.info(f"  • Monetary: ${df['monetary'].mean():,.2f} (avg)")

        logger.info("\n🎯 Customer Segments:")
        segment_summary = df['segment'].value_counts().sort_index()
        for segment, count in segment_summary.items():
            pct = (count / len(df)) * 100
            logger.info(f"  • {segment}: {count:,} ({pct:.1f}%)")

        # Save results
        self.results['rfm_analysis'] = df

        return df

    def _categorize_rfm(self, score):
        """Categorize customers based on RFM score"""
        if score >= 4.5:
            return "Champions"
        elif score >= 4.0:
            return "Loyal Customers"
        elif score >= 3.5:
            return "Potential Loyalists"
        elif score >= 3.0:
            return "Recent Customers"
        elif score >= 2.5:
            return "Promising"
        elif score >= 2.0:
            return "Need Attention"
        elif score >= 1.5:
            return "About to Sleep"
        else:
            return "At Risk"

    # ========================================================================
    # 2. ABC ANALYSIS - Product Classification
    # ========================================================================

    def abc_analysis(self):
        """
        Perform ABC Analysis on products

        Classifies products based on revenue contribution:
        - Class A: Top 20% revenue (typically 70% of total)
        - Class B: Next 30% revenue (typically 20% of total)
        - Class C: Bottom 50% revenue (typically 10% of total)

        Returns:
            DataFrame: Products with ABC classification
        """
        logger.info("\n" + "="*70)
        logger.info("📦 ABC ANALYSIS - Product Classification")
        logger.info("="*70)

        # Fetch product sales data (using actual schema)
        query = text("""
            SELECT 
                p.product_id,
                p.product_name,
                p.category,
                p.sub_category,
                COUNT(DISTINCT fs.sales_key) as transaction_count,
                SUM(fs.quantity_sold) as total_quantity,
                SUM(fs.sales_amount) as total_revenue
            FROM fact_sales fs
            JOIN dim_product p ON fs.product_key = p.product_key
            GROUP BY p.product_id, p.product_name, p.category, p.sub_category
            HAVING SUM(fs.sales_amount) > 0
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        logger.info(f"Analyzing {len(df):,} products")

        # Sort by revenue (descending)
        df = df.sort_values('total_revenue', ascending=False).reset_index(drop=True)

        # Calculate cumulative revenue percentage
        df['cumulative_revenue'] = df['total_revenue'].cumsum()
        df['cumulative_percentage'] = (df['cumulative_revenue'] / df['total_revenue'].sum()) * 100

        # Calculate individual revenue percentage
        df['revenue_percentage'] = (df['total_revenue'] / df['total_revenue'].sum()) * 100

        # Classify into ABC
        df['abc_class'] = pd.cut(
            df['cumulative_percentage'],
            bins=[0, 70, 90, 100],
            labels=['A', 'B', 'C']
        )

        # Summary statistics
        logger.info("\n📊 ABC Distribution:")
        abc_summary = df.groupby('abc_class').agg({
            'product_id': 'count',
            'total_revenue': 'sum',
            'revenue_percentage': 'sum'
        }).round(2)

        for abc_class in ['A', 'B', 'C']:
            if abc_class in abc_summary.index:
                count = abc_summary.loc[abc_class, 'product_id']
                revenue = abc_summary.loc[abc_class, 'total_revenue']
                pct = abc_summary.loc[abc_class, 'revenue_percentage']
                logger.info(f"  • Class {abc_class}: {count:,} products ({pct:.1f}% revenue) - ${revenue:,.2f}")

        # Save results
        self.results['abc_analysis'] = df

        return df

    # ========================================================================
    # 3. COHORT ANALYSIS - Customer Retention
    # ========================================================================

    def cohort_analysis(self, period='M'):
        """
        Perform Cohort Analysis to track customer retention

        Groups customers by their first purchase month and tracks
        behavior over time

        Args:
            period: Time period ('M' for month, 'Q' for quarter, 'Y' for year)

        Returns:
            DataFrame: Cohort retention matrix
        """
        logger.info("\n" + "="*70)
        logger.info("📈 COHORT ANALYSIS - Customer Retention")
        logger.info("="*70)

        # Fetch transaction data
        query = text("""
            SELECT 
                c.customer_id,
                t.date as purchase_date,
                fs.sales_amount
            FROM fact_sales fs
            JOIN dim_customer c ON fs.customer_key = c.customer_key
            JOIN dim_time t ON fs.time_key = t.time_key
            WHERE c.is_current = TRUE
            ORDER BY c.customer_id, t.date
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df['purchase_date'] = pd.to_datetime(df['purchase_date'])

        logger.info(f"Analyzing {df['customer_id'].nunique():,} customers")

        # Determine cohort (first purchase period)
        df['cohort'] = df.groupby('customer_id')['purchase_date'].transform('min').dt.to_period(period)

        # Calculate period index (periods since first purchase)
        df['purchase_period'] = df['purchase_date'].dt.to_period(period)
        df['cohort_index'] = (df['purchase_period'] - df['cohort']).apply(lambda x: x.n)

        # Count unique customers per cohort and period
        cohort_data = df.groupby(['cohort', 'cohort_index'])['customer_id'].nunique().reset_index()
        cohort_data.columns = ['cohort', 'cohort_index', 'customers']

        # Pivot to create retention matrix
        cohort_matrix = cohort_data.pivot(
            index='cohort',
            columns='cohort_index',
            values='customers'
        )

        # Calculate retention percentages
        cohort_size = cohort_matrix.iloc[:, 0]
        retention_matrix = cohort_matrix.divide(cohort_size, axis=0) * 100

        # Summary
        logger.info("\n📊 Cohort Summary:")
        logger.info(f"  • Total Cohorts: {len(cohort_matrix)}")
        logger.info(f"  • Avg Cohort Size: {cohort_size.mean():.0f} customers")

        if len(retention_matrix.columns) > 1:
            avg_retention = retention_matrix.iloc[:, 1].mean()
            logger.info(f"  • Avg Month-1 Retention: {avg_retention:.1f}%")

        # Save results
        self.results['cohort_retention'] = retention_matrix
        self.results['cohort_counts'] = cohort_matrix

        return retention_matrix, cohort_matrix

    # ========================================================================
    # 4. CUSTOMER LIFETIME VALUE (CLV)
    # ========================================================================

    def calculate_clv(self, discount_rate=0.10):
        """
        Calculate Customer Lifetime Value

        CLV = (Average Purchase Value × Purchase Frequency × Customer Lifespan)

        Args:
            discount_rate: Annual discount rate for future value

        Returns:
            DataFrame: Customers with CLV
        """
        logger.info("\n" + "="*70)
        logger.info("💰 CUSTOMER LIFETIME VALUE (CLV)")
        logger.info("="*70)

        # Fetch customer metrics
        query = text("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.state,
                c.city,
                COUNT(DISTINCT fs.sales_key) as purchase_count,
                AVG(fs.sales_amount) as avg_purchase_value,
                SUM(fs.sales_amount) as total_revenue,
                MIN(t.date) as first_purchase,
                MAX(t.date) as last_purchase,
                COUNT(DISTINCT t.date) as active_days
            FROM fact_sales fs
            JOIN dim_customer c ON fs.customer_key = c.customer_key
            JOIN dim_time t ON fs.time_key = t.time_key
            WHERE c.is_current = TRUE
            GROUP BY c.customer_id, c.customer_name, c.state, c.city
            HAVING COUNT(DISTINCT fs.sales_key) > 0
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        # Calculate customer lifespan (in years)
        df['first_purchase'] = pd.to_datetime(df['first_purchase'])
        df['last_purchase'] = pd.to_datetime(df['last_purchase'])
        df['lifespan_days'] = (df['last_purchase'] - df['first_purchase']).dt.days
        df['lifespan_years'] = df['lifespan_days'] / 365.25
        df['lifespan_years'] = df['lifespan_years'].apply(lambda x: max(x, 0.1))  # Min 0.1 year

        # Calculate purchase frequency (purchases per year)
        df['purchase_frequency'] = df['purchase_count'] / df['lifespan_years']

        # Calculate CLV
        df['clv'] = (df['avg_purchase_value'] * 
                    df['purchase_frequency'] * 
                    df['lifespan_years'])

        # Adjust for discount rate
        df['clv_discounted'] = df['clv'] / (1 + discount_rate) ** df['lifespan_years']

        # Categorize customers by CLV
        df['clv_segment'] = pd.qcut(
            df['clv_discounted'],
            q=4,
            labels=['Low Value', 'Medium Value', 'High Value', 'Very High Value'],
            duplicates='drop'
        )

        # Summary
        logger.info(f"\n📊 CLV Summary:")
        logger.info(f"  • Total Customers: {len(df):,}")
        logger.info(f"  • Avg CLV: ${df['clv_discounted'].mean():,.2f}")
        logger.info(f"  • Median CLV: ${df['clv_discounted'].median():,.2f}")
        logger.info(f"  • Total CLV: ${df['clv_discounted'].sum():,.2f}")

        logger.info("\n🎯 CLV Segments:")
        clv_summary = df.groupby('clv_segment')['clv_discounted'].agg(['count', 'mean', 'sum'])
        for segment in clv_summary.index:
            count = clv_summary.loc[segment, 'count']
            avg = clv_summary.loc[segment, 'mean']
            total = clv_summary.loc[segment, 'sum']
            logger.info(f"  • {segment}: {count:,} customers (Avg: ${avg:,.2f}, Total: ${total:,.2f})")

        # Save results
        self.results['clv_analysis'] = df

        return df

    # ========================================================================
    # 5. MARKET BASKET ANALYSIS
    # ========================================================================

    def market_basket_analysis(self, min_support=0.01):
        """
        Perform Market Basket Analysis to find product associations

        Identifies products frequently bought together

        Args:
            min_support: Minimum support threshold (fraction of transactions)

        Returns:
            DataFrame: Product pairs with association metrics
        """
        logger.info("\n" + "="*70)
        logger.info("🛒 MARKET BASKET ANALYSIS - Product Associations")
        logger.info("="*70)

        # Fetch transaction data
        query = text("""
            SELECT 
                CONCAT(fs.time_key, '-', fs.customer_key) as transaction_id,
                p.product_name as product
            FROM fact_sales fs
            JOIN dim_product p ON fs.product_key = p.product_key
            WHERE p.product_name IS NOT NULL
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        total_transactions = df['transaction_id'].nunique()
        logger.info(f"Analyzing {total_transactions:,} transactions")

        # Create transaction matrix
        basket = df.groupby(['transaction_id', 'product'])['product'].count().unstack().fillna(0)
        basket = basket.applymap(lambda x: 1 if x > 0 else 0)

        # Calculate support for individual items
        item_support = basket.sum() / len(basket)
        frequent_items = item_support[item_support >= min_support].index.tolist()

        logger.info(f"Found {len(frequent_items)} frequent items (support >= {min_support})")

        # Find product pairs
        from itertools import combinations

        pairs = []
        for transaction_items in basket.iterrows():
            items_in_transaction = [item for item in transaction_items[1].index if transaction_items[1][item] == 1]
            if len(items_in_transaction) >= 2:
                for pair in combinations(items_in_transaction, 2):
                    if pair[0] in frequent_items and pair[1] in frequent_items:
                        pairs.append(sorted(pair))

        if not pairs:
            logger.warning("No product pairs found. Try lowering min_support.")
            return pd.DataFrame(columns=['product_a', 'product_b', 'count', 'support', 
                                        'confidence_a_to_b', 'confidence_b_to_a'])

        # Count pair occurrences
        pair_counts = pd.DataFrame(pairs, columns=['product_a', 'product_b'])
        pair_counts = pair_counts.groupby(['product_a', 'product_b']).size().reset_index(name='count')

        # Calculate metrics
        pair_counts['support'] = pair_counts['count'] / total_transactions
        pair_counts['confidence_a_to_b'] = pair_counts.apply(
            lambda row: row['count'] / basket[row['product_a']].sum(), axis=1
        )
        pair_counts['confidence_b_to_a'] = pair_counts.apply(
            lambda row: row['count'] / basket[row['product_b']].sum(), axis=1
        )

        # Sort by support
        pair_counts = pair_counts.sort_values('support', ascending=False)

        # Summary
        logger.info(f"\n📊 Top Product Associations:")
        for idx, row in pair_counts.head(10).iterrows():
            logger.info(f"  • {row['product_a'][:30]} + {row['product_b'][:30]}")
            logger.info(f"    Support: {row['support']:.3f}, Confidence: {row['confidence_a_to_b']:.3f}")

        # Save results
        self.results['market_basket'] = pair_counts

        return pair_counts

    # ========================================================================
    # 6. GENERATE COMPREHENSIVE REPORT
    # ========================================================================

    def generate_analytics_report(self):
        """Generate comprehensive analytics report"""
        logger.info("\n" + "="*70)
        logger.info("📊 COMPREHENSIVE ANALYTICS REPORT")
        logger.info("="*70)

        # Run all analyses
        logger.info("\nRunning all analytics...")

        rfm_df = self.rfm_analysis()
        abc_df = self.abc_analysis()
        retention_matrix, cohort_counts = self.cohort_analysis()
        clv_df = self.calculate_clv()
        basket_df = self.market_basket_analysis()

        logger.info("\n" + "="*70)
        logger.info("✅ ANALYTICS COMPLETE")
        logger.info("="*70)

        return {
            'rfm': rfm_df,
            'abc': abc_df,
            'cohort_retention': retention_matrix,
            'cohort_counts': cohort_counts,
            'clv': clv_df,
            'market_basket': basket_df
        }


if __name__ == "__main__":
    print("🧪 Testing Advanced Analytics...")
    analytics = AdvancedAnalytics()
    results = analytics.generate_analytics_report()
    print("\n✅ Analytics test complete!")