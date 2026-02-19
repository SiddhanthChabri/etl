-- ============================================================================
-- ANALYTICS VERIFICATION QUERIES
-- Cross-check Python analytics results with PostgreSQL database
-- ============================================================================

-- ============================================================================
-- 1. RFM ANALYSIS VERIFICATION
-- ============================================================================

-- Total customers analyzed
SELECT COUNT(DISTINCT c.customer_id) as total_customers
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
WHERE c.is_current = TRUE;

-- Customer segments (manually calculate RFM)
WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.state,
        MAX(t.date) as last_purchase_date,
        COUNT(DISTINCT fs.sales_key) as frequency,
        SUM(fs.sales_amount) as monetary,
        CURRENT_DATE - MAX(t.date) as recency_days
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_name, c.state
)
SELECT 
    COUNT(*) as customers,
    AVG(recency_days) as avg_recency,
    AVG(frequency) as avg_frequency,
    AVG(monetary) as avg_monetary,
    SUM(monetary) as total_revenue
FROM customer_rfm;

-- Top 10 customers by revenue (Champions)
WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.state,
        c.city,
        COUNT(DISTINCT fs.sales_key) as frequency,
        SUM(fs.sales_amount) as monetary,
        CURRENT_DATE - MAX(t.date) as recency_days
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_name, c.state, c.city
)
SELECT 
    customer_name,
    state,
    city,
    frequency,
    monetary,
    recency_days
FROM customer_rfm
ORDER BY monetary DESC
LIMIT 10;


-- ============================================================================
-- 2. ABC ANALYSIS VERIFICATION
-- ============================================================================

-- Total products analyzed
SELECT COUNT(DISTINCT p.product_id) as total_products
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key;

-- Product revenue distribution
WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        SUM(fs.sales_amount) as total_revenue,
        COUNT(DISTINCT fs.sales_key) as transaction_count
    FROM fact_sales fs
    JOIN dim_product p ON fs.product_key = p.product_key
    GROUP BY p.product_id, p.product_name, p.category
),
revenue_with_cumulative AS (
    SELECT 
        product_id,
        product_name,
        category,
        total_revenue,
        transaction_count,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) as cumulative_revenue,
        SUM(total_revenue) OVER () as total_all_revenue
    FROM product_revenue
)
SELECT 
    COUNT(*) as products,
    SUM(total_revenue) as revenue,
    ROUND(SUM(total_revenue) / MAX(total_all_revenue) * 100, 2) as revenue_percentage
FROM revenue_with_cumulative
WHERE (cumulative_revenue / total_all_revenue * 100) <= 70;  -- Class A

-- Top 10 products by revenue (Class A)
SELECT 
    p.product_name,
    p.category,
    SUM(fs.sales_amount) as total_revenue,
    COUNT(DISTINCT fs.sales_key) as transactions
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================================
-- 3. COHORT ANALYSIS VERIFICATION
-- ============================================================================

-- Cohort retention - First purchase cohorts
WITH customer_first_purchase AS (
    SELECT 
        c.customer_id,
        DATE_TRUNC('month', MIN(t.date)) as cohort_month
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id
),
customer_purchases AS (
    SELECT 
        cfp.customer_id,
        cfp.cohort_month,
        DATE_TRUNC('month', t.date) as purchase_month
    FROM customer_first_purchase cfp
    JOIN fact_sales fs ON fs.customer_key = (
        SELECT customer_key FROM dim_customer WHERE customer_id = cfp.customer_id AND is_current = TRUE LIMIT 1
    )
    JOIN dim_time t ON fs.time_key = t.time_key
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) as cohort_size,
    COUNT(DISTINCT CASE 
        WHEN purchase_month = cohort_month + INTERVAL '1 month' 
        THEN customer_id 
    END) as month_1_retention,
    ROUND(
        COUNT(DISTINCT CASE WHEN purchase_month = cohort_month + INTERVAL '1 month' THEN customer_id END)::numeric 
        / COUNT(DISTINCT customer_id) * 100, 
        2
    ) as month_1_retention_pct
FROM customer_purchases
GROUP BY cohort_month
ORDER BY cohort_month DESC
LIMIT 10;


-- ============================================================================
-- 4. CLV (CUSTOMER LIFETIME VALUE) VERIFICATION
-- ============================================================================

-- Calculate CLV for all customers
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.state,
        COUNT(DISTINCT fs.sales_key) as purchase_count,
        AVG(fs.sales_amount) as avg_purchase_value,
        SUM(fs.sales_amount) as total_revenue,
        MIN(t.date) as first_purchase,
        MAX(t.date) as last_purchase,
        EXTRACT(DAYS FROM (MAX(t.date) - MIN(t.date)))::numeric / 365.25 as lifespan_years
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_name, c.state
),
customer_clv AS (
    SELECT 
        customer_id,
        customer_name,
        state,
        purchase_count,
        avg_purchase_value,
        total_revenue,
        lifespan_years,
        purchase_count / GREATEST(lifespan_years, 0.1) as purchase_frequency,
        (avg_purchase_value * (purchase_count / GREATEST(lifespan_years, 0.1)) * GREATEST(lifespan_years, 0.1)) 
        / POWER(1.10, GREATEST(lifespan_years, 0.1)) as clv_discounted
    FROM customer_metrics
)
SELECT 
    COUNT(*) as total_customers,
    ROUND(AVG(clv_discounted), 2) as avg_clv,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY clv_discounted), 2) as median_clv,
    ROUND(SUM(clv_discounted), 2) as total_clv,
    ROUND(MAX(clv_discounted), 2) as max_clv
FROM customer_clv;

-- Top 10 customers by CLV
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.state,
        COUNT(DISTINCT fs.sales_key) as purchase_count,
        AVG(fs.sales_amount) as avg_purchase_value,
        SUM(fs.sales_amount) as total_revenue,
        EXTRACT(DAYS FROM (MAX(t.date) - MIN(t.date)))::numeric / 365.25 as lifespan_years
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.customer_name, c.state
)
SELECT 
    customer_name,
    state,
    purchase_count,
    ROUND(avg_purchase_value, 2) as avg_purchase,
    ROUND(total_revenue, 2) as total_revenue,
    ROUND(
        (avg_purchase_value * (purchase_count / GREATEST(lifespan_years, 0.1)) * GREATEST(lifespan_years, 0.1)) 
        / POWER(1.10, GREATEST(lifespan_years, 0.1)), 
        2
    ) as clv_discounted
FROM customer_metrics
ORDER BY clv_discounted DESC
LIMIT 10;


-- ============================================================================
-- 5. MARKET BASKET ANALYSIS VERIFICATION
-- ============================================================================

-- Total transactions
SELECT 
    COUNT(DISTINCT CONCAT(fs.time_key, '-', fs.customer_key)) as total_transactions
FROM fact_sales fs;

-- Product pairs bought together
WITH transactions AS (
    SELECT 
        CONCAT(fs.time_key, '-', fs.customer_key) as transaction_id,
        p.product_name
    FROM fact_sales fs
    JOIN dim_product p ON fs.product_key = p.product_key
    WHERE p.product_name IS NOT NULL
),
product_pairs AS (
    SELECT 
        t1.transaction_id,
        t1.product_name as product_a,
        t2.product_name as product_b
    FROM transactions t1
    JOIN transactions t2 ON t1.transaction_id = t2.transaction_id
    WHERE t1.product_name < t2.product_name  -- Avoid duplicates
)
SELECT 
    product_a,
    product_b,
    COUNT(*) as pair_count,
    ROUND(
        COUNT(*)::numeric / (SELECT COUNT(DISTINCT transaction_id) FROM transactions), 
        4
    ) as support
FROM product_pairs
GROUP BY product_a, product_b
HAVING COUNT(*) > 10  -- Minimum threshold
ORDER BY pair_count DESC
LIMIT 10;


-- ============================================================================
-- 6. OVERALL DATA QUALITY CHECKS
-- ============================================================================

-- Total records per table
SELECT 'fact_sales' as table_name, COUNT(*) as record_count FROM fact_sales
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'dim_store', COUNT(*) FROM dim_store;

-- Date range of sales data
SELECT 
    MIN(t.date) as earliest_sale,
    MAX(t.date) as latest_sale,
    COUNT(DISTINCT t.date) as days_with_sales
FROM fact_sales fs
JOIN dim_time t ON fs.time_key = t.time_key;

-- Revenue summary
SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    COUNT(DISTINCT fs.product_key) as unique_products,
    ROUND(SUM(fs.sales_amount), 2) as total_revenue,
    ROUND(AVG(fs.sales_amount), 2) as avg_transaction_value,
    ROUND(SUM(fs.quantity_sold), 0) as total_quantity_sold
FROM fact_sales fs;

-- Revenue by year
SELECT 
    t.year,
    COUNT(*) as transactions,
    ROUND(SUM(fs.sales_amount), 2) as revenue
FROM fact_sales fs
JOIN dim_time t ON fs.time_key = t.time_key
GROUP BY t.year
ORDER BY t.year;


-- ============================================================================
-- 7. COMPARE WITH CSV RESULTS
-- ============================================================================

-- To compare with your CSV files, run these queries:

-- RFM: Compare total customers
-- Expected: Should match row count in rfm_analysis_results.csv
SELECT COUNT(*) FROM (
    SELECT DISTINCT c.customer_id
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    WHERE c.is_current = TRUE
) x;

-- ABC: Compare total products
-- Expected: Should match row count in abc_analysis_results.csv
SELECT COUNT(*) FROM (
    SELECT DISTINCT p.product_id
    FROM fact_sales fs
    JOIN dim_product p ON fs.product_key = p.product_key
) x;

-- CLV: Compare average CLV
-- Expected: Should match avg_clv in dashboard
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        COUNT(DISTINCT fs.sales_key) as purchase_count,
        AVG(fs.sales_amount) as avg_purchase_value,
        EXTRACT(DAYS FROM (MAX(t.date) - MIN(t.date)))::numeric / 365.25 as lifespan_years
    FROM fact_sales fs
    JOIN dim_customer c ON fs.customer_key = c.customer_key
    JOIN dim_time t ON fs.time_key = t.time_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id
)
SELECT 
    ROUND(AVG(
        (avg_purchase_value * (purchase_count / GREATEST(lifespan_years, 0.1)) * GREATEST(lifespan_years, 0.1)) 
        / POWER(1.10, GREATEST(lifespan_years, 0.1))
    ), 2) as avg_clv_sql
FROM customer_metrics;