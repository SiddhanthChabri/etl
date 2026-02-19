import time
from sqlalchemy import text
from db_connection import engine
import pandas as pd

class PerformanceBenchmark:
    """Benchmark query performance with and without optimizations"""
    
    def __init__(self):
        self.results = []
    
    def run_query_benchmark(self, test_name, query, description, optimization="NONE"):
        """Execute query and measure performance"""
        print(f"\n🔍 Running: {test_name}")
        print(f"   Optimization: {optimization}")
        
        start_time = time.time()
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            row_count = len(rows)
        
        execution_time_ms = (time.time() - start_time) * 1000
        
        # Log to database
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO query_performance_log 
                    (test_name, query_description, optimization_applied, 
                     execution_time_ms, rows_returned)
                    VALUES (:name, :desc, :opt, :time, :rows)
                """),
                {
                    "name": test_name,
                    "desc": description,
                    "opt": optimization,
                    "time": execution_time_ms,
                    "rows": row_count
                }
            )
        
        self.results.append({
            'test_name': test_name,
            'optimization': optimization,
            'execution_time_ms': execution_time_ms,
            'rows_returned': row_count
        })
        
        print(f"   ✅ Completed in {execution_time_ms:.2f}ms | Rows: {row_count}")
        
        return execution_time_ms, row_count
    
    def run_all_benchmarks(self):
        """Run comprehensive performance benchmarks"""
        
        print("\n" + "="*70)
        print("📊 PERFORMANCE BENCHMARK SUITE")
        print("="*70)
        
        # Benchmark 1: Aggregate sales by month
        self.run_query_benchmark(
            "Monthly Sales Aggregation",
            """
            SELECT 
                t.year,
                t.month,
                SUM(f.sales_amount) as total_revenue,
                COUNT(*) as transaction_count
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
            """,
            "Aggregate sales by month with JOIN",
            "INDEXED"
        )
        
        # Benchmark 2: Same query using materialized view
        self.run_query_benchmark(
            "Monthly Sales Aggregation (MV)",
            """
            SELECT 
                year,
                month,
                total_revenue,
                total_transactions as transaction_count
            FROM mv_monthly_sales
            ORDER BY year, month
            """,
            "Aggregate sales using materialized view",
            "MATERIALIZED_VIEW"
        )
        
        # Benchmark 3: Top products by revenue
        self.run_query_benchmark(
            "Top 10 Products by Revenue",
            """
            SELECT 
                p.product_name,
                p.category,
                SUM(f.sales_amount) as total_revenue,
                SUM(f.quantity_sold) as units_sold
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_key, p.product_name, p.category
            ORDER BY total_revenue DESC
            LIMIT 10
            """,
            "Top products with JOIN and GROUP BY",
            "INDEXED"
        )
        
        # Benchmark 4: Same using materialized view
        self.run_query_benchmark(
            "Top 10 Products by Revenue (MV)",
            """
            SELECT 
                product_name,
                category,
                total_revenue,
                total_units_sold as units_sold
            FROM mv_product_performance
            ORDER BY total_revenue DESC
            LIMIT 10
            """,
            "Top products using materialized view",
            "MATERIALIZED_VIEW"
        )
        
        # Benchmark 5: Customer segment analysis
        self.run_query_benchmark(
            "Customer Segment Analysis",
            """
            SELECT 
                c.customer_segment,
                COUNT(DISTINCT c.customer_key) as customers,
                COUNT(f.sales_key) as purchases,
                SUM(f.sales_amount) as revenue
            FROM dim_customer c
            LEFT JOIN fact_sales f ON c.customer_key = f.customer_key
            WHERE c.is_current = TRUE
            GROUP BY c.customer_segment
            """,
            "Customer segmentation with partial index",
            "PARTIAL_INDEX"
        )
        
        # Benchmark 6: Year-over-year comparison
        self.run_query_benchmark(
            "Year-over-Year Sales Comparison",
            """
            SELECT 
                t.year,
                SUM(f.sales_amount) as annual_revenue,
                COUNT(DISTINCT f.customer_key) as unique_customers
            FROM fact_sales f
            JOIN dim_time t ON f.time_key = t.time_key
            GROUP BY t.year
            ORDER BY t.year
            """,
            "Annual aggregation with composite index",
            "COMPOSITE_INDEX"
        )
        
        # Benchmark 7: Geographic sales
        self.run_query_benchmark(
            "Geographic Sales by Region",
            """
            SELECT 
                s.region,
                COUNT(f.sales_key) as transactions,
                SUM(f.sales_amount) as revenue
            FROM fact_sales f
            JOIN dim_store s ON f.store_key = s.store_key
            GROUP BY s.region
            ORDER BY revenue DESC
            """,
            "Geographic aggregation",
            "INDEXED"
        )
        
        print("\n" + "="*70)
        print("📊 BENCHMARK RESULTS SUMMARY")
        print("="*70)
        
        # Display results
        df = pd.DataFrame(self.results)
        
        print("\n" + df.to_string(index=False))
        
        # Calculate improvements
        print("\n" + "="*70)
        print("⚡ PERFORMANCE IMPROVEMENTS")
        print("="*70)
        
        # Compare MV vs non-MV queries
        monthly_indexed = df[df['test_name'] == 'Monthly Sales Aggregation']['execution_time_ms'].values[0]
        monthly_mv = df[df['test_name'] == 'Monthly Sales Aggregation (MV)']['execution_time_ms'].values[0]
        monthly_improvement = ((monthly_indexed - monthly_mv) / monthly_indexed) * 100
        
        product_indexed = df[df['test_name'] == 'Top 10 Products by Revenue']['execution_time_ms'].values[0]
        product_mv = df[df['test_name'] == 'Top 10 Products by Revenue (MV)']['execution_time_ms'].values[0]
        product_improvement = ((product_indexed - product_mv) / product_indexed) * 100
        
        print(f"\n📈 Monthly Sales Query:")
        print(f"   Without MV: {monthly_indexed:.2f}ms")
        print(f"   With MV:    {monthly_mv:.2f}ms")
        print(f"   Improvement: {monthly_improvement:.1f}% faster")
        
        print(f"\n📈 Top Products Query:")
        print(f"   Without MV: {product_indexed:.2f}ms")
        print(f"   With MV:    {product_mv:.2f}ms")
        print(f"   Improvement: {product_improvement:.1f}% faster")
        
        return df


def capture_table_sizes():
    """Capture current table sizes"""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO table_size_history (table_name, total_size_bytes, table_size_bytes, index_size_bytes, row_count)
            SELECT 
                tablename,
                pg_total_relation_size(schemaname||'.'||tablename),
                pg_relation_size(schemaname||'.'||tablename),
                pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename),
                CASE 
                    WHEN tablename = 'fact_sales' THEN (SELECT COUNT(*) FROM fact_sales)
                    WHEN tablename = 'dim_customer' THEN (SELECT COUNT(*) FROM dim_customer)
                    WHEN tablename = 'dim_product' THEN (SELECT COUNT(*) FROM dim_product)
                    WHEN tablename = 'dim_time' THEN (SELECT COUNT(*) FROM dim_time)
                    WHEN tablename = 'dim_store' THEN (SELECT COUNT(*) FROM dim_store)
                    ELSE 0
                END
            FROM pg_tables
            WHERE schemaname = 'public'
                AND tablename IN ('fact_sales', 'dim_customer', 'dim_product', 'dim_time', 'dim_store')
        """))
    
    print("✅ Table sizes captured")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 PERFORMANCE OPTIMIZATION DEMO")
    print("="*70)
    
    # Capture table sizes
    capture_table_sizes()
    
    # Run benchmarks
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    
    print("\n" + "="*70)
    print("✅ BENCHMARKING COMPLETE")
    print("="*70)
