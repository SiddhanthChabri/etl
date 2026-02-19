"""
Analytics Dashboard Generator
Creates interactive HTML dashboard with all analytics visualizations
"""

import pandas as pd
from datetime import datetime
import json


def generate_analytics_dashboard():
    """Generate comprehensive analytics dashboard with all insights"""

    print("="*70)
    print("📊 GENERATING ANALYTICS DASHBOARD")
    print("="*70)

    # Load CSV results
    try:
        rfm_df = pd.read_csv('rfm_analysis_results.csv')
        abc_df = pd.read_csv('abc_analysis_results.csv')
        cohort_df = pd.read_csv('cohort_retention_matrix.csv', index_col=0)
        clv_df = pd.read_csv('clv_analysis_results.csv')
        basket_df = pd.read_csv('market_basket_results.csv')

        print("✅ Loaded all analytics results")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("\n💡 Run test_advanced_analytics.py first to generate results")
        return None

    # Calculate summary metrics
    total_customers = len(rfm_df)
    total_products = len(abc_df)
    avg_clv = clv_df['clv_discounted'].mean()

    # Segment distributions
    rfm_segments = rfm_df['segment'].value_counts().to_dict()
    abc_segments = abc_df['abc_class'].value_counts().to_dict()
    clv_segments = clv_df['clv_segment'].value_counts().to_dict()

    # Top items (using correct column names)
    top_customers = rfm_df.nlargest(5, 'monetary')[['customer_name', 'monetary', 'segment']].to_dict('records')

    # Handle NULL product names
    abc_df['product_name'] = abc_df['product_name'].fillna('Unknown Product')
    top_products = abc_df.nlargest(5, 'total_revenue')[['product_name', 'total_revenue', 'abc_class']].to_dict('records')

    # Top associations (if any exist)
    if len(basket_df) > 0:
        top_associations = basket_df.head(5)[['product_a', 'product_b', 'support', 'confidence_a_to_b']].to_dict('records')
    else:
        top_associations = []

    # Generate HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Advanced Analytics Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                min-height: 100vh;
            }}

            .container {{
                max-width: 1600px;
                margin: 0 auto;
            }}

            .header {{
                background: white;
                padding: 40px;
                border-radius: 15px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                margin-bottom: 30px;
                text-align: center;
            }}

            .header h1 {{
                color: #667eea;
                font-size: 3em;
                margin-bottom: 10px;
            }}

            .header p {{
                color: #666;
                font-size: 1.2em;
            }}

            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}

            .metric-card {{
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                text-align: center;
                transition: transform 0.3s;
            }}

            .metric-card:hover {{
                transform: translateY(-5px);
            }}

            .metric-icon {{
                font-size: 3em;
                margin-bottom: 15px;
            }}

            .metric-value {{
                font-size: 2.5em;
                font-weight: bold;
                margin: 10px 0;
                color: #667eea;
            }}

            .metric-label {{
                color: #666;
                font-size: 1em;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}

            .section {{
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }}

            .section h2 {{
                color: #333;
                margin-bottom: 25px;
                font-size: 1.8em;
                border-bottom: 3px solid #667eea;
                padding-bottom: 10px;
            }}

            .charts-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
                gap: 30px;
                margin-bottom: 30px;
            }}

            .chart-container {{
                background: white;
                padding: 25px;
                border-radius: 15px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }}

            .chart-container h3 {{
                color: #333;
                margin-bottom: 20px;
                font-size: 1.3em;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}

            th {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 15px;
                text-align: left;
                font-weight: 600;
            }}

            td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e9ecef;
            }}

            tr:hover {{
                background: #f8f9fa;
            }}

            .segment-badge {{
                padding: 5px 12px;
                border-radius: 20px;
                font-size: 0.85em;
                font-weight: bold;
            }}

            .champions {{ background: #d4edda; color: #155724; }}
            .loyal {{ background: #cfe2ff; color: #084298; }}
            .potential {{ background: #fff3cd; color: #664d03; }}
            .class-a {{ background: #d4edda; color: #155724; }}
            .class-b {{ background: #cfe2ff; color: #084298; }}
            .class-c {{ background: #f8d7da; color: #721c24; }}

            .insight-box {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 25px;
                border-radius: 15px;
                margin: 20px 0;
            }}

            .insight-box h3 {{
                margin-bottom: 15px;
                font-size: 1.5em;
            }}

            .insight-box ul {{
                list-style: none;
                padding: 0;
            }}

            .insight-box li {{
                padding: 10px 0;
                border-bottom: 1px solid rgba(255,255,255,0.2);
            }}

            .insight-box li:last-child {{
                border-bottom: none;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🧮 Advanced Analytics Dashboard</h1>
                <p>Comprehensive Business Intelligence • Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>

            <!-- KPI Metrics -->
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-icon">👥</div>
                    <div class="metric-value">{total_customers:,}</div>
                    <div class="metric-label">Total Customers</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon">📦</div>
                    <div class="metric-value">{total_products:,}</div>
                    <div class="metric-label">Total Products</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon">💰</div>
                    <div class="metric-value">${avg_clv:,.0f}</div>
                    <div class="metric-label">Avg Customer CLV</div>
                </div>

                <div class="metric-card">
                    <div class="metric-icon">⭐</div>
                    <div class="metric-value">{rfm_segments.get('Champions', 0):,}</div>
                    <div class="metric-label">Champion Customers</div>
                </div>
            </div>

            <!-- RFM Analysis Section -->
            <div class="section">
                <h2>🎯 RFM Analysis - Customer Segmentation</h2>

                <div class="charts-grid">
                    <div class="chart-container">
                        <h3>Customer Segments Distribution</h3>
                        <canvas id="rfmChart"></canvas>
                    </div>

                    <div class="chart-container">
                        <h3>Top 5 Segments</h3>
                        <canvas id="rfmBarChart"></canvas>
                    </div>
                </div>

                <h3>🏆 Top 5 Customers by Revenue</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Customer Name</th>
                            <th>Total Revenue</th>
                            <th>Segment</th>
                        </tr>
                    </thead>
                    <tbody>
    """

    # Add top customers
    for customer in top_customers:
        segment_class = customer['segment'].lower().replace(' ', '-')
        html += f"""
                        <tr>
                            <td>{customer['customer_name']}</td>
                            <td>${customer['monetary']:,.2f}</td>
                            <td><span class="segment-badge {segment_class}">{customer['segment']}</span></td>
                        </tr>
        """

    html += """
                    </tbody>
                </table>

                <div class="insight-box">
                    <h3>💡 Key Insights - RFM</h3>
                    <ul>
    """

    # Add RFM insights
    champions_pct = (rfm_segments.get('Champions', 0) / total_customers) * 100
    at_risk_pct = (rfm_segments.get('At Risk', 0) / total_customers) * 100

    html += f"""
                        <li>🏆 <strong>{rfm_segments.get('Champions', 0):,}</strong> Champions ({champions_pct:.1f}%) - Your best customers driving revenue</li>
                        <li>⚠️ <strong>{rfm_segments.get('At Risk', 0):,}</strong> At Risk ({at_risk_pct:.1f}%) - Need immediate retention efforts</li>
                        <li>📈 Focus on moving "Potential Loyalists" to "Loyal Customers" with targeted campaigns</li>
                        <li>💌 Re-engage "About to Sleep" segment with personalized offers</li>
                    </ul>
                </div>
            </div>

            <!-- ABC Analysis Section -->
            <div class="section">
                <h2>📦 ABC Analysis - Product Classification</h2>

                <div class="charts-grid">
                    <div class="chart-container">
                        <h3>ABC Class Distribution</h3>
                        <canvas id="abcChart"></canvas>
                    </div>

                    <div class="chart-container">
                        <h3>Revenue by ABC Class</h3>
                        <canvas id="abcRevenueChart"></canvas>
                    </div>
                </div>

                <h3>⭐ Top 5 Products (Class A)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Product Name</th>
                            <th>Total Revenue</th>
                            <th>Class</th>
                        </tr>
                    </thead>
                    <tbody>
    """

    # Add top products
    for product in top_products:
        product_name = product['product_name'][:60] if product['product_name'] else 'Unknown Product'
        html += f"""
                        <tr>
                            <td>{product_name}</td>
                            <td>${product['total_revenue']:,.2f}</td>
                            <td><span class="segment-badge class-{product['abc_class'].lower()}">{product['abc_class']}</span></td>
                        </tr>
        """

    html += f"""
                    </tbody>
                </table>

                <div class="insight-box">
                    <h3>💡 Key Insights - ABC</h3>
                    <ul>
                        <li>📊 Class A: <strong>{abc_segments.get('A', 0):,}</strong> products generating ~70% of revenue</li>
                        <li>📈 Class B: <strong>{abc_segments.get('B', 0):,}</strong> products contributing ~20% of revenue</li>
                        <li>📉 Class C: <strong>{abc_segments.get('C', 0):,}</strong> products making up ~10% of revenue</li>
                        <li>🎯 Focus inventory management and marketing efforts on Class A products</li>
                    </ul>
                </div>
            </div>

            <!-- CLV Analysis Section -->
            <div class="section">
                <h2>💰 Customer Lifetime Value (CLV)</h2>

                <div class="charts-grid">
                    <div class="chart-container">
                        <h3>CLV Segments Distribution</h3>
                        <canvas id="clvChart"></canvas>
                    </div>

                    <div class="chart-container">
                        <h3>CLV Statistics</h3>
                        <canvas id="clvStatsChart"></canvas>
                    </div>
                </div>

                <div class="insight-box">
                    <h3>💡 Key Insights - CLV</h3>
                    <ul>
                        <li>💎 Average Customer Lifetime Value: <strong>${avg_clv:,.2f}</strong></li>
                        <li>📊 High Value segment: <strong>{clv_segments.get('Very High Value', 0):,}</strong> customers</li>
                        <li>🎯 Target high CLV customers with premium products and loyalty programs</li>
                        <li>📈 Invest in increasing purchase frequency for medium value customers</li>
                    </ul>
                </div>
            </div>
    """

    # Only add market basket section if data exists
    if len(top_associations) > 0:
        html += """
            <!-- Market Basket Analysis Section -->
            <div class="section">
                <h2>🛒 Market Basket Analysis - Product Associations</h2>

                <h3>🔗 Top 5 Product Associations</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Product A</th>
                            <th>Product B</th>
                            <th>Support</th>
                            <th>Confidence</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        # Add top associations
        for assoc in top_associations:
            html += f"""
                            <tr>
                                <td>{assoc['product_a'][:40]}</td>
                                <td>{assoc['product_b'][:40]}</td>
                                <td>{assoc['support']:.3f}</td>
                                <td>{assoc['confidence_a_to_b']:.3f}</td>
                            </tr>
            """

        html += f"""
                        </tbody>
                    </table>

                    <div class="insight-box">
                        <h3>💡 Key Insights - Market Basket</h3>
                        <ul>
                            <li>🔗 Found <strong>{len(basket_df):,}</strong> significant product associations</li>
                            <li>🎯 Use associations for cross-selling and product recommendations</li>
                            <li>📦 Optimize product placement and bundling strategies</li>
                            <li>💼 Create combo offers based on frequently bought together items</li>
                        </ul>
                    </div>
                </div>
        """

    html += """
        </div>

        <script>
            // RFM Segments Chart
            const rfmCtx = document.getElementById('rfmChart').getContext('2d');
            new Chart(rfmCtx, {
                type: 'doughnut',
                data: {
                    labels: """ + json.dumps(list(rfm_segments.keys())) + """,
                    datasets: [{
                        data: """ + json.dumps(list(rfm_segments.values())) + """,
                        backgroundColor: [
                            '#48bb78', '#4299e1', '#ed8936', '#f6ad55',
                            '#fc8181', '#f56565', '#e53e3e', '#c53030'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'right' }
                    }
                }
            });

            // RFM Bar Chart
            const rfmBarCtx = document.getElementById('rfmBarChart').getContext('2d');
            new Chart(rfmBarCtx, {
                type: 'bar',
                data: {
                    labels: """ + json.dumps(list(rfm_segments.keys())[:5]) + """,
                    datasets: [{
                        label: 'Number of Customers',
                        data: """ + json.dumps(list(rfm_segments.values())[:5]) + """,
                        backgroundColor: '#667eea'
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });

            // ABC Chart
            const abcCtx = document.getElementById('abcChart').getContext('2d');
            new Chart(abcCtx, {
                type: 'pie',
                data: {
                    labels: """ + json.dumps(list(abc_segments.keys())) + """,
                    datasets: [{
                        data: """ + json.dumps(list(abc_segments.values())) + """,
                        backgroundColor: ['#48bb78', '#4299e1', '#f56565']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'bottom' }
                    }
                }
            });

            // ABC Revenue Chart
            const abcRevenueCtx = document.getElementById('abcRevenueChart').getContext('2d');
            new Chart(abcRevenueCtx, {
                type: 'doughnut',
                data: {
                    labels: ['Class A (70%)', 'Class B (20%)', 'Class C (10%)'],
                    datasets: [{
                        data: [70, 20, 10],
                        backgroundColor: ['#48bb78', '#4299e1', '#f56565']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'bottom' }
                    }
                }
            });

            // CLV Chart
            const clvCtx = document.getElementById('clvChart').getContext('2d');
            new Chart(clvCtx, {
                type: 'bar',
                data: {
                    labels: """ + json.dumps(list(clv_segments.keys())) + """,
                    datasets: [{
                        label: 'Number of Customers',
                        data: """ + json.dumps(list(clv_segments.values())) + """,
                        backgroundColor: '#667eea'
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });

            // CLV Stats Chart
            const clvStatsCtx = document.getElementById('clvStatsChart').getContext('2d');
            new Chart(clvStatsCtx, {
                type: 'bar',
                data: {
                    labels: ['Avg CLV', 'Median CLV', 'Max CLV'],
                    datasets: [{
                        label: 'CLV ($)',
                        data: [""" + str(clv_df['clv_discounted'].mean()) + """, 
                               """ + str(clv_df['clv_discounted'].median()) + """, 
                               """ + str(clv_df['clv_discounted'].max()) + """],
                        backgroundColor: ['#48bb78', '#4299e1', '#f6ad55']
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        </script>
    </body>
    </html>
    """

    # Save HTML
    filename = f"analytics_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ Analytics dashboard generated: {filename}")
    return filename


if __name__ == "__main__":
    print("="*70)
    print("📊 GENERATING ANALYTICS DASHBOARD")
    print("="*70)

    filename = generate_analytics_dashboard()

    if filename:
        print(f"\n✅ Dashboard saved to: {filename}")
        print("\n📝 Open this file in your web browser to view all analytics")
        print("\n💡 The dashboard includes:")
        print("  • RFM Customer Segmentation")
        print("  • ABC Product Classification")
        print("  • Customer Lifetime Value")
        print("  • Market Basket Analysis (if available)")
        print("  • Interactive charts and insights")