"""
Real-Time Performance Dashboard Generator
Creates interactive HTML dashboard for ETL performance monitoring
"""

from sqlalchemy import text
from db_connection import engine
from datetime import datetime, timedelta
import json


def generate_performance_dashboard():
    """Generate interactive HTML performance dashboard"""

    # Fetch metrics from database
    metrics = fetch_performance_metrics()

    if not metrics:
        print("❌ No performance data available")
        return None

    # Generate HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ETL Performance Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}

            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                padding: 20px;
                min-height: 100vh;
            }}

            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}

            .header {{
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                margin-bottom: 20px;
                text-align: center;
            }}

            .header h1 {{
                color: #1e3c72;
                font-size: 2.5em;
                margin-bottom: 10px;
            }}

            .header p {{
                color: #666;
                font-size: 1.1em;
            }}

            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 20px;
            }}

            .metric-card {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                text-align: center;
                transition: transform 0.2s;
            }}

            .metric-card:hover {{
                transform: translateY(-5px);
            }}

            .metric-value {{
                font-size: 2.5em;
                font-weight: bold;
                margin: 10px 0;
            }}

            .metric-label {{
                color: #666;
                font-size: 0.9em;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}

            .metric-change {{
                font-size: 0.9em;
                margin-top: 5px;
            }}

            .metric-change.positive {{
                color: #28a745;
            }}

            .metric-change.negative {{
                color: #dc3545;
            }}

            .metric-executions {{ color: #667eea; }}
            .metric-duration {{ color: #f6ad55; }}
            .metric-records {{ color: #48bb78; }}
            .metric-success {{ color: #38b2ac; }}

            .charts-container {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                gap: 20px;
                margin-bottom: 20px;
            }}

            .chart-card {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }}

            .chart-card h2 {{
                color: #333;
                margin-bottom: 20px;
                font-size: 1.3em;
            }}

            .table-container {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                overflow-x: auto;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
            }}

            th {{
                background: #f8f9fa;
                padding: 15px;
                text-align: left;
                font-weight: 600;
                color: #333;
                border-bottom: 2px solid #dee2e6;
            }}

            td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e9ecef;
            }}

            tr:hover {{
                background: #f8f9fa;
            }}

            .status-badge {{
                padding: 5px 12px;
                border-radius: 20px;
                font-size: 0.8em;
                font-weight: bold;
            }}

            .status-success {{
                background: #d4edda;
                color: #155724;
            }}

            .status-failed {{
                background: #f8d7da;
                color: #721c24;
            }}

            .refresh-btn {{
                position: fixed;
                bottom: 30px;
                right: 30px;
                background: #667eea;
                color: white;
                border: none;
                padding: 15px 30px;
                border-radius: 50px;
                font-size: 1em;
                cursor: pointer;
                box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
                transition: all 0.3s;
            }}

            .refresh-btn:hover {{
                background: #5568d3;
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>⚡ ETL Performance Dashboard</h1>
                <p>Real-time monitoring • Last updated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>

            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Total Executions</div>
                    <div class="metric-value metric-executions">{metrics['total_executions']}</div>
                    <div class="metric-change positive">Last 7 days</div>
                </div>

                <div class="metric-card">
                    <div class="metric-label">Avg Duration</div>
                    <div class="metric-value metric-duration">{metrics['avg_duration']:.1f}s</div>
                    <div class="metric-change">Per execution</div>
                </div>

                <div class="metric-card">
                    <div class="metric-label">Total Records</div>
                    <div class="metric-value metric-records">{metrics['total_records']:,.0f}</div>
                    <div class="metric-change positive">Processed</div>
                </div>

                <div class="metric-card">
                    <div class="metric-label">Success Rate</div>
                    <div class="metric-value metric-success">{metrics['success_rate']:.1f}%</div>
                    <div class="metric-change positive">
                        {metrics['successful_executions']}/{metrics['total_executions']} successful
                    </div>
                </div>
            </div>

            <div class="charts-container">
                <div class="chart-card">
                    <h2>📈 Execution Time Trend</h2>
                    <canvas id="durationChart"></canvas>
                </div>

                <div class="chart-card">
                    <h2>📊 Records Processed</h2>
                    <canvas id="recordsChart"></canvas>
                </div>
            </div>

            <div class="charts-container">
                <div class="chart-card">
                    <h2>💻 Resource Usage</h2>
                    <canvas id="resourceChart"></canvas>
                </div>

                <div class="chart-card">
                    <h2>✅ Success vs Failures</h2>
                    <canvas id="statusChart"></canvas>
                </div>
            </div>

            <div class="table-container">
                <h2 style="margin-bottom: 20px;">📋 Recent Executions</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Process Name</th>
                            <th>Start Time</th>
                            <th>Duration</th>
                            <th>Records</th>
                            <th>Status</th>
                            <th>CPU %</th>
                            <th>Memory (MB)</th>
                        </tr>
                    </thead>
                    <tbody>
    """

    # Add recent executions to table
    for execution in metrics['recent_executions']:
        status_class = 'status-success' if execution['status'] == 'SUCCESS' else 'status-failed'
        html += f"""
                        <tr>
                            <td>{execution['process_name']}</td>
                            <td>{execution['start_time'].strftime('%Y-%m-%d %H:%M:%S')}</td>
                            <td>{execution['duration_seconds']:.2f}s</td>
                            <td>{execution['records_processed']:,}</td>
                            <td><span class="status-badge {status_class}">{execution['status']}</span></td>
                            <td>{execution['cpu_percent']:.1f}%</td>
                            <td>{execution['memory_mb']:.1f}</td>
                        </tr>
        """

    # Close HTML and add charts
    html += f"""
                    </tbody>
                </table>
            </div>
        </div>

        <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>

        <script>
            // Duration Trend Chart
            const durationCtx = document.getElementById('durationChart').getContext('2d');
            new Chart(durationCtx, {{
                type: 'line',
                data: {{
                    labels: {json.dumps([t['date'] for t in metrics['trend_data']])},
                    datasets: [{{
                        label: 'Avg Duration (seconds)',
                        data: {json.dumps([t['avg_duration'] for t in metrics['trend_data']])},
                        borderColor: '#f6ad55',
                        backgroundColor: 'rgba(246, 173, 85, 0.1)',
                        tension: 0.4,
                        fill: true
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: true }}
                    }},
                    scales: {{
                        y: {{ beginAtZero: true }}
                    }}
                }}
            }});

            // Records Chart
            const recordsCtx = document.getElementById('recordsChart').getContext('2d');
            new Chart(recordsCtx, {{
                type: 'bar',
                data: {{
                    labels: {json.dumps([t['date'] for t in metrics['trend_data']])},
                    datasets: [{{
                        label: 'Records Processed',
                        data: {json.dumps([t['total_records'] for t in metrics['trend_data']])},
                        backgroundColor: '#48bb78'
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: true }}
                    }},
                    scales: {{
                        y: {{ beginAtZero: true }}
                    }}
                }}
            }});

            // Resource Chart
            const resourceCtx = document.getElementById('resourceChart').getContext('2d');
            new Chart(resourceCtx, {{
                type: 'line',
                data: {{
                    labels: {json.dumps([t['date'] for t in metrics['trend_data']])},
                    datasets: [
                        {{
                            label: 'CPU %',
                            data: {json.dumps([t['avg_cpu'] for t in metrics['trend_data']])},
                            borderColor: '#667eea',
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Memory (MB)',
                            data: {json.dumps([t['avg_memory'] for t in metrics['trend_data']])},
                            borderColor: '#f6ad55',
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    interaction: {{
                        mode: 'index',
                        intersect: false
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            display: true,
                            position: 'left'
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            grid: {{
                                drawOnChartArea: false
                            }}
                        }}
                    }}
                }}
            }});

            // Status Chart (Pie)
            const statusCtx = document.getElementById('statusChart').getContext('2d');
            new Chart(statusCtx, {{
                type: 'doughnut',
                data: {{
                    labels: ['Success', 'Failed'],
                    datasets: [{{
                        data: [{metrics['successful_executions']}, {metrics['failed_executions']}],
                        backgroundColor: ['#48bb78', '#f56565']
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{
                            position: 'bottom'
                        }}
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """

    # Save HTML file
    filename = f"performance_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ Performance dashboard generated: {filename}")
    return filename


def fetch_performance_metrics():
    """Fetch performance metrics from database"""

    try:
        # Summary metrics
        summary_query = text("""
            SELECT 
                COUNT(*) as total_executions,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_executions,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_executions,
                AVG(duration_seconds) as avg_duration,
                SUM(COALESCE(records_processed, 0)) as total_records
            FROM etl_execution_log
            WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
        """)

        with engine.connect() as conn:
            result = conn.execute(summary_query).fetchone()

            total_exec = result[0] or 0
            success = result[1] or 0

            metrics = {
                'total_executions': total_exec,
                'successful_executions': success,
                'failed_executions': result[2] or 0,
                'avg_duration': result[3] or 0,
                'total_records': result[4] or 0,
                'success_rate': (success / total_exec * 100) if total_exec > 0 else 0
            }

        # Trend data
        trend_query = text("""
            SELECT 
                TO_CHAR(DATE(start_time), 'MM/DD') as date,
                AVG(duration_seconds) as avg_duration,
                SUM(COALESCE(records_processed, 0)) as total_records,
                AVG(cpu_percent) as avg_cpu,
                AVG(memory_mb) as avg_memory
            FROM etl_execution_log
            WHERE start_time >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY DATE(start_time)
            ORDER BY DATE(start_time)
        """)

        with engine.connect() as conn:
            trend_result = conn.execute(trend_query).fetchall()

            metrics['trend_data'] = []
            for row in trend_result:
                metrics['trend_data'].append({
                    'date': row[0],
                    'avg_duration': float(row[1] or 0),
                    'total_records': int(row[2] or 0),
                    'avg_cpu': float(row[3] or 0),
                    'avg_memory': float(row[4] or 0)
                })

        # Recent executions
        recent_query = text("""
            SELECT 
                process_name,
                start_time,
                duration_seconds,
                records_processed,
                status,
                cpu_percent,
                memory_mb
            FROM etl_execution_log
            ORDER BY start_time DESC
            LIMIT 10
        """)

        with engine.connect() as conn:
            recent_result = conn.execute(recent_query).fetchall()

            metrics['recent_executions'] = []
            for row in recent_result:
                metrics['recent_executions'].append({
                    'process_name': row[0],
                    'start_time': row[1],
                    'duration_seconds': float(row[2] or 0),
                    'records_processed': int(row[3] or 0),
                    'status': row[4],
                    'cpu_percent': float(row[5] or 0),
                    'memory_mb': float(row[6] or 0)
                })

        return metrics

    except Exception as e:
        print(f"❌ Error fetching metrics: {e}")
        return None


if __name__ == "__main__":
    print("="*70)
    print("📊 GENERATING PERFORMANCE DASHBOARD")
    print("="*70)

    filename = generate_performance_dashboard()

    if filename:
        print(f"\n✅ Dashboard saved to: {filename}")
        print("\n📝 Open this file in your web browser to view the dashboard")
        print("\n💡 The dashboard will show:")
        print("  • Execution summary (last 7 days)")
        print("  • Duration trends")
        print("  • Records processed")
        print("  • Resource usage (CPU, Memory)")
        print("  • Success rate")
        print("  • Recent execution details")