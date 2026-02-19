"""
Quality Dashboard Generator
Creates HTML reports from quality check results
"""

from sqlalchemy import text
from db_connection import engine
from datetime import datetime
import json


def generate_quality_dashboard():
    """
    Generate HTML dashboard from quality reports

    Returns:
        str: Path to generated HTML file
    """

    # Fetch latest quality report
    query = text("""
        SELECT 
            report_id,
            report_timestamp,
            total_checks,
            checks_passed,
            checks_failed,
            checks_warning,
            report_details
        FROM etl_quality_reports
        ORDER BY report_timestamp DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchone()

    if not result:
        print("No quality reports found")
        return None

    report_id, timestamp, total, passed, failed, warning, details = result
    details = json.loads(details) if isinstance(details, str) else details

    # Calculate metrics
    pass_rate = (passed / total * 100) if total > 0 else 0

    # Generate HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Quality Dashboard - Report #{report_id}</title>
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
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 10px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                overflow: hidden;
            }}

            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}

            .header h1 {{
                font-size: 2.5em;
                margin-bottom: 10px;
            }}

            .header p {{
                opacity: 0.9;
                font-size: 1.1em;
            }}

            .metrics {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                padding: 30px;
                background: #f8f9fa;
            }}

            .metric-card {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                text-align: center;
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

            .metric-passed {{ color: #28a745; }}
            .metric-failed {{ color: #dc3545; }}
            .metric-warning {{ color: #ffc107; }}
            .metric-total {{ color: #667eea; }}

            .details {{
                padding: 30px;
            }}

            .details h2 {{
                margin-bottom: 20px;
                color: #333;
            }}

            .check-item {{
                background: #f8f9fa;
                padding: 15px;
                margin-bottom: 15px;
                border-radius: 8px;
                border-left: 4px solid #ddd;
            }}

            .check-item.pass {{
                border-left-color: #28a745;
                background: #d4edda;
            }}

            .check-item.fail {{
                border-left-color: #dc3545;
                background: #f8d7da;
            }}

            .check-item.warning {{
                border-left-color: #ffc107;
                background: #fff3cd;
            }}

            .check-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }}

            .check-name {{
                font-weight: bold;
                font-size: 1.1em;
            }}

            .status-badge {{
                padding: 5px 15px;
                border-radius: 20px;
                font-size: 0.8em;
                font-weight: bold;
                text-transform: uppercase;
            }}

            .status-pass {{
                background: #28a745;
                color: white;
            }}

            .status-fail {{
                background: #dc3545;
                color: white;
            }}

            .status-warning {{
                background: #ffc107;
                color: black;
            }}

            .check-details {{
                margin-top: 10px;
                padding-top: 10px;
                border-top: 1px solid rgba(0,0,0,0.1);
            }}

            .check-details p {{
                margin: 5px 0;
                color: #555;
            }}

            .footer {{
                background: #f8f9fa;
                padding: 20px;
                text-align: center;
                color: #666;
                border-top: 1px solid #ddd;
            }}

            .progress-bar {{
                width: 100%;
                height: 30px;
                background: #e9ecef;
                border-radius: 15px;
                overflow: hidden;
                margin: 20px 0;
            }}

            .progress-fill {{
                height: 100%;
                background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-weight: bold;
                transition: width 0.3s ease;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Data Quality Dashboard</h1>
                <p>Report #{report_id} - {timestamp.strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>

            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-label">Total Checks</div>
                    <div class="metric-value metric-total">{total}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">✅ Passed</div>
                    <div class="metric-value metric-passed">{passed}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">⚠️ Warnings</div>
                    <div class="metric-value metric-warning">{warning}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">❌ Failed</div>
                    <div class="metric-value metric-failed">{failed}</div>
                </div>
            </div>

            <div style="padding: 0 30px;">
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {pass_rate}%">
                        {pass_rate:.1f}% Pass Rate
                    </div>
                </div>
            </div>

            <div class="details">
                <h2>📋 Check Details</h2>
    """

    # Add each check detail
    for detail in details:
        status = detail.get('status', 'UNKNOWN').lower()
        check_name = detail.get('check_name', 'Unknown Check')

        status_class = 'pass' if status == 'pass' else 'fail' if status == 'fail' else 'warning'
        badge_class = 'status-pass' if status == 'pass' else 'status-fail' if status == 'fail' else 'status-warning'

        html += f"""
                <div class="check-item {status_class}">
                    <div class="check-header">
                        <span class="check-name">{check_name}</span>
                        <span class="status-badge {badge_class}">{status.upper()}</span>
                    </div>
                    <div class="check-details">
        """

        # Add relevant details based on check type
        for key, value in detail.items():
            if key not in ['check_name', 'status']:
                if isinstance(value, (int, float)):
                    html += f"<p><strong>{key.replace('_', ' ').title()}:</strong> {value:,}</p>"
                elif isinstance(value, list) and value:
                    html += f"<p><strong>{key.replace('_', ' ').title()}:</strong> {', '.join(map(str, value[:5]))}</p>"
                elif isinstance(value, dict):
                    html += f"<p><strong>{key.replace('_', ' ').title()}:</strong> {value}</p>"
                elif value and not isinstance(value, (list, dict)):
                    html += f"<p><strong>{key.replace('_', ' ').title()}:</strong> {value}</p>"

        html += """
                    </div>
                </div>
        """

    # Close HTML
    html += f"""
            </div>

            <div class="footer">
                <p>Generated by Retail Data Warehouse ETL System</p>
                <p>Report ID: {report_id} | Timestamp: {timestamp}</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Save HTML file
    filename = f"quality_report_{report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ Quality dashboard generated: {filename}")
    return filename


if __name__ == "__main__":
    print("=" * 70)
    print("📊 GENERATING QUALITY DASHBOARD")
    print("=" * 70)

    filename = generate_quality_dashboard()

    if filename:
        print(f"\n✅ Dashboard saved to: {filename}")
        print("\n📝 Open this file in your web browser to view the report")