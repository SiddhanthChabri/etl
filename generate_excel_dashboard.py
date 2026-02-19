"""
Option 5: Excel Dashboard Generator
Creates a professional multi-sheet Excel report from analytics results
"""

import pandas as pd
import numpy as np
from datetime import datetime
from logger_config import setup_logger

logger = setup_logger('excel_dashboard')


def clean(val):
    """Replace NaN/Inf with empty string or 0 safely"""
    if val is None:
        return ''
    try:
        if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
            return ''
    except Exception:
        pass
    return val


def write_row_safe(ws, row_idx, dataframe, formats):
    """Write a dataframe row safely, skipping NaN/Inf"""
    for c in range(len(dataframe.columns)):
        val = dataframe.iloc[row_idx - 1, c]
        val = clean(val)
        f   = formats[c] if c < len(formats) else formats[-1]
        if val == '':
            ws.write_blank(row_idx, c, None, f)
        else:
            ws.write(row_idx, c, val, f)


def generate_excel_dashboard():
    logger.info("="*70)
    logger.info("📊 GENERATING EXCEL DASHBOARD")
    logger.info("="*70)

    # ── Load CSVs ──────────────────────────────────────────────────────────
    try:
        rfm_df    = pd.read_csv('rfm_analysis_results.csv')
        abc_df    = pd.read_csv('abc_analysis_results.csv')
        cohort_df = pd.read_csv('cohort_retention_matrix.csv', index_col=0)
        clv_df    = pd.read_csv('clv_analysis_results.csv')
        basket_df = pd.read_csv('market_basket_results.csv')
        logger.info("✅ Loaded all analytics CSVs")
    except FileNotFoundError as e:
        logger.error(f"❌ {e} — run test_advanced_analytics.py first")
        return None

    # Clean all DataFrames — replace NaN/Inf with 0 or empty
    for df in [rfm_df, abc_df, clv_df, basket_df]:
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
    rfm_df.fillna({'customer_name': 'Unknown', 'state': '', 'city': '',
                   'recency': 0, 'frequency': 0, 'monetary': 0,
                   'r_score': 0, 'f_score': 0, 'm_score': 0,
                   'rfm_value': 0, 'segment': 'Unknown'}, inplace=True)
    abc_df.fillna({'product_name': 'Unknown', 'category': '', 'sub_category': '',
                   'total_revenue': 0, 'transaction_count': 0,
                   'total_quantity': 0, 'revenue_percentage': 0,
                   'cumulative_percentage': 0, 'abc_class': 'C'}, inplace=True)
    clv_df.fillna({'customer_name': 'Unknown', 'state': '', 'city': '',
                   'purchase_count': 0, 'avg_purchase_value': 0,
                   'total_revenue': 0, 'lifespan_years': 0,
                   'clv_discounted': 0, 'clv_segment': 'Low Value'}, inplace=True)

    filename = f"analytics_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    # Enable nan_inf_to_errors option to avoid crash on any stray values
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        wb = writer.book
        wb.nan_inf_to_errors = True 

        # ── Common formats ─────────────────────────────────────────────────
        fmt_title   = wb.add_format({'bold':True,'font_size':18,'font_color':'#FFFFFF',
                                     'bg_color':'#4472C4','align':'center','valign':'vcenter'})
        fmt_header  = wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#4472C4',
                                     'align':'center','valign':'vcenter','border':1,'text_wrap':True})
        fmt_kpi_lbl = wb.add_format({'bold':True,'font_size':12,'font_color':'#4472C4'})
        fmt_kpi_val = wb.add_format({'bold':True,'font_size':16,'font_color':'#2E75B6'})
        fmt_money   = wb.add_format({'num_format':'$#,##0.00','border':1})
        fmt_int     = wb.add_format({'num_format':'#,##0','border':1,'align':'center'})
        fmt_pct     = wb.add_format({'num_format':'0.00%','border':1})
        fmt_text    = wb.add_format({'border':1,'text_wrap':True})
        fmt_alt     = wb.add_format({'bg_color':'#EBF3FB','border':1,'text_wrap':True})
        fmt_alt_cur = wb.add_format({'bg_color':'#EBF3FB','num_format':'$#,##0.00','border':1})
        fmt_alt_int = wb.add_format({'bg_color':'#EBF3FB','num_format':'#,##0','border':1,'align':'center'})

        # ==================================================================
        # SHEET 1 — EXECUTIVE SUMMARY
        # ==================================================================
        pd.DataFrame().to_excel(writer, sheet_name='Executive Summary', index=False)
        ws = writer.sheets['Executive Summary']
        ws.set_tab_color('#4472C4')
        ws.merge_range('A1:H1', '📊 RETAIL ANALYTICS EXECUTIVE SUMMARY', fmt_title)
        ws.set_row(0, 40)
        ws.write('A2', f'Generated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}',
                 wb.add_format({'italic':True,'font_color':'#7F7F7F'}))

        # KPIs
        kpis = [
            (1, 3, '👥 Total Customers',  len(rfm_df)),
            (4, 6, '📦 Total Products',   len(abc_df)),
            (1, 3, '⭐ Champions',         int((rfm_df['segment']=='Champions').sum())),
            (4, 6, '🏆 Class A Products', int((abc_df['abc_class']=='A').sum())),
        ]
        positions = [(4,4), (4,4), (7,7), (7,7)]
        for i, (col_s, col_e, lbl, val) in enumerate(kpis):
            r = positions[i][0]
            col_letter_s = chr(64 + col_s)
            col_letter_e = chr(64 + col_e)
            ws.merge_range(f'{col_letter_s}{r}:{col_letter_e}{r}', lbl, fmt_kpi_lbl)
            ws.merge_range(f'{col_letter_s}{r+1}:{col_letter_e}{r+1}', f'{val:,}', fmt_kpi_val)

        ws.write('A4', '💰 Total Revenue', fmt_kpi_lbl)
        ws.write('A5', f"${abc_df['total_revenue'].sum():,.2f}", fmt_kpi_val)
        ws.write('A7', '💎 Avg CLV', fmt_kpi_lbl)
        ws.write('A8', f"${clv_df['clv_discounted'].mean():,.2f}", fmt_kpi_val)

        # RFM summary table
        ws.merge_range('A10:D10', '🎯 RFM Customer Segments', fmt_header)
        ws.write_row('A11', ['Segment','Customers','% of Total','Avg Revenue ($)'], fmt_header)
        seg_order = ['Champions','Loyal Customers','Potential Loyalists','Recent Customers',
                     'Promising','Need Attention','About to Sleep','At Risk']
        for i, seg in enumerate(seg_order):
            sub = rfm_df[rfm_df['segment'] == seg]
            cnt = len(sub)
            pct = cnt / len(rfm_df) if len(rfm_df) > 0 else 0
            avg = float(sub['monetary'].mean()) if cnt > 0 else 0
            f_t = fmt_alt if i % 2 else fmt_text
            f_i = fmt_alt_int if i % 2 else fmt_int
            f_c = fmt_alt_cur if i % 2 else fmt_money
            ws.write(11+i, 0, seg,  f_t)
            ws.write(11+i, 1, cnt,  f_i)
            ws.write(11+i, 2, pct,  fmt_pct)
            ws.write(11+i, 3, avg,  f_c)

        # ABC summary table
        ws.merge_range('F10:I10', '📦 ABC Product Classes', fmt_header)
        ws.write_row(10, 5, ['Class','Products','% Products','Revenue ($)'], fmt_header)
        for i, cls in enumerate(['A','B','C']):
            sub = abc_df[abc_df['abc_class'] == cls]
            f_t = fmt_alt if i % 2 else fmt_text
            f_i = fmt_alt_int if i % 2 else fmt_int
            f_c = fmt_alt_cur if i % 2 else fmt_money
            ws.write(11+i, 5, f'Class {cls}', f_t)
            ws.write(11+i, 6, len(sub),         f_i)
            ws.write(11+i, 7, len(sub)/len(abc_df) if len(abc_df) else 0, fmt_pct)
            ws.write(11+i, 8, float(sub['total_revenue'].sum()), f_c)

        ws.set_column('A:I', 18)

        # ==================================================================
        # SHEET 2 — RFM ANALYSIS
        # ==================================================================
        rfm_export = rfm_df[['customer_id','customer_name','state','city',
                              'recency','frequency','monetary',
                              'r_score','f_score','m_score','rfm_value','segment']].copy()
        rfm_export.columns = ['Customer ID','Customer Name','State','City',
                               'Recency (days)','Frequency','Monetary ($)',
                               'R Score','F Score','M Score','RFM Value','Segment']
        rfm_export.to_excel(writer, sheet_name='RFM Analysis', index=False)
        ws2 = writer.sheets['RFM Analysis']
        ws2.set_tab_color('#70AD47')

        for col, val in enumerate(rfm_export.columns):
            ws2.write(0, col, val, fmt_header)

        seg_colours = {
            'Champions':'#70AD47','Loyal Customers':'#4472C4',
            'Potential Loyalists':'#ED7D31','Recent Customers':'#FFC000',
            'Promising':'#5B9BD5','Need Attention':'#FF7F00',
            'About to Sleep':'#FF4444','At Risk':'#C00000',
        }
        col_fmts     = [fmt_int, fmt_text, fmt_text, fmt_text,
                        fmt_int, fmt_int,  fmt_money,
                        fmt_int, fmt_int,  fmt_int, fmt_money, fmt_text]
        col_fmts_alt = [fmt_alt_int, fmt_alt, fmt_alt, fmt_alt,
                        fmt_alt_int, fmt_alt_int, fmt_alt_cur,
                        fmt_alt_int, fmt_alt_int, fmt_alt_int, fmt_alt_cur, fmt_alt]

        for row_idx in range(1, len(rfm_export)+1):
            alt  = row_idx % 2
            fmts = col_fmts_alt if alt else col_fmts
            for c in range(11):
                val = clean(rfm_export.iloc[row_idx-1, c])
                if val == '':
                    ws2.write_blank(row_idx, c, None, fmts[c])
                else:
                    ws2.write(row_idx, c, val, fmts[c])
            # Segment badge
            seg   = str(rfm_export.iloc[row_idx-1, 11])
            color = seg_colours.get(seg, '#888888')
            sfmt  = wb.add_format({'bold':True,'font_color':'#FFFFFF',
                                   'bg_color':color,'align':'center','border':1})
            ws2.write(row_idx, 11, seg, sfmt)

        ws2.set_column('A:A', 12); ws2.set_column('B:B', 25)
        ws2.set_column('C:D', 15); ws2.set_column('E:L', 14)
        ws2.freeze_panes(1, 0)
        ws2.autofilter(0, 0, len(rfm_export), len(rfm_export.columns)-1)

        # ==================================================================
        # SHEET 3 — ABC ANALYSIS
        # ==================================================================
        abc_export = abc_df[['product_id','product_name','category','sub_category',
                              'total_revenue','transaction_count','total_quantity',
                              'revenue_percentage','cumulative_percentage','abc_class']].copy()
        abc_export.columns = ['Product ID','Product Name','Category','Sub-Category',
                               'Total Revenue ($)','Transactions','Total Quantity',
                               'Revenue %','Cumulative %','Class']
        abc_export.to_excel(writer, sheet_name='ABC Analysis', index=False)
        ws3 = writer.sheets['ABC Analysis']
        ws3.set_tab_color('#ED7D31')

        for col, val in enumerate(abc_export.columns):
            ws3.write(0, col, val, fmt_header)

        class_fmts = {
            'A': wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#70AD47','align':'center','border':1}),
            'B': wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#4472C4','align':'center','border':1}),
            'C': wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#FF4444','align':'center','border':1}),
        }
        abc_col_fmts     = [fmt_text, fmt_text, fmt_text, fmt_text,
                            fmt_money, fmt_int, fmt_int, fmt_pct, fmt_pct]
        abc_col_fmts_alt = [fmt_alt, fmt_alt, fmt_alt, fmt_alt,
                            fmt_alt_cur, fmt_alt_int, fmt_alt_int,
                            wb.add_format({'bg_color':'#EBF3FB','num_format':'0.00%','border':1}),
                            wb.add_format({'bg_color':'#EBF3FB','num_format':'0.00%','border':1})]

        for row_idx in range(1, len(abc_export)+1):
            alt  = row_idx % 2
            fmts = abc_col_fmts_alt if alt else abc_col_fmts
            for c in range(9):
                val = clean(abc_export.iloc[row_idx-1, c])
                if val == '':
                    ws3.write_blank(row_idx, c, None, fmts[c])
                else:
                    ws3.write(row_idx, c, val, fmts[c])
            cls  = str(abc_export.iloc[row_idx-1, 9])
            ws3.write(row_idx, 9, cls, class_fmts.get(cls, fmt_text))

        ws3.set_column('A:A', 14); ws3.set_column('B:B', 35)
        ws3.set_column('C:D', 18); ws3.set_column('E:J', 15)
        ws3.freeze_panes(1, 0)
        ws3.autofilter(0, 0, len(abc_export), len(abc_export.columns)-1)

        # ==================================================================
        # SHEET 4 — COHORT ANALYSIS
        # ==================================================================
        pd.DataFrame().to_excel(writer, sheet_name='Cohort Analysis', index=False)
        ws4 = writer.sheets['Cohort Analysis']
        ws4.set_tab_color('#FFC000')

        ws4.write(0, 0, 'Cohort \ Month', fmt_header)
        for c, col in enumerate(cohort_df.columns, start=1):
            ws4.write(0, c, f'Month {col}', fmt_header)

        for r, idx in enumerate(cohort_df.index, start=1):
            ws4.write(r, 0, str(idx), fmt_header)
            for c, val in enumerate(cohort_df.iloc[r-1], start=1):
                if pd.isna(val) or np.isinf(val):
                    ws4.write_blank(r, c, None, fmt_text)
                else:
                    v = float(val)
                    intensity = min(int(v / 100 * 180), 180)
                    hex_color = f'#{(255-intensity):02X}{min(255, 155+intensity):02X}FF'
                    cell_fmt  = wb.add_format({'bg_color':hex_color,
                                               'num_format':'0.0"%"',
                                               'align':'center','border':1})
                    ws4.write(r, c, v/100, cell_fmt)

        ws4.set_column('A:A', 18)
        ws4.set_column(1, len(cohort_df.columns), 10)

        # ==================================================================
        # SHEET 5 — CLV ANALYSIS
        # ==================================================================
        clv_export = clv_df[['customer_id','customer_name','state','city',
                              'purchase_count','avg_purchase_value','total_revenue',
                              'lifespan_years','clv_discounted','clv_segment']].copy()
        clv_export.columns = ['Customer ID','Customer Name','State','City',
                               'Purchases','Avg Purchase ($)','Total Revenue ($)',
                               'Lifespan (yrs)','CLV ($)','CLV Segment']
        clv_export = clv_export.sort_values('CLV ($)', ascending=False)
        clv_export.to_excel(writer, sheet_name='CLV Analysis', index=False)
        ws5 = writer.sheets['CLV Analysis']
        ws5.set_tab_color('#5B9BD5')

        for col, val in enumerate(clv_export.columns):
            ws5.write(0, col, val, fmt_header)

        seg_clv_fmts = {
            'Very High Value': wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#70AD47','align':'center','border':1}),
            'High Value':      wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#4472C4','align':'center','border':1}),
            'Medium Value':    wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#ED7D31','align':'center','border':1}),
            'Low Value':       wb.add_format({'bold':True,'font_color':'#FFFFFF','bg_color':'#FF4444','align':'center','border':1}),
        }
        clv_col_fmts     = [fmt_int, fmt_text, fmt_text, fmt_text,
                            fmt_int, fmt_money, fmt_money, fmt_money, fmt_money]
        clv_col_fmts_alt = [fmt_alt_int, fmt_alt, fmt_alt, fmt_alt,
                            fmt_alt_int, fmt_alt_cur, fmt_alt_cur, fmt_alt_cur, fmt_alt_cur]

        for row_idx in range(1, len(clv_export)+1):
            alt  = row_idx % 2
            fmts = clv_col_fmts_alt if alt else clv_col_fmts
            for c in range(9):
                val = clean(clv_export.iloc[row_idx-1, c])
                if val == '':
                    ws5.write_blank(row_idx, c, None, fmts[c])
                else:
                    ws5.write(row_idx, c, val, fmts[c])
            seg = str(clv_export.iloc[row_idx-1, 9])
            ws5.write(row_idx, 9, seg, seg_clv_fmts.get(seg, fmt_text))

        ws5.set_column('A:A', 12); ws5.set_column('B:B', 25)
        ws5.set_column('C:D', 15); ws5.set_column('E:J', 16)
        ws5.freeze_panes(1, 0)
        ws5.autofilter(0, 0, len(clv_export), len(clv_export.columns)-1)

        # ==================================================================
        # SHEET 6 — MARKET BASKET
        # ==================================================================
        if len(basket_df) > 0:
            basket_df.fillna('', inplace=True)
            basket_df.to_excel(writer, sheet_name='Market Basket', index=False)
            ws6 = writer.sheets['Market Basket']
            ws6.set_tab_color('#FF4444')
            for col, val in enumerate(basket_df.columns):
                ws6.write(0, col, val, fmt_header)
            for row_idx in range(1, len(basket_df)+1):
                alt = row_idx % 2
                for c in range(len(basket_df.columns)):
                    val = clean(basket_df.iloc[row_idx-1, c])
                    f   = fmt_alt if alt else fmt_text
                    if val == '':
                        ws6.write_blank(row_idx, c, None, f)
                    else:
                        ws6.write(row_idx, c, val, f)
            ws6.set_column('A:B', 35)
            ws6.set_column('C:F', 15)
            ws6.freeze_panes(1, 0)
            ws6.autofilter(0, 0, len(basket_df), len(basket_df.columns)-1)

    logger.info(f"\n✅ Excel dashboard saved: {filename}")
    logger.info("\n📋 Sheets created:")
    logger.info("  1. Executive Summary  — KPIs + segment overview")
    logger.info("  2. RFM Analysis       — Colour-coded customer segments")
    logger.info("  3. ABC Analysis       — Green/Blue/Red class badges")
    logger.info("  4. Cohort Analysis    — Blue heat-map retention matrix")
    logger.info("  5. CLV Analysis       — Customers sorted by CLV")
    logger.info("  6. Market Basket      — Product associations")
    return filename


if __name__ == "__main__":
    generate_excel_dashboard()