"""
Test Advanced Analytics Module
Demonstrates RFM, ABC, Cohort, CLV, and Market Basket Analysis
"""

from advanced_analytics import AdvancedAnalytics
from logger_config import setup_logger

logger = setup_logger('test_analytics')

# Initialize analytics instance
analytics = AdvancedAnalytics()


def test_rfm_analysis():
    """Test RFM Customer Segmentation"""
    logger.info("\n" + "="*70)
    logger.info("TEST 1: RFM Analysis")
    logger.info("="*70)

    rfm_df = analytics.rfm_analysis()

    # Show top customers
    logger.info("\n🏆 Top 10 Champions:")
    champions = rfm_df[rfm_df['segment'] == 'Champions'].head(10)
    for _, customer in champions.iterrows():
        state = customer['state'] or 'Unknown'
        city = customer['city'] or 'Unknown'
        logger.info(f"  • {customer['customer_name']} ({state}, {city})")
        logger.info(f"    Recency: {customer['recency']} days, Frequency: {customer['frequency']}, "
                   f"Monetary: ${customer['monetary']:,.2f}")

    # Export to CSV
    rfm_df.to_csv('rfm_analysis_results.csv', index=False)
    logger.info("\n✅ Exported: rfm_analysis_results.csv")

    return rfm_df


def test_abc_analysis():
    """Test ABC Product Classification"""
    logger.info("\n" + "="*70)
    logger.info("TEST 2: ABC Analysis")
    logger.info("="*70)

    abc_df = analytics.abc_analysis()

    # Show Class A products
    logger.info("\n⭐ Top 10 Class A Products:")
    class_a = abc_df[abc_df['abc_class'] == 'A'].head(10)
    for _, product in class_a.iterrows():
        # Handle NULL product names
        product_name = product['product_name'] if product['product_name'] else 'Unknown Product'
        logger.info(f"  • {product_name[:50]}")
        logger.info(f"    Revenue: ${product['total_revenue']:,.2f} "
                   f"({product['revenue_percentage']:.2f}%)")

    # Export to CSV
    abc_df.to_csv('abc_analysis_results.csv', index=False)
    logger.info("\n✅ Exported: abc_analysis_results.csv")

    return abc_df


def test_cohort_analysis():
    """Test Cohort Retention Analysis"""
    logger.info("\n" + "="*70)
    logger.info("TEST 3: Cohort Analysis")
    logger.info("="*70)

    retention_matrix, cohort_counts = analytics.cohort_analysis(period='M')

    logger.info("\n📊 Retention Matrix (First 5 cohorts, First 6 months):")
    print(retention_matrix.iloc[:5, :6].round(1))

    # Export to CSV
    retention_matrix.to_csv('cohort_retention_matrix.csv')
    cohort_counts.to_csv('cohort_counts_matrix.csv')
    logger.info("\n✅ Exported: cohort_retention_matrix.csv, cohort_counts_matrix.csv")

    return retention_matrix, cohort_counts


def test_clv_analysis():
    """Test Customer Lifetime Value"""
    logger.info("\n" + "="*70)
    logger.info("TEST 4: Customer Lifetime Value (CLV)")
    logger.info("="*70)

    clv_df = analytics.calculate_clv(discount_rate=0.10)

    # Show top CLV customers
    logger.info("\n💎 Top 10 High-Value Customers:")
    top_clv = clv_df.nlargest(10, 'clv_discounted')
    for _, customer in top_clv.iterrows():
        state = customer['state'] or 'Unknown'
        city = customer['city'] or 'Unknown'
        logger.info(f"  • {customer['customer_name']} ({state}, {city})")
        logger.info(f"    CLV: ${customer['clv_discounted']:,.2f}, "
                   f"Purchases: {customer['purchase_count']}, "
                   f"Avg Value: ${customer['avg_purchase_value']:,.2f}")

    # Export to CSV
    clv_df.to_csv('clv_analysis_results.csv', index=False)
    logger.info("\n✅ Exported: clv_analysis_results.csv")

    return clv_df


def test_market_basket():
    """Test Market Basket Analysis"""
    logger.info("\n" + "="*70)
    logger.info("TEST 5: Market Basket Analysis")
    logger.info("="*70)

    basket_df = analytics.market_basket_analysis(min_support=0.01)

    if len(basket_df) > 0:
        # Show top associations
        logger.info("\n🛒 Top 10 Product Associations:")
        for idx, row in basket_df.head(10).iterrows():
            prod_a = row['product_a'][:40] if row['product_a'] else 'Unknown'
            prod_b = row['product_b'][:40] if row['product_b'] else 'Unknown'
            logger.info(f"  • {prod_a} + {prod_b}")
            logger.info(f"    Support: {row['support']:.3f}, "
                       f"Confidence: {row['confidence_a_to_b']:.3f}, "
                       f"Count: {row['count']}")
    else:
        logger.warning("⚠️  No product associations found. Try lowering min_support.")

    # Export to CSV
    basket_df.to_csv('market_basket_results.csv', index=False)
    logger.info("\n✅ Exported: market_basket_results.csv")

    return basket_df


def run_all_analytics_tests():
    """Run all analytics tests"""
    logger.info("="*70)
    logger.info("🚀 TESTING ADVANCED ANALYTICS MODULE")
    logger.info("="*70)

    try:
        # Run individual tests
        rfm_df = test_rfm_analysis()
        abc_df = test_abc_analysis()
        retention_matrix, cohort_counts = test_cohort_analysis()
        clv_df = test_clv_analysis()
        basket_df = test_market_basket()

        logger.info("\n" + "="*70)
        logger.info("✅ ALL ANALYTICS TESTS PASSED")
        logger.info("="*70)
        logger.info("\n📁 Generated Files:")
        logger.info("  • rfm_analysis_results.csv")
        logger.info("  • abc_analysis_results.csv")
        logger.info("  • cohort_retention_matrix.csv")
        logger.info("  • cohort_counts_matrix.csv")
        logger.info("  • clv_analysis_results.csv")
        logger.info("  • market_basket_results.csv")

        logger.info("\n💡 Next Steps:")
        logger.info("  1. Review CSV files for detailed insights")
        logger.info("  2. Generate dashboard: python generate_analytics_dashboard.py")

        logger.info("\n📊 Quick Summary:")
        logger.info(f"  • Customers Analyzed: {len(rfm_df):,}")
        logger.info(f"  • Products Analyzed: {len(abc_df):,}")
        logger.info(f"  • Champions: {len(rfm_df[rfm_df['segment']=='Champions']):,}")
        logger.info(f"  • Class A Products: {len(abc_df[abc_df['abc_class']=='A']):,}")
        logger.info(f"  • Avg CLV: ${clv_df['clv_discounted'].mean():,.2f}")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_all_analytics_tests()