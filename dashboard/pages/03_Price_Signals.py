import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from db import get_db_connection

# Set page configuration
st.set_page_config(
    page_title="Price Signals",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="signals_refresh")

# Custom CSS for better styling
st.markdown("""
<style>
    .insight-card {background-color:#f8f9fa; padding:15px; border-radius:5px; margin-bottom:10px; box-shadow: 1px 1px 3px rgba(0,0,0,0.1);}
    .high-priority {background-color: rgba(255, 0, 0, 0.05); border-left: 4px solid red;}
    .medium-priority {background-color: rgba(255, 165, 0, 0.05); border-left: 4px solid orange;}
    .low-priority {background-color: rgba(0, 128, 0, 0.05); border-left: 4px solid green;}
    .alert-type-tag {display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 12px; margin-right: 5px;}
    .undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
    .overpriced {background-color: #e8f5e9; color: #1b5e20; border: 1px solid #2e7d32;}
    .price-increase {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
    .stockout {background-color: #fff3e0; color: #e65100; border: 1px solid #f57c00;}
    .info-box {background-color: #f5f5f5; padding:12px; border-radius:5px; margin-bottom:15px; border-left: 4px solid #2196F3;}
    .insight-card, .info-box {
            background-color: #135382 !important;
    }
</style>
""", unsafe_allow_html=True)

# Page title
st.title("Price Signals")
st.markdown("""
<div class="info-box">
Identify products at risk and monitor competitor pricing across your catalog.
</div>
""", unsafe_allow_html=True)

# Functions to load data
@st.cache_data(ttl=300)
def load_price_signals(days=7):
    engine = get_db_connection()
    query = f"""
    SELECT 
        ps.product_sku,
        pp.name AS product_name,
        pp.category,
        pp.current_price AS platform_price,
        pp.cost AS cost_price,
        ps.competitor_id,
        ec.name AS competitor_name,
        ps.signal_type,
        ps.competitor_price,
        ps.percentage_diff,
        ps.recommendation,
        ps.priority,
        ps.in_stock,
        ps.signal_timestamp
    FROM price_signals ps
    JOIN platform_products pp ON ps.product_sku = pp.sku
    JOIN external_competitors ec ON ps.competitor_id = ec.id
    WHERE ps.signal_timestamp > NOW() - INTERVAL '{days} days'
    ORDER BY ps.priority, ps.signal_timestamp DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['signal_timestamp'] = pd.to_datetime(df['signal_timestamp'])
        
        # Calculate profit margins
        df['current_margin'] = ((df['platform_price'] - df['cost_price']) / df['platform_price'] * 100)
        
        # Classify signals
        df['signal_category'] = df['signal_type'].map({
            'undercut': 'Competitor price is lower than our price',
            'overpriced': 'Competitor price is higher than our price',
            'price_increase_24hrs_min': 'Price increased from 24-hour minimum'
        })
    return df

@st.cache_data(ttl=300)
def load_product_summary():
    engine = get_db_connection()
    query = """
    SELECT 
        pp.sku AS product_sku,
        pp.name AS product_name,
        pp.category,
        pp.current_price,
        pp.cost,
        COUNT(DISTINCT ps.competitor_id) AS competitors_with_signals,
        SUM(CASE WHEN ps.signal_type = 'undercut' THEN 1 ELSE 0 END) AS undercut_count,
        MIN(CASE WHEN ps.signal_type = 'undercut' THEN ps.competitor_price ELSE NULL END) AS min_competitor_price,
        MIN(ps.priority) AS highest_priority
    FROM platform_products pp
    LEFT JOIN price_signals ps ON pp.sku = ps.product_sku AND ps.signal_timestamp > NOW() - INTERVAL '7 days'
    GROUP BY pp.sku, pp.name, pp.category, pp.current_price, pp.cost
    ORDER BY highest_priority NULLS LAST, undercut_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        # Calculate margins
        df['margin'] = ((df['current_price'] - df['cost']) / df['current_price'] * 100)
        
        # Identify products with pricing issues
        df['has_pricing_issue'] = (df['undercut_count'] > 0) | (df['min_competitor_price'].notna() & (df['min_competitor_price'] < df['cost']))
    return df

# Controls section
col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    days = st.slider("Data timeframe (days)", 1, 30, 7)
with col3:
    if st.button("Refresh Data", key="refresh_btn", use_container_width=True):
        st.cache_data.clear()

# Load data
signals_df = load_price_signals(days)
product_summary = load_product_summary()

if signals_df.empty or product_summary.empty:
    st.info("No price signals data available for the selected timeframe.")
    st.stop()

# Show key metrics
total_products = len(product_summary)
products_undercut = product_summary[product_summary['undercut_count'] > 0].shape[0]
high_priority_signals = signals_df[signals_df['priority'] == 1].shape[0]
products_with_issues = product_summary[product_summary['has_pricing_issue']].shape[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Products Monitored", f"{total_products}")
col2.metric("Products Undercut", f"{products_undercut}", f"{products_undercut/total_products*100:.1f}%")
col3.metric("Products With Issues", f"{products_with_issues}", f"{products_with_issues/total_products*100:.1f}%")
col4.metric("High Priority Signals", f"{high_priority_signals}")

# Create two tabs for different views
tab1, tab2 = st.tabs(["Products at Risk", "Product Details"])

with tab1:
    st.subheader("Products at Risk")
    
    # Filter to show only products with pricing issues
    at_risk_products = product_summary[product_summary['has_pricing_issue']].copy()
    
    if not at_risk_products.empty:
        # Add priority indicator
        at_risk_products['priority_indicator'] = 'Normal'
        if 'highest_priority' in at_risk_products.columns:
            at_risk_products.loc[at_risk_products['highest_priority'] == 1, 'priority_indicator'] = '🔴 High'
            at_risk_products.loc[at_risk_products['highest_priority'] == 2, 'priority_indicator'] = '🟠 Medium'
            at_risk_products.loc[at_risk_products['highest_priority'] == 3, 'priority_indicator'] = '🟢 Low'
        
        # Show table of at-risk products
        st.dataframe(
            at_risk_products,
            use_container_width=True,
            column_config={
                "product_sku": st.column_config.TextColumn("SKU"),
                "product_name": st.column_config.TextColumn("Product Name"),
                "category": st.column_config.TextColumn("Category"),
                "current_price": st.column_config.NumberColumn("Our Price", format="$%.2f"),
                "min_competitor_price": st.column_config.NumberColumn("Lowest Competitor", format="$%.2f"),
                "margin": st.column_config.NumberColumn("Current Margin", format="%.1f%%"),
                "undercut_count": st.column_config.NumberColumn("Times Undercut"),
                "priority_indicator": st.column_config.TextColumn("Priority")
            },
            hide_index=True
        )
        
        # Show pricing position scatter plot
        st.subheader("Pricing Position of At-Risk Products")
        
        fig = px.scatter(
            at_risk_products,
            x="current_price",
            y="margin",
            size="undercut_count",
            color="priority_indicator",
            hover_name="product_name",
            labels={
                "current_price": "Current Price ($)",
                "margin": "Profit Margin (%)",
                "undercut_count": "Times Undercut"
            },
            title="Products at Risk - Pricing Position"
        )
        
        # Add horizontal line at 0% margin
        fig.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="Break-even")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show category distribution
        category_counts = at_risk_products.groupby('category').size().reset_index(name='count')
        
        fig = px.pie(
            category_counts, 
            values='count', 
            names='category',
            title='At-Risk Products by Category',
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        
        col1, col2 = st.columns([3, 2])
        with col1:
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown("""
            ### Key Insights
            - Products appearing in this view are being undercut by at least one competitor
            - Larger bubbles indicate products undercut by more competitors
            - Products with negative margins (below the red line) require immediate attention
            - High priority items (🔴) should be addressed first
            """)
    else:
        st.info("No products with pricing issues found in the selected timeframe.")

with tab2:
    # Product selection for detailed view
    st.subheader("Product Signal Details")
    
    # Get products with signals
    products_with_signals = signals_df['product_sku'].unique()
    product_names = {sku: name for sku, name in zip(signals_df['product_sku'], signals_df['product_name'])}
    
    if len(products_with_signals) > 0:
        # Create product options with product name
        product_options = [f"{sku} - {product_names[sku]}" for sku in products_with_signals]
        
        selected_product_option = st.selectbox("Select a product to view detailed signals", product_options)
        selected_product = selected_product_option.split(" - ")[0]
        
        # Filter signals for the selected product
        product_signals = signals_df[signals_df['product_sku'] == selected_product].copy()
        
        if not product_signals.empty:
            # Get product info from first row
            product_info = product_signals.iloc[0]
            
            # Show product summary card
            st.markdown(f"""
            <div class="insight-card">
                <h3>{product_info['product_name']} ({selected_product})</h3>
                <p><b>Category:</b> {product_info['category']} | <b>Our Price:</b> ${product_info['platform_price']:.2f}</p>
                <p><b>Cost:</b> ${product_info['cost_price']:.2f} | <b>Margin:</b> {product_info['current_margin']:.1f}%</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Show competitor price comparison chart
            latest_signals = product_signals.sort_values('signal_timestamp').drop_duplicates(
                subset=['competitor_id'], keep='last'
            ).sort_values('competitor_price')
            
            st.subheader("Competitor Price Comparison")
            
            fig = px.bar(
                latest_signals,
                x='competitor_name',
                y='competitor_price',
                color='signal_type',
                title=f"Competitor Prices for {product_info['product_name']}",
                labels={
                    'competitor_price': 'Price ($)', 
                    'competitor_name': 'Competitor',
                    'signal_type': 'Signal Type'
                },
                color_discrete_map={
                    'undercut': '#FF4B4B',
                    'overpriced': '#0068C9',
                    'price_increase_24hrs_min': '#4CAF50',
                    'stockout': '#FF9800'
                }
            )
            
            # Add horizontal lines for our price and cost
            fig.add_hline(
                y=product_info['platform_price'], 
                line_dash="dash", 
                line_color="black", 
                annotation_text="Our Price"
            )
            fig.add_hline(
                y=product_info['cost_price'], 
                line_dash="dot", 
                line_color="red", 
                annotation_text="Our Cost"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show actionable recommendations
            st.subheader("Pricing Recommendations")
            
            # Group by signal type
            for signal_type in ['undercut', 'price_increase_24hrs_min', 'overpriced']:
                type_signals = product_signals[product_signals['signal_type'] == signal_type]
                
                if not type_signals.empty:
                    # Get the highest priority signal for this type
                    priority_signal = type_signals.sort_values('priority').iloc[0]
                    
                    # Set the priority class
                    priority_class = "high-priority" if priority_signal['priority'] == 1 else \
                                     "medium-priority" if priority_signal['priority'] == 2 else \
                                     "low-priority"
                    
                    # Set the signal tag
                    signal_tag_class = "undercut" if signal_type == "undercut" else \
                                      "price-increase" if signal_type == "price_increase_24hrs_min" else \
                                      "overpriced"
                    
                    signal_tag = f"""<span class="alert-type-tag {signal_tag_class}">{signal_type}</span>"""
                    
                    st.markdown(f"""
                    <div class="insight-card {priority_class}">
                        <h4>{signal_tag} {priority_signal['signal_category']}</h4>
                        <p><b>Recommendation:</b> {priority_signal['recommendation']}</p>
                        <p><b>Competitors:</b> {len(type_signals)} competitor(s) with this signal</p>
                        <p><b>Price Gap:</b> Up to ${abs(priority_signal['competitor_price'] - priority_signal['platform_price']):.2f} ({abs(priority_signal['percentage_diff']):.1f}%)</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Show all competitors in an expander
            with st.expander("View All Competitor Signals"):
                st.dataframe(
                    product_signals[['competitor_name', 'signal_type', 'competitor_price', 
                                      'percentage_diff', 'priority', 'signal_timestamp']],
                    use_container_width=True,
                    column_config={
                        "competitor_name": st.column_config.TextColumn("Competitor"),
                        "signal_type": st.column_config.TextColumn("Signal Type"),
                        "competitor_price": st.column_config.NumberColumn("Competitor Price", format="$%.2f"),
                        "percentage_diff": st.column_config.NumberColumn("% Difference", format="%.2f%%"),
                        "priority": st.column_config.NumberColumn("Priority"),
                        "signal_timestamp": st.column_config.DatetimeColumn("Timestamp", format="DD/MM/YYYY HH:mm")
                    }
                )
        else:
            st.info(f"No signals found for product {selected_product}.")
    else:
        st.info("No products with signals found in the selected timeframe.")
