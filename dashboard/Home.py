import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime
from sqlalchemy import create_engine

# Set page configuration
st.set_page_config(
    page_title="Price Intelligence Dashboard",
    page_icon="📊",
    layout="wide"
)

# Database connection
def get_db_connection():
    db_url = os.getenv(
        'POSTGRES_URL', 
        'postgresql+psycopg2://postgres:postgres@postgres:5432/price_intelligence'
    )
    return create_engine(db_url)

# Load data from PostgreSQL
@st.cache_data(ttl=60)  # Cache for 1 minute
def load_price_alerts(days=7):
    engine = get_db_connection()
    query = f"""
    SELECT 
        product_sku,
        competitor_name,
        previous_price,
        new_price,
        price_change,
        percentage_change,
        alert_type,
        alert_timestamp
    FROM price_alerts
    WHERE alert_timestamp > NOW() - INTERVAL '{days} days'
    ORDER BY alert_timestamp DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
        df['date'] = df['alert_timestamp'].dt.date
    return df

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_competitor_stats():
    engine = get_db_connection()
    query = """
    SELECT 
        competitor_name,
        COUNT(*) as alert_count,
        AVG(ABS(percentage_change)) as avg_change_pct,
        SUM(CASE WHEN alert_type = 'price_decrease' THEN 1 ELSE 0 END) as price_decreases,
        SUM(CASE WHEN alert_type = 'price_increase' THEN 1 ELSE 0 END) as price_increases
    FROM price_alerts
    WHERE alert_timestamp > NOW() - INTERVAL '30 days'
    GROUP BY competitor_name
    ORDER BY alert_count DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_product_stats():
    engine = get_db_connection()
    query = """
    SELECT 
        product_sku,
        COUNT(*) as alert_count,
        AVG(ABS(percentage_change)) as avg_change_pct,
        MIN(new_price) as min_price,
        MAX(new_price) as max_price,
        MAX(alert_timestamp) as last_alert
    FROM price_alerts
    WHERE alert_timestamp > NOW() - INTERVAL '30 days'
    GROUP BY product_sku
    ORDER BY alert_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['last_alert'] = pd.to_datetime(df['last_alert'])
        df['price_range'] = df['max_price'] - df['min_price']
    return df

# Main dashboard title
st.title("Price Intelligence Dashboard")
st.markdown("Real-time monitoring of competitor price changes")

# Date filter and refresh controls
col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    days = st.slider("Data timeframe (days)", 1, 90, 7)
with col3:
    if st.button("Refresh Data", icon=":material/refresh:"):
        # Clear all cached data
        st.cache_data.clear()

# Load data
try:
    price_alerts = load_price_alerts(days)
    
    if price_alerts.empty:
        st.info("No price alerts found in the selected timeframe. Try extending the date range or check if the price monitoring job is running.")
    else:
        # Dashboard metrics
        total_alerts = len(price_alerts)
        avg_change = price_alerts['percentage_change'].abs().mean()
        price_decreases = price_alerts[price_alerts['alert_type'] == 'price_decrease'].shape[0]
        price_increases = price_alerts[price_alerts['alert_type'] == 'price_increase'].shape[0]
        
        # Display metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Alerts", f"{total_alerts}")
        col2.metric("Avg Price Change", f"{avg_change:.2f}%")
        col3.metric("Price Decreases", f"{price_decreases}")
        col4.metric("Price Increases", f"{price_increases}")
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Price Alerts", "Competitor Analysis", "Product Analysis"])
        
        with tab1:
            st.subheader("Recent Price Alerts")
            
            # Price alerts table
            st.dataframe(
                price_alerts[[
                    'product_sku', 'competitor_name', 'previous_price', 
                    'new_price', 'percentage_change', 'alert_type', 'alert_timestamp'
                ]].sort_values(by='alert_timestamp', ascending=False),
                use_container_width=True,
                column_config={
                    "previous_price": st.column_config.NumberColumn(
                        "Previous Price", format="$%.2f"
                    ),
                    "new_price": st.column_config.NumberColumn(
                        "New Price", format="$%.2f"
                    ),
                    "percentage_change": st.column_config.NumberColumn(
                        "% Change", format="%.2f%%"
                    ),
                    "alert_type": st.column_config.TextColumn(
                        "Alert Type"
                    ),
                    "alert_timestamp": st.column_config.DatetimeColumn(
                        "Timestamp", format="DD/MM/YYYY HH:mm"
                    ),
                }
            )
            
            # Price alerts over per day chart
            st.subheader("Price Alerts Over Time")
            alerts_by_date = price_alerts.groupby('date').size().reset_index(name='count')
            alerts_by_date['date'] = pd.to_datetime(alerts_by_date['date'])
            
            fig = px.line(
                alerts_by_date, 
                x='date', 
                y='count',
                title='Number of Price Alerts per Day',
                labels={'date': 'Date', 'count': 'Number of Alerts'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            st.subheader("Competitor Analysis")
            
            # Load competitor stats
            competitor_stats = load_competitor_stats()
            
            if not competitor_stats.empty:
                # Competitor price change behavior
                col1, col2 = st.columns(2)
                
                with col1:
                    # Competitor alert count
                    fig = px.bar(
                        competitor_stats.sort_values('alert_count', ascending=False), 
                        x='competitor_name', 
                        y='alert_count',
                        title='Alert Count by Competitor',
                        labels={'competitor_name': 'Competitor', 'alert_count': 'Number of Alerts'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Competitor price change direction
                    fig = px.bar(
                        competitor_stats, 
                        x='competitor_name', 
                        y=['price_increases', 'price_decreases'],
                        title='Price Increase vs Decrease by Competitor',
                        labels={'competitor_name': 'Competitor', 'value': 'Count', 'variable': 'Direction'},
                        barmode='group',
                        color_discrete_map={
                            'price_increases': '#FF4B4B',  # Red for increases
                            'price_decreases': '#0068C9'   # Blue for decreases
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Average price change by competitor
                fig = px.bar(
                    competitor_stats.sort_values('avg_change_pct', ascending=False), 
                    x='competitor_name', 
                    y='avg_change_pct',
                    title='Average Price Change % by Competitor',
                    labels={'competitor_name': 'Competitor', 'avg_change_pct': 'Avg. Price Change (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Competitor stats table
                st.subheader("Competitor Statistics")
                st.dataframe(
                    competitor_stats,
                    use_container_width=True,
                    column_config={
                        "avg_change_pct": st.column_config.NumberColumn(
                            "Avg % Change", format="%.2f%%"
                        ),
                    }
                )
            else:
                st.info("No competitor data available in the selected timeframe.")
        
        with tab3:
            st.subheader("Product Analysis")
            
            # Load product stats
            product_stats = load_product_stats()
            
            if not product_stats.empty:
                # Products with most price changes
                col1, col2 = st.columns(2)
                
                with col1:
                    # Products by alert count
                    top_products = product_stats.sort_values('alert_count', ascending=False).head(10)
                    fig = px.bar(
                        top_products, 
                        x='product_sku', 
                        y='alert_count',
                        title='Top 10 Products by Alert Count',
                        labels={'product_sku': 'Product SKU', 'alert_count': 'Number of Alerts'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Products by price Range
                    top_range = product_stats.sort_values('price_range', ascending=False).head(10)
                    fig = px.bar(
                        top_range, 
                        x='product_sku', 
                        y='price_range',
                        title='Top 10 Products by Price Range ($)',
                        labels={'product_sku': 'Product SKU', 'price_range': 'Price Range ($)'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Product stats table
                st.subheader("Product Statistics")
                st.dataframe(
                    product_stats,
                    use_container_width=True,
                    column_config={
                        "avg_change_pct": st.column_config.NumberColumn(
                            "Avg % Change", format="%.2f%%"
                        ),
                        "min_price": st.column_config.NumberColumn(
                            "Min Price", format="$%.2f"
                        ),
                        "max_price": st.column_config.NumberColumn(
                            "Max Price", format="$%.2f"
                        ),
                        "price_range": st.column_config.NumberColumn(
                            "Price Range", format="$%.2f"
                        ),
                        "last_alert": st.column_config.DatetimeColumn(
                            "Last Alert", format="DD/MM/YYYY HH:mm"
                        ),
                    }
                )
            else:
                st.info("No product data available in the selected timeframe.")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the PostgreSQL service is running and accessible.")

# Footer
st.markdown("---")
st.markdown("Price Intelligence Dashboard | Last refreshed: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
st.info("Data is automatically refreshed every minute, or click the 'Refresh Data' button for immediate updates.")
