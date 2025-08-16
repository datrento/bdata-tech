import pandas as pd
import plotly.express as px
import streamlit as st
from db import get_db_connection
from streamlit_autorefresh import st_autorefresh

# --------------------
# Page Config
# --------------------
st.set_page_config(page_title="Price Trends", page_icon="📈", layout="wide")
st.title("📊 Price Trends Dashboard")
st.markdown("Trends based on event time.")

_, _ , col3 = st.columns([1, 2, 1])

# Auto-refresh every 100 seconds
st_autorefresh(interval=100000, key="trends_refresh")
with col3:
    if st.button("Refresh Data", icon=":material/refresh:"):
        # Clear all cached data
        st.cache_data.clear()

# --------------------
# Data Loader
# --------------------
@st.cache_data(ttl=10)
def load_trends(days: int = 1):
    engine = get_db_connection()
    query = f"""
        SELECT
            product_sku,
            competitor_name,
            window_start,
            window_end,
            avg_price,
            price_volatility,
            trend_direction
        FROM price_trends
        WHERE window_end > NOW() - INTERVAL '{days} days'
        ORDER BY window_end DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['window_start'] = pd.to_datetime(df['window_start'])
        df['window_end'] = pd.to_datetime(df['window_end'])
    return df

# --------------------
# Controls
# --------------------
col_a, col_b, col_c = st.columns([1, 2, 2])
with col_a:
    days = st.slider("Timeframe (days)", 1, 7, 1)

trends = load_trends(days)
if trends.empty:
    st.info("No trend data available.")
    st.stop()

with col_b:
    products = sorted(trends['product_sku'].unique())
    selected_products = st.multiselect("Products", products, default=products[:min(3, len(products))])
with col_c:
    competitors = sorted(trends['competitor_name'].unique())
    selected_competitors = st.multiselect("Competitors", competitors, default=competitors[:min(5, len(competitors))])

filtered = trends.copy()
if selected_products:
    filtered = filtered[filtered['product_sku'].isin(selected_products)]
if selected_competitors:
    filtered = filtered[filtered['competitor_name'].isin(selected_competitors)]

# --------------------
# KPIs
# --------------------
avg_price_now = filtered['avg_price'].mean()
max_volatility = filtered['price_volatility'].max()

# Calculate trend direction distribution
trend_counts = filtered['trend_direction'].value_counts(normalize=True)
upward_trend_pct = trend_counts.get('up', 0) * 100


kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Avg Price", f"${avg_price_now:.2f}")
kpi2.metric("Max Volatility", f"{max_volatility:.2f}")
kpi3.metric("Upward Trend %", f"{upward_trend_pct:.2f}%")
# --------------------
# Main Trend Chart (Event Time)
# --------------------
# Average Price Over Event Time of 5 Minutes time window
st.subheader("Average Price Over Event Time within 5 Minutes window")
fig_event = px.line(
    filtered.sort_values('window_start'),
    x='window_start',
    y='avg_price',
    color='competitor_name',
    markers=True,
    line_dash='product_sku',
    hover_data=['competitor_name', 'price_volatility']
)
fig_event.update_layout(
    xaxis_title='Event Time (Window Start)',
    yaxis_title='Avg Price',
    legend_title='Product'
)
st.plotly_chart(fig_event, use_container_width=True)


# Price Volatility Over Time of 5 Minutes time window
st.subheader("Price Volatility Over Time")
fig_vol = px.line(
    filtered.sort_values('window_start'),
    x='window_start',
    y='price_volatility',
    color='competitor_name',
    line_dash='product_sku',
    markers=True,
    hover_data=['competitor_name', 'product_sku', 'avg_price']
)
fig_vol.update_layout(
    xaxis_title='Event Time',
    yaxis_title='Price Volatility',
    legend_title='Competitor / Product'
)
st.plotly_chart(fig_vol, use_container_width=True)

st.subheader("Average Price Heatmap (Hourly Trend by Product and Competitor)")

# Prepare data
heatmap_data = filtered.copy()
heatmap_data['hour'] = heatmap_data['window_end'].dt.floor('H')
heatmap_data['hour_formatted'] = heatmap_data['hour'].dt.strftime('%H:%M')

# Create a combined label for row index
heatmap_data['product_competitor'] = (
    heatmap_data['product_sku'].astype(str) + " | " + heatmap_data['competitor_name'].astype(str)
)

# Pivot table: rows = product+competitor, columns = hour, values = avg_price
heatmap_pivot = heatmap_data.pivot_table(
    index='product_competitor',
    columns='hour',
    values='avg_price',
    aggfunc='mean'
)

formatted_columns = {col: col.strftime('%H:%M') for col in heatmap_pivot.columns}
heatmap_pivot.rename(columns=formatted_columns, inplace=True)

with st.expander("Show raw data", expanded=False):
    st.dataframe(heatmap_pivot)

# Create heatmap
fig_heatmap = px.imshow(
    heatmap_pivot,
    text_auto=".2f",
    color_continuous_scale='RdBu_r',
    aspect='auto',
    labels=dict(x="Hour", y="Product | Competitor", color="Avg Price")
)

fig_heatmap.update_layout(
    xaxis_title='Hour',
    yaxis_title='Product | Competitor',
    title='Hourly Average Price Heatmap',
    height=max(400, len(heatmap_pivot) * 30),
    margin=dict(l=10, r=10, t=30, b=10)

)
fig_heatmap.update_xaxes(tickangle=45)

st.plotly_chart(fig_heatmap, use_container_width=True)


# --------------------
# Average Volatility
# --------------------
st.subheader("Price Volatility Heatmap")
volatility_pivot = filtered.pivot_table(
    index='competitor_name', 
    columns='product_sku', 
    values='price_volatility', 
    aggfunc='mean'
)

fig_vol_heat = px.imshow(
    volatility_pivot,
    text_auto=True,
    aspect="auto",
    color_continuous_scale="Viridis",
    labels=dict(x="Product SKU", y="Competitor", color="Avg Volatility")
)
st.plotly_chart(fig_vol_heat, use_container_width=True)

# --------------------
# Data Table
# --------------------
st.subheader("Detailed Data")
st.dataframe(
    filtered,
    use_container_width=True,
    column_config={
        'avg_price': st.column_config.NumberColumn("Avg Price", format="$%.2f"),
        'price_volatility': st.column_config.NumberColumn("Volatility", format="%.2f"),
        'window_start': st.column_config.DatetimeColumn("Window Start", format="DD/MM/YYYY HH:mm"),
        'window_end': st.column_config.DatetimeColumn("Window End", format="DD/MM/YYYY HH:mm"),
    }
)
