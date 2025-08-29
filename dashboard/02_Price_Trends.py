import pandas as pd
import plotly.express as px
import streamlit as st
from db import get_db_connection
from streamlit_autorefresh import st_autorefresh

# Set page configuration
st.set_page_config(
    page_title="Price Trends", page_icon="📈", layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 100 seconds
st_autorefresh(interval=100000, key="trends_refresh")

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {color:#1E88E5; font-size:35px !important;}
    .sub-header {font-size:25px !important; margin-bottom:20px;}
    .insight-card {background-color:#f0f2f6; padding:15px; border-radius:5px; margin-bottom:10px;}
    .metric-title {font-weight:bold; font-size:16px;}
    .alert-section {padding: 5px; margin: 2px 0px; border-radius: 3px;}
    .price-increase {background-color: rgba(255, 75, 75, 0.1);}
    .price-decrease {background-color: rgba(0, 104, 201, 0.1);}
    .insight-text {font-style: italic; color: #555; font-size: 14px;}
    .high-priority {background-color: rgba(255, 0, 0, 0.1); border-left: 3px solid red;}
    .medium-priority {background-color: rgba(255, 165, 0, 0.1); border-left: 3px solid orange;}
    .low-priority {background-color: rgba(0, 128, 0, 0.1); border-left: 3px solid green;}
    .alert-type-tag {display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 12px; margin-right: 5px;}
    .sequential-change {background-color: #e0f7fa; color: #006064; border: 1px solid #00838f;}
    .competitor-undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
    .price-increase-24h {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
    .stockout {background-color: #fce4ec; color: #880e4f; border: 1px solid #ad1457;}
    .info-box {background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 15px;}
</style>
""", unsafe_allow_html=True)
# --------------------
# Page Config
# --------------------
st.title("📊 Price Trends Analysis")
st.markdown("""
This dashboard shows real-time price trends across competitors and products, analyzing data in 5-minute windows.
* **Volatility**: Higher values indicate frequent price changes
* **Trend Direction**: Shows if prices are trending up or down
* **Event Time**: When the price changes were detected
""")

col1, col2, col3 = st.columns([1, 2, 1])

with col3:
    if st.button("Refresh Data", key="trends_refresh_btn", use_container_width=True):
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
    st.markdown("### Select the timeframe to visualize price trends")
    st.markdown("Adjust the slider below to choose how many past days of price trend data you want to analyze.")
    days = st.slider("Last (day(s))", min_value=1, max_value=7, value=1, 
                    help="Choose the number of days to display price trends for. Larger timeframes will show longer-term patterns.")

trends = load_trends(days)
if trends.empty:
    st.info("No trend data available for the selected timeframe. Try extending the date range or check if the trend processing job is running.")
    st.stop()

with col_b:
    products = sorted(trends['product_sku'].unique())
    selected_products = st.multiselect("Select Products to Compare", products, 
                                      default=products[:min(3, len(products))],
                                      help="Choose specific products to analyze. Select multiple to compare trends.")
with col_c:
    competitors = sorted(trends['competitor_name'].unique())
    selected_competitors = st.multiselect("Select Competitors to Monitor", competitors, 
                                         default=competitors[:min(5, len(competitors))],
                                         help="Choose which competitors to include in the analysis.")

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
downward_trend_pct = trend_counts.get('down', 0) * 100

st.markdown("### Key Performance Indicators")
st.markdown("""
These metrics provide a quick overview of the current market situation:
- **Avg Price**: Current average price across all selected products and competitors
- **Max Volatility**: Highest price fluctuation detected (higher = more unstable pricing)
- **Trend Direction**: Percentage of prices trending upward or downward
""")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Avg Price", f"${avg_price_now:.2f}", help="Average price across all selected products and competitors")
kpi2.metric("Max Volatility", f"{max_volatility:.2f}", help="Highest price volatility detected (higher values indicate unstable pricing)")
kpi3.metric("Upward Price Trend %", f"{upward_trend_pct:.2f}%", help="Percentage of prices trending upward")
kpi4.metric("Downward Price Trend %", f"{downward_trend_pct:.2f}%", help="Percentage of prices trending downward")

# --------------------
# Main Trend Chart (Event Time)
# --------------------
# Average Price Over Event Time of 5 Minutes time window
st.subheader("Average Price Over Event Time within 5 Minutes window")
st.markdown("""
This chart shows how prices change over time for each selected product and competitor.
- Each line represents a unique competitor
- Line patterns differentiate between products
- Steeper slopes indicate rapid price changes
""")

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
    yaxis_title='Avg Price ($)',
    legend_title='Competitor',
    hovermode="x unified"
)
st.plotly_chart(fig_event, use_container_width=True)


# Price Volatility Over Time of 5 Minutes time window
st.subheader("Price Volatility Over Time")
st.markdown("""
Price volatility measures how frequently and dramatically prices change:
- Higher values indicate more unstable pricing strategies
- Spikes may indicate promotional events or competitive responses
- Consistent high volatility suggests an aggressive pricing environment
""")

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
    legend_title='Competitor / Product',
    hovermode="x unified"
)
st.plotly_chart(fig_vol, use_container_width=True)

st.subheader("Average Price Heatmap (Hourly Trend by Product and Competitor)")
st.markdown("""
This heatmap visualizes how prices vary throughout the day:
- Each row represents a product-competitor combination
- Each column represents an hour of the day
- Colors indicate price levels (red = higher, blue = lower)
- Look for patterns in pricing behavior at specific times of day
""")

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
st.markdown("""
This heatmap shows which product-competitor combinations have the most volatile pricing:
- Darker colors indicate higher price volatility
- Use this to identify which competitors change prices most frequently for specific products
- Products with high volatility across all competitors may be in highly competitive categories
""")

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
fig_vol_heat.update_layout(
    title="Average Price Volatility by Product and Competitor",
    height=max(400, len(volatility_pivot) * 30)
)
st.plotly_chart(fig_vol_heat, use_container_width=True)

# --------------------
# Data Table
# --------------------
st.subheader("Detailed Trend Data")
st.markdown("""
This table provides the raw data behind the visualizations:
- **Avg Price**: The average price during each 5-minute window
- **Price Volatility**: Measure of price instability (higher = more changes)
- **Trend Direction**: Whether prices are trending up, down, or stable
- **Window Start/End**: The time period for the trend calculation
""")

st.dataframe(
    filtered,
    use_container_width=True,
    column_config={
        'avg_price': st.column_config.NumberColumn("Avg Price", format="$%.2f", help="Average price during this time window"),
        'price_volatility': st.column_config.NumberColumn("Volatility", format="%.2f", help="Measure of price instability - higher values indicate more frequent changes"),
        'window_start': st.column_config.DatetimeColumn("Window Start", format="DD/MM/YYYY HH:mm", help="Start time of the 5-minute window"),
        'window_end': st.column_config.DatetimeColumn("Window End", format="DD/MM/YYYY HH:mm", help="End time of the 5-minute window"),
        'trend_direction': st.column_config.TextColumn("Trend Direction", help="Direction of price movement (up, down, or stable)"),
    }
)
