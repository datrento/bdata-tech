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
            competitor_name AS competitor_code,
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

@st.cache_data(ttl=10)
def load_price_movements(days: int = 1):
    engine = get_db_connection()
    try:
        query = f"""
            SELECT 
                pm.product_sku,
                pm.competitor_id,
                ec.code AS competitor_code,
                pm.previous_price,
                pm.new_price,
                pm.pct_change,
                pm.direction,
                pm.in_stock,
                pm.event_time
            FROM price_movements pm
            JOIN external_competitors ec ON pm.competitor_id = ec.id
            WHERE pm.event_time > NOW() - INTERVAL '{days} days'
            ORDER BY pm.event_time DESC
        """
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['event_time'] = pd.to_datetime(df['event_time'])
            # Floor to 5-minute buckets
            df['bucket_5m'] = df['event_time'].dt.floor('5min')
        return df
    except Exception:
        # Table may not exist yet
        return pd.DataFrame(columns=['product_sku','competitor_code','previous_price','new_price','pct_change','direction','in_stock','event_time','bucket_5m'])

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
    competitors = sorted(trends['competitor_code'].unique())
    selected_competitors = st.multiselect("Select Competitors to Monitor", competitors, 
                                         default=competitors[:min(5, len(competitors))],
                                         help="Choose which competitors to include in the analysis.")

filtered = trends.copy()
if selected_products:
    filtered = filtered[filtered['product_sku'].isin(selected_products)]
if selected_competitors:
    filtered = filtered[filtered['competitor_code'].isin(selected_competitors)]

# --------------------
# KPIs (focused, selection-aware)
# --------------------
# Trend distribution
trend_counts = filtered['trend_direction'].value_counts(normalize=True)
upward_trend_pct = trend_counts.get('up', 0) * 100
downward_trend_pct = trend_counts.get('down', 0) * 100
stable_trend_pct = max(0.0, 100.0 - upward_trend_pct - downward_trend_pct)

# Compute top movers (net % change) and most volatile pair
def compute_top_movers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["product_sku", "competitor_code", "avg_price_first", "avg_price_last", "net_change", "pct_change"])
    ordered = df.sort_values(['product_sku', 'competitor_code', 'window_start'])
    first = ordered.groupby(['product_sku', 'competitor_code']).first().reset_index()[['product_sku', 'competitor_code', 'avg_price']]
    last = ordered.groupby(['product_sku', 'competitor_code']).last().reset_index()[['product_sku', 'competitor_code', 'avg_price']]
    merged = first.merge(last, on=['product_sku', 'competitor_code'], suffixes=('_first', '_last'))
    merged['net_change'] = merged['avg_price_last'] - merged['avg_price_first']
    merged['pct_change'] = (merged['net_change'] / merged['avg_price_first'].replace(0, pd.NA)) * 100
    merged = merged.dropna(subset=['pct_change'])
    return merged

movers = compute_top_movers(filtered)
most_volatile = (
    filtered.groupby(['product_sku', 'competitor_code'])['price_volatility']
    .mean()
    .reset_index()
    .sort_values('price_volatility', ascending=False)
)

top_inc_label, top_inc_val = "—", None
top_dec_label, top_dec_val = "—", None
volatile_label, volatile_val = "—", None

if not movers.empty:
    top_inc = movers.sort_values('pct_change', ascending=False).iloc[0]
    top_inc_label = f"{top_inc['product_sku']} • {top_inc['competitor_code']}"
    top_inc_val = f"{top_inc['pct_change']:.2f}%"
    top_dec = movers.sort_values('pct_change', ascending=True).iloc[0]
    top_dec_label = f"{top_dec['product_sku']} • {top_dec['competitor_code']}"
    top_dec_val = f"{top_dec['pct_change']:.2f}%"

if not most_volatile.empty:
    mv = most_volatile.iloc[0]
    volatile_label = f"{mv['product_sku']} • {mv['competitor_code']}"
    volatile_val = f"{mv['price_volatility']:.2f}"

st.markdown("### Key Performance Indicators")
st.markdown("""
Focused metrics for the selected timeframe/products/competitors (5-minute windows):
- Top net increases/decreases identify the biggest movers
- Most volatile pair shows highest within-window price variability
- Trend share summarizes direction across windows
""")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Top Increase (net %)", top_inc_val or "—", help=top_inc_label)
kpi2.metric("Top Decrease (net %)", top_dec_val or "—", help=top_dec_label)
kpi3.metric("Most Volatile Pair (σ)", volatile_val or "—", help=volatile_label)
kpi4.metric("Up / Down / Stable", f"{upward_trend_pct:.0f}% / {downward_trend_pct:.0f}% / {stable_trend_pct:.0f}%", help="Share of 5-minute windows across the selection")

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
    color='competitor_code',
    markers=True,
    line_dash='product_sku',
    hover_data=['competitor_code', 'price_volatility']
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
    color='competitor_code',
    line_dash='product_sku',
    markers=True,
    hover_data=['competitor_code', 'product_sku', 'avg_price']
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
    heatmap_data['product_sku'].astype(str) + " | " + heatmap_data['competitor_code'].astype(str)
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
    index='competitor_code', 
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
# Summary from current selection
# --------------------
st.subheader("Summary for Selected Data (5-minute windows)")

trend_counts_abs = filtered['trend_direction'].value_counts()
upward_trend_count = int(trend_counts_abs.get('up', 0))
downward_trend_count = int(trend_counts_abs.get('down', 0))
stable_trend_count = int(trend_counts_abs.get('stable', 0))

cs1, cs2, cs3 = st.columns(3)
cs1.metric("Windows Up", f"{upward_trend_count}")
cs2.metric("Windows Down", f"{downward_trend_count}")
cs3.metric("Windows Stable", f"{stable_trend_count}")

def compute_top_movers(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["product_sku", "competitor_code", "first_price", "last_price", "net_change", "pct_change"])
    ordered = df.sort_values(['product_sku', 'competitor_code', 'window_start'])
    first = ordered.groupby(['product_sku', 'competitor_code']).first().reset_index()[['product_sku', 'competitor_code', 'avg_price']]
    last = ordered.groupby(['product_sku', 'competitor_code']).last().reset_index()[['product_sku', 'competitor_code', 'avg_price']]
    merged = first.merge(last, on=['product_sku', 'competitor_code'], suffixes=('_first', '_last'))
    merged['net_change'] = merged['avg_price_last'] - merged['avg_price_first']
    merged['pct_change'] = (merged['net_change'] / merged['avg_price_first'].replace(0, pd.NA)) * 100
    merged = merged.dropna(subset=['pct_change'])
    return merged

tm = compute_top_movers(filtered)
col_inc, col_dec = st.columns(2)
with col_inc:
    st.markdown("**Top Increases (net %)**")
    if not tm.empty:
        st.dataframe(
            tm.sort_values('pct_change', ascending=False).head(5)[['product_sku', 'competitor_code', 'pct_change']]
            .rename(columns={'product_sku': 'Product SKU', 'competitor_code': 'Competitor', 'pct_change': 'Net %'}),
            use_container_width=True
        )
    else:
        st.info("No data.")
with col_dec:
    st.markdown("**Top Decreases (net %)**")
    if not tm.empty:
        st.dataframe(
            tm.sort_values('pct_change', ascending=True).head(5)[['product_sku', 'competitor_code', 'pct_change']]
            .rename(columns={'product_sku': 'Product SKU', 'competitor_code': 'Competitor', 'pct_change': 'Net %'}),
            use_container_width=True
        )
    else:
        st.info("No data.")

# --------------------
# Data Table
# --------------------
st.subheader("Detailed Trend Data")
st.markdown("""
This table provides the raw data behind the visualizations (5-minute windows):
- **Avg Price**: The average price during each 5-minute window
- **Price Volatility**: Measure of price instability (higher = more changes)
- **Trend**: Whether prices are trending up, down, or stable
- **Window Start/End**: The time period for the trend calculation
""")

st.dataframe(
    filtered.assign(trend=lambda d: d['trend_direction'].map({'up': '🟢 up', 'down': '🔴 down', 'stable': '⚪ stable'})),
    use_container_width=True,
    column_config={
        'avg_price': st.column_config.NumberColumn("Avg Price", format="$%.2f", help="Average price during this time window"),
        'price_volatility': st.column_config.NumberColumn("Volatility", format="%.2f", help="Measure of price instability - higher values indicate more frequent changes"),
        'window_start': st.column_config.DatetimeColumn("Window Start", format="DD/MM/YYYY HH:mm", help="Start time of the 5-minute window"),
        'window_end': st.column_config.DatetimeColumn("Window End", format="DD/MM/YYYY HH:mm", help="End time of the 5-minute window"),
        'trend_direction': st.column_config.TextColumn("Trend Direction", help="Direction of price movement (up, down, or stable)"),
        'trend': st.column_config.TextColumn("Trend"),
    }
)

st.download_button(
    label="Download CSV",
    data=filtered.to_csv(index=False).encode('utf-8'),
    file_name=f"price_trends_{days}d.csv",
    mime='text/csv'
)

# --------------------
# Raw Price Movements (5-minute buckets)
# --------------------
st.subheader("Raw Price Movements (5-minute buckets)")
try:
    import os
    seq_thr = float(os.getenv('SEQUENTIAL_CHANGE_THRESHOLD', '10'))
    st.caption(f"Rule: Sequential change events are recorded when |Δ%| ≥ {seq_thr:.1f}%")
except Exception:
    pass
movements = load_price_movements(days)
if movements.empty:
    st.info("No price movement telemetry available for the selected timeframe.")
else:
    # Apply current selections if any
    mv = movements.copy()
    if 'selected_products' in locals() and selected_products:
        mv = mv[mv['product_sku'].isin(selected_products)]
    if 'selected_competitors' in locals() and selected_competitors:
        mask = mv['competitor_code'].isin(selected_competitors) if 'competitor_code' in mv.columns else pd.Series([True]*len(mv))
        mv = mv[mask]
    # remove debug


    if mv.empty:
        st.info("No movements under current selections. Showing all for the timeframe.")
        mv = movements.copy()
    if not mv.empty:
        # Counts per 5-min bucket by direction
        counts = (
            mv.groupby(['bucket_5m','direction']).size().reset_index(name='count')
        )
        fig_counts = px.bar(
            counts, x='bucket_5m', y='count', color='direction', barmode='stack',
            title='Movement Count by 5-minute Bucket', labels={'bucket_5m':'Time Bucket','count':'Movements'}
        )
        st.plotly_chart(fig_counts, use_container_width=True)

        # Up vs Down share
        dir_share = mv['direction'].value_counts().reset_index()
        dir_share.columns = ['Direction','Count']
        fig_share = px.pie(dir_share, names='Direction', values='Count', title='Up vs Down Share', hole=0.4)
        fig_share.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_share, use_container_width=True)

        # Histogram of pct_change (absolute)
        st.markdown("**Distribution of |pct_change|**")
        mv['abs_pct_change'] = mv['pct_change'].abs()
        fig_hist = px.histogram(mv, x='abs_pct_change', nbins=30, title='Histogram of Absolute % Change', labels={'abs_pct_change':'Absolute % Change'})
        st.plotly_chart(fig_hist, use_container_width=True)

        # Recent movements table
        st.markdown("**Recent Movements**")
        st.dataframe(
            mv[['event_time','product_sku','competitor_code','previous_price','new_price','pct_change','direction']]
              .sort_values('event_time', ascending=False)
              .head(500),
            use_container_width=True,
            column_config={
                'event_time': st.column_config.DatetimeColumn("Event Time", format="DD/MM/YYYY HH:mm"),
                'product_sku': st.column_config.TextColumn("Product SKU"),
                'competitor_code': st.column_config.TextColumn("Competitor"),
                'previous_price': st.column_config.NumberColumn("Previous Price", format="$%.2f"),
                'new_price': st.column_config.NumberColumn("New Price", format="$%.2f"),
                'pct_change': st.column_config.NumberColumn("% Change", format="%.2f%%"),
                'direction': st.column_config.TextColumn("Direction")
            },
            hide_index=True
        )
    else:
        st.info("No price movement telemetry available for the timeframe.")
