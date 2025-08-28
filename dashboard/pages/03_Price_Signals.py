import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from db import get_db_connection
from sqlalchemy import text

# Set page configuration
st.set_page_config(
    page_title="Price Signals",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="signals_refresh")

# Custom CSS for better styling and priority tags
st.markdown("""
<style>
    .insight-card {background-color:#f8f9fa; padding:15px; border-radius:5px; margin-bottom:10px; box-shadow: 1px 1px 3px rgba(0,0,0,0.1);}    
    .high-priority {background-color: rgba(255, 0, 0, 0.05); border-left: 4px solid red;}
    .medium-priority {background-color: rgba(255, 165, 0, 0.05); border-left: 4px solid orange;}
    .low-priority {background-color: rgba(0, 128, 0, 0.05); border-left: 4px solid green;}
    .alert-type-tag {display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 12px; margin-right: 5px;}
    .competitor-undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
    .overpriced {background-color: #e8f5e9; color: #1b5e20; border: 1px solid #2e7d32;}
    .abrupt-increase {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
    .stockout {background-color: #fff3e0; color: #e65100; border: 1px solid #f57c00;}
    .info-box {background-color: #f5f5f5; padding:12px; border-radius:5px; margin-bottom:15px; border-left: 4px solid #2196F3;}
    .status-row {display: flex; gap: 8px; flex-wrap: wrap; margin: 6px 0 12px 0;}
    .status-chip {padding: 6px 10px; border-radius: 16px; font-size: 13px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; border: 1px solid transparent;}
    .status-ok {background: rgba(46,125,50,0.12); color: #2e7d32; border-color: #2e7d32;}
    .status-stale {background: rgba(255,143,0,0.12); color: #e65100; border-color: #e65100;}
    .status-down {background: rgba(198,40,40,0.12); color: #c62828; border-color: #c62828;}
    @media (prefers-color-scheme: dark) {
        .insight-card, .info-box {background-color: #135382 !important;}
    }
</style>
""", unsafe_allow_html=True)

# Page title
st.title("Price Signals")
st.markdown("""
<div class="info-box">
Deep-dive into actionable price signals. Use filters to triage by type, priority, category, and competitor. Sequential changes are excluded.
</div>
""", unsafe_allow_html=True)

# Data loaders
@st.cache_data(ttl=60)
def load_competitor_lookup():
    engine = get_db_connection()
    try:
        return pd.read_sql("SELECT id, name FROM external_competitors", engine)
    except Exception:
        return pd.DataFrame(columns=["id", "name"]) 

@st.cache_data(ttl=60)
def load_product_lookup():
    engine = get_db_connection()
    try:
        return pd.read_sql("SELECT sku, name, category, current_price, cost FROM platform_products", engine)
    except Exception:
        return pd.DataFrame(columns=["sku", "name", "category", "current_price", "cost"]) 

@st.cache_data(ttl=60)
def load_price_signals(days: int = 7) -> pd.DataFrame:
    engine = get_db_connection()
    query = f"""
    SELECT 
        ps.product_sku,
        pp.name AS product_name,
        pp.category,
        ps.competitor_id,
        ec.name AS competitor_name,
        ps.signal_type,
        ps.platform_price,
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
    if df.empty:
        return df

    df['signal_timestamp'] = pd.to_datetime(df['signal_timestamp'])
    df['signal_type_norm'] = df['signal_type'].astype(str).str.upper()
    display_map = {
        'UNDERCUT': 'Undercut',
        'OVERPRICED': 'Overpriced',
        'STOCKOUT': 'Stockout',
        'PRICE_INCREASE_24H': 'Abrupt Increase',
        'SEQUENTIAL_CHANGE': 'Sequential Change'
    }
    df['signal_display'] = df['signal_type_norm'].map(display_map).fillna(df['signal_type'])
    return df

# Controls section (global filters)
prod_lookup = load_product_lookup()
comp_lookup = load_competitor_lookup()

col1, col2, col3, col4 = st.columns([1, 1, 2, 2])
with col1:
    days = st.slider("Timeframe (days)", 1, 90, 7)
with col3:
    categories = sorted(prod_lookup['category'].dropna().unique().tolist()) if not prod_lookup.empty else []
    sel_categories = st.multiselect("Categories", options=categories, default=categories)
with col4:
    comp_options = comp_lookup.sort_values('name')['name'].tolist() if not comp_lookup.empty else []
    sel_competitors = st.multiselect("Competitors", options=comp_options, default=[])

col5, col6, col7 = st.columns([1, 1, 1])
with col5:
    base_types = ["Undercut", "Overpriced", "Abrupt Increase", "Stockout"]
    type_options = base_types
    default_types = base_types
    sel_types = st.multiselect("Signal Types", options=type_options, default=default_types)
with col6:
    sel_priorities = st.multiselect("Priority", options=[1, 2, 3], default=[1, 2, 3])
with col7:
    min_gap = st.number_input("Min |gap %|", min_value=0.0, max_value=100.0, value=0.0, step=0.5, help="Absolute percentage difference threshold")

r1, r2, r3 = st.columns([1, 2, 1])
with r3:
    if st.button("Refresh Data", key="signals_refresh_btn", use_container_width=True):
        st.cache_data.clear()

# Load data
signals_df = load_price_signals(days)

if signals_df.empty:
    st.info("No price signals data available for the selected timeframe.")
    st.stop()

# Apply filters
df = signals_df.copy()

# Always exclude sequential changes
df = df[~df['signal_type_norm'].eq('SEQUENTIAL_CHANGE')]

if sel_categories:
    df = df[df['category'].isin(sel_categories)]

if sel_competitors and not comp_lookup.empty:
    df = df[df['competitor_name'].isin(sel_competitors)]

if sel_types:
    df = df[df['signal_display'].isin(sel_types)]

if sel_priorities:
    df = df[df['priority'].isin(sel_priorities)]

if min_gap and min_gap > 0:
    df = df[df['percentage_diff'].abs() >= min_gap]

if df.empty:
    st.info("No signals match the selected filters.")
    st.stop()

# Data Freshness
last_signal_ts = df['signal_timestamp'].max()
now_ts = datetime.now()
age_minutes = (now_ts - last_signal_ts).total_seconds() / 60 if pd.notna(last_signal_ts) else None
last_hour_count = df[df['signal_timestamp'] > now_ts - timedelta(hours=1)].shape[0]
last_day_count = df[df['signal_timestamp'] > now_ts - timedelta(days=1)].shape[0]

st.markdown("<h3>Data Freshness</h3>", unsafe_allow_html=True)
f1, f2, f3, f4 = st.columns(4)
with f1:
    st.metric("Latest Signal", last_signal_ts.strftime('%Y-%m-%d %H:%M') if pd.notna(last_signal_ts) else "—")
with f2:
    st.metric("Data Age", f"{age_minutes:.0f} min" if age_minutes is not None else "—")
with f3:
    st.metric("Signals: 1h / 24h", f"{last_hour_count} / {last_day_count}")
with f4:
    try:
        engine = get_db_connection()
        ts = pd.read_sql(text("SELECT NOW() AS now"), engine)['now'].iloc[0]
        st.metric("DB", "OK", help=f"DB time: {pd.to_datetime(ts)}")
    except Exception as e:
        st.metric("DB", "DOWN", help=str(e))

# Top KPIs
total_signals = len(df)
high_priority = df[df['priority'] == 1].shape[0]
undercuts = df[df['signal_type_norm'] == 'UNDERCUT']
overpriced = df[df['signal_type_norm'] == 'OVERPRICED']
abrupts = df[df['signal_type_norm'] == 'PRICE_INCREASE_24H']
stockouts = df[df['signal_type_norm'] == 'STOCKOUT']

avg_undercut_gap = undercuts['percentage_diff'].abs().mean() if not undercuts.empty else 0
avg_overpriced_gap = overpriced['percentage_diff'].abs().mean() if not overpriced.empty else 0
products_affected = df['product_sku'].nunique()
repeat_undercuts = undercuts.groupby('product_sku').size()
repeat_undercuts_cnt = int((repeat_undercuts[repeat_undercuts >= 2]).shape[0]) if not undercuts.empty else 0

k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("Actionable Signals", f"{total_signals}")
k2.metric("High Priority", f"{high_priority}", f"{(high_priority/total_signals*100):.1f}%" if total_signals>0 else "0%")
k3.metric("Avg Undercut Gap", f"{avg_undercut_gap:.1f}%")
k4.metric("Products Affected", f"{products_affected}")
k5.metric("Repeat Undercuts (Products)", f"{repeat_undercuts_cnt}")

# Tabs for triage and analysis
triage_tab, analysis_tab, product_tab, competitor_tab = st.tabs([
    "Action Queue", "Signal Insights", "Product Details", "Competitor Details"
])

with triage_tab:
    st.subheader("Newest Actionable Signals")
    triage_cols = [
        'signal_timestamp','product_name','product_sku','category','competitor_name',
        'signal_display','priority','platform_price','competitor_price','percentage_diff','recommendation'
    ]
    triage_df = df[triage_cols].sort_values('signal_timestamp', ascending=False).copy()

    st.dataframe(
        triage_df,
        use_container_width=True,
        column_config={
            'signal_timestamp': st.column_config.DatetimeColumn("Timestamp", format="DD/MM/YYYY HH:mm"),
            'product_name': st.column_config.TextColumn("Product Name"),
            'product_sku': st.column_config.TextColumn("SKU"),
            'category': st.column_config.TextColumn("Category"),
            'competitor_name': st.column_config.TextColumn("Competitor"),
            'signal_display': st.column_config.TextColumn("Signal Type"),
            'priority': st.column_config.NumberColumn("Priority"),
            'platform_price': st.column_config.NumberColumn("Your Price", format="$%.2f"),
            'competitor_price': st.column_config.NumberColumn("Competitor Price", format="$%.2f"),
            'percentage_diff': st.column_config.NumberColumn("% Difference", format="%.2f%%"),
            'recommendation': st.column_config.TextColumn("Recommendation")
        },
        hide_index=True
    )

    csv = triage_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, file_name="price_signals_triage.csv", mime="text/csv")

with analysis_tab:
    st.subheader("Distributions")
    # Signal type distribution
    sig_counts = df['signal_display'].value_counts().reset_index()
    sig_counts.columns = ['Signal Type', 'Count']
    fig = px.pie(sig_counts, values='Count', names='Signal Type', title='Signal Types', hole=0.4)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)

    # By competitor
    comp_counts = df.groupby('competitor_name').size().reset_index(name='Signals').sort_values('Signals', ascending=False).head(20)
    fig = px.bar(comp_counts, x='competitor_name', y='Signals', title='Signals by Competitor (Top 20)')
    st.plotly_chart(fig, use_container_width=True)

    # By category stacked
    if 'category' in df.columns:
        cat_counts = df.groupby(['category','signal_display']).size().reset_index(name='count')
        fig = px.bar(cat_counts, x='category', y='count', color='signal_display', barmode='stack', title='Signals by Category (Stacked)')
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Signals Over Time")
    ts_df = df.set_index('signal_timestamp').copy()
    # Resample to hourly bins for readability
    ts_counts = ts_df['signal_display'].resample('1H').count().reset_index(name='signals')
    fig = px.line(ts_counts, x='signal_timestamp', y='signals', title='Signals per Hour')
    st.plotly_chart(fig, use_container_width=True)

with product_tab:
    st.subheader("Product Drill-down")
    # Build product label mapping
    df['product_label'] = df.apply(lambda r: f"{r['product_name']} ({r['product_sku']})", axis=1)
    prod_options = sorted(df['product_label'].unique().tolist())
    if prod_options:
        sel_prod_label = st.selectbox("Select Product", options=prod_options)
        sel_sku = sel_prod_label.split('(')[-1].strip(')')
        p_df = df[df['product_sku'] == sel_sku].copy().sort_values('signal_timestamp')
        if not p_df.empty:
            p_info = p_df.iloc[-1]
            st.markdown(f"""
            <div class="insight-card">
                <h3>{p_info['product_name']} ({p_info['product_sku']})</h3>
                <p><b>Category:</b> {p_info['category']} | <b>Your Price (latest):</b> ${p_info['platform_price']:.2f}</p>
            </div>
            """, unsafe_allow_html=True)

            latest_by_comp = p_df.sort_values('signal_timestamp').drop_duplicates(subset=['competitor_id'], keep='last').sort_values('competitor_price')
            fig = px.bar(
                latest_by_comp,
                x='competitor_name', y='competitor_price', color='signal_display',
                title=f"Competitor Prices for {p_info['product_name']}",
                labels={'competitor_price': 'Price ($)', 'competitor_name': 'Competitor', 'signal_display': 'Signal Type'},
                color_discrete_map={'Undercut':'#FF4B4B','Overpriced':'#0068C9','Abrupt Increase':'#4CAF50','Stockout':'#FF9800'}
            )
            fig.add_hline(y=p_info['platform_price'], line_dash='dash', line_color='black', annotation_text='Our Price')
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Recent Signals")
            st.dataframe(
                p_df[['signal_timestamp','competitor_name','signal_display','competitor_price','percentage_diff','priority','recommendation']].sort_values('signal_timestamp', ascending=False),
                use_container_width=True,
                column_config={
                    'signal_timestamp': st.column_config.DatetimeColumn("Timestamp", format="DD/MM/YYYY HH:mm"),
                    'competitor_name': st.column_config.TextColumn("Competitor"),
                    'signal_display': st.column_config.TextColumn("Signal"),
                    'competitor_price': st.column_config.NumberColumn("Competitor Price", format="$%.2f"),
                    'percentage_diff': st.column_config.NumberColumn("% Difference", format="%.2f%%"),
                    'priority': st.column_config.NumberColumn("Priority"),
                    'recommendation': st.column_config.TextColumn("Recommendation")
                },
                hide_index=True
            )
    else:
        st.info("No products available under current filters.")

with competitor_tab:
    st.subheader("Competitor Drill-down")
    comp_options = sorted(df['competitor_name'].unique().tolist())
    if comp_options:
        sel_comp = st.selectbox("Select Competitor", options=comp_options)
        c_df = df[df['competitor_name'] == sel_comp].copy()
        if not c_df.empty:
            # Aggressiveness: frequency * avg gap (on undercuts)
            c_undercuts = c_df[c_df['signal_type_norm'] == 'UNDERCUT']
            freq = len(c_undercuts) / max(1, df.shape[0])
            avg_gap = c_undercuts['percentage_diff'].abs().mean() if not c_undercuts.empty else 0
            aggression = freq * (avg_gap if pd.notna(avg_gap) else 0)

            st.markdown(f"""
            <div class="insight-card">
                <p><b>Signals:</b> {len(c_df)} | <b>Undercuts:</b> {len(c_undercuts)} | <b>Aggression Score:</b> {aggression:.3f}</p>
            </div>
            """, unsafe_allow_html=True)

            # Top affected products by this competitor (by gap)
            c_latest = c_df.sort_values('signal_timestamp').drop_duplicates(subset=['product_sku'], keep='last')
            top_aff = c_latest.sort_values('percentage_diff', key=lambda s: s.abs(), ascending=False).head(20)
            top_aff['product_label'] = top_aff.apply(lambda r: f"{r['product_name']} ({r['product_sku']})", axis=1)
            fig = px.bar(top_aff, x='percentage_diff', y='product_label', orientation='h', title=f"{sel_comp}: Top Products by Gap %")
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

            # Signal mix
            mix = c_df['signal_display'].value_counts().reset_index()
            mix.columns = ['Signal Type', 'Count']
            fig = px.pie(mix, values='Count', names='Signal Type', title=f"{sel_comp}: Signal Mix", hole=0.4)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No competitors available under current filters.")
