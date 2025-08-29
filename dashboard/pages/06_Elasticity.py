import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from db import get_db_connection
import os

# Page config
st.set_page_config(
    page_title="Elasticity & Sensitivity",
    page_icon="📉",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=5 * 60 * 1000, key="elasticity_refresh") # refresh every 5 minutes

st.title("Elasticity & Price Sensitivity")
st.markdown(
    """
Track how estimated price elasticity and sensitivity labels evolve over time.
Elasticity is estimated per SKU using a constant-elasticity (log-log) model from demand observations.
"""
)

# Threshold caption
try:
    high_thr = float(os.getenv("ELASTICITY_HIGH_THRESHOLD", "-1.0"))
    med_thr = float(os.getenv("ELASTICITY_MEDIUM_THRESHOLD", "-0.3"))
    st.caption(f"Sensitivity thresholds: high ≤ {high_thr:.2f}, medium ≤ {med_thr:.2f}, else low")
except Exception:
    pass


@st.cache_data(ttl=60)
def load_product_lookup() -> pd.DataFrame:
    engine = get_db_connection()
    try:
        return pd.read_sql("""
                SELECT sku, name, category 
                FROM platform_products 
                WHERE is_active = TRUE AND in_stock = TRUE
                ORDER BY sku""", engine)
    except Exception:
        return pd.DataFrame(columns=["sku", "name", "category"]) 


@st.cache_data(ttl=60)
def load_price_sensitivity_history(days: int) -> pd.DataFrame:
    engine = get_db_connection()
    query = f"""
        SELECT sku, updated_at, price_elasticity, price_sensitivity, observations
        FROM price_sensitivity_history
        WHERE updated_at > NOW() - INTERVAL '{days} days'
        ORDER BY updated_at DESC
    """
    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['updated_at'] = pd.to_datetime(df['updated_at'])
        return df
    except Exception:
        return pd.DataFrame(columns=["sku","updated_at","price_elasticity","price_sensitivity","observations"]) 


@st.cache_data(ttl=60)
def load_platform_sensitivity() -> pd.DataFrame:
    engine = get_db_connection()
    query = """
        SELECT sku, price_sensitivity, price_elasticity, sensitivity_last_updated, is_active, in_stock
        FROM platform_products
        WHERE is_active = TRUE AND in_stock = TRUE
    """
    try:
        df = pd.read_sql(query, engine)
        if not df.empty and 'sensitivity_last_updated' in df.columns:
            df['sensitivity_last_updated'] = pd.to_datetime(df['sensitivity_last_updated'])
        return df
    except Exception:
        return pd.DataFrame(columns=["sku","price_sensitivity","price_elasticity","sensitivity_last_updated"]) 


@st.cache_data(ttl=60)
def load_observation_count_24h() -> int:
    engine = get_db_connection()
    try:
        n = pd.read_sql("""
            SELECT COUNT(*) AS n 
            FROM price_demand_observations 
            WHERE observed_at > NOW() - INTERVAL '24 hours'
            AND sku IN (SELECT sku 
                        FROM platform_products 
                        WHERE is_active = TRUE AND in_stock = TRUE)
            """, engine)['n'].iloc[0]
        return int(n)
    except Exception:
        return 0


# Controls
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    days = st.slider("Timeframe (days)", min_value=1, max_value=90, value=30)

products = load_product_lookup()
with col2:
    cats = sorted(products['category'].dropna().unique().tolist()) if not products.empty else []
    sel_cats = st.multiselect("Categories", options=cats, default=cats)
with col3:
    sku_options = products
    if sel_cats:
        sku_options = sku_options[sku_options['category'].isin(sel_cats)]
    sku_list = sku_options.sort_values('sku')['sku'].tolist()
    sel_skus = st.multiselect("SKUs", options=sku_list, default=[])

r1, r2, r3 = st.columns([1, 2, 1])
with r3:
    if st.button("Refresh Data", key="elasticity_refresh_btn", use_container_width=True):
        st.cache_data.clear()


# Load data
hist = load_price_sensitivity_history(days)
pp = load_platform_sensitivity()
obs_24h = load_observation_count_24h()

if not products.empty:
    # enrich with names/categories
    hist = hist.merge(products, on='sku', how='left') if not hist.empty else hist
    pp = pp.merge(products, on='sku', how='left') if not pp.empty else pp
    # st.write(pp)
    # st.write(hist)

# Apply filters
if sel_cats:
    if not hist.empty:
        hist = hist[hist['category'].isin(sel_cats)]
    if not pp.empty:
        pp = pp[pp['category'].isin(sel_cats)]
if sel_skus:
    if not hist.empty:
        hist = hist[hist['sku'].isin(sel_skus)]
    if not pp.empty:
        pp = pp[pp['sku'].isin(sel_skus)]


# KPIs
pp_nonnull = pp[pp['price_sensitivity'].notna()] if not pp.empty else pd.DataFrame()
num_with_sens = int(pp_nonnull['sku'].nunique()) if not pp_nonnull.empty else 0
high_cnt = int((pp_nonnull['price_sensitivity'] == 'high').sum()) if not pp_nonnull.empty else 0
med_cnt = int((pp_nonnull['price_sensitivity'] == 'medium').sum()) if not pp_nonnull.empty else 0
low_cnt = int((pp_nonnull['price_sensitivity'] == 'low').sum()) if not pp_nonnull.empty else 0
last_run = None
if not pp.empty and 'sensitivity_last_updated' in pp.columns and pp['sensitivity_last_updated'].notna().any():
    last_run = pd.to_datetime(pp['sensitivity_last_updated']).max()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Products with sensitivity", f"{num_with_sens}")
k2.metric("High / Medium / Low", f"{high_cnt} / {med_cnt} / {low_cnt}")
k3.metric("Number of observations (24h)", f"{obs_24h}")
k4.metric("Last updated", last_run.strftime('%Y-%m-%d %H:%M') if last_run else "—")

# Elasticity over time
st.subheader("Elasticity over Time")
if hist.empty:
    st.info("No elasticity history available for the selected timeframe/filters.")
else:
    # If many SKUs selected, plot average; otherwise plot per-SKU lines
    if sel_skus and len(sel_skus) <= len(products.sku.unique()):
        fig = px.line(
            hist.sort_values('updated_at'),
            x='updated_at', y='price_elasticity', color='sku',
            hover_data=['name','category','price_sensitivity','observations'],
            title='Elasticity per SKU'
        )
    else:
        avg_ts = (
            hist.groupby('updated_at')['price_elasticity'].mean().reset_index()
        )
        fig = px.line(avg_ts, x='updated_at', y='price_elasticity', title='Average Elasticity (selected set)')
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Elasticity Distribution")
if not hist.empty:
    fig_h = px.histogram(hist, x='price_elasticity', nbins=40, title='Distribution of Elasticity (history rows)')
    st.plotly_chart(fig_h, use_container_width=True)

st.subheader("Category Breakdown (Avg Elasticity)")
if not hist.empty and 'category' in hist.columns:
    cat_avg = (
        hist.groupby('category')['price_elasticity'].mean().reset_index().sort_values('price_elasticity')
    )
    fig_c = px.bar(cat_avg, x='category', y='price_elasticity', title='Average Elasticity by Category')
    st.plotly_chart(fig_c, use_container_width=True)


# Table + CSV
st.subheader("Recent Sensitivity Updates")
if hist.empty:
    st.info("No history found.")
else:
    table_cols = ['updated_at','sku','name','category','price_elasticity','price_sensitivity','observations']
    table_df = hist[table_cols].sort_values('updated_at', ascending=False).head(500)
    st.dataframe(
        table_df,
        use_container_width=True,
        column_config={
            'updated_at': st.column_config.DatetimeColumn("Updated At", format="DD/MM/YYYY HH:mm"),
            'price_elasticity': st.column_config.NumberColumn("Elasticity", format="%.3f"),
            'price_sensitivity': st.column_config.TextColumn("Sensitivity"),
            'observations': st.column_config.NumberColumn("Obs")
        },
        hide_index=True
    )
    st.download_button(
        "Download CSV",
        data=table_df.to_csv(index=False).encode('utf-8'),
        file_name="price_sensitivity_history.csv",
        mime="text/csv"
    )


