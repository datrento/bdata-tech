import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
from db import get_db_connection
from sqlalchemy import text
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Demand vs Signals",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st_autorefresh(interval=5 * 60 * 1000, key="dvs_refresh")

# Simple styles
st.markdown("""
<style>
  .info-box {background-color: #f5f5f5; padding: 12px; border-radius: 6px; border-left: 4px solid #2196F3;}
  .subtle {color: #6b7280; font-size: 13px;}
  @media (prefers-color-scheme: dark) { .info-box {background-color: #135382 !important;} }
</style>
""", unsafe_allow_html=True)

st.title("Demand vs Signals")
st.markdown(
    """
<div class="info-box">
Correlates user engagement shifts with pricing signals to surface products with the highest opportunity.
Use the filters to scope by timeframe, category, SKU, and competitor.
</div>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=30)
def load_product_lookup() -> pd.DataFrame:
    engine = get_db_connection()
    try:
        return pd.read_sql("SELECT sku, name, category FROM platform_products", engine)
    except Exception:
        return pd.DataFrame(columns=["sku", "name", "category"]) 


@st.cache_data(ttl=30)
def load_competitors() -> pd.DataFrame:
    engine = get_db_connection()
    try:
        return pd.read_sql("SELECT id, name FROM external_competitors", engine)
    except Exception:
        return pd.DataFrame(columns=["id", "name"]) 


@st.cache_data(ttl=20)
def load_dvs(days: int) -> pd.DataFrame:
    engine = get_db_connection()
    query = f"""
        SELECT 
            product_sku,
            window_start,
            window_end,
            engagement_delta,
            undercut_cnt,
            abrupt_inc_cnt,
            overpriced_cnt,
            avg_undercut_gap_pct,
            avg_abrupt_inc_gap_pct,
            avg_overpriced_gap_pct,
            price_position,
            score
        FROM demand_vs_signals
        WHERE window_end > NOW() - INTERVAL '{days} days'
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['window_start'] = pd.to_datetime(df['window_start'])
        df['window_end'] = pd.to_datetime(df['window_end'])
    return df


@st.cache_data(ttl=20)
def load_signal_skus_for_competitors(days: int, competitor_ids: list[int]) -> pd.DataFrame:
    if not competitor_ids:
        return pd.DataFrame(columns=['product_sku'])
    engine = get_db_connection()
    ids_csv = ",".join(str(i) for i in competitor_ids)
    query = f"""
        SELECT DISTINCT product_sku
        FROM price_signals
        WHERE signal_timestamp > NOW() - INTERVAL '{days} days'
          AND competitor_id IN ({ids_csv})
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame(columns=['product_sku'])


# Controls
col1, col2, col3, col4 = st.columns([1, 1.2, 1.8, 1.8])
with col1:
    days = st.slider("Timeframe (days)", 1, 30, 7)

prod_lookup = load_product_lookup()
comp_lookup = load_competitors()

with col2:
    categories = sorted(prod_lookup['category'].dropna().unique().tolist()) if not prod_lookup.empty else []
    sel_categories = st.multiselect("Category", options=categories, default=categories)

with col3:
    sku_options = prod_lookup
    if sel_categories:
        sku_options = sku_options[sku_options['category'].isin(sel_categories)]
    sku_list = sku_options.sort_values('sku')['sku'].tolist()
    sel_skus = st.multiselect("SKU", options=sku_list, default=[])

with col4:
    comp_options = comp_lookup.sort_values('name')['name'].tolist() if not comp_lookup.empty else []
    sel_comp_names = st.multiselect("Competitor", options=comp_options, default=[])
    sel_comp_ids = (
        comp_lookup[comp_lookup['name'].isin(sel_comp_names)]['id'].tolist()
        if sel_comp_names and not comp_lookup.empty else []
    )

# Align refresh button styling/location with other pages
r1, r2, r3 = st.columns([1, 2, 1])
with r3:
    if st.button("Refresh Data", key="dvs_refresh_btn", use_container_width=True):
        st.cache_data.clear()


# Load data
dvs = load_dvs(days)
if dvs.empty:
    st.info("No Demand vs Signals data available for the selected timeframe.")
    st.stop()

# Join product metadata
if not prod_lookup.empty:
    dvs = dvs.merge(prod_lookup, left_on='product_sku', right_on='sku', how='left')
    dvs['product_label'] = dvs.apply(
        lambda r: f"{r['name']} ({r['product_sku']})" if pd.notna(r.get('name')) else r['product_sku'], axis=1
    )
else:
    dvs['product_label'] = dvs['product_sku']

# Apply filters
if sel_categories:
    dvs = dvs[dvs['category'].isin(sel_categories)]

if sel_skus:
    dvs = dvs[dvs['product_sku'].isin(sel_skus)]

if sel_comp_ids:
    sku_df = load_signal_skus_for_competitors(days, sel_comp_ids)
    allowed = sku_df['product_sku'].unique().tolist()
    dvs = dvs[dvs['product_sku'].isin(allowed)]

if dvs.empty:
    st.info("No data matches the current filters.")
    st.stop()


# KPIs
avg_eng_delta = dvs['engagement_delta'].mean()
total_undercuts = int(dvs['undercut_cnt'].sum())
avg_score = dvs['score'].mean()

pos_mix = dvs['price_position'].value_counts(normalize=True) * 100.0
mix_up = pos_mix.get('competitor_cheaper', 0.0)
mix_down = pos_mix.get('we_cheaper', 0.0)
mix_unknown = pos_mix.get('unknown', 0.0)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Avg Engagement Δ", f"{avg_eng_delta:.3f}")
k2.metric("Total Undercuts", f"{total_undercuts}")
k3.metric("Avg Opportunity Score", f"{avg_score:.3f}")
k4.metric("Price Position Mix", f"{mix_up:.0f}% / {mix_down:.0f}% / {mix_unknown:.0f}%", help="competitor_cheaper / we_cheaper / unknown")


# Charts
st.subheader("Score Over Time")
score_ts = (
    dvs.groupby('window_end')['score'].mean().reset_index()
)
fig_ts = px.line(score_ts, x='window_end', y='score', title='Average Score per Window')
st.plotly_chart(fig_ts, use_container_width=True)

st.subheader("Score vs Price Position")
fig_scatter = px.scatter(
    dvs,
    x='score', y='price_position', color='category',
    hover_data=['product_label','engagement_delta','undercut_cnt'],
    title='Score vs Price Position'
)
st.plotly_chart(fig_scatter, use_container_width=True)

# Engagement Δ by category trend
if 'category' in dvs.columns:
    st.subheader("Engagement Δ by Category")
    cat_ts = (
        dvs.groupby(['category','window_end'])['engagement_delta']
           .mean()
           .reset_index()
           .sort_values('window_end')
    )
    fig_cat_ts = px.line(
        cat_ts, x='window_end', y='engagement_delta', color='category',
        title='Average Engagement Δ per Category over Time'
    )
    st.plotly_chart(fig_cat_ts, use_container_width=True)

st.subheader("Top Scored Products (latest window)")
latest = dvs.sort_values('window_end').drop_duplicates(subset=['product_sku'], keep='last')
top = latest.sort_values('score', ascending=False).head(15).copy()
fig_top = px.bar(
    top,
    x='score', y='product_label', orientation='h',
    hover_data=['engagement_delta','undercut_cnt','price_position'],
    title='Top Products by Latest Score'
)
fig_top.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_top, use_container_width=True)

# Risk triage: top negative engagement deltas where competitor is cheaper
st.subheader("Risk Triage: Biggest Negative Engagement Δ (Competitor Cheaper)")
risks = latest[(latest['price_position'] == 'competitor_cheaper')]
if not risks.empty:
    risks = risks.sort_values('engagement_delta').head(15).copy()
    st.dataframe(
        risks[['product_label','engagement_delta','undercut_cnt','avg_undercut_gap_pct','score','window_end']]
             .rename(columns={
                 'product_label':'Product',
                 'engagement_delta':'Engagement Δ',
                 'undercut_cnt':'Undercuts',
                 'avg_undercut_gap_pct':'Avg Undercut Gap %',
                 'window_end':'As Of'
             }),
        use_container_width=True,
        column_config={
            'As Of': st.column_config.DatetimeColumn("As Of", format="DD/MM/YYYY HH:mm"),
            'Engagement Δ': st.column_config.NumberColumn("Engagement Δ", format="%.3f"),
            'Avg Undercut Gap %': st.column_config.NumberColumn("Avg Undercut Gap %", format="%.2f%%"),
            'score': st.column_config.NumberColumn("Score", format="%.3f"),
        },
        hide_index=True
    )
else:
    st.info("No competitor-cheaper risks under current filters.")

st.subheader("Category Impact (Avg Score)")
if 'category' in dvs.columns:
    cat_impact = (
        dvs.groupby('category')['score'].mean().reset_index().sort_values('score', ascending=False)
    )
    fig_cat = px.bar(cat_impact, x='category', y='score', title='Average Score by Category')
    st.plotly_chart(fig_cat, use_container_width=True)


# Table + CSV export
st.subheader("Details")
cols = [
    'product_sku', 'window_start', 'window_end', 'engagement_delta', 'undercut_cnt',
    'avg_undercut_gap_pct', 'avg_abrupt_inc_gap_pct', 'avg_overpriced_gap_pct',
    'price_position', 'score'
]
present_cols = [c for c in cols if c in dvs.columns]
table_df = dvs[present_cols].sort_values(['product_sku','window_end'], ascending=[True, False])

st.dataframe(
    table_df,
    use_container_width=True,
    column_config={
        'window_start': st.column_config.DatetimeColumn("Window Start", format="DD/MM/YYYY HH:mm"),
        'window_end': st.column_config.DatetimeColumn("Window End", format="DD/MM/YYYY HH:mm"),
        'engagement_delta': st.column_config.NumberColumn("Engagement Δ", format="%.3f"),
        'avg_undercut_gap_pct': st.column_config.NumberColumn("Avg Undercut Gap %", format="%.2f%%"),
        'avg_abrupt_inc_gap_pct': st.column_config.NumberColumn("Avg Abrupt Inc Gap %", format="%.2f%%"),
        'avg_overpriced_gap_pct': st.column_config.NumberColumn("Avg Overpriced Gap %", format="%.2f%%"),
        'score': st.column_config.NumberColumn("Score", format="%.3f"),
    },
    hide_index=True
)

csv = table_df.to_csv(index=False).encode('utf-8')
st.download_button("Download CSV", csv, file_name="demand_vs_signals.csv", mime="text/csv")


