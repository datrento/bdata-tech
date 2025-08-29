import streamlit as st
import os
import socket
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
from db import get_db_connection
from sqlalchemy import text

# Set page configuration
st.set_page_config(
    page_title="Price Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="data_refresh")

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
    .competitor-undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
    .overpriced {background-color: #e8f5e9; color: #1b5e20; border: 1px solid #2e7d32;}
    .abrupt-increase {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
    .stockout {background-color: #fce4ec; color: #880e4f; border: 1px solid #ad1457;}
    .info-box {background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 20px;}
    .key-metrics {padding: 15px; background-color: #f5f5f5; border-radius: 5px; margin-bottom: 20px;}
    .banner {background-color: #f1f8e9; padding: 10px; border-left: 4px solid #7cb342; margin-bottom: 15px;}
    .tooltip {position: relative; display: inline-block; cursor: help; color: #1E88E5;}
    .tooltip:hover .tooltip-text {visibility: visible; opacity: 1;}
    .tooltip-text {visibility: hidden; width: 200px; background-color: #555; color: #fff; text-align: center; padding: 5px; border-radius: 6px; position: absolute; z-index: 1; bottom: 125%; left: 50%; margin-left: -100px; opacity: 0; transition: opacity 0.3s;}
    .status-row {display: flex; gap: 8px; flex-wrap: wrap; margin: 6px 0 12px 0;}
    .status-chip {padding: 6px 10px; border-radius: 16px; font-size: 13px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; border: 1px solid transparent;}
    .status-ok {background: rgba(46,125,50,0.12); color: #2e7d32; border-color: #2e7d32;}
    .status-stale {background: rgba(255,143,0,0.12); color: #e65100; border-color: #e65100;}
    .status-down {background: rgba(198,40,40,0.12); color: #c62828; border-color: #c62828;}
    @media (prefers-color-scheme: dark) {
        .insight-card, .info-box {
            background-color: #135382 !important;
        }

        .banner:first-child {
            background-color: #295364 !important;
        }
    }
    
</style>
""", unsafe_allow_html=True)

# Load data from PostgreSQL
# price_alerts has been merged into price_signals. All metrics now derive from price_signals

@st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
def load_competitor_stats(days=30):
    engine = get_db_connection()
    query = f"""
    SELECT 
        ps.competitor_id,
        ec.code AS competitor_name,
        ec.name AS competitor_full_name,
        COUNT(*) AS signal_count,
        AVG(ABS(ps.percentage_diff)) AS avg_change_pct,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'UNDERCUT' THEN 1 ELSE 0 END) AS undercuts,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'OVERPRICED' THEN 1 ELSE 0 END) AS overpriced_count,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'PRICE_INCREASE_24H' THEN 1 ELSE 0 END) AS price_increase_24h_count,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'STOCKOUT' THEN 1 ELSE 0 END) AS stockout_count,
        AVG(CASE WHEN UPPER(ps.signal_type) = 'UNDERCUT' THEN ABS(ps.percentage_diff) ELSE NULL END) AS avg_undercut_pct,
        AVG(CASE WHEN UPPER(ps.signal_type) = 'OVERPRICED' THEN ABS(ps.percentage_diff) ELSE NULL END) AS avg_overpriced_pct
    FROM price_signals ps
    LEFT JOIN external_competitors ec ON ps.competitor_id = ec.id
    WHERE ps.signal_timestamp > NOW() - INTERVAL '{days} days'
    GROUP BY ps.competitor_id, ec.code, ec.name
    ORDER BY signal_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        # Competitive aggression: how often they undercut and by how much
        denom = (df['undercuts'] + df['overpriced_count']).replace(0, pd.NA)
        df['decrease_frequency'] = df['undercuts'] / denom
        df['price_aggression'] = df['decrease_frequency'].fillna(0) * df['avg_change_pct'].fillna(0)
    return df

@st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
def load_product_stats(days=30):
    engine = get_db_connection()
    query = f"""
    SELECT 
        ps.product_sku,
        pp.name AS product_name,
        pp.category,
        COUNT(*) AS signal_count,
        AVG(ABS(ps.percentage_diff)) AS avg_change_pct,
        MIN(ps.competitor_price) AS min_price,
        MAX(ps.competitor_price) AS max_price,
        MAX(ps.signal_timestamp) AS last_signal,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'UNDERCUT' THEN 1 ELSE 0 END) AS undercut_count,
        SUM(CASE WHEN UPPER(ps.signal_type) = 'OVERPRICED' THEN 1 ELSE 0 END) AS overpriced_count
    FROM price_signals ps
    JOIN platform_products pp ON ps.product_sku = pp.sku
    WHERE ps.signal_timestamp > NOW() - INTERVAL '{days} days'
    GROUP BY ps.product_sku, pp.name, pp.category
    ORDER BY signal_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['last_signal'] = pd.to_datetime(df['last_signal'])
        df['price_range'] = df['max_price'] - df['min_price']
        df['price_volatility'] = (df['price_range'] / df['min_price']) * 100
    return df

@st.cache_data(ttl=10)
def load_price_signals(days=7):
    engine = get_db_connection()
    query = f"""
    SELECT 
        ps.product_sku,
        pp.name AS product_name,
        ps.competitor_id,
        ec.name AS competitor_name,
        ps.signal_type,
        ps.platform_price,
        ps.competitor_price,
        ps.percentage_diff,
        ps.recommendation,
        ps.priority,
        ps.in_stock,
        ps.signal_timestamp,
        pp.category
    FROM price_signals ps
    JOIN platform_products pp ON ps.product_sku = pp.sku
    JOIN external_competitors ec ON ps.competitor_id = ec.id
    WHERE ps.signal_timestamp > NOW() - INTERVAL '{days} days'
    ORDER BY ps.priority, ps.signal_timestamp DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['signal_timestamp'] = pd.to_datetime(df['signal_timestamp'])
        # Normalize signal type for robust downstream logic
        df['signal_type_norm'] = df['signal_type'].astype(str).str.upper()
        display_map = {
            'UNDERCUT': 'Undercut',
            'OVERPRICED': 'Overpriced',
            'STOCKOUT': 'Stockout',
            'PRICE_INCREASE_24H': 'Abrupt Increase',
            'SEQUENTIAL_CHANGE': 'Sequential Change'
        }
        df['signal_display'] = df['signal_type_norm'].map(display_map).fillna(df['signal_type'])
        df['signal_category'] = df['signal_type_norm'].map({
            'UNDERCUT': 'Competitor has lower price than our platform',
            'OVERPRICED': 'Competitor has higher price than our platform',
            'STOCKOUT': 'Competitor is out of stock',
            'PRICE_INCREASE_24H': 'Abrupt increase from recent minimum',
            'SEQUENTIAL_CHANGE': 'Competitor changed price vs previous'
        })
    return df

@st.cache_data(ttl=10)
def load_market_summary():
    engine = get_db_connection()
    query = """
    SELECT 
        product_sku,
        cheapest_competitor_id,
        cheapest_price,
        platform_price,
        gap_pct,
        competitor_count,
        in_stock_competitor_count,
        summary_timestamp
    FROM product_market_summary
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['summary_timestamp'] = pd.to_datetime(df['summary_timestamp'])
    return df

@st.cache_data(ttl=10)
def load_trend_snapshot(days: int = 1):
    engine = get_db_connection()
    query = f"""
    SELECT trend_direction
    FROM price_trends
    WHERE window_end > NOW() - INTERVAL '{days} days'
    """
    df = pd.read_sql(query, engine)
    return df

@st.cache_data(ttl=10)
def load_user_behavior_summary(days: int = 2):
    engine = get_db_connection()
    try:
        query = f"""
        SELECT product_sku, window_start, window_end, search_rate, unique_visitors
        FROM user_behavior_summary
        WHERE window_end > NOW() - INTERVAL '{days} days'
        """
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['window_start'] = pd.to_datetime(df['window_start'])
            df['window_end'] = pd.to_datetime(df['window_end'])
        return df
    except Exception:
        return pd.DataFrame(columns=['product_sku','window_start','window_end','search_rate','unique_visitors'])

@st.cache_data(ttl=10)
def load_demand_vs_signals(days: int = 1):
    engine = get_db_connection()
    try:
        query = f"""
        SELECT product_sku, window_start, window_end, engagement_delta, undercut_cnt,
               avg_undercut_gap_pct, avg_abrupt_inc_gap_pct, avg_overpriced_gap_pct,
               price_position, score
        FROM demand_vs_signals
        WHERE window_end > NOW() - INTERVAL '{days} days'
        """
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['window_start'] = pd.to_datetime(df['window_start'])
            df['window_end'] = pd.to_datetime(df['window_end'])
        return df
    except Exception:
        return pd.DataFrame(columns=['product_sku','window_start','window_end','engagement_delta','undercut_cnt','avg_undercut_gap_pct','avg_abrupt_inc_gap_pct','avg_overpriced_gap_pct','price_position','score'])

@st.cache_data(ttl=60)
def load_competitor_lookup():
    engine = get_db_connection()
    try:
        df = pd.read_sql("SELECT id, name FROM external_competitors", engine)
        return df
    except Exception:
        return pd.DataFrame(columns=['id', 'name'])

@st.cache_data(ttl=60)
def load_product_lookup():
    engine = get_db_connection()
    try:
        df = pd.read_sql("SELECT sku, name, category FROM platform_products", engine)
        return df
    except Exception:
        return pd.DataFrame(columns=['sku', 'name', 'category'])

# Main dashboard title
st.title("Price Intelligence Dashboard")
st.markdown("""
<div class="banner">
Welcome to your real-time price intelligence dashboard. Here you'll find actionable insights to optimize your pricing strategy and stay ahead of the competition.
</div>

This dashboard provides comprehensive competitive price monitoring and analytics based on unified price signals:
* **Undercut & Overpriced**: See when competitors price below or above your platform
* **Abrupt Increases**: Detect competitor price spikes from recent lows
* **Stockouts**: Spot competitor stockouts that create opportunities
* **Competitor/Product Insights**: Analyze aggression, volatility, and signal frequency
""", unsafe_allow_html=True)

# Date filter and refresh controls
col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    days = st.slider("Data timeframe (days)", 1, 90, 7, 
                    help="Choose how many days of historical data to display in the dashboard")
with col3:
    if st.button("Refresh Data", key="refresh_btn", use_container_width=True):
        # Clear all cached data
        st.cache_data.clear()

# Load data
try:
    price_signals = load_price_signals(days)
    market_summary = load_market_summary()
    product_lookup = load_product_lookup()
    
    if price_signals.empty:
        st.info("No price data found in the selected timeframe. Try extending the date range or check if the data streaming services are running.")
    else:
        # Category filter (scopes entire Home)
        try:
            categories_signals = price_signals['category'].dropna().unique().tolist() if 'category' in price_signals.columns else []
            categories_products = product_lookup['category'].dropna().unique().tolist() if product_lookup is not None and not product_lookup.empty else []
            all_categories = sorted(set(categories_signals) | set(categories_products))
            if all_categories:
                st.markdown("<h2 class='sub-header'>Filters</h2>", unsafe_allow_html=True)
                sel = st.multiselect("Categories", options=all_categories, default=all_categories, help="Filter all metrics and charts to selected product categories")
                if sel:
                    # Filter signals
                    if 'category' in price_signals.columns:
                        price_signals = price_signals[price_signals['category'].isin(sel)]
                    # Filter market summary by SKU category
                    if market_summary is not None and not market_summary.empty and product_lookup is not None and not product_lookup.empty:
                        allowed_skus = product_lookup[product_lookup['category'].isin(sel)]['sku'].unique().tolist()
                        market_summary = market_summary[market_summary['product_sku'].isin(allowed_skus)]
        except Exception:
            pass

        # Information box to explain the different types of signals
        st.markdown("""
        <div class="info-box">
            <h4>Understanding Price Signals</h4>
            <p>The dashboard consolidates all pricing intelligence into strategic signals that may require immediate action:</p>
            <ul>
                <li><span class="alert-type-tag competitor-undercut">Undercut</span> Competitor price below your platform price</li>
                <li><span class="alert-type-tag overpriced">Overpriced</span> Competitor price above your platform price</li>
                <li><span class="alert-type-tag abrupt-increase">Abrupt Increase</span> Competitor raised price sharply from a recent minimum</li>
                <li><span class="alert-type-tag stockout">Stockout</span> Competitor is out of stock while you have inventory</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        # Data freshness + service health
        last_signal_ts = price_signals['signal_timestamp'].max()
        now_ts = datetime.now()
        age_minutes = (now_ts - last_signal_ts).total_seconds() / 60 if pd.notna(last_signal_ts) else None
        last_hour_count = price_signals[price_signals['signal_timestamp'] > now_ts - timedelta(hours=1)].shape[0]
        last_day_count = price_signals[price_signals['signal_timestamp'] > now_ts - timedelta(days=1)].shape[0]

        st.markdown("<h2 class='sub-header'>Data Freshness</h2>", unsafe_allow_html=True)
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            st.metric("Latest Signal", last_signal_ts.strftime('%Y-%m-%d %H:%M') if pd.notna(last_signal_ts) else "—")
        with f2:
            st.metric("Data Age", f"{age_minutes:.0f} min" if age_minutes is not None else "—")
        with f3:
            st.metric("Signals: 1h / 24h", f"{last_hour_count} / {last_day_count}")
        with f4:
            # Simple DB heartbeat: query current timestamp
            try:
                engine = get_db_connection()
                ts = pd.read_sql(text("SELECT NOW() AS now"), engine)['now'].iloc[0]
                st.metric("DB", "OK", help=f"DB time: {pd.to_datetime(ts)}")
            except Exception as e:
                st.metric("DB", "DOWN", help=str(e))

        # Service Health (Kafka + Flink pipelines)
        st.markdown("<h2 class='sub-header'>Service Health</h2>", unsafe_allow_html=True)
        # Build status chips instead of many metrics
        chip_html = "<div class='status-row'>"

        # Kafka broker connectivity check
        kafka_label = "Kafka"
        try:
            bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'kafka:9093')
            host, port = bootstrap.split(':')[0], int(bootstrap.split(':')[1])
            with socket.create_connection((host, port), timeout=1):
                kafka_status = ("status-ok", "OK", f"bootstrap: {bootstrap}")
        except Exception as e:
            kafka_status = ("status-down", "DOWN", str(e))
        chip_html += f"<span class='status-chip {kafka_status[0]}' title='{kafka_status[2]}'>\u25CF {kafka_label}: {kafka_status[1]}</span>"

        # Flink pipelines freshness
        try:
            engine = get_db_connection()
            max_signal = pd.read_sql(text("SELECT MAX(signal_timestamp) AS ts FROM price_signals"), engine)['ts'].iloc[0]
            max_trend = pd.read_sql(text("SELECT MAX(window_end) AS ts FROM price_trends"), engine)['ts'].iloc[0]
            max_summary = pd.read_sql(text("SELECT MAX(summary_timestamp) AS ts FROM product_market_summary"), engine)['ts'].iloc[0]

            def freshness(ts_value, minutes_ok):
                if pd.isna(ts_value):
                    return ("status-down", "DOWN", "no data")
                age_min = (datetime.now() - pd.to_datetime(ts_value)).total_seconds() / 60
                return ("status-ok", "OK", f"age: {age_min:.0f} min") if age_min <= minutes_ok else ("status-stale", "STALE", f"age: {age_min:.0f} min")

            sig_chip = freshness(max_signal, 10)
            tr_chip = freshness(max_trend, 20)
            ms_chip = freshness(max_summary, 30)

            chip_html += f"<span class='status-chip {sig_chip[0]}' title='{sig_chip[2]}'>\u25CF Flink Signals: {sig_chip[1]}</span>"
            chip_html += f"<span class='status-chip {tr_chip[0]}' title='{tr_chip[2]}'>\u25CF Flink Trends: {tr_chip[1]}</span>"
            chip_html += f"<span class='status-chip {ms_chip[0]}' title='{ms_chip[2]}'>\u25CF Flink Summary: {ms_chip[1]}</span>"
        except Exception as e:
            err = str(e).replace("'", "\'")
            chip_html += f"<span class='status-chip status-down' title='{err}'>\u25CF Flink: ERR</span>"

        chip_html += "</div>"
        st.markdown(chip_html, unsafe_allow_html=True)

        # Compact display of active signal rule thresholds
        try:
            u_thr = float(os.getenv('UNDERCUT_PERCENT_THRESHOLD', '5'))
            o_thr = float(os.getenv('OVERPRICED_PERCENT_THRESHOLD', '5'))
            a_thr = float(os.getenv('PRICE_INCREASE_24HRS_THRESHOLD', '10'))
            st.caption("Signal rules")
            rules_html = "<div class='status-row'>"
            rules_html += f"<span class='status-chip' title='Competitor below our price by at least this gap'>Undercut ≥ {u_thr:.1f}%</span>"
            rules_html += f"<span class='status-chip' title='Our price below competitor by at least this gap'>Overpriced ≥ {o_thr:.1f}%</span>"
            rules_html += f"<span class='status-chip' title='Increase from recent minimum within 24h'>Abrupt Increase ≥ {a_thr:.1f}%</span>"
            rules_html += "</div>"
            st.markdown(rules_html, unsafe_allow_html=True)
        except Exception:
            pass

        # Demand & Opportunity (Engagement shifts and opportunity score)
        try:
            behavior = load_user_behavior_summary(days=2)
            dvs = load_demand_vs_signals(days=1)

            # Respect category filters when possible
            allowed_skus = None
            try:
                if 'product_lookup' in locals() and product_lookup is not None and not product_lookup.empty:
                    if 'sel' in locals() and sel:
                        allowed_skus = product_lookup[product_lookup['category'].isin(sel)]['sku'].unique().tolist()
            except Exception:
                pass

            if allowed_skus is not None:
                if behavior is not None and not behavior.empty:
                    behavior = behavior[behavior['product_sku'].isin(allowed_skus)]
                if dvs is not None and not dvs.empty:
                    dvs = dvs[dvs['product_sku'].isin(allowed_skus)]

            st.markdown("<h2 class='sub-header'>Demand & Opportunity</h2>", unsafe_allow_html=True)
            d1, d2, d3 = st.columns(3)

            # Engagement delta 1h and 24h using search_rate as engagement metric
            if behavior is not None and not behavior.empty:
                now_ts = datetime.now()
                last_1h = behavior[behavior['window_end'] > now_ts - timedelta(hours=1)]
                prev_1h = behavior[(behavior['window_end'] <= now_ts - timedelta(hours=1)) & (behavior['window_end'] > now_ts - timedelta(hours=2))]
                last_24h = behavior[behavior['window_end'] > now_ts - timedelta(days=1)]
                prev_24h = behavior[(behavior['window_end'] <= now_ts - timedelta(days=1)) & (behavior['window_end'] > now_ts - timedelta(days=2))]

                def avg_sr(df):
                    return df['search_rate'].mean() if df is not None and not df.empty else None

                sr_1h_now = avg_sr(last_1h)
                sr_1h_prev = avg_sr(prev_1h)
                sr_24h_now = avg_sr(last_24h)
                sr_24h_prev = avg_sr(prev_24h)

                delta_1h = (sr_1h_now - sr_1h_prev) if (sr_1h_now is not None and sr_1h_prev is not None) else None
                delta_24h = (sr_24h_now - sr_24h_prev) if (sr_24h_now is not None and sr_24h_prev is not None) else None

                with d1:
                    st.metric("Engagement Δ (1h)", f"{delta_1h:.3f}" if delta_1h is not None else "—",
                              help="Change in average search_rate vs previous hour")
                with d2:
                    st.metric("Engagement Δ (24h)", f"{delta_24h:.3f}" if delta_24h is not None else "—",
                              help="Change in average search_rate vs previous day")
            else:
                with d1:
                    st.metric("Engagement Δ (1h)", "—")
                with d2:
                    st.metric("Engagement Δ (24h)", "—")

            # Top 5 opportunity SKUs from demand_vs_signals
            if dvs is not None and not dvs.empty:
                # Latest window per SKU
                latest = dvs.sort_values('window_end').drop_duplicates(subset=['product_sku'], keep='last')
                top5 = latest.sort_values('score', ascending=False).head(5).copy()
                # Join product names for readability
                try:
                    if product_lookup is not None and not product_lookup.empty:
                        top5 = top5.merge(product_lookup[['sku','name','category']], left_on='product_sku', right_on='sku', how='left')
                        top5['label'] = top5.apply(lambda r: f"{r['name']} ({r['product_sku']})" if pd.notna(r.get('name')) else r['product_sku'], axis=1)
                    else:
                        top5['label'] = top5['product_sku']
                except Exception:
                    top5['label'] = top5['product_sku']

                with d3:
                    st.markdown("**Top 5 Opportunity Products**")
                    st.dataframe(
                        top5[['label','score','engagement_delta','undercut_cnt','price_position']]
                            .rename(columns={'label':'Product','undercut_cnt':'Undercuts','price_position':'Position'}),
                        use_container_width=True,
                        column_config={
                            'Product': st.column_config.TextColumn("Product"),
                            'score': st.column_config.NumberColumn("Score", format="%.3f"),
                            'engagement_delta': st.column_config.NumberColumn("Engagement Δ", format="%.3f"),
                            'Undercuts': st.column_config.NumberColumn("Undercuts"),
                            'Position': st.column_config.TextColumn("Price Position")
                        },
                        hide_index=True
                    )
        except Exception:
            pass
        
        # Overview metrics section
        st.markdown("<h2 class='sub-header'>Market Overview</h2>", unsafe_allow_html=True)
        
        # Create metrics from signals only
        total_signals = len(price_signals)
        high_priority = price_signals[price_signals['priority'] == 1].shape[0]
        undercuts = price_signals[price_signals['signal_type_norm'] == 'UNDERCUT'].shape[0]
        overpriced_cnt = price_signals[price_signals['signal_type_norm'] == 'OVERPRICED'].shape[0]
        stockouts = price_signals[price_signals['signal_type_norm'] == 'STOCKOUT'].shape[0]
        inc24h = price_signals[price_signals['signal_type_norm'] == 'PRICE_INCREASE_24H'].shape[0]

        avg_undercut = price_signals.loc[price_signals['signal_type_norm'] == 'UNDERCUT', 'percentage_diff'].abs().mean() if undercuts > 0 else 0
        avg_overpriced = price_signals.loc[price_signals['signal_type_norm'] == 'OVERPRICED', 'percentage_diff'].abs().mean() if overpriced_cnt > 0 else 0
        # Actionable signals exclude sequential changes
        actionable_signals = price_signals[~price_signals['signal_type_norm'].eq('SEQUENTIAL_CHANGE')].copy()
        # Recompute KPIs to strictly exclude sequential changes
        total_signals = len(actionable_signals)
        high_priority = actionable_signals[actionable_signals['priority'] == 1].shape[0]
        undercuts = actionable_signals[actionable_signals['signal_type_norm'] == 'UNDERCUT'].shape[0]
        overpriced_cnt = actionable_signals[actionable_signals['signal_type_norm'] == 'OVERPRICED'].shape[0]
        stockouts = actionable_signals[actionable_signals['signal_type_norm'] == 'STOCKOUT'].shape[0]
        inc24h = actionable_signals[actionable_signals['signal_type_norm'] == 'PRICE_INCREASE_24H'].shape[0]

        avg_undercut = actionable_signals.loc[actionable_signals['signal_type_norm'] == 'UNDERCUT', 'percentage_diff'].abs().mean() if undercuts > 0 else 0
        avg_overpriced = actionable_signals.loc[actionable_signals['signal_type_norm'] == 'OVERPRICED', 'percentage_diff'].abs().mean() if overpriced_cnt > 0 else 0

        # Display metrics in columns with improved layout
        st.markdown('<div class="key-metrics">', unsafe_allow_html=True)
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric(
            "Total Signals", 
            f"{total_signals}", 
            help="Total actionable pricing signals detected in the selected timeframe."
        )
        
        col2.metric(
            "Competitor Cheaper (Undercuts)", 
            f"{undercuts}", 
            f"{avg_undercut:.1f}% avg" if avg_undercut != 0 else "0%", 
            help="Products where the cheapest competitor is below our price; delta shown as average % gap."
        )
        
        col3.metric(
            "Competitor Pricier (Overpriced)", 
            f"{overpriced_cnt}", 
            f"{avg_overpriced:.1f}% avg" if avg_overpriced != 0 else "0%", 
            help="Products where our price is lower than competitors; delta shown as average % gap."
        )
        
        col4.metric(
            "Abrupt Increases", 
            f"{inc24h}", 
            help="Competitors recently raised price from their 24-hour minimum (opportunity)."
        )
        
        col5.metric(
            "High Priority Signals", 
            f"{high_priority}", 
            f"{high_priority/total_signals*100:.1f}%" if total_signals > 0 else "0%", 
            help="Highest-impact signals to address first (priority = 1)."
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Create tabs for different views (keep it minimal)
        tab1, tab2 = st.tabs(["Action Center", "Overview"])
        
        with tab1:
            st.markdown("<h3>Actionable Price Signals</h3>", unsafe_allow_html=True)
            st.markdown("""
            <div class="info-box">
            This section highlights strategic price signals that need attention based on your price position versus competitors. Focus on high‑priority items first.
            </div>
            """, unsafe_allow_html=True)
            
            # Market Snapshot KPIs (moved here from Overview)
            try:
                if market_summary is not None and not market_summary.empty:
                    st.markdown("<h4>Market Snapshot</h4>", unsafe_allow_html=True)
                    ms1, ms2, ms3, ms4 = st.columns(4)
                    undercut_now = (market_summary['gap_pct'] > 0).sum()
                    cheaper_now = (market_summary['gap_pct'] < 0).sum()
                    avg_gap_undercut = market_summary.loc[market_summary['gap_pct'] > 0, 'gap_pct'].mean()
                    avg_competitors = market_summary['competitor_count'].mean()
                    ms1.metric(
                        "Products where competitor is cheaper",
                        f"{undercut_now}",
                        help="Count of products where the cheapest competitor price is below your current price (gap_pct > 0)."
                    )
                    ms2.metric(
                        "Products where our price is lowest",
                        f"{cheaper_now}",
                        help="Count of products where your current price is below the cheapest competitor (gap_pct < 0)."
                    )
                    ms3.metric(
                        "Average gap % (competitor cheaper)",
                        f"{avg_gap_undercut:.1f}%" if pd.notna(avg_gap_undercut) else "—",
                        help="Average percent you are above the cheapest competitor across products where competitor is cheaper."
                    )
                    ms4.metric(
                        "Average competitors per product",
                        f"{avg_competitors:.1f}" if pd.notna(avg_competitors) else "—",
                        help="Average number of competitors tracked per product in the current snapshot."
                    )
            except Exception:
                pass
            
            if not price_signals.empty:
                # First show high priority signals (actionable only)
                high_signals = actionable_signals[actionable_signals['priority'] == 1]
                if not high_signals.empty:
                    st.markdown("<div class='insight-card high-priority'><p class='metric-title'>⚠️ High Priority Actions</p></div>", unsafe_allow_html=True)
                    for _, signal in high_signals.head(5).iterrows():
                        tag_class = 'competitor-undercut' if signal['signal_type_norm'] == 'UNDERCUT' else ('abrupt-increase' if signal['signal_type_norm'] == 'PRICE_INCREASE_24H' else ('stockout' if signal['signal_type_norm'] == 'STOCKOUT' else 'overpriced'))
                        signal_tag = f"""<span class=\"alert-type-tag {tag_class}\">{signal.get('signal_display', signal['signal_type'])}</span>"""
                        signal_card = f"""
                        <div class='insight-card high-priority'>
                            <p>{signal_tag} <b>Product:</b> {signal['product_name']} ({signal['product_sku']})</p>
                            <p><b>Competitor:</b> {signal['competitor_name']} | <b>Category:</b> {signal['category']}</p>
                            <p><b>Your Price:</b> ${signal['platform_price']:.2f} | <b>Competitor Price:</b> ${signal['competitor_price']:.2f} | <b>Diff:</b> {signal['percentage_diff']:.2f}%</p>
                            <p><b>Signal Meaning:</b> {signal['signal_category']}</p>
                            <p><b>Recommendation:</b> {signal['recommendation']}</p>
                            <p class='insight-text'>Detected {(datetime.now() - signal['signal_timestamp']).total_seconds() / 3600:.1f} hours ago</p>
                        </div>
                        """
                        st.markdown(signal_card, unsafe_allow_html=True)
                # Optional: additional actions hidden by default
                medium_signals = actionable_signals[actionable_signals['priority'] == 2]
                if not medium_signals.empty:
                    with st.expander("More actions (medium priority)"):
                        for _, signal in medium_signals.head(5).iterrows():
                            tag_class = 'competitor-undercut' if signal['signal_type_norm'] == 'UNDERCUT' else ('abrupt-increase' if signal['signal_type_norm'] == 'PRICE_INCREASE_24H' else ('stockout' if signal['signal_type_norm'] == 'STOCKOUT' else 'overpriced'))
                            signal_tag = f"""<span class=\"alert-type-tag {tag_class}\">{signal.get('signal_display', signal['signal_type'])}</span>"""
                            signal_card = f"""
                            <div class='insight-card medium-priority'>
                                <p>{signal_tag} <b>Product:</b> {signal['product_name']} ({signal['product_sku']})</p>
                                <p><b>Competitor:</b> {signal['competitor_name']} | <b>Category:</b> {signal['category']}</p>
                                <p><b>Your Price:</b> ${signal['platform_price']:.2f} | <b>Competitor Price:</b> ${signal['competitor_price']:.2f} | <b>Diff:</b> {signal['percentage_diff']:.2f}%</p>
                                <p><b>Recommendation:</b> {signal['recommendation']}</p>
                            </div>
                            """
                            st.markdown(signal_card, unsafe_allow_html=True)
                
                # Summary of signal types
                st.subheader("Signal Type Distribution")
                st.markdown("""
                This chart shows the distribution of different types of price signals that require action:
                * **Undercut**: Competitor price is below our platform price by a significant percentage
                * **Overpriced**: Competitor price is above our platform price by a significant percentage
                * **Abrupt Increase**: Competitor current price has increased sharply from a recent low
                * **Stockout**: Competitor is out of stock while we have inventory available
                
                These signals help identify potential pricing opportunities and competitive threats.
                """)
                
                signal_counts = actionable_signals['signal_display'].value_counts().reset_index()
                signal_counts.columns = ['Signal Type', 'Count']
                
                fig = px.pie(
                    signal_counts, 
                    values='Count', 
                    names='Signal Type',
                    title='Distribution of Price Signal Types',
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
                
                # Add signal distribution by category
                if 'category' in actionable_signals.columns:
                    category_signals = actionable_signals.groupby(['category', 'signal_display']).size().reset_index(name='count')
                    
                    fig = px.bar(
                        category_signals,
                        x='category',
                        y='count',
                        color='signal_display',
                        title='Signal Distribution by Product Category',
                        labels={'count': 'Number of Signals', 'category': 'Product Category', 'signal_display': 'Signal Type'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Show complete signals table
                with st.expander("View All Actionable Signals"):
                    st.dataframe(
                        actionable_signals,
                        use_container_width=True,
                        column_config={
                            "product_sku": st.column_config.TextColumn("Product SKU"),
                            "product_name": st.column_config.TextColumn("Product Name"),
                            "competitor_name": st.column_config.TextColumn("Competitor"),
                            "platform_price": st.column_config.NumberColumn(
                                "Your Price", format="$%.2f"
                            ),
                            "competitor_price": st.column_config.NumberColumn(
                                "Competitor Price", format="$%.2f"
                            ),
                            "percentage_diff": st.column_config.NumberColumn(
                                "% Difference", format="%.2f%%"
                            ),
                            "signal_timestamp": st.column_config.DatetimeColumn(
                                "Timestamp", format="DD/MM/YYYY HH:mm"
                            ),
                            "signal_category": st.column_config.TextColumn(
                                "Signal Description", help="Detailed explanation of what this signal means"
                            )
                        }
                    )
            else:
                st.info("No price signals found in the selected timeframe.")
        
        with tab2:
            if not price_signals.empty:
                st.markdown("<h3>Overview</h3>", unsafe_allow_html=True)
                st.markdown("""
                <div class="info-box">This section summarizes actionable signals and your current market posture. Sequential changes are excluded.</div>
                """, unsafe_allow_html=True)

                # Filter actionable signals only (exclude sequential)
                actionable = price_signals[~price_signals['signal_type_norm'].eq('SEQUENTIAL_CHANGE')].copy()

                # Market Snapshot (current posture from product_market_summary)
                try:
                    if market_summary is not None and not market_summary.empty:

                        # Detail table with competitor name
                        comp_lookup = load_competitor_lookup()
                        if not comp_lookup.empty:
                            market_display = market_summary.merge(comp_lookup, left_on='cheapest_competitor_id', right_on='id', how='left')
                            market_display.rename(columns={'name': 'cheapest_competitor_name'}, inplace=True)
                        else:
                            market_display = market_summary.copy()
                            market_display['cheapest_competitor_name'] = market_display['cheapest_competitor_id']

                        with st.expander("Current Market Summary (Top 20 by Gap %)"):
                            top_ms = market_display.sort_values('gap_pct', ascending=False).head(20)
                            st.dataframe(
                                top_ms[['product_sku', 'cheapest_competitor_name', 'cheapest_price', 'platform_price', 'gap_pct', 'competitor_count', 'in_stock_competitor_count', 'summary_timestamp']],
                                use_container_width=True,
                                column_config={
                                    'cheapest_competitor_name': st.column_config.TextColumn('Cheapest Competitor'),
                                    'cheapest_price': st.column_config.NumberColumn('Cheapest Price', format='$%.2f'),
                                    'platform_price': st.column_config.NumberColumn('Our Price', format='$%.2f'),
                                    'gap_pct': st.column_config.NumberColumn('Gap %', format='%.2f%%'),
                                    'competitor_count': st.column_config.NumberColumn('# Competitors'),
                                    'in_stock_competitor_count': st.column_config.NumberColumn('# In-Stock Competitors'),
                                    'summary_timestamp': st.column_config.DatetimeColumn('As Of', format='DD/MM/YYYY HH:mm')
                                }
                            )

                        # Join product names for richer charts
                        prod_lookup = load_product_lookup()
                        ms_joined = market_display.merge(prod_lookup, left_on='product_sku', right_on='sku', how='left')
                        ms_joined['product_label'] = ms_joined.apply(
                            lambda r: f"{r['name']} ({r['product_sku']})" if pd.notna(r.get('name')) else r['product_sku'], axis=1
                        )

                        # Top Undercut Products (bar)
                        undercut_rows = ms_joined[ms_joined['gap_pct'] > 0].copy()
                        if not undercut_rows.empty:
                            top_undercut = undercut_rows.sort_values('gap_pct', ascending=False).head(20)
                            fig = px.bar(
                                top_undercut,
                                x='gap_pct',
                                y='product_label',
                                orientation='h',
                                hover_data=['cheapest_competitor_name','cheapest_price','platform_price','competitor_count','in_stock_competitor_count'],
                                labels={'gap_pct': 'Gap % (we are above)', 'product_label': 'Product'},
                                title='Top Products Where Competitors Are Cheaper'
                            )
                            fig.update_layout(yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig, use_container_width=True)

                        # Market Exposure (bubble)
                        if not ms_joined.empty:
                            fig = px.scatter(
                                ms_joined,
                                x='gap_pct',
                                y='competitor_count',
                                size='in_stock_competitor_count',
                                color='category',
                                hover_name='product_label',
                                labels={'gap_pct': 'Gap % (positive = competitor cheaper)', 'competitor_count': '# Competitors', 'in_stock_competitor_count': '# In-Stock Competitors'},
                                title='Market Exposure by Gap and Competition'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    pass

                # Top competitors by undercut signals (last N days)
                try:
                    top_undercutters = (
                        actionable[actionable['signal_type_norm'] == 'UNDERCUT']
                        .groupby('competitor_name')
                        .size()
                        .reset_index(name='Undercuts')
                        .sort_values('Undercuts', ascending=False)
                        .head(5)
                    )
                    if not top_undercutters.empty:
                        fig = px.bar(top_undercutters, x='competitor_name', y='Undercuts', title='Top Competitors by Undercuts')
                        st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    pass

                # Signals by Category (stacked by type)
                if 'category' in actionable.columns:
                    category_signals = actionable.groupby(['category', 'signal_display']).size().reset_index(name='count')
                    fig = px.bar(
                        category_signals,
                        x='category',
                        y='count',
                        color='signal_display',
                        barmode='stack',
                        title='Signals by Category (Stacked)',
                        labels={'count': 'Number of Signals', 'category': 'Product Category', 'signal_display': 'Signal Type'}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Keep recent signals compact

                # Recent signals table
                with st.expander("Recent Signals"):
                    st.dataframe(
                        actionable[['product_sku', 'product_name', 'competitor_name', 'platform_price', 'competitor_price',
                                       'percentage_diff', 'signal_display', 'signal_timestamp', 'recommendation']]
                        .sort_values(by='signal_timestamp', ascending=False),
                        use_container_width=True,
                        column_config={
                            "product_sku": st.column_config.TextColumn("Product SKU"),
                            "product_name": st.column_config.TextColumn("Product Name"),
                            "competitor_name": st.column_config.TextColumn("Competitor"),
                            "platform_price": st.column_config.NumberColumn(
                                "Your Price", format="$%.2f"
                            ),
                            "competitor_price": st.column_config.NumberColumn(
                                "Competitor Price", format="$%.2f"
                            ),
                            "percentage_diff": st.column_config.NumberColumn(
                                "% Difference", format="%.2f%%"
                            ),
                            "signal_display": st.column_config.TextColumn(
                                "Signal Type"
                            ),
                            "signal_timestamp": st.column_config.DatetimeColumn(
                                "Timestamp", format="DD/MM/YYYY HH:mm"
                            ),
                            "recommendation": st.column_config.TextColumn(
                                "Recommendation"
                            )
                        }
                    )
            else:
                st.info("No signals found in the selected timeframe.")
        


except Exception as e:
    st.error(f"Error loading dashboard data: {e}")
    st.info("Make sure the PostgreSQL service is running and accessible.")
    st.code(str(e), language="python")
