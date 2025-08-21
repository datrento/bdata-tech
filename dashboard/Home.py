# import streamlit as st
# from streamlit_autorefresh import st_autorefresh
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import numpy as np
# from datetime import datetime, timedelta
# from db import get_db_connection

# # Set page configuration
# st.set_page_config(
#     page_title="Price Intelligence Dashboard",
#     page_icon="📊",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# # Auto refresh every 5 minutes
# st_autorefresh(interval=5 * 60 * 1000, key="data_refresh")

# # Custom CSS for better styling
# st.markdown("""
# <style>
#     .main-header {color:#1E88E5; font-size:35px !important;}
#     .sub-header {font-size:25px !important; margin-bottom:20px;}
#     .insight-card {background-color:#f0f2f6; padding:15px; border-radius:5px; margin-bottom:10px;}
#     .metric-title {font-weight:bold; font-size:16px;}
#     .alert-section {padding: 5px; margin: 2px 0px; border-radius: 3px;}
#     .price-increase {background-color: rgba(255, 75, 75, 0.1);}
#     .price-decrease {background-color: rgba(0, 104, 201, 0.1);}
#     .insight-text {font-style: italic; color: #555; font-size: 14px;}
#     .high-priority {background-color: rgba(255, 0, 0, 0.1); border-left: 3px solid red;}
#     .medium-priority {background-color: rgba(255, 165, 0, 0.1); border-left: 3px solid orange;}
#     .low-priority {background-color: rgba(0, 128, 0, 0.1); border-left: 3px solid green;}
#     .alert-type-tag {display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 12px; margin-right: 5px;}
#     .sequential-change {background-color: #e0f7fa; color: #006064; border: 1px solid #00838f;}
#     .competitor-undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
#     .price-increase-24h {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
#     .stockout {background-color: #fce4ec; color: #880e4f; border: 1px solid #ad1457;}
#     .info-box {background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 15px;}
# </style>
# """, unsafe_allow_html=True)

# # Load data from PostgreSQL
# @st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
# def load_price_alerts(days=7):
#     engine = get_db_connection()
#     query = f"""
#     SELECT 
#         product_sku,
#         competitor_name,
#         previous_price,
#         new_price,
#         price_change,
#         percentage_change,
#         alert_type,
#         alert_timestamp
#     FROM price_alerts
#     WHERE alert_timestamp > NOW() - INTERVAL '{days} days'
#     ORDER BY alert_timestamp DESC
#     """
#     df = pd.read_sql(query, engine)
#     if not df.empty:
#         df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
#         df['date'] = df['alert_timestamp'].dt.date
#     return df

# @st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
# def load_competitor_stats(days=30):
#     engine = get_db_connection()
#     query = f"""
#     SELECT 
#         competitor_name,
#         COUNT(*) as alert_count,
#         AVG(ABS(percentage_change)) as avg_change_pct,
#         SUM(CASE WHEN alert_type = 'price_decrease' THEN 1 ELSE 0 END) as price_decreases,
#         SUM(CASE WHEN alert_type = 'price_increase' THEN 1 ELSE 0 END) as price_increases,
#         AVG(CASE WHEN alert_type = 'price_decrease' THEN percentage_change ELSE NULL END) as avg_decrease_pct,
#         AVG(CASE WHEN alert_type = 'price_increase' THEN percentage_change ELSE NULL END) as avg_increase_pct
#     FROM price_alerts
#     WHERE alert_timestamp > NOW() - INTERVAL '{days} days'
#     GROUP BY competitor_name
#     ORDER BY alert_count DESC
#     """
#     df = pd.read_sql(query, engine)
#     if not df.empty:
#         # Calculate competitive aggression score (higher means more aggressive pricing)
#         df['decrease_frequency'] = df['price_decreases'] / (df['price_decreases'] + df['price_increases'])
#         df['price_aggression'] = df['decrease_frequency'] * df['avg_change_pct']
#     return df

# @st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
# def load_product_stats(days=30):
#     engine = get_db_connection()
#     query = f"""
#     SELECT 
#         product_sku,
#         COUNT(*) as alert_count,
#         AVG(ABS(percentage_change)) as avg_change_pct,
#         MIN(new_price) as min_price,
#         MAX(new_price) as max_price,
#         MAX(alert_timestamp) as last_alert,
#         SUM(CASE WHEN alert_type = 'price_decrease' THEN 1 ELSE 0 END) as price_decreases,
#         SUM(CASE WHEN alert_type = 'price_increase' THEN 1 ELSE 0 END) as price_increases
#     FROM price_alerts
#     WHERE alert_timestamp > NOW() - INTERVAL '{days} days'
#     GROUP BY product_sku
#     ORDER BY alert_count DESC
#     """
#     df = pd.read_sql(query, engine)
#     if not df.empty:
#         df['last_alert'] = pd.to_datetime(df['last_alert'])
#         df['price_range'] = df['max_price'] - df['min_price']
#         df['price_volatility'] = df['price_range'] / df['min_price'] * 100
#     return df

# @st.cache_data(ttl=10)
# def load_price_signals(days=7):
#     engine = get_db_connection()
#     query = f"""
#     SELECT 
#         product_sku,
#         competitor_id,
#         signal_type,
#         platform_price,
#         competitor_price,
#         percentage_diff,
#         recommendation,
#         priority,
#         in_stock,
#         signal_timestamp
#     FROM price_signals
#     WHERE signal_timestamp > NOW() - INTERVAL '{days} days'
#     ORDER BY priority, signal_timestamp DESC
#     """
#     df = pd.read_sql(query, engine)
#     if not df.empty:
#         df['signal_timestamp'] = pd.to_datetime(df['signal_timestamp'])
        
#         # Add an additional column with description for clarity
#         df['signal_category'] = df['signal_type'].map({
#             'undercut': 'Competitor has lower price than our platform',
#             'overpriced': 'Competitor has higher price than our platform',
#             'stockout': 'Competitor is out of stock',
#             'price_increase_24hrs_min': '24h price increase from competitor\'s lowest price'
#         })
#     return df

# # Main dashboard title
# st.title("Price Intelligence Dashboard")
# st.markdown("""
# This dashboard provides real-time monitoring of competitor prices and actionable price signals to help you make informed pricing decisions.
# * **Price Signals**: Detects patterns like undercutting, overpricing, and price increases
# * **Competitor Analysis**: Tracks pricing behaviors across multiple competitors
# * **Market Trends**: Summarizes market-wide pricing patterns and opportunities
# """)

# # Date filter and refresh controls
# col1, col2, col3 = st.columns([1, 2, 1])
# with col1:
#     days = st.slider("Data timeframe (days)", 1, 90, 7, 
#                     help="Choose how many days of historical data to display in the dashboard")
# with col3:
#     if st.button("Refresh Data", key="refresh_btn", use_container_width=True):
#         # Clear all cached data
#         st.cache_data.clear()

# # Load data
# try:
#     price_alerts = load_price_alerts(days)
#     competitor_stats = load_competitor_stats(days)
#     product_stats = load_product_stats(days)
#     price_signals = load_price_signals(days)
    
#     if price_alerts.empty and price_signals.empty:
#         st.info("No price data found in the selected timeframe. Try extending the date range or check if the data streaming services are running.")
#     else:
#         # Information box to explain the different types of alerts
#         st.markdown("""
#         <div class="info-box">
#             <h4>Understanding Price Alerts & Signals</h4>
#             <p>This dashboard tracks two different types of price intelligence:</p>
#             <ol>
#                 <li><b>Sequential Price Changes</b> <span class="alert-type-tag sequential-change">Sequential</span> - 
#                    Tracks how competitor prices change from their previous value (from price_monitoring_job)</li>
#                 <li><b>Price Signals</b> - More complex price patterns requiring action:
#                     <ul>
#                         <li><span class="alert-type-tag competitor-undercut">Undercut</span> Competitor price is below our platform price</li>
#                         <li><span class="alert-type-tag price-increase-24h">24h Price Increase</span> Competitor raised price from their 24-hour low</li>
#                         <li><span class="alert-type-tag stockout">Stockout</span> Competitor is out of stock for a product we have</li>
#                     </ul>
#                 </li>
#             </ol>
#         </div>
#         """, unsafe_allow_html=True)
        
#         # Overview metrics section
#         st.markdown("<h2 class='sub-header'>Market Overview</h2>", unsafe_allow_html=True)
        
#         # Create metrics
#         total_alerts = len(price_alerts) if not price_alerts.empty else 0
#         price_decreases = price_alerts[price_alerts['alert_type'] == 'price_decrease'].shape[0] if not price_alerts.empty else 0
#         price_increases = price_alerts[price_alerts['alert_type'] == 'price_increase'].shape[0] if not price_alerts.empty else 0
        
#         # Additional metrics from signals
#         total_signals = len(price_signals) if not price_signals.empty else 0
#         high_priority = price_signals[price_signals['priority'] == 1].shape[0] if not price_signals.empty else 0
        
#         # Display metrics in columns
#         col1, col2, col3, col4, col5 = st.columns(5)
#         col1.metric("Sequential Price Changes", f"{total_alerts}", help="Number of times competitor prices changed from their previous value")
#         col2.metric("Price Decreases", f"{price_decreases}", f"{price_decreases/total_alerts*100:.1f}%" if total_alerts > 0 else "0%", help="Sequential price decreases by competitors")
#         col3.metric("Price Increases", f"{price_increases}", f"{price_increases/total_alerts*100:.1f}%" if total_alerts > 0 else "0%", help="Sequential price increases by competitors")
#         col4.metric("Price Signals", f"{total_signals}", help="Number of actionable price signals detected (undercut, 24h increase, etc.)")
#         col5.metric("High Priority Signals", f"{high_priority}", f"{high_priority/total_signals*100:.1f}%" if total_signals > 0 else "0%", help="Price signals requiring immediate attention")
        
#         # Create tabs for different views
#         tab1, tab2, tab3, tab4 = st.tabs(["Action Center", "Sequential Price Changes", "Competitor Analysis", "Product Analysis"])
        
#         with tab1:
#             st.markdown("<h3>Actionable Price Signals</h3>", unsafe_allow_html=True)
#             st.markdown("""
#             This section shows price patterns that require attention based on your platform's price position
#             compared to competitors. These signals come from the alerts_job.py process.
#             """)
            
#             if not price_signals.empty:
#                 # First show high priority signals
#                 high_signals = price_signals[price_signals['priority'] == 1]
#                 if not high_signals.empty:
#                     st.markdown("<div class='insight-card high-priority'><p class='metric-title'>⚠️ High Priority Actions</p></div>", unsafe_allow_html=True)
#                     for _, signal in high_signals.head(5).iterrows():
#                         signal_tag = f"""<span class="alert-type-tag {'competitor-undercut' if signal['signal_type'] == 'undercut' else 'price-increase-24h' if signal['signal_type'] == 'price_increase_24hrs_min' else 'stockout' if signal['signal_type'] == 'stockout' else ''}">{signal['signal_type']}</span>"""
#                         signal_card = f"""
#                         <div class='insight-card high-priority'>
#                             <p>{signal_tag} <b>Product:</b> {signal['product_sku']}</p>
#                             <p><b>Platform Price:</b> ${signal['platform_price']:.2f} | <b>Competitor Price:</b> ${signal['competitor_price']:.2f} | <b>Diff:</b> {signal['percentage_diff']:.2f}%</p>
#                             <p><b>Signal Meaning:</b> {signal['signal_category']}</p>
#                             <p><b>Recommendation:</b> {signal['recommendation']}</p>
#                             <p class='insight-text'>Detected {(datetime.now() - signal['signal_timestamp']).total_seconds() / 3600:.1f} hours ago</p>
#                         </div>
#                         """
#                         st.markdown(signal_card, unsafe_allow_html=True)
                
#                 # Then medium priority
#                 medium_signals = price_signals[price_signals['priority'] == 2]
#                 if not medium_signals.empty:
#                     st.markdown("<div class='insight-card medium-priority'><p class='metric-title'>⚠️ Medium Priority Actions</p></div>", unsafe_allow_html=True)
#                     for _, signal in medium_signals.head(3).iterrows():
#                         signal_tag = f"""<span class="alert-type-tag {'competitor-undercut' if signal['signal_type'] == 'undercut' else 'price-increase-24h' if signal['signal_type'] == 'price_increase_24hrs_min' else 'stockout' if signal['signal_type'] == 'stockout' else ''}">{signal['signal_type']}</span>"""
#                         signal_card = f"""
#                         <div class='insight-card medium-priority'>
#                             <p>{signal_tag} <b>Product:</b> {signal['product_sku']}</p>
#                             <p><b>Platform Price:</b> ${signal['platform_price']:.2f} | <b>Competitor Price:</b> ${signal['competitor_price']:.2f} | <b>Diff:</b> {signal['percentage_diff']:.2f}%</p>
#                             <p><b>Signal Meaning:</b> {signal['signal_category']}</p>
#                             <p><b>Recommendation:</b> {signal['recommendation']}</p>
#                             <p class='insight-text'>Detected {(datetime.now() - signal['signal_timestamp']).total_seconds() / 3600:.1f} hours ago</p>
#                         </div>
#                         """
#                         st.markdown(signal_card, unsafe_allow_html=True)
                
#                 # Summary of signal types
#                 st.subheader("Signal Type Distribution")
#                 st.markdown("""
#                 This chart shows the distribution of different types of price signals that require action:
#                 * **Undercut**: Competitor price is below our platform price by a significant percentage
#                 * **Overpriced**: Competitor price is above our platform price by a significant percentage
#                 * **Price Increase (24hr)**: Competitor current price has increased significantly from its 24-hour low
#                 * **Stockout**: Competitor is out of stock while we have inventory available
                
#                 These signals help identify potential pricing opportunities and competitive threats.
#                 """)
                
#                 signal_counts = price_signals['signal_type'].value_counts().reset_index()
#                 signal_counts.columns = ['Signal Type', 'Count']
                
#                 fig = px.pie(
#                     signal_counts, 
#                     values='Count', 
#                     names='Signal Type',
#                     title='Distribution of Price Signal Types',
#                     color_discrete_sequence=px.colors.qualitative.Bold,
#                     hole=0.4
#                 )
#                 fig.update_traces(textposition='inside', textinfo='percent+label')
#                 st.plotly_chart(fig, use_container_width=True)
                
#                 # Show complete signals table
#                 with st.expander("View All Price Signals"):
#                     st.dataframe(
#                         price_signals,
#                         use_container_width=True,
#                         column_config={
#                             "platform_price": st.column_config.NumberColumn(
#                                 "Platform Price", format="$%.2f"
#                             ),
#                             "competitor_price": st.column_config.NumberColumn(
#                                 "Competitor Price", format="$%.2f"
#                             ),
#                             "percentage_diff": st.column_config.NumberColumn(
#                                 "% Difference", format="%.2f%%"
#                             ),
#                             "signal_timestamp": st.column_config.DatetimeColumn(
#                                 "Timestamp", format="DD/MM/YYYY HH:mm"
#                             ),
#                             "signal_category": st.column_config.TextColumn(
#                                 "Signal Description", help="Detailed explanation of what this signal means"
#                             )
#                         }
#                     )
#             else:
#                 st.info("No price signals found in the selected timeframe.")
        
#         with tab2:
#             if not price_alerts.empty:
#                 st.markdown("<h3>Sequential Price Changes</h3>", unsafe_allow_html=True)
#                 st.markdown("""
#                 This section shows how competitor prices change over time compared to their previous values.
#                 These are sequential price changes detected by the price_monitoring_job.py process.
#                 <span class="alert-type-tag sequential-change">Sequential</span> price changes track when a competitor 
#                 changes their price from what it was previously.
#                 """, unsafe_allow_html=True)
                
#                 # Price alerts by day
#                 price_alerts['date'] = price_alerts['alert_timestamp'].dt.date
#                 alerts_by_date = price_alerts.groupby(['date', 'alert_type']).size().reset_index(name='count')
#                 alerts_by_date['date'] = pd.to_datetime(alerts_by_date['date'])
                
#                 fig = px.line(
#                     alerts_by_date, 
#                     x='date', 
#                     y='count',
#                     color='alert_type',
#                     title='Sequential Price Changes Over Time by Type',
#                     labels={'date': 'Date', 'count': 'Number of Changes', 'alert_type': 'Change Type'},
#                     color_discrete_map={
#                         'price_increase': '#FF4B4B',  # Red for increases
#                         'price_decrease': '#0068C9'   # Blue for decreases
#                     }
#                 )
#                 st.plotly_chart(fig, use_container_width=True)
                
#                 # Price alerts distribution
#                 col1, col2 = st.columns(2)
                
#                 with col1:
#                     # Percentage change distribution
#                     fig = px.histogram(
#                         price_alerts, 
#                         x='percentage_change',
#                         nbins=20,
#                         title='Distribution of Sequential Price Change Percentages',
#                         labels={'percentage_change': 'Price Change (%)'},
#                         color_discrete_sequence=['#1E88E5']
#                     )
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 with col2:
#                     # Alert type distribution
#                     alert_counts = price_alerts['alert_type'].value_counts().reset_index()
#                     alert_counts.columns = ['Alert Type', 'Count']
                    
#                     fig = px.pie(
#                         alert_counts, 
#                         values='Count', 
#                         names='Alert Type',
#                         title='Distribution of Sequential Price Change Types',
#                         color_discrete_map={
#                             'price_increase': '#FF4B4B',  # Red for increases
#                             'price_decrease': '#0068C9'   # Blue for decreases
#                         }
#                     )
#                     fig.update_traces(textposition='inside', textinfo='percent+label')
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 # Recent alerts table
#                 st.subheader("Recent Sequential Price Changes")
#                 st.markdown("""
#                 This table shows the most recent sequential price changes detected from competitors:
#                 * **Previous Price**: The price before the change was detected
#                 * **New Price**: The current price after the change
#                 * **Price Change**: The absolute difference in price (positive for increases, negative for decreases)
#                 * **Percentage Change**: The relative magnitude of the price change
#                 * **Alert Type**: The category of price change (increase or decrease)
                
#                 These sequential changes are tracked in real-time and can help identify immediate pricing opportunities.
#                 """)
                
#                 st.dataframe(
#                     price_alerts[['product_sku', 'competitor_name', 'previous_price', 'new_price', 
#                                  'percentage_change', 'alert_type', 'alert_timestamp']]
#                     .sort_values(by='alert_timestamp', ascending=False),
#                     use_container_width=True,
#                     column_config={
#                         "previous_price": st.column_config.NumberColumn(
#                             "Previous Price", format="$%.2f"
#                         ),
#                         "new_price": st.column_config.NumberColumn(
#                             "New Price", format="$%.2f"
#                         ),
#                         "percentage_change": st.column_config.NumberColumn(
#                             "% Change", format="%.2f%%"
#                         ),
#                         "alert_type": st.column_config.TextColumn(
#                             "Change Type", help="Whether the price increased or decreased from its previous value"
#                         ),
#                         "alert_timestamp": st.column_config.DatetimeColumn(
#                             "Timestamp", format="DD/MM/YYYY HH:mm"
#                         ),
#                     }
#                 )
#             else:
#                 st.info("No sequential price changes found in the selected timeframe.")
        
#         with tab3:
#             if not competitor_stats.empty:
#                 st.markdown("<h3>Competitor Analysis</h3>", unsafe_allow_html=True)
#                 st.markdown("""
#                 This section analyzes how competitors change their prices over time, based on sequential price changes.
#                 It helps identify which competitors are most aggressive with pricing and how they typically adjust prices.
#                 """)
                
#                 # Competitor competitive metrics
#                 col1, col2 = st.columns(2)
                
#                 with col1:
#                     # Competitor price change behavior
#                     fig = px.bar(
#                         competitor_stats.sort_values('price_aggression', ascending=False), 
#                         x='competitor_name', 
#                         y='price_aggression',
#                         title='Competitor Price Aggression Score',
#                         labels={'competitor_name': 'Competitor', 'price_aggression': 'Aggression Score'},
#                         color='price_aggression',
#                         color_continuous_scale='reds'
#                     )
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 with col2:
#                     # Competitor price change direction
#                     fig = px.bar(
#                         competitor_stats, 
#                         x='competitor_name', 
#                         y=['price_increases', 'price_decreases'],
#                         title='Price Increase vs Decrease by Competitor',
#                         labels={'competitor_name': 'Competitor', 'value': 'Count', 'variable': 'Direction'},
#                         barmode='group',
#                         color_discrete_map={
#                             'price_increases': '#FF4B4B',  # Red for increases
#                             'price_decreases': '#0068C9'   # Blue for decreases
#                         }
#                     )
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 # Competitor average price changes
#                 st.subheader("Average Price Changes by Competitor")
#                 st.markdown("""
#                 This chart visualizes the average magnitude of price changes by each competitor:
#                 * **Avg Increase**: The average percentage increase when prices go up
#                 * **Avg Decrease**: The average percentage decrease when prices go down
                
#                 Competitors with larger values tend to make more dramatic price changes, which could indicate aggressive pricing strategies or promotional activities. This information can help you anticipate competitor behavior and set more competitive pricing strategies.
#                 """)
                
#                 avg_changes = pd.melt(
#                     competitor_stats, 
#                     id_vars=['competitor_name'], 
#                     value_vars=['avg_decrease_pct', 'avg_increase_pct'],
#                     var_name='Change Type', 
#                     value_name='Average Percentage'
#                 )
#                 avg_changes['Change Type'] = avg_changes['Change Type'].map({
#                     'avg_decrease_pct': 'Average Decrease %',
#                     'avg_increase_pct': 'Average Increase %'
#                 })
                
#                 fig = px.bar(
#                     avg_changes, 
#                     x='competitor_name', 
#                     y='Average Percentage',
#                     color='Change Type',
#                     barmode='group',
#                     title='Average Price Change Percentages by Competitor',
#                     color_discrete_map={
#                         'Average Increase %': '#FF4B4B',  # Red for increases
#                         'Average Decrease %': '#0068C9'   # Blue for decreases
#                     }
#                 )
#                 st.plotly_chart(fig, use_container_width=True)
                
#                 # Competitor stats table
#                 with st.expander("View Detailed Competitor Statistics"):
#                     st.dataframe(
#                         competitor_stats,
#                         use_container_width=True,
#                         column_config={
#                             "avg_change_pct": st.column_config.NumberColumn("Avg % Change", format="%.2f%%"),
#                             "avg_decrease_pct": st.column_config.NumberColumn("Avg Decrease %", format="%.2f%%"),
#                             "avg_increase_pct": st.column_config.NumberColumn("Avg Increase %", format="%.2f%%"),
#                             "price_aggression": st.column_config.NumberColumn("Price Aggression Score", format="%.3f"),
#                             "decrease_frequency": st.column_config.NumberColumn("Decrease Frequency", format="%.2f")
#                         }
#                     )
#             else:
#                 st.info("No competitor data available in the selected timeframe.")
        
#         with tab4:
#             if not product_stats.empty:
#                 st.markdown("<h3>Product Analysis</h3>", unsafe_allow_html=True)
#                 st.markdown("""
#                 This section analyzes how different products experience price changes across competitors.
#                 It helps identify which products have the most volatile pricing and how frequently their prices change.
#                 """)
                
#                 # Product volatility metrics
#                 col1, col2 = st.columns(2)
                
#                 with col1:
#                     # Top products by price volatility
#                     top_volatile = product_stats.sort_values('price_volatility', ascending=False).head(10)
#                     fig = px.bar(
#                         top_volatile, 
#                         x='product_sku', 
#                         y='price_volatility',
#                         title='Top 10 Products by Price Volatility',
#                         labels={'product_sku': 'Product SKU', 'price_volatility': 'Price Volatility (%)'},
#                         color='price_volatility',
#                         color_continuous_scale='Viridis'
#                     )
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 with col2:
#                     # Products by alert count
#                     top_products = product_stats.sort_values('alert_count', ascending=False).head(10)
#                     fig = px.bar(
#                         top_products, 
#                         x='product_sku', 
#                         y='alert_count',
#                         title='Top 10 Products by Price Change Frequency',
#                         labels={'product_sku': 'Product SKU', 'alert_count': 'Number of Price Changes'},
#                         color='alert_count',
#                         color_continuous_scale='Blues'
#                     )
#                     st.plotly_chart(fig, use_container_width=True)
                
#                 # Price increase vs decrease by product
#                 top_products_both = product_stats.sort_values('alert_count', ascending=False).head(10)
#                 melted_products = pd.melt(
#                     top_products_both,
#                     id_vars=['product_sku'],
#                     value_vars=['price_increases', 'price_decreases'],
#                     var_name='Change Type',
#                     value_name='Count'
#                 )
                
#                 fig = px.bar(
#                     melted_products,
#                     x='product_sku',
#                     y='Count',
#                     color='Change Type',
#                     title='Price Increase vs Decrease by Top Products',
#                     barmode='group',
#                     color_discrete_map={
#                         'price_increases': '#FF4B4B',  # Red for increases
#                         'price_decreases': '#0068C9'   # Blue for decreases
#                     }
#                 )
#                 st.plotly_chart(fig, use_container_width=True)
                
#                 # Product stats table
#                 with st.expander("View Detailed Product Statistics"):
#                     st.dataframe(
#                         product_stats,
#                         use_container_width=True,
#                         column_config={
#                             "avg_change_pct": st.column_config.NumberColumn("Avg % Change", format="%.2f%%"),
#                             "min_price": st.column_config.NumberColumn("Min Price", format="$%.2f"),
#                             "max_price": st.column_config.NumberColumn("Max Price", format="$%.2f"),
#                             "price_range": st.column_config.NumberColumn("Price Range", format="$%.2f"),
#                             "price_volatility": st.column_config.NumberColumn("Price Volatility", format="%.2f%%"),
#                             "last_alert": st.column_config.DatetimeColumn("Last Alert", format="DD/MM/YYYY HH:mm")
#                         }
#                     )
#             else:
#                 st.info("No product data available in the selected timeframe.")

# except Exception as e:
#     st.error(f"Error loading dashboard data: {e}")
#     st.info("Make sure the PostgreSQL service is running and accessible.")
#     st.code(str(e), language="python")


import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
from db import get_db_connection

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
    .sequential-change {background-color: #e0f7fa; color: #006064; border: 1px solid #00838f;}
    .competitor-undercut {background-color: #ffebee; color: #b71c1c; border: 1px solid #c62828;}
    .price-increase-24h {background-color: #ede7f6; color: #4527a0; border: 1px solid #512da8;}
    .stockout {background-color: #fce4ec; color: #880e4f; border: 1px solid #ad1457;}
    .info-box {background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 20px;}
    .key-metrics {padding: 15px; background-color: #f5f5f5; border-radius: 5px; margin-bottom: 20px;}
    .banner {background-color: #f1f8e9; padding: 10px; border-left: 4px solid #7cb342; margin-bottom: 15px;}
    .tooltip {position: relative; display: inline-block; cursor: help; color: #1E88E5;}
    .tooltip:hover .tooltip-text {visibility: visible; opacity: 1;}
    .tooltip-text {visibility: hidden; width: 200px; background-color: #555; color: #fff; text-align: center; padding: 5px; border-radius: 6px; position: absolute; z-index: 1; bottom: 125%; left: 50%; margin-left: -100px; opacity: 0; transition: opacity 0.3s;}
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
@st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
def load_price_alerts(days=7):
    engine = get_db_connection()
    query = f"""
    SELECT 
        pa.product_sku,
        pp.name AS product_name,
        pa.competitor_name,
        ec.name AS competitor_full_name,
        pa.previous_price,
        pa.new_price,
        pa.price_change,
        pa.percentage_change,
        pa.alert_type,
        pa.alert_timestamp,
        pp.category
    FROM price_alerts pa
    JOIN platform_products pp ON pa.product_sku = pp.sku
    LEFT JOIN external_competitors ec ON pa.competitor_name = ec.code
    WHERE pa.alert_timestamp > NOW() - INTERVAL '{days} days'
    ORDER BY pa.alert_timestamp DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
        df['date'] = df['alert_timestamp'].dt.date
    return df

@st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
def load_competitor_stats(days=30):
    engine = get_db_connection()
    query = f"""
    SELECT 
        pa.competitor_name,
        ec.name AS competitor_full_name,
        COUNT(*) as alert_count,
        AVG(ABS(pa.percentage_change)) as avg_change_pct,
        SUM(CASE WHEN pa.alert_type = 'price_decrease' THEN 1 ELSE 0 END) as price_decreases,
        SUM(CASE WHEN pa.alert_type = 'price_increase' THEN 1 ELSE 0 END) as price_increases,
        AVG(CASE WHEN pa.alert_type = 'price_decrease' THEN pa.percentage_change ELSE NULL END) as avg_decrease_pct,
        AVG(CASE WHEN pa.alert_type = 'price_increase' THEN pa.percentage_change ELSE NULL END) as avg_increase_pct
    FROM price_alerts pa
    LEFT JOIN external_competitors ec ON pa.competitor_name = ec.code
    WHERE pa.alert_timestamp > NOW() - INTERVAL '{days} days'
    GROUP BY pa.competitor_name, ec.name
    ORDER BY alert_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        # Calculate competitive aggression score (higher means more aggressive pricing)
        df['decrease_frequency'] = df['price_decreases'] / (df['price_decreases'] + df['price_increases'])
        df['price_aggression'] = df['decrease_frequency'] * df['avg_change_pct']
    return df

@st.cache_data(ttl=10)  # Auto-refresh friendly cache (10s)
def load_product_stats(days=30):
    engine = get_db_connection()
    query = f"""
    SELECT 
        pa.product_sku,
        pp.name AS product_name,
        pp.category,
        COUNT(*) as alert_count,
        AVG(ABS(pa.percentage_change)) as avg_change_pct,
        MIN(pa.new_price) as min_price,
        MAX(pa.new_price) as max_price,
        MAX(pa.alert_timestamp) as last_alert,
        SUM(CASE WHEN pa.alert_type = 'price_decrease' THEN 1 ELSE 0 END) as price_decreases,
        SUM(CASE WHEN pa.alert_type = 'price_increase' THEN 1 ELSE 0 END) as price_increases
    FROM price_alerts pa
    JOIN platform_products pp ON pa.product_sku = pp.sku
    WHERE pa.alert_timestamp > NOW() - INTERVAL '{days} days'
    GROUP BY pa.product_sku, pp.name, pp.category
    ORDER BY alert_count DESC
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['last_alert'] = pd.to_datetime(df['last_alert'])
        df['price_range'] = df['max_price'] - df['min_price']
        df['price_volatility'] = df['price_range'] / df['min_price'] * 100
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
        
        # Add an additional column with description for clarity
        df['signal_category'] = df['signal_type'].map({
            'undercut': 'Competitor has lower price than our platform',
            'overpriced': 'Competitor has higher price than our platform',
            'stockout': 'Competitor is out of stock',
            'price_increase_24hrs_min': '24h price increase from competitor\'s lowest price'
        })
    return df

# Main dashboard title
st.title("Price Intelligence Dashboard")
st.markdown("""
<div class="banner">
Welcome to your real-time price intelligence dashboard. Here you'll find actionable insights to optimize your pricing strategy and stay ahead of the competition.
</div>

This dashboard provides comprehensive competitive price monitoring and analytics:
* **Real-time Price Signals**: Detect when competitors undercut your prices or raise their own
* **Competitor Behavior Analysis**: Track pricing strategies and identify aggressive competitors
* **Product Performance**: Discover which products face the most price volatility
* **Market Trends**: Visualize pricing patterns across your entire product catalog
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
    price_alerts = load_price_alerts(days)
    competitor_stats = load_competitor_stats(days)
    product_stats = load_product_stats(days)
    price_signals = load_price_signals(days)
    
    if price_alerts.empty and price_signals.empty:
        st.info("No price data found in the selected timeframe. Try extending the date range or check if the data streaming services are running.")
    else:
        # Information box to explain the different types of alerts
        st.markdown("""
        <div class="info-box">
            <h4>Understanding Price Intelligence Data</h4>
            <p>This dashboard integrates two complementary types of pricing intelligence:</p>
            <ol>
                <li><b>Sequential Price Changes</b> <span class="alert-type-tag sequential-change">Sequential</span> - 
                   Tracks historical price movements by competitor and product. These show how a competitor's price
                   has changed over time compared to their previous pricing.</li>
                <li><b>Strategic Price Signals</b> - Actionable pricing insights that may require immediate attention:
                    <ul>
                        <li><span class="alert-type-tag competitor-undercut">Undercut</span> A competitor's price is below your price, potentially impacting your sales</li>
                        <li><span class="alert-type-tag price-increase-24h">24h Price Increase</span> A competitor has raised their price from their 24-hour low, creating a potential opportunity</li>
                        <li><span class="alert-type-tag stockout">Stockout</span> A competitor is out of stock for a product you have available, representing a sales opportunity</li>
                    </ul>
                </li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # Overview metrics section
        st.markdown("<h2 class='sub-header'>Market Overview</h2>", unsafe_allow_html=True)
        
        # Create metrics
        total_alerts = len(price_alerts) if not price_alerts.empty else 0
        price_decreases = price_alerts[price_alerts['alert_type'] == 'price_decrease'].shape[0] if not price_alerts.empty else 0
        price_increases = price_alerts[price_alerts['alert_type'] == 'price_increase'].shape[0] if not price_alerts.empty else 0
        
        # Additional metrics from signals
        total_signals = len(price_signals) if not price_signals.empty else 0
        high_priority = price_signals[price_signals['priority'] == 1].shape[0] if not price_signals.empty else 0
        
        # Calculate average price changes
        avg_decrease = price_alerts[price_alerts['alert_type'] == 'price_decrease']['percentage_change'].mean() if not price_alerts.empty and price_decreases > 0 else 0
        avg_increase = price_alerts[price_alerts['alert_type'] == 'price_increase']['percentage_change'].mean() if not price_alerts.empty and price_increases > 0 else 0
        
        # Display metrics in columns with improved layout
        st.markdown('<div class="key-metrics">', unsafe_allow_html=True)
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric(
            "Sequential Price Changes", 
            f"{total_alerts}", 
            help="Number of times competitor prices changed from their previous value"
        )
        
        col2.metric(
            "Price Decreases", 
            f"{price_decreases}", 
            f"{avg_decrease:.1f}% avg" if avg_decrease != 0 else "0%", 
            help="Sequential price decreases by competitors and their average magnitude"
        )
        
        col3.metric(
            "Price Increases", 
            f"{price_increases}", 
            f"{avg_increase:.1f}% avg" if avg_increase != 0 else "0%", 
            help="Sequential price increases by competitors and their average magnitude"
        )
        
        col4.metric(
            "Strategic Signals", 
            f"{total_signals}", 
            help="Number of actionable price signals detected (undercut, 24h increase, etc.)"
        )
        
        col5.metric(
            "High Priority Actions", 
            f"{high_priority}", 
            f"{high_priority/total_signals*100:.1f}%" if total_signals > 0 else "0%", 
            help="Price signals requiring immediate attention based on business impact"
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Create tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["Action Center", "Sequential Price Changes", "Competitor Analysis", "Product Analysis"])
        
        with tab1:
            st.markdown("<h3>Actionable Price Signals</h3>", unsafe_allow_html=True)
            st.markdown("""
            <div class="info-box">
            This section displays strategic price signals that require attention based on your platform's price position
            compared to competitors. These insights help you identify immediate pricing opportunities and threats.
            
            Price signals are categorized by priority:
            <ul>
                <li><b>High priority</b>: Requires immediate attention due to significant price differences or business impact</li>
                <li><b>Medium priority</b>: Important but less urgent pricing situations</li>
                <li><b>Low priority</b>: Minor price differences that should be monitored</li>
            </ul>
            </div>
            """, unsafe_allow_html=True)
            
            if not price_signals.empty:
                # First show high priority signals
                high_signals = price_signals[price_signals['priority'] == 1]
                if not high_signals.empty:
                    st.markdown("<div class='insight-card high-priority'><p class='metric-title'>⚠️ High Priority Actions</p></div>", unsafe_allow_html=True)
                    for _, signal in high_signals.head(5).iterrows():
                        signal_tag = f"""<span class="alert-type-tag {'competitor-undercut' if signal['signal_type'] == 'undercut' else 'price-increase-24h' if signal['signal_type'] == 'price_increase_24hrs_min' else 'stockout' if signal['signal_type'] == 'stockout' else ''}">{signal['signal_type']}</span>"""
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
                
                # Then medium priority
                medium_signals = price_signals[price_signals['priority'] == 2]
                if not medium_signals.empty:
                    st.markdown("<div class='insight-card medium-priority'><p class='metric-title'>⚠️ Medium Priority Actions</p></div>", unsafe_allow_html=True)
                    for _, signal in medium_signals.head(3).iterrows():
                        signal_tag = f"""<span class="alert-type-tag {'competitor-undercut' if signal['signal_type'] == 'undercut' else 'price-increase-24h' if signal['signal_type'] == 'price_increase_24hrs_min' else 'stockout' if signal['signal_type'] == 'stockout' else ''}">{signal['signal_type']}</span>"""
                        signal_card = f"""
                        <div class='insight-card medium-priority'>
                            <p>{signal_tag} <b>Product:</b> {signal['product_name']} ({signal['product_sku']})</p>
                            <p><b>Competitor:</b> {signal['competitor_name']} | <b>Category:</b> {signal['category']}</p>
                            <p><b>Your Price:</b> ${signal['platform_price']:.2f} | <b>Competitor Price:</b> ${signal['competitor_price']:.2f} | <b>Diff:</b> {signal['percentage_diff']:.2f}%</p>
                            <p><b>Signal Meaning:</b> {signal['signal_category']}</p>
                            <p><b>Recommendation:</b> {signal['recommendation']}</p>
                            <p class='insight-text'>Detected {(datetime.now() - signal['signal_timestamp']).total_seconds() / 3600:.1f} hours ago</p>
                        </div>
                        """
                        st.markdown(signal_card, unsafe_allow_html=True)
                
                # Summary of signal types
                st.subheader("Signal Type Distribution")
                st.markdown("""
                This chart shows the distribution of different types of price signals that require action:
                * **Undercut**: Competitor price is below our platform price by a significant percentage
                * **Overpriced**: Competitor price is above our platform price by a significant percentage
                * **Price Increase (24hr)**: Competitor current price has increased significantly from its 24-hour low
                * **Stockout**: Competitor is out of stock while we have inventory available
                
                These signals help identify potential pricing opportunities and competitive threats.
                """)
                
                signal_counts = price_signals['signal_type'].value_counts().reset_index()
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
                if 'category' in price_signals.columns:
                    category_signals = price_signals.groupby(['category', 'signal_type']).size().reset_index(name='count')
                    
                    fig = px.bar(
                        category_signals,
                        x='category',
                        y='count',
                        color='signal_type',
                        title='Signal Distribution by Product Category',
                        labels={'count': 'Number of Signals', 'category': 'Product Category', 'signal_type': 'Signal Type'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Show complete signals table
                with st.expander("View All Price Signals"):
                    st.dataframe(
                        price_signals,
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
            if not price_alerts.empty:
                st.markdown("<h3>Sequential Price Changes</h3>", unsafe_allow_html=True)
                st.markdown("""
                <div class="info-box">
                This section analyzes how competitor prices change over time compared to their previous values.
                <span class="alert-type-tag sequential-change">Sequential</span> price changes track when a competitor 
                changes their price from what it was previously, showing price movement patterns and volatility.
                
                Understanding sequential price changes helps you:
                <ul>
                    <li>Identify which competitors change prices most frequently</li>
                    <li>Track the magnitude of price changes in your market</li>
                    <li>Detect seasonal pricing patterns or promotional activities</li>
                    <li>Gauge the overall price stability of your product categories</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                # Price alerts by day
                price_alerts['date'] = price_alerts['alert_timestamp'].dt.date
                alerts_by_date = price_alerts.groupby(['date', 'alert_type']).size().reset_index(name='count')
                alerts_by_date['date'] = pd.to_datetime(alerts_by_date['date'])
                
                fig = px.line(
                    alerts_by_date, 
                    x='date', 
                    y='count',
                    color='alert_type',
                    title='Sequential Price Changes Over Time by Type',
                    labels={'date': 'Date', 'count': 'Number of Changes', 'alert_type': 'Change Type'},
                    color_discrete_map={
                        'price_increase': '#FF4B4B',  # Red for increases
                        'price_decrease': '#0068C9'   # Blue for decreases
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Price alerts distribution
                col1, col2 = st.columns(2)
                
                with col1:
                    # Percentage change distribution
                    fig = px.histogram(
                        price_alerts, 
                        x='percentage_change',
                        nbins=20,
                        title='Distribution of Sequential Price Change Percentages',
                        labels={'percentage_change': 'Price Change (%)'},
                        color_discrete_sequence=['#1E88E5']
                    )
                    # Add a vertical line at 0% for reference
                    fig.add_vline(x=0, line_dash="dash", line_color="red")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Alert type distribution
                    alert_counts = price_alerts['alert_type'].value_counts().reset_index()
                    alert_counts.columns = ['Alert Type', 'Count']
                    
                    fig = px.pie(
                        alert_counts, 
                        values='Count', 
                        names='Alert Type',
                        title='Distribution of Sequential Price Change Types',
                        color_discrete_map={
                            'price_increase': '#FF4B4B',  # Red for increases
                            'price_decrease': '#0068C9'   # Blue for decreases
                        }
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True)
                
                # Add category breakdown
                if 'category' in price_alerts.columns:
                    category_alerts = price_alerts.groupby(['category', 'alert_type']).size().reset_index(name='count')
                    
                    fig = px.bar(
                        category_alerts,
                        x='category',
                        y='count',
                        color='alert_type',
                        title='Price Changes by Product Category',
                        labels={'count': 'Number of Changes', 'category': 'Product Category', 'alert_type': 'Change Type'},
                        color_discrete_map={
                            'price_increase': '#FF4B4B',  # Red for increases
                            'price_decrease': '#0068C9'   # Blue for decreases
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Recent alerts table
                st.subheader("Recent Sequential Price Changes")
                st.markdown("""
                This table shows the most recent sequential price changes detected from competitors:
                * **Previous Price**: The price before the change was detected
                * **New Price**: The current price after the change
                * **Percentage Change**: The relative magnitude of the price change
                * **Alert Type**: The category of price change (increase or decrease)
                
                These sequential changes are tracked in real-time and can help identify immediate pricing opportunities.
                """)
                
                st.dataframe(
                    price_alerts[['product_sku', 'product_name', 'competitor_name', 'competitor_full_name', 'previous_price', 'new_price', 
                                 'percentage_change', 'alert_type', 'alert_timestamp']]
                    .sort_values(by='alert_timestamp', ascending=False),
                    use_container_width=True,
                    column_config={
                        "product_sku": st.column_config.TextColumn("Product SKU"),
                        "product_name": st.column_config.TextColumn("Product Name"),
                        "competitor_full_name": st.column_config.TextColumn("Competitor"),
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
                            "Change Type", help="Whether the price increased or decreased from its previous value"
                        ),
                        "alert_timestamp": st.column_config.DatetimeColumn(
                            "Timestamp", format="DD/MM/YYYY HH:mm"
                        ),
                    }
                )
            else:
                st.info("No sequential price changes found in the selected timeframe.")
        
        with tab3:
            if not competitor_stats.empty:
                st.markdown("<h3>Competitor Analysis</h3>", unsafe_allow_html=True)
                st.markdown("""
                <div class="info-box">
                This section analyzes how competitors change their prices over time, based on sequential price changes.
                It helps identify which competitors are most aggressive with pricing and how they typically adjust prices.
                
                Key competitor metrics:
                <ul>
                    <li><b>Price Aggression Score</b>: Measures how aggressively a competitor changes prices (frequency and magnitude)</li>
                    <li><b>Decrease vs Increase Ratio</b>: Shows if a competitor tends to lower or raise prices more often</li>
                    <li><b>Average Change Magnitude</b>: The typical size of price changes by each competitor</li>
                </ul>
                
                Use these insights to anticipate competitor moves and adjust your pricing strategy accordingly.
                </div>
                """, unsafe_allow_html=True)
                
                # Competitor competitive metrics
                col1, col2 = st.columns(2)
                
                with col1:
                    # Competitor price change behavior
                    competitor_display = competitor_stats.copy()
                    if 'competitor_full_name' in competitor_display.columns:
                        competitor_display['competitor_display'] = competitor_display['competitor_full_name'].fillna(competitor_display['competitor_name'])
                    else:
                        competitor_display['competitor_display'] = competitor_display['competitor_name']
                        
                    fig = px.bar(
                        competitor_display.sort_values('price_aggression', ascending=False), 
                        x='competitor_display', 
                        y='price_aggression',
                        title='Competitor Price Aggression Score',
                        labels={'competitor_display': 'Competitor', 'price_aggression': 'Aggression Score'},
                        color='price_aggression',
                        color_continuous_scale='reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Competitor price change direction
                    competitor_direction = competitor_display.copy()
                    fig = px.bar(
                        competitor_direction, 
                        x='competitor_display', 
                        y=['price_increases', 'price_decreases'],
                        title='Price Increase vs Decrease by Competitor',
                        labels={'competitor_display': 'Competitor', 'value': 'Count', 'variable': 'Direction'},
                        barmode='group',
                        color_discrete_map={
                            'price_increases': '#FF4B4B',  # Red for increases
                            'price_decreases': '#0068C9'   # Blue for decreases
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Competitor average price changes
                st.subheader("Average Price Changes by Competitor")
                st.markdown("""
                This chart visualizes the average magnitude of price changes by each competitor:
                * **Avg Increase**: The average percentage increase when prices go up
                * **Avg Decrease**: The average percentage decrease when prices go down
                
                Competitors with larger values tend to make more dramatic price changes, which could indicate aggressive 
                pricing strategies or promotional activities. This information can help you anticipate competitor behavior 
                and set more competitive pricing strategies.
                """)
                
                avg_changes = pd.melt(
                    competitor_display, 
                    id_vars=['competitor_display'], 
                    value_vars=['avg_decrease_pct', 'avg_increase_pct'],
                    var_name='Change Type', 
                    value_name='Average Percentage'
                )
                avg_changes['Change Type'] = avg_changes['Change Type'].map({
                    'avg_decrease_pct': 'Average Decrease %',
                    'avg_increase_pct': 'Average Increase %'
                })
                
                fig = px.bar(
                    avg_changes, 
                    x='competitor_display', 
                    y='Average Percentage',
                    color='Change Type',
                    barmode='group',
                    title='Average Price Change Percentages by Competitor',
                    color_discrete_map={
                        'Average Increase %': '#FF4B4B',  # Red for increases
                        'Average Decrease %': '#0068C9'   # Blue for decreases
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Competitor stats table
                with st.expander("View Detailed Competitor Statistics"):
                    st.dataframe(
                        competitor_stats,
                        use_container_width=True,
                        column_config={
                            "competitor_name": st.column_config.TextColumn("Competitor Code"),
                            "competitor_full_name": st.column_config.TextColumn("Competitor Name"),
                            "avg_change_pct": st.column_config.NumberColumn("Avg % Change", format="%.2f%%"),
                            "avg_decrease_pct": st.column_config.NumberColumn("Avg Decrease %", format="%.2f%%"),
                            "avg_increase_pct": st.column_config.NumberColumn("Avg Increase %", format="%.2f%%"),
                            "price_aggression": st.column_config.NumberColumn("Price Aggression Score", format="%.3f"),
                            "decrease_frequency": st.column_config.NumberColumn("Decrease Frequency", format="%.2f")
                        }
                    )
            else:
                st.info("No competitor data available in the selected timeframe.")
        
        with tab4:
            if not product_stats.empty:
                st.markdown("<h3>Product Analysis</h3>", unsafe_allow_html=True)
                st.markdown("""
                <div class="info-box">
                This section analyzes how different products experience price changes across competitors.
                It helps identify which products have the most volatile pricing and how frequently their prices change.
                
                Key product metrics:
                <ul>
                    <li><b>Price Volatility</b>: Measures how much a product's price fluctuates in the market</li>
                    <li><b>Price Range</b>: The difference between the highest and lowest observed prices</li>
                    <li><b>Change Frequency</b>: How often competitors change prices for this product</li>
                </ul>
                
                Products with high volatility or frequent price changes may require more active monitoring and pricing adjustments.
                </div>
                """, unsafe_allow_html=True)
                
                # Product volatility metrics
                col1, col2 = st.columns(2)
                
                with col1:
                    # Top products by price volatility
                    top_volatile = product_stats.sort_values('price_volatility', ascending=False).head(10)
                    fig = px.bar(
                        top_volatile, 
                        x='product_sku', 
                        y='price_volatility',
                        title='Top 10 Products by Price Volatility',
                        labels={'product_sku': 'Product SKU', 'price_volatility': 'Price Volatility (%)'},
                        color='price_volatility',
                        hover_data=['product_name', 'category'],
                        color_continuous_scale='Viridis'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Products by alert count
                    top_products = product_stats.sort_values('alert_count', ascending=False).head(10)
                    fig = px.bar(
                        top_products, 
                        x='product_sku', 
                        y='alert_count',
                        title='Top 10 Products by Price Change Frequency',
                        labels={'product_sku': 'Product SKU', 'alert_count': 'Number of Price Changes'},
                        color='alert_count',
                        hover_data=['product_name', 'category'],
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Price increase vs decrease by product
                top_products_both = product_stats.sort_values('alert_count', ascending=False).head(10)
                melted_products = pd.melt(
                    top_products_both,
                    id_vars=['product_sku', 'product_name'],
                    value_vars=['price_increases', 'price_decreases'],
                    var_name='Change Type',
                    value_name='Count'
                )
                
                fig = px.bar(
                    melted_products,
                    x='product_sku',
                    y='Count',
                    color='Change Type',
                    title='Price Increase vs Decrease by Top Products',
                    hover_data=['product_name'],
                    barmode='group',
                    color_discrete_map={
                        'price_increases': '#FF4B4B',  # Red for increases
                        'price_decreases': '#0068C9'   # Blue for decreases
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add category analysis
                if 'category' in product_stats.columns:
                    category_volatility = product_stats.groupby('category').agg(
                        avg_volatility=('price_volatility', 'mean'),
                        product_count=('product_sku', 'count'),
                        total_changes=('alert_count', 'sum')
                    ).reset_index()
                    
                    fig = px.scatter(
                        category_volatility,
                        x='avg_volatility',
                        y='total_changes',
                        size='product_count',
                        color='category',
                        title='Category Price Volatility vs Change Frequency',
                        labels={
                            'avg_volatility': 'Average Price Volatility (%)',
                            'total_changes': 'Total Price Changes',
                            'product_count': 'Number of Products'
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Product stats table
                with st.expander("View Detailed Product Statistics"):
                    st.dataframe(
                        product_stats,
                        use_container_width=True,
                        column_config={
                            "product_sku": st.column_config.TextColumn("Product SKU"),
                            "product_name": st.column_config.TextColumn("Product Name"),
                            "category": st.column_config.TextColumn("Category"),
                            "avg_change_pct": st.column_config.NumberColumn("Avg % Change", format="%.2f%%"),
                            "min_price": st.column_config.NumberColumn("Min Price", format="$%.2f"),
                            "max_price": st.column_config.NumberColumn("Max Price", format="$%.2f"),
                            "price_range": st.column_config.NumberColumn("Price Range", format="$%.2f"),
                            "price_volatility": st.column_config.NumberColumn("Price Volatility", format="%.2f%%"),
                            "last_alert": st.column_config.DatetimeColumn("Last Alert", format="DD/MM/YYYY HH:mm")
                        }
                    )
            else:
                st.info("No product data available in the selected timeframe.")

except Exception as e:
    st.error(f"Error loading dashboard data: {e}")
    st.info("Make sure the PostgreSQL service is running and accessible.")
    st.code(str(e), language="python")
