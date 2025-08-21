# pages/03_Competitor_Analysis.py
import streamlit as st
import pandas as pd
import plotly.express as px
from db import get_db_connection
from streamlit_autorefresh import st_autorefresh

# Set page configuration
st.set_page_config(
    page_title="Competitor Analysis", 
    page_icon="🏢", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 5 minutes
st_autorefresh(interval=5 * 60 * 1000, key="competitor_refresh")

# Custom CSS similar to your other pages
st.markdown("""
<style>
    .main-header {color:#1E88E5; font-size:35px !important;}
    .sub-header {font-size:25px !important; margin-bottom:20px;}
    .insight-card {background-color:#f0f2f6; padding:15px; border-radius:5px; margin-bottom:10px;}
    .metric-title {font-weight:bold; font-size:16px;}
</style>
""", unsafe_allow_html=True)

st.title("🏢 Competitor Analysis")
st.markdown("""
This dashboard provides in-depth analysis of individual competitors and their pricing behaviors:
* **Pricing Strategies**: Understand how competitors price products across categories
* **Undercut Analysis**: Identify which products are most frequently undercut by each competitor
* **Market Positioning**: Compare each competitor's average prices relative to yours
""")

# Functions to load data
@st.cache_data(ttl=300)
def load_competitor_data(days=7):
    engine = get_db_connection()
    query = f"""
    SELECT 
        cp.competitor_id,
        ec.code as competitor_name,
        ec.name as competitor_full_name,
        pp.category,
        cp.product_sku,
        pp.name as product_name,
        cp.price as competitor_price,
        pp.current_price as our_price,
        ((cp.price - pp.current_price) / pp.current_price * 100) as price_diff_percent,
        cp.in_stock,
        cp.data_timestamp
    FROM competitor_price_history cp
    JOIN platform_products pp ON cp.product_sku = pp.sku
    JOIN external_competitors ec ON cp.competitor_id = ec.id
    WHERE cp.data_timestamp > NOW() - INTERVAL '{days} days'
    ORDER BY cp.data_timestamp DESC
    """
    return pd.read_sql(query, engine)

# Controls section
col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    days = st.slider("Data timeframe (days)", 1, 90, 7, 
                   help="Choose how many days of historical data to display")
with col3:
    if st.button("Refresh Data", key="refresh_btn", use_container_width=True):
        st.cache_data.clear()

# Load data
df = load_competitor_data(days)

if df.empty:
    st.info("No competitor data available for the selected timeframe.")
    st.stop()

# Competitor multi-selection
competitors = sorted(df['competitor_name'].unique())
selected_competitors = st.multiselect("Select Competitors to Analyze", competitors, default=[competitors[0]] if competitors else [])

if not selected_competitors:
    st.warning("Please select at least one competitor to analyze.")
    st.stop()

# Filter for selected competitors
competitor_df = df[df['competitor_name'].isin(selected_competitors)].copy()
competitor_names = {comp: df[df['competitor_name'] == comp]['competitor_full_name'].iloc[0] for comp in selected_competitors}

st.header("Competitor Analysis")

# Create competitor comparison KPIs
st.subheader("Key Performance Indicators")

# Calculate KPIs for each competitor
kpi_data = []
for comp in selected_competitors:
    comp_df = df[df['competitor_name'] == comp].copy()
    products_count = comp_df['product_sku'].nunique()
    avg_price_diff = comp_df['price_diff_percent'].mean()
    undercut_count = len(comp_df[comp_df['price_diff_percent'] < -5])
    undercut_percent = (undercut_count / len(comp_df)) * 100 if len(comp_df) > 0 else 0
    
    kpi_data.append({
        "Competitor": competitor_names[comp],
        "Products Tracked": products_count,
        "Avg Price Difference": f"{avg_price_diff:.2f}%",
        "Number of Undercuts (5%)": undercut_count,
        "Undercut Percentage": f"{undercut_percent:.2f}%"
    })

# Display KPIs as a table for comparison
kpi_df = pd.DataFrame(kpi_data)
st.dataframe(
    kpi_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Competitor": st.column_config.TextColumn("Competitor"),
        "Products Tracked": st.column_config.NumberColumn("Products Tracked"),
        "Avg Price Difference": st.column_config.TextColumn("Avg Price Difference"),
        "Number of Undercuts (5%)": st.column_config.NumberColumn("Undercuts (>5%)"),
        "Undercut Percentage": st.column_config.TextColumn("Undercut %")
    }
)

# Price comparison chart
st.subheader("Price Comparison by Category")
st.markdown("This chart shows how competitors' prices compare to ours across different product categories.")

# Group by category and competitor
category_comp_df = df.groupby(['category', 'competitor_name']).agg(
    competitor_avg_price=('competitor_price', 'mean'),
    our_avg_price=('our_price', 'mean'),
    price_diff_percent=('price_diff_percent', 'mean'),
    product_count=('product_sku', 'nunique')
).reset_index()

# Filter for selected competitors
category_comp_df = category_comp_df[category_comp_df['competitor_name'].isin(selected_competitors)]

# Create multi-line chart for price comparison
fig = px.line(
    category_comp_df,
    x='category',
    y='competitor_avg_price',
    color='competitor_name',
    markers=True,
    labels={
        'competitor_avg_price': 'Average Price ($)',
        'category': 'Product Category',
        'competitor_name': 'Competitor'
    },
    title='Average Prices by Category'
)

# Add our prices as a reference line
our_prices = df.groupby('category')['our_price'].mean().reset_index()
fig.add_trace(
    px.line(
        our_prices,
        x='category',
        y='our_price',
        line_dash_sequence=['dash'],
        color_discrete_sequence=['#0068C9']
    ).data[0]
)
fig.data[-1].name = 'Our Prices'

fig.update_layout(
    legend_title_text='Competitor',
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

# Provide a toggle to switch between line chart and grouped bar chart
if st.checkbox("Show as Bar Chart"):
    # Create a grouped bar chart as an alternative view
    # Add our prices to the dataframe
    category_our_df = df.groupby('category').agg(
        our_avg_price=('our_price', 'mean')
    ).reset_index()
    category_our_df['competitor_name'] = 'Our Prices'
    category_our_df['competitor_avg_price'] = category_our_df['our_avg_price']
    
    # Combine with competitor data
    viz_df = pd.concat([
        category_comp_df[['category', 'competitor_name', 'competitor_avg_price']],
        category_our_df[['category', 'competitor_name', 'competitor_avg_price']]
    ])
    
    fig = px.bar(
        viz_df,
        x='category',
        y='competitor_avg_price',
        color='competitor_name',
        barmode='group',
        labels={
            'competitor_avg_price': 'Average Price ($)',
            'category': 'Product Category',
            'competitor_name': 'Competitor'
        },
        title='Average Prices by Category'
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Price difference distribution
st.subheader("Price Difference Distribution")
st.markdown("This chart shows the distribution of price differences between competitors and our prices.")

# Create tabs for each competitor
comp_tabs = st.tabs(selected_competitors)

for i, comp in enumerate(selected_competitors):
    with comp_tabs[i]:
        comp_df = df[df['competitor_name'] == comp].copy()
        comp_name = competitor_names[comp]
        
        col1, col2 = st.columns([3, 1])
        with col1:
            # Histogram of price differences
            fig = px.histogram(
                comp_df,
                x='price_diff_percent',
                nbins=50,
                labels={'price_diff_percent': 'Price Difference (%)'},
                color_discrete_sequence=['#1E88E5'],
                title=f"Price Difference Distribution - {comp_name}"
            )
            fig.add_vline(x=0, line_dash="dash", line_color="red")
            fig.update_layout(
                xaxis_title="Price Difference (%)",
                yaxis_title="Number of Products",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Summary statistics
            diff_stats = {
                "Mean Difference": f"{comp_df['price_diff_percent'].mean():.2f}%",
                "Median Difference": f"{comp_df['price_diff_percent'].median():.2f}%",
                "Min Difference": f"{comp_df['price_diff_percent'].min():.2f}%",
                "Max Difference": f"{comp_df['price_diff_percent'].max():.2f}%",
                "Std Dev": f"{comp_df['price_diff_percent'].std():.2f}%",
                "Products Cheaper Than Us": f"{(comp_df['price_diff_percent'] < 0).sum()} ({(comp_df['price_diff_percent'] < 0).sum() / len(comp_df) * 100:.1f}%)",
                "Products More Expensive": f"{(comp_df['price_diff_percent'] > 0).sum()} ({(comp_df['price_diff_percent'] > 0).sum() / len(comp_df) * 100:.1f}%)"
            }
            
            # Display as a dataframe
            stats_df = pd.DataFrame(list(diff_stats.items()), columns=["Metric", "Value"])
            st.dataframe(stats_df, use_container_width=True, hide_index=True)

# Top undercut products
st.subheader("Top Undercut Products by Competitor")
st.markdown("These are the products where competitors' prices are significantly lower than ours (< -5%).")

# Show undercut products for all selected competitors
for comp in selected_competitors:
    comp_df = df[df['competitor_name'] == comp].copy()
    comp_name = competitor_names[comp]
    
    with st.expander(f"{comp_name} - Top Undercut Products"):
        undercut_df = comp_df[comp_df['price_diff_percent'] < -5].sort_values('price_diff_percent')
        if not undercut_df.empty:
            undercut_df = undercut_df.drop_duplicates(subset=['product_sku'], keep='first')
            
            st.dataframe(
                undercut_df[['product_sku', 'product_name', 'competitor_price', 'our_price', 'price_diff_percent']].head(10),
                use_container_width=True,
                column_config={
                    'product_sku': st.column_config.TextColumn("SKU"),
                    'product_name': st.column_config.TextColumn("Product Name"),
                    'competitor_price': st.column_config.NumberColumn("Competitor Price", format="$%.2f"),
                    'our_price': st.column_config.NumberColumn("Our Price", format="$%.2f"),
                    'price_diff_percent': st.column_config.NumberColumn("Difference %", format="%.2f%%"),
                }
            )
        else:
            st.info(f"No undercut products found for {comp_name}.")

# Price history timeline with multi-select for products
st.subheader("Price History Comparison")
st.markdown("Compare price trends for specific products across the selected competitors.")

# Get all products that have prices from the selected competitors
all_products = df[df['competitor_name'].isin(selected_competitors)]['product_sku'].unique()
product_names = {sku: name for sku, name in zip(df['product_sku'], df['product_name'])}

# Create selectable options with product name
product_options = [f"{sku} - {product_names.get(sku, sku)}" for sku in all_products]

# Allow multiple product selection
selected_product_options = st.multiselect(
    "Select Products to Compare", 
    options=product_options,
    default=[product_options[0]] if product_options else []
)

if selected_product_options:
    # Extract product SKUs from the options
    selected_products = [option.split(" - ")[0] for option in selected_product_options]
    
    # Filter data for selected products and competitors
    product_df = df[
        (df['product_sku'].isin(selected_products)) & 
        (df['competitor_name'].isin(selected_competitors))
    ].copy()
    
    # Convert timestamp to datetime if needed
    if 'data_timestamp' in product_df.columns:
        product_df['data_timestamp'] = pd.to_datetime(product_df['data_timestamp'])
    
    # Create price history chart
    fig = px.line(
        product_df,
        x='data_timestamp',
        y='competitor_price',
        color='competitor_name',
        line_dash='product_name',  # Use line style to distinguish products
        labels={
            'competitor_price': 'Price ($)',
            'data_timestamp': 'Date',
            'competitor_name': 'Competitor',
            'product_name': 'Product'
        },
        title='Price History Comparison',
        markers=True
    )
    
    # Add our prices as reference lines
    for product in selected_products:
        our_df = df[df['product_sku'] == product].copy()
        if not our_df.empty:
            product_name = our_df['product_name'].iloc[0]
            our_price = our_df['our_price'].iloc[0]  # Using the first occurrence
            
            fig.add_hline(
                y=our_price,
                line_dash="dash",
                line_color="#0068C9",
                annotation_text=f"Our Price: {product_name}",
                annotation_position="bottom right"
            )
    
    # Update layout for better readability
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Option to show data in tabular format
    if st.checkbox("Show Raw Price Data"):
        st.dataframe(
            product_df[['product_name', 'competitor_name', 'competitor_price', 'our_price', 'data_timestamp']],
            use_container_width=True,
            column_config={
                'product_name': st.column_config.TextColumn("Product"),
                'competitor_name': st.column_config.TextColumn("Competitor"),
                'competitor_price': st.column_config.NumberColumn("Competitor Price", format="$%.2f"),
                'our_price': st.column_config.NumberColumn("Our Price", format="$%.2f"),
                'data_timestamp': st.column_config.DatetimeColumn("Date", format="DD/MM/YYYY")
            }
        )
else:
    st.info("Please select at least one product to view price history.")