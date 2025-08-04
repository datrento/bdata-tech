import pandas as pd
import numpy as np

# Set seeds for reproducibility
NP_SEED = 42
np.random.seed(NP_SEED)

# File paths
sales_file = 'competitors_modified.csv'
ecommerce_file = 'customers.csv'
output_file = 'customers_modified.csv'

chunk_size = 100000  # Adjust based on your memory

# Step 1: Load product mapping from sales data
print("Loading product mapping from sales data...")
product_mapping = {}

# Read sales data to create product_id to category and price mapping
for chunk in pd.read_csv(sales_file, chunksize=chunk_size):
    # Get unique product_ids in this chunk that aren't already mapped
    unique_products = chunk['product_id'].unique()
    new_products = [
        pid for pid in unique_products if pid not in product_mapping]

    # Process only new, unique products
    for product_id in new_products:
        # Get the first occurrence of this product in the chunk
        product_row = chunk[chunk['product_id'] == product_id].iloc[0]
        category = product_row.get('category', 'Unknown')
        price = product_row.get('price', 0.0)
        product_mapping[product_id] = {
            'category': category,
            'price': float(str(price).replace(',', '')) if pd.notna(price) else 0.0
        }

print(f"Loaded {len(product_mapping)} unique products from sales data")

# Get list of all product_ids for random selection
product_ids = list(product_mapping.keys())

# Step 2: Process ecommerce data in chunks
print("Processing ecommerce data...")
first_chunk = True

# Define columns to keep (excluding Purchase Date, Returns, and Churn)
columns_to_keep = ['Customer ID', 'Product Category', 'Product Price', 'Quantity',
                   'Total Purchase Amount', 'Payment Method', 'Customer Age',
                   'Customer Name', 'Age', 'Gender']

for chunk_idx, chunk in enumerate(pd.read_csv(ecommerce_file, usecols=columns_to_keep, chunksize=chunk_size)):
    print(f"Processing chunk {chunk_idx + 1}...")

    # Add random product_id to each row
    chunk['product_id'] = np.random.choice(product_ids, size=len(chunk))

    # Update Product Category and Product Price based on sales data
    chunk['Product Category'] = chunk['product_id'].map(
        lambda pid: product_mapping[pid]['category']
    )
    chunk['Product Price'] = chunk['product_id'].map(
        lambda pid: product_mapping[pid]['price']
    )

    # Recalculate Total Purchase Amount based on new price and quantity (keep 2 decimal places)
    chunk['Total Purchase Amount'] = (
        chunk['Product Price'] * chunk['Quantity']).apply(lambda x: f"{x:.2f}")

    # Write to output file
    if first_chunk:
        chunk.to_csv(output_file, index=False, mode='w')
        first_chunk = False
    else:
        chunk.to_csv(output_file, index=False, header=False, mode='a')

print(f"Enriched ecommerce data saved to {output_file}")
print("Process completed successfully!")
