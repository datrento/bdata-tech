import pandas as pd
import numpy as np
from faker import Faker

chunk_size = 100000  # Adjust based on your memory
input_file = 'sales_small.csv'
output_file = 'sales_modified.csv'


# Set seeds for reproducibility
FAKER_SEED = 0
NP_SEED = 0
fake = Faker()
Faker.seed(FAKER_SEED)
np.random.seed(NP_SEED)

# List of common objects for realistic product categories
object_types = [
    "Headphones", "Backpack", "Lamp", "Watch", "Speaker", "Bottle", "Chair", "Table", "Camera", "Shoes",
    "Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Printer", "Sofa", "Desk", "Jacket"
]

# Step 1: Build product_id to product_name, brand, and category mapping
product_map = {}

# First pass: collect all unique product_ids
for chunk in pd.read_csv(input_file, usecols=['product_id'], chunksize=chunk_size):
    for pid in chunk['product_id'].unique():
        if pid not in product_map:
            brand = fake.color_name()
            category = np.random.choice(object_types)
            product_name = f"{brand} {category}"
            product_map[pid] = {'product_name': product_name,
                                'brand': brand, 'category': category}

# randomly change the price between -20% and +20%


def randomize_price(price):
    if pd.isna(price):
        return price
    change = np.random.uniform(-0.2, 0.2)  # Random float between -0.2 and 0.2
    new_price = price * (1 + change)
    return f"{new_price:.2f}"  # always keep 2 decimal digits


reader = pd.read_csv(input_file, chunksize=chunk_size)
first_chunk = True

for chunk in reader:
    # Add product_name, brand, and category based on product_id
    chunk['product_name'] = chunk['product_id'].map(
        lambda pid: product_map[pid]['product_name'])
    chunk['brand'] = chunk['product_id'].map(
        lambda pid: product_map[pid]['brand'])
    chunk['category'] = chunk['product_id'].map(
        lambda pid: product_map[pid]['category'])
    chunk['price'] = chunk['price'].apply(randomize_price)
    if first_chunk:
        chunk.to_csv(output_file, index=False, mode='w')
        first_chunk = False
    else:
        chunk.to_csv(output_file, index=False, header=False, mode='a')
