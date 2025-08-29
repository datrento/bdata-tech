"""
Shared Product Catalog - Central product mapping for all modules
"""
from models import ProductMapping

# Essential product catalog - shared across all modules
PRODUCT_CATALOG = {
    "IPHONE-15-PRO-128": ProductMapping(
        universal_sku="IPHONE-15-PRO-128",
        amazon_asin="B09B8RJWXX",
        ebay_item_id="334567890123", 
        bestbuy_sku="6418599",
        title="Apple iPhone 15 Pro 128GB",
        category="Smartphones"
    ),
    "MACBOOK-AIR-M2-13": ProductMapping(
        universal_sku="MACBOOK-AIR-M2-13",
        amazon_asin="B08C1W5N87",
        ebay_item_id="334567890124",
        bestbuy_sku="6509650", 
        title="Apple MacBook Air M2 13-inch",
        category="Laptops"
    ),
    "PS5-CONSOLE": ProductMapping(
        universal_sku="PS5-CONSOLE",
        amazon_asin="B0BZKCBWQY",
        ebay_item_id="334567890125",
        bestbuy_sku="6426149",
        title="Sony PlayStation 5 Console", 
        category="Gaming"
    )
}