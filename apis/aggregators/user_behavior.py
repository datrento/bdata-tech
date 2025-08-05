
"""
User Behavior Analytics - Internal Platform Data Collection
Collects user interaction data from our hypothetical e-commerce platform
"""
from fastapi import APIRouter, HTTPException
import random
from datetime import datetime
from typing import Dict
from catalog import PRODUCT_CATALOG

router = APIRouter()

async def simulate_user_behavior_signals(product_sku: str) -> Dict:
    """
    Simulate user behavior data from your e-commerce platform
    In production: Replace with actual user analytics data
    """
    # Simulate realistic user behavior patterns
    base_demand = {
        "IPHONE-15-PRO-128": 1000,
        "MACBOOK-AIR-M2-13": 600,
        "PS5-CONSOLE": 800
    }
    
    daily_base = base_demand.get(product_sku, 400)
    
    # Add realistic variations
    page_views = daily_base + random.randint(-100, 200)
    searches = int(page_views * random.uniform(0.3, 0.7))
    cart_adds = int(page_views * random.uniform(0.05, 0.15))
    purchases = int(cart_adds * random.uniform(0.15, 0.35))
    price_comparisons = int(page_views * random.uniform(0.1, 0.3))
    
    # Return RAW DATA ONLY - no calculated indicators
    # Conversion rates, abandonment rates, etc. should be calculated downstream
    return {
        "page_views": page_views,
        "searches": searches,
        "cart_additions": cart_adds,
        "purchases": purchases,
        "price_comparisons": price_comparisons,
        "timestamp": datetime.now().isoformat()
    }

@router.get("/user-behavior/{universal_sku}")
async def get_user_behavior_data(universal_sku: str):
    """
    Internal Platform User Behavior Data: Fetch raw user behavior from our hypothetical platform
    Analytics data from our own e-commerce platform (page views, purchases, etc.)
    
    Returns: Raw user behavior data only - no external market data
    """
    if universal_sku not in PRODUCT_CATALOG:
        raise HTTPException(status_code=404, detail=f"Product {universal_sku} not found")

    product = PRODUCT_CATALOG[universal_sku]

    # Simulate user behavior signals for the product
    user_behavior = await simulate_user_behavior_signals(universal_sku)

    return {
        "product": {
            "sku": universal_sku,
            "title": product.title,
            "category": product.category
        },
        "user_behavior": user_behavior,
        "data_source": "internal_platform",
    }


@router.get("/products")
async def list_tracked_products():
    """List all products being tracked for user behavior analytics"""
    return {
        "tracked_products": list(PRODUCT_CATALOG.keys()),
        "total_count": len(PRODUCT_CATALOG),
        "data_source": "internal_platform"
    }