"""
Comprehensive Price Aggregator - Big Data Price Intelligence System
Core purpose: Aggregate data from e-commerce platforms, external price aggregators, and user behavior
Provides market intelligence for ML-driven dynamic pricing decisions
"""
from fastapi import APIRouter, HTTPException
import httpx
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from models import SimplePriceResponse
from catalog import PRODUCT_CATALOG

# Import competitor modules directly for better performance
from competitors.amazon import get_amazon_price
from competitors.ebay import get_ebay_price
from competitors.bestbuy import get_bestbuy_price

router = APIRouter()

# External price aggregators (as per project description)
EXTERNAL_PRICE_AGGREGATORS = [
    "PriceGrabber", "Shopping.com", "Google Shopping"
]

async def fetch_competitor_price(competitor: str, sku: str) -> Optional[SimplePriceResponse]:
    """Fetch price from direct e-commerce competitor - now using direct function calls"""
    try:
        if competitor == "amazon":
            return await get_amazon_price(sku)
        elif competitor == "ebay":
            return await get_ebay_price(sku)
        elif competitor == "bestbuy":
            return await get_bestbuy_price(sku)
        else:
            print(f"Unknown competitor: {competitor}")
            return None
    except HTTPException as e:
        # Product not found in competitor's catalog
        print(f"Product {sku} not found in {competitor}: {e.detail}")
        return None
    except Exception as e:
        print(f"Error fetching {competitor} price for {sku}: {e}")
        return None

async def simulate_external_aggregator_data(aggregator: str, product_sku: str) -> Dict:
    """
    Simulate external price aggregator data - RAW DATA ONLY
    In production: Replace with actual web scraping or API calls to PriceGrabber, Shopping.com, etc.
    """
    base_prices = {
        "IPHONE-15-PRO-128": 999.99,
        "MACBOOK-AIR-M2-13": 1199.99,
        "PS5-CONSOLE": 499.99
    }
    
    base_price = base_prices.get(product_sku, 500.00)
    
    # Different aggregators have different store coverage (raw data)
    store_ranges = {
        "PriceGrabber": (15, 30),
        "Shopping.com": (20, 40), 
        "Google Shopping": (50, 100)
    }
    
    stores_range = store_ranges.get(aggregator, (10, 30))
    stores_tracked = random.randint(*stores_range)
    
    # Simulate realistic price ranges (what aggregators actually report)
    price_variance = random.uniform(0.10, 0.25)  # 10-25% variance is realistic
    min_price = round(base_price * (1 - price_variance), 2)
    max_price = round(base_price * (1 + price_variance), 2)
    avg_price = round(random.uniform(min_price, max_price), 2)
    
    # Return RAW DATA ONLY 
    return {
        "aggregator": aggregator,
        "available": random.choice([True] * 8 + [False] * 2),  # 80% availability
        "avg_price": avg_price,
        "price_range": {
            "min": min_price,
            "max": max_price
        },
        "stores_tracked": stores_tracked,
        "url": f"https://{aggregator.lower().replace(' ', '-')}.com/product/{product_sku}",
        "last_updated": (datetime.now() - timedelta(minutes=random.randint(5, 60))).isoformat()
    }

@router.get("/data/{universal_sku}")
async def get_market_data_sources(universal_sku: str):
    """
    Data Collection Only: Fetch raw data from all 3 sources
    1. E-commerce platforms (Amazon, eBay, Best Buy)
    2. External price aggregators (PriceGrabber, Shopping.com, etc.)
    3. User behavior signals (page views, purchases, etc.)
    
    Returns: Raw data only - no analysis or intelligence calculations
    """
    if universal_sku not in PRODUCT_CATALOG:
        raise HTTPException(status_code=404, detail=f"Product {universal_sku} not found")
    
    product = PRODUCT_CATALOG[universal_sku]
    
    
    # 1. FETCH COMPETITOR DATA (real-time)
    competitors = {}
    for competitor, sku in [("amazon", product.amazon_asin), ("ebay", product.ebay_item_id), ("bestbuy", product.bestbuy_sku)]:
        if sku and (price_data := await fetch_competitor_price(competitor, sku)):

            # Keep simulated event time close to collection time to ease event-time analysis
            collection_time = datetime.now().astimezone()
            jitter_seconds = random.randint(0, 30)  # at most 30s earlier than collection
            simulated_time = collection_time - timedelta(seconds=jitter_seconds)
            competitors[competitor] = {
                "price": price_data.price_amount,
                "in_stock": price_data.in_stock,
                "sku": price_data.sku,
                "url": price_data.url,
                "data_timestamp": simulated_time.isoformat(),  # When the price was changed
            }

    # 2. FETCH EXTERNAL AGGREGATORS DATA (simulated)
    aggregators = {}
    for aggregator in EXTERNAL_PRICE_AGGREGATORS:
        if (data := await simulate_external_aggregator_data(aggregator, universal_sku)) and data["available"]:
            aggregators[aggregator] = {
                **data,
                "data_last_updated": datetime.now().astimezone().isoformat()  # mark with tz
            }

    
    # Return RAW DATA ONLY - no calculations or analysis
    return {
        "product": {
            "sku": universal_sku,
            "title": product.title,
            "category": product.category
        },
        "data_sources": {
            "competitors": competitors,
            "aggregators": aggregators,
        },
        "collection_timestamp": datetime.now().astimezone().isoformat(), # tz-aware collection time
        "collection_status": "external_market_data_only"
    }

@router.get("/products")
async def list_products():
    """List all products available for price intelligence"""
    return list(PRODUCT_CATALOG.keys())
