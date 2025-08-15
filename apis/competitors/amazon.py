"""
Simplified Amazon API - focused on core price intelligence needs
Returns basic price data using ASIN as identifier
"""
from fastapi import APIRouter, HTTPException
import random
from datetime import datetime
from models import SimplePriceResponse

router = APIRouter()

# Product catalog with ASIN mapping
AMAZON_PRODUCTS = {
    "B09B8RJWXX": {"title": "Apple iPhone 15 Pro 128GB", "base_price": 999.99},
    "B08C1W5N87": {"title": "Apple MacBook Air M2 13-inch", "base_price": 1099.99},
    "B0BZKCBWQY": {"title": "Sony PlayStation 5 Console", "base_price": 499.99},
    "B09B8RJWXY": {"title": "Apple AirPods Pro 2nd Gen", "base_price": 249.99},
    "B08N5WRWNW": {"title": "Echo Dot 5th Gen Smart Speaker", "base_price": 49.99}
}

@router.get("/price/{asin}", response_model=SimplePriceResponse)
async def get_amazon_price(asin: str):
    """Get current Amazon price by ASIN"""
    if asin not in AMAZON_PRODUCTS:
        raise HTTPException(status_code=404, detail=f"ASIN {asin} not found")
    
    product = AMAZON_PRODUCTS[asin]
    
    # Simple price variation (±10%)
    price_variation = random.uniform(0.9, 1.1)
    current_price = round(product["base_price"] * price_variation, 2)
    
    return SimplePriceResponse(
        sku=asin,
        title=product["title"],
        price_amount=current_price,
        in_stock=random.choice([True, True, True, False]),  # 75% in stock
        url=f"https://amazon.com/dp/{asin}",
        competitor="amazon",
        timestamp=datetime.now()
    )

@router.get("/products")
async def list_amazon_products():
    """List all available ASINs"""
    return {"asins": list(AMAZON_PRODUCTS.keys())}
