"""
Simplified eBay API - focused on core price intelligence needs
Returns basic price data using eBay item ID as identifier
"""
from fastapi import APIRouter, HTTPException
import random
from datetime import datetime
from models import SimplePriceResponse

router = APIRouter()

# eBay items with item IDs
EBAY_PRODUCTS = {
    "334567890123": {"title": "Apple iPhone 15 Pro 128GB Unlocked", "base_price": 949.99},
    "334567890124": {"title": "Apple MacBook Air M2 13-inch", "base_price": 1049.99},
    "334567890125": {"title": "Sony PlayStation 5 Console New", "base_price": 479.99},
    "334567890126": {"title": "Apple AirPods Pro 2nd Generation", "base_price": 229.99},
    "334567890127": {"title": "Amazon Echo Dot 5th Gen", "base_price": 39.99}
}

@router.get("/price/{item_id}", response_model=SimplePriceResponse)
async def get_ebay_price(item_id: str):
    """Get current eBay price by item ID"""
    if item_id not in EBAY_PRODUCTS:
        raise HTTPException(status_code=404, detail=f"eBay item {item_id} not found")
    
    product = EBAY_PRODUCTS[item_id]
    
    # eBay typically has lower prices (5-15% below MSRP)
    price_variation = random.uniform(0.85, 0.95)
    current_price = round(product["base_price"] * price_variation, 2)
    
    return SimplePriceResponse(
        sku=item_id,
        title=product["title"],
        price_amount=current_price,
        in_stock=random.choice([True, True, False]),  # 67% available (auctions end)
        url=f"https://ebay.com/itm/{item_id}",
        competitor="ebay",
        timestamp=datetime.now()
    )

@router.get("/products")
async def list_ebay_products():
    """List all available eBay item IDs"""
    return {"item_ids": list(EBAY_PRODUCTS.keys())}
