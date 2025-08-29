"""
Simplified Best Buy API - focused on core price intelligence needs
Returns basic price data using Best Buy SKU as identifier
"""
from fastapi import APIRouter, HTTPException
import random
from datetime import datetime
from models import SimplePriceResponse

router = APIRouter()

# Best Buy products with SKUs
BESTBUY_PRODUCTS = {
    "6418599": {"title": "Apple iPhone 15 Pro 128GB", "base_price": 999.99},
    "6509650": {"title": "Apple MacBook Air M2 13-inch", "base_price": 1099.99},
    "6426149": {"title": "Sony PlayStation 5 Console", "base_price": 499.99},
    "6502372": {"title": "Apple AirPods Pro 2nd Gen", "base_price": 249.99},
    "6418234": {"title": "Amazon Echo Dot 5th Gen", "base_price": 49.99}
}

@router.get("/price/{sku}", response_model=SimplePriceResponse)
async def get_bestbuy_price(sku: str):
    """Get current Best Buy price by SKU"""
    if sku not in BESTBUY_PRODUCTS:
        raise HTTPException(status_code=404, detail=f"Best Buy SKU {sku} not found")
    
    product = BESTBUY_PRODUCTS[sku]
    
    # Best Buy pricing (usually at MSRP, sometimes with member discounts)
    price_variation = random.uniform(0.95, 1.05)
    current_price = round(product["base_price"] * price_variation, 2)
    
    return SimplePriceResponse(
        sku=sku,
        title=product["title"],
        price_amount=current_price,
        in_stock=random.choice([True, True, True, True, False]),  # 80% in stock
        url=f"https://bestbuy.com/product/{sku}",
        competitor="bestbuy",
        timestamp=datetime.now()
    )

@router.get("/products")
async def list_bestbuy_products():
    """List all available Best Buy SKUs"""
    return {"skus": list(BESTBUY_PRODUCTS.keys())}
