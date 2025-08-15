"""
Simplified models for price intelligence - focused on core business needs
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class SimplePriceResponse(BaseModel):
    """Core price data that matters for price intelligence"""
    sku: str  # Universal product identifier (ASIN, SKU, UPC, etc.)
    title: str
    price_amount: float
    in_stock: bool
    url: Optional[str] = None
    competitor: str
    timestamp: datetime
    
class ProductMapping(BaseModel):
    """Maps a universal product to competitor-specific identifiers"""
    universal_sku: str
    amazon_asin: Optional[str] = None
    ebay_item_id: Optional[str] = None  
    bestbuy_sku: Optional[str] = None
    title: str
    category: str
