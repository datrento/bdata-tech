"""
Big Data Price Intelligence API - Data Collection System
Clean endpoints for collecting market data from multiple sources for Kafka streaming
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from competitors.amazon import router as amazon_router
from competitors.ebay import router as ebay_router  
from competitors.bestbuy import router as bestbuy_router
from aggregators.price_aggregator import router as aggregator_router
from aggregators.user_behavior import router as user_behavior_router

app = FastAPI(
    title="Price Intelligence Data Collection API",
    description="Collect market data from competitors, aggregators, and user behavior for big data analysis",
    version="2.0.0"
)

# CORS for web frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include simplified routers
app.include_router(amazon_router, prefix="/api/amazon", tags=["Amazon"])
app.include_router(ebay_router, prefix="/api/ebay", tags=["eBay"])
app.include_router(bestbuy_router, prefix="/api/bestbuy", tags=["Best Buy"])
app.include_router(aggregator_router, prefix="/api/aggregator", tags=["Data Collection"])
app.include_router(user_behavior_router, prefix="/api/user-behavior", tags=["User Behavior"])

@app.get("/")
async def root():
    return {
        "message": "Price Intelligence Data Collection API",
        "purpose": "Collect market data from multiple sources for big data analysis",
        "endpoints": {
            "collect_market_data": "/api/aggregator/data/{universal_sku}",
            "collect_user_behavior": "/api/user-behavior/data/{universal_sku}",
            "list_products": "/api/aggregator/products",
            "individual_competitors": {
                "amazon": "/api/amazon/price/{asin}",
                "ebay": "/api/ebay/price/{item_id}",
                "bestbuy": "/api/bestbuy/price/{sku}"
            }
        },
        "data_sources": [
            "E-commerce competitors (Amazon, eBay, Best Buy)",
            "External price aggregators (PriceGrabber, Shopping.com, etc.)",
            "User behavior signals (page views, purchases, etc.)"
        ],
        "sample_products": [
            "IPHONE-15-PRO-128",
            "MACBOOK-AIR-M2-13", 
            "PS5-CONSOLE"
        ],
        "usage": "curl http://localhost:8000/api/aggregator/data/IPHONE-15-PRO-128"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "version": "2.0.0",
        "purpose": "data_collection_for_big_data_analysis",
        "kafka_ready": True
    }
