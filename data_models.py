"""
Data Models for E-commerce Price Intelligence System
"""

from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import uuid


class PlatformType(Enum):
    AMAZON = "amazon"
    EBAY = "ebay"
    WALMART = "walmart"
    TARGET = "target"
    BEST_BUY = "best_buy"


class ProductCategory(Enum):
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    HOME_GARDEN = "home_garden"
    SPORTS = "sports"
    BOOKS = "books"
    AUTOMOTIVE = "automotive"
    HEALTH = "health"
    TOYS = "toys"


@dataclass
class CompetitorPrice:
    """Represents a competitor's price for a product"""
    platform: PlatformType
    price: float
    currency: str = "USD"
    timestamp: datetime = None
    availability: bool = True
    shipping_cost: float = 0.0
    seller_rating: Optional[float] = None
    seller_name: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class Product:
    """Represents a product in the system"""
    product_id: str
    name: str
    category: ProductCategory
    brand: str
    sku: str
    current_price: float
    currency: str = "USD"
    inventory_level: int = 0
    min_price: float = 0.0
    max_price: float = float('inf')
    cost_price: float = 0.0
    created_at: datetime = None
    updated_at: datetime = None
    competitor_prices: List[CompetitorPrice] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
        if self.competitor_prices is None:
            self.competitor_prices = []
        if self.tags is None:
            self.tags = []


@dataclass
class PriceHistory:
    """Historical price data for a product"""
    product_id: str
    price: float
    timestamp: datetime
    reason: str = "manual"
    competitor_avg_price: Optional[float] = None
    demand_score: Optional[float] = None
    inventory_level: Optional[int] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class UserBehavior:
    """User behavior data for demand analysis"""
    user_id: str
    product_id: str
    action: str  # view, add_to_cart, purchase, search
    timestamp: datetime
    session_id: str
    platform: PlatformType
    price_at_time: float
    competitor_price_at_time: Optional[float] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class PricingDecision:
    """Record of pricing decisions made by the system"""
    product_id: str
    old_price: float
    new_price: float
    timestamp: datetime
    reason: str
    confidence_score: float
    factors: Dict[str, Any]
    algorithm_version: str
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class MarketSignal:
    """Market signals that influence pricing decisions"""
    signal_id: str
    signal_type: str  # competitor_price_drop, demand_surge, inventory_low, etc.
    product_id: str
    severity: float  # 0.0 to 1.0
    timestamp: datetime
    description: str
    data: Dict[str, Any]
    
    def __post_init__(self):
        if self.signal_id is None:
            self.signal_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class DemandMetrics:
    """Demand analysis metrics for a product"""
    product_id: str
    timestamp: datetime
    view_count: int
    add_to_cart_count: int
    purchase_count: int
    conversion_rate: float
    search_volume: int
    competitor_interest: float
    seasonality_factor: float
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class MongoDBCollections:
    """MongoDB collection names and schemas"""
    
    PRODUCTS = "products"
    COMPETITOR_PRICES = "competitor_prices"
    PRICE_HISTORY = "price_history"
    USER_BEHAVIOR = "user_behavior"
    PRICING_DECISIONS = "pricing_decisions"
    MARKET_SIGNALS = "market_signals"
    DEMAND_METRICS = "demand_metrics"
    
    # Index definitions for optimal query performance
    INDEXES = {
        PRODUCTS: [
            [("product_id", 1)],
            [("category", 1)],
            [("brand", 1)],
            [("updated_at", -1)]
        ],
        COMPETITOR_PRICES: [
            [("product_id", 1), ("timestamp", -1)],
            [("platform", 1), ("timestamp", -1)],
            [("price", 1)]
        ],
        PRICE_HISTORY: [
            [("product_id", 1), ("timestamp", -1)],
            [("timestamp", -1)]
        ],
        USER_BEHAVIOR: [
            [("product_id", 1), ("timestamp", -1)],
            [("user_id", 1), ("timestamp", -1)],
            [("action", 1), ("timestamp", -1)]
        ],
        PRICING_DECISIONS: [
            [("product_id", 1), ("timestamp", -1)],
            [("timestamp", -1)]
        ],
        MARKET_SIGNALS: [
            [("product_id", 1), ("timestamp", -1)],
            [("signal_type", 1), ("severity", -1)]
        ],
        DEMAND_METRICS: [
            [("product_id", 1), ("timestamp", -1)],
            [("timestamp", -1)]
        ]
    }


def create_product_document(product: Product) -> Dict:
    """Convert Product object to MongoDB document"""
    doc = asdict(product)
    doc['competitor_prices'] = [asdict(cp) for cp in product.competitor_prices]
    return doc


def create_price_history_document(price_history: PriceHistory) -> Dict:
    """Convert PriceHistory object to MongoDB document"""
    return asdict(price_history)


def create_user_behavior_document(user_behavior: UserBehavior) -> Dict:
    """Convert UserBehavior object to MongoDB document"""
    return asdict(user_behavior)


def create_pricing_decision_document(pricing_decision: PricingDecision) -> Dict:
    """Convert PricingDecision object to MongoDB document"""
    return asdict(pricing_decision)


def create_market_signal_document(market_signal: MarketSignal) -> Dict:
    """Convert MarketSignal object to MongoDB document"""
    return asdict(market_signal)


def create_demand_metrics_document(demand_metrics: DemandMetrics) -> Dict:
    """Convert DemandMetrics object to MongoDB document"""
    return asdict(demand_metrics) 