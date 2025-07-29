"""
Database Layer for E-commerce Price Intelligence System
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import yaml
import os
from contextlib import contextmanager

from .data_models import (
    Product, CompetitorPrice, PriceHistory, UserBehavior, 
    PricingDecision, MarketSignal, DemandMetrics,
    MongoDBCollections, create_product_document, create_price_history_document,
    create_user_behavior_document, create_pricing_decision_document,
    create_market_signal_document, create_demand_metrics_document
)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.mongo_client = None
        self.mongo_db = None
        self.pg_pool = None
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def connect_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_config = self.config['database']['mongodb']
            self.mongo_client = MongoClient(
                mongo_config['uri'],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test connection
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[mongo_config['database']]
            
            # Create indexes
            self._create_mongodb_indexes()
            
            self.logger.info("MongoDB connection established successfully")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def connect_postgresql(self):
        """Establish PostgreSQL connection pool"""
        try:
            pg_config = self.config['database']['postgresql']
            self.pg_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=pg_config['host'],
                port=pg_config['port'],
                database=pg_config['database'],
                user=pg_config['user'],
                password=pg_config['password']
            )
            
            # Test connection
            with self.pg_pool.getconn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            
            self.logger.info("PostgreSQL connection pool established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def _create_mongodb_indexes(self):
        """Create MongoDB indexes for optimal performance"""
        for collection_name, indexes in MongoDBCollections.INDEXES.items():
            collection = self.mongo_db[collection_name]
            for index in indexes:
                try:
                    collection.create_index(index)
                except Exception as e:
                    self.logger.warning(f"Failed to create index for {collection_name}: {e}")
    
    @contextmanager
    def get_postgres_connection(self):
        """Context manager for PostgreSQL connections"""
        conn = None
        try:
            conn = self.pg_pool.getconn()
            yield conn
        finally:
            if conn:
                self.pg_pool.putconn(conn)
    
    def close_connections(self):
        """Close all database connections"""
        if self.mongo_client:
            self.mongo_client.close()
        if self.pg_pool:
            self.pg_pool.closeall()
        self.logger.info("Database connections closed")


class MongoDBRepository:
    """MongoDB data access layer"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager.mongo_db
        self.logger = logging.getLogger(__name__)
    
    def insert_product(self, product: Product) -> bool:
        """Insert a new product"""
        try:
            collection = self.db[MongoDBCollections.PRODUCTS]
            doc = create_product_document(product)
            result = collection.insert_one(doc)
            self.logger.info(f"Product inserted with ID: {result.inserted_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert product: {e}")
            return False
    
    def update_product(self, product_id: str, updates: Dict) -> bool:
        """Update product information"""
        try:
            collection = self.db[MongoDBCollections.PRODUCTS]
            updates['updated_at'] = datetime.utcnow()
            result = collection.update_one(
                {'product_id': product_id},
                {'$set': updates}
            )
            return result.modified_count > 0
        except Exception as e:
            self.logger.error(f"Failed to update product {product_id}: {e}")
            return False
    
    def get_product(self, product_id: str) -> Optional[Product]:
        """Get product by ID"""
        try:
            collection = self.db[MongoDBCollections.PRODUCTS]
            doc = collection.find_one({'product_id': product_id})
            if doc:
                return self._document_to_product(doc)
            return None
        except Exception as e:
            self.logger.error(f"Failed to get product {product_id}: {e}")
            return None
    
    def get_products_by_category(self, category: str, limit: int = 100) -> List[Product]:
        """Get products by category"""
        try:
            collection = self.db[MongoDBCollections.PRODUCTS]
            cursor = collection.find({'category': category}).limit(limit)
            return [self._document_to_product(doc) for doc in cursor]
        except Exception as e:
            self.logger.error(f"Failed to get products by category {category}: {e}")
            return []
    
    def insert_competitor_price(self, competitor_price: CompetitorPrice, product_id: str) -> bool:
        """Insert competitor price data"""
        try:
            collection = self.db[MongoDBCollections.COMPETITOR_PRICES]
            doc = create_price_history_document(competitor_price)
            doc['product_id'] = product_id
            result = collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert competitor price: {e}")
            return False
    
    def get_competitor_prices(self, product_id: str, hours: int = 24) -> List[CompetitorPrice]:
        """Get recent competitor prices for a product"""
        try:
            collection = self.db[MongoDBCollections.COMPETITOR_PRICES]
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            cursor = collection.find({
                'product_id': product_id,
                'timestamp': {'$gte': cutoff_time}
            }).sort('timestamp', -1)
            
            return [self._document_to_competitor_price(doc) for doc in cursor]
        except Exception as e:
            self.logger.error(f"Failed to get competitor prices for {product_id}: {e}")
            return []
    
    def insert_price_history(self, price_history: PriceHistory) -> bool:
        """Insert price history record"""
        try:
            collection = self.db[MongoDBCollections.PRICE_HISTORY]
            doc = create_price_history_document(price_history)
            result = collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert price history: {e}")
            return False
    
    def get_price_history(self, product_id: str, days: int = 30) -> List[PriceHistory]:
        """Get price history for a product"""
        try:
            collection = self.db[MongoDBCollections.PRICE_HISTORY]
            cutoff_time = datetime.utcnow() - timedelta(days=days)
            cursor = collection.find({
                'product_id': product_id,
                'timestamp': {'$gte': cutoff_time}
            }).sort('timestamp', -1)
            
            return [self._document_to_price_history(doc) for doc in cursor]
        except Exception as e:
            self.logger.error(f"Failed to get price history for {product_id}: {e}")
            return []
    
    def insert_user_behavior(self, user_behavior: UserBehavior) -> bool:
        """Insert user behavior data"""
        try:
            collection = self.db[MongoDBCollections.USER_BEHAVIOR]
            doc = create_user_behavior_document(user_behavior)
            result = collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert user behavior: {e}")
            return False
    
    def insert_pricing_decision(self, pricing_decision: PricingDecision) -> bool:
        """Insert pricing decision record"""
        try:
            collection = self.db[MongoDBCollections.PRICING_DECISIONS]
            doc = create_pricing_decision_document(pricing_decision)
            result = collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert pricing decision: {e}")
            return False
    
    def insert_market_signal(self, market_signal: MarketSignal) -> bool:
        """Insert market signal"""
        try:
            collection = self.db[MongoDBCollections.MARKET_SIGNALS]
            doc = create_market_signal_document(market_signal)
            result = collection.insert_one(doc)
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert market signal: {e}")
            return False
    
    def get_recent_market_signals(self, hours: int = 24) -> List[MarketSignal]:
        """Get recent market signals"""
        try:
            collection = self.db[MongoDBCollections.MARKET_SIGNALS]
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            cursor = collection.find({
                'timestamp': {'$gte': cutoff_time}
            }).sort('timestamp', -1)
            
            return [self._document_to_market_signal(doc) for doc in cursor]
        except Exception as e:
            self.logger.error(f"Failed to get recent market signals: {e}")
            return []
    
    def _document_to_product(self, doc: Dict) -> Product:
        """Convert MongoDB document to Product object"""
        from .data_models import PlatformType, ProductCategory
        
        # Convert competitor prices
        competitor_prices = []
        for cp_doc in doc.get('competitor_prices', []):
            cp = CompetitorPrice(
                platform=PlatformType(cp_doc['platform']),
                price=cp_doc['price'],
                currency=cp_doc.get('currency', 'USD'),
                timestamp=cp_doc.get('timestamp'),
                availability=cp_doc.get('availability', True),
                shipping_cost=cp_doc.get('shipping_cost', 0.0),
                seller_rating=cp_doc.get('seller_rating'),
                seller_name=cp_doc.get('seller_name')
            )
            competitor_prices.append(cp)
        
        return Product(
            product_id=doc['product_id'],
            name=doc['name'],
            category=ProductCategory(doc['category']),
            brand=doc['brand'],
            sku=doc['sku'],
            current_price=doc['current_price'],
            currency=doc.get('currency', 'USD'),
            inventory_level=doc.get('inventory_level', 0),
            min_price=doc.get('min_price', 0.0),
            max_price=doc.get('max_price', float('inf')),
            cost_price=doc.get('cost_price', 0.0),
            created_at=doc.get('created_at'),
            updated_at=doc.get('updated_at'),
            competitor_prices=competitor_prices,
            tags=doc.get('tags', [])
        )
    
    def _document_to_competitor_price(self, doc: Dict) -> CompetitorPrice:
        """Convert MongoDB document to CompetitorPrice object"""
        from .data_models import PlatformType
        
        return CompetitorPrice(
            platform=PlatformType(doc['platform']),
            price=doc['price'],
            currency=doc.get('currency', 'USD'),
            timestamp=doc.get('timestamp'),
            availability=doc.get('availability', True),
            shipping_cost=doc.get('shipping_cost', 0.0),
            seller_rating=doc.get('seller_rating'),
            seller_name=doc.get('seller_name')
        )
    
    def _document_to_price_history(self, doc: Dict) -> PriceHistory:
        """Convert MongoDB document to PriceHistory object"""
        return PriceHistory(
            product_id=doc['product_id'],
            price=doc['price'],
            timestamp=doc['timestamp'],
            reason=doc.get('reason', 'manual'),
            competitor_avg_price=doc.get('competitor_avg_price'),
            demand_score=doc.get('demand_score'),
            inventory_level=doc.get('inventory_level')
        )
    
    def _document_to_market_signal(self, doc: Dict) -> MarketSignal:
        """Convert MongoDB document to MarketSignal object"""
        return MarketSignal(
            signal_id=doc['signal_id'],
            signal_type=doc['signal_type'],
            product_id=doc['product_id'],
            severity=doc['severity'],
            timestamp=doc['timestamp'],
            description=doc['description'],
            data=doc['data']
        ) 