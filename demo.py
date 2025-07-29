"""
Demo Script for E-commerce Price Intelligence System
"""

import time
import random
import logging
from datetime import datetime, timedelta
import threading

from src.data_models import (
    Product, CompetitorPrice, UserBehavior, PriceHistory, 
    PricingDecision, MarketSignal, PlatformType, ProductCategory
)
from src.database import DatabaseManager, MongoDBRepository
from src.dynamic_pricing import DynamicPricingEngine
from src.data_ingestion import DataIngestionManager


class PriceIntelligenceDemo:
    """Demo class to showcase the price intelligence system"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = None
        self.mongo_repo = None
        self.pricing_engine = None
        self.data_ingestion = None
        self.running = False
        
    def initialize(self):
        """Initialize the demo system"""
        try:
            self.logger.info("Initializing Price Intelligence Demo...")
            
            # Initialize database
            self.db_manager = DatabaseManager()
            self.db_manager.connect_mongodb()
            self.mongo_repo = MongoDBRepository(self.db_manager)
            
            # Initialize pricing engine
            self.pricing_engine = DynamicPricingEngine()
            self.pricing_engine.initialize()
            
            # Initialize data ingestion
            self.data_ingestion = DataIngestionManager()
            self.data_ingestion.initialize()
            
            self.logger.info("Demo system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize demo: {e}")
            raise
    
    def create_sample_data(self):
        """Create sample data for demonstration"""
        try:
            self.logger.info("Creating sample data...")
            
            # Create sample products
            sample_products = [
                Product(
                    product_id="PROD001",
                    name="iPhone 15 Pro 128GB",
                    category=ProductCategory.ELECTRONICS,
                    brand="Apple",
                    sku="IPH15PRO-128",
                    current_price=999.99,
                    inventory_level=50,
                    cost_price=700.00,
                    min_price=899.99,
                    max_price=1099.99
                ),
                Product(
                    product_id="PROD002",
                    name="Samsung Galaxy S24 256GB",
                    category=ProductCategory.ELECTRONICS,
                    brand="Samsung",
                    sku="SAMS24-256",
                    current_price=899.99,
                    inventory_level=30,
                    cost_price=650.00,
                    min_price=799.99,
                    max_price=999.99
                ),
                Product(
                    product_id="PROD003",
                    name="Nike Air Max 270",
                    category=ProductCategory.SPORTS,
                    brand="Nike",
                    sku="NIKE-AM270",
                    current_price=150.00,
                    inventory_level=100,
                    cost_price=80.00,
                    min_price=120.00,
                    max_price=180.00
                ),
                Product(
                    product_id="PROD004",
                    name="Sony WH-1000XM5 Headphones",
                    category=ProductCategory.ELECTRONICS,
                    brand="Sony",
                    sku="SONY-WH1000XM5",
                    current_price=349.99,
                    inventory_level=25,
                    cost_price=250.00,
                    min_price=299.99,
                    max_price=399.99
                ),
                Product(
                    product_id="PROD005",
                    name="Adidas Ultraboost 22",
                    category=ProductCategory.SPORTS,
                    brand="Adidas",
                    sku="ADIDAS-UB22",
                    current_price=180.00,
                    inventory_level=75,
                    cost_price=100.00,
                    min_price=150.00,
                    max_price=220.00
                )
            ]
            
            # Store products in database
            for product in sample_products:
                self.mongo_repo.insert_product(product)
            
            self.logger.info(f"Created {len(sample_products)} sample products")
            
            # Create sample competitor prices
            self._create_sample_competitor_prices(sample_products)
            
            # Create sample user behavior
            self._create_sample_user_behavior(sample_products)
            
            # Create sample price history
            self._create_sample_price_history(sample_products)
            
            self.logger.info("Sample data creation completed")
            
        except Exception as e:
            self.logger.error(f"Error creating sample data: {e}")
            raise
    
    def _create_sample_competitor_prices(self, products):
        """Create sample competitor prices"""
        platforms = [PlatformType.AMAZON, PlatformType.EBAY, PlatformType.WALMART]
        
        for product in products:
            for platform in platforms:
                # Create price variations
                base_price = product.current_price
                variation = random.uniform(-0.15, 0.15)  # ±15% variation
                competitor_price = base_price * (1 + variation)
                
                competitor_price_obj = CompetitorPrice(
                    platform=platform,
                    price=round(competitor_price, 2),
                    timestamp=datetime.utcnow() - timedelta(hours=random.randint(0, 24)),
                    availability=random.choice([True, True, True, False]),  # 75% availability
                    shipping_cost=random.uniform(0, 15),
                    seller_rating=random.uniform(3.5, 5.0),
                    seller_name=f"Seller_{random.randint(1, 100)}"
                )
                
                self.mongo_repo.insert_competitor_price(competitor_price_obj, product.product_id)
    
    def _create_sample_user_behavior(self, products):
        """Create sample user behavior data"""
        actions = ['view', 'add_to_cart', 'purchase', 'search']
        
        for _ in range(100):  # Create 100 user behavior records
            product = random.choice(products)
            action = random.choice(actions)
            
            user_behavior = UserBehavior(
                user_id=f"user_{random.randint(1000, 9999)}",
                product_id=product.product_id,
                action=action,
                timestamp=datetime.utcnow() - timedelta(hours=random.randint(0, 48)),
                session_id=f"session_{random.randint(10000, 99999)}",
                platform=random.choice(list(PlatformType)),
                price_at_time=product.current_price,
                competitor_price_at_time=product.current_price * random.uniform(0.9, 1.1)
            )
            
            self.mongo_repo.insert_user_behavior(user_behavior)
    
    def _create_sample_price_history(self, products):
        """Create sample price history"""
        for product in products:
            # Create historical price changes
            current_price = product.current_price
            for i in range(30):  # 30 days of history
                date = datetime.utcnow() - timedelta(days=i)
                
                # Simulate price fluctuations
                if i % 7 == 0:  # Weekly price changes
                    change = random.uniform(-0.1, 0.1)  # ±10% change
                    current_price = current_price * (1 + change)
                
                price_history = PriceHistory(
                    product_id=product.product_id,
                    price=round(current_price, 2),
                    timestamp=date,
                    reason=random.choice(['competitor_price_change', 'demand_adjustment', 'inventory_management']),
                    competitor_avg_price=current_price * random.uniform(0.95, 1.05),
                    demand_score=random.uniform(0.1, 0.9),
                    inventory_level=random.randint(10, 100)
                )
                
                self.mongo_repo.insert_price_history(price_history)
    
    def run_demo_scenarios(self):
        """Run various demo scenarios"""
        try:
            self.logger.info("Starting demo scenarios...")
            
            # Scenario 1: Competitor price drop
            self._scenario_competitor_price_drop()
            
            # Scenario 2: Demand surge
            self._scenario_demand_surge()
            
            # Scenario 3: Inventory alert
            self._scenario_inventory_alert()
            
            # Scenario 4: Price optimization
            self._scenario_price_optimization()
            
            self.logger.info("Demo scenarios completed")
            
        except Exception as e:
            self.logger.error(f"Error running demo scenarios: {e}")
            raise
    
    def _scenario_competitor_price_drop(self):
        """Demo scenario: Competitor drops price significantly"""
        self.logger.info("Scenario 1: Competitor Price Drop")
        
        # Simulate competitor price drop
        product_id = "PROD001"
        product = self.mongo_repo.get_product(product_id)
        
        if product:
            # Create competitor price drop
            competitor_price = CompetitorPrice(
                platform=PlatformType.AMAZON,
                price=product.current_price * 0.85,  # 15% drop
                timestamp=datetime.utcnow(),
                availability=True
            )
            
            self.mongo_repo.insert_competitor_price(competitor_price, product_id)
            
            # Create market signal
            market_signal = MarketSignal(
                signal_id=None,
                signal_type="competitor_price_drop",
                product_id=product_id,
                severity=0.8,
                timestamp=datetime.utcnow(),
                description=f"Amazon dropped price by 15% for {product.name}",
                data={"old_price": product.current_price, "new_price": competitor_price.price}
            )
            
            self.mongo_repo.insert_market_signal(market_signal)
            
            # Make pricing decision
            decision = self.pricing_engine.make_pricing_decision(product_id)
            
            if decision:
                self.logger.info(f"Price adjusted from ${decision.old_price} to ${decision.new_price}")
            
            time.sleep(2)
    
    def _scenario_demand_surge(self):
        """Demo scenario: Sudden increase in demand"""
        self.logger.info("Scenario 2: Demand Surge")
        
        product_id = "PROD003"
        
        # Create surge in user behavior
        for _ in range(20):
            user_behavior = UserBehavior(
                user_id=f"user_{random.randint(1000, 9999)}",
                product_id=product_id,
                action="view",
                timestamp=datetime.utcnow(),
                session_id=f"session_{random.randint(10000, 99999)}",
                platform=PlatformType.AMAZON,
                price_at_time=150.00
            )
            self.mongo_repo.insert_user_behavior(user_behavior)
        
        # Create market signal
        market_signal = MarketSignal(
            signal_id=None,
            signal_type="demand_surge",
            product_id=product_id,
            severity=0.7,
            timestamp=datetime.utcnow(),
            description="Sudden increase in product views",
            data={"view_count": 20, "time_period": "1 hour"}
        )
        
        self.mongo_repo.insert_market_signal(market_signal)
        
        time.sleep(2)
    
    def _scenario_inventory_alert(self):
        """Demo scenario: Low inventory warning"""
        self.logger.info("Scenario 3: Inventory Alert")
        
        product_id = "PROD002"
        
        # Update product with low inventory
        self.mongo_repo.update_product(product_id, {"inventory_level": 5})
        
        # Create market signal
        market_signal = MarketSignal(
            signal_id=None,
            signal_type="low_inventory",
            product_id=product_id,
            severity=0.9,
            timestamp=datetime.utcnow(),
            description="Low inventory warning - only 5 units remaining",
            data={"inventory_level": 5, "threshold": 10}
        )
        
        self.mongo_repo.insert_market_signal(market_signal)
        
        time.sleep(2)
    
    def _scenario_price_optimization(self):
        """Demo scenario: Price optimization based on market conditions"""
        self.logger.info("Scenario 4: Price Optimization")
        
        # Run pricing optimization for all products
        products = self.mongo_repo.get_products_by_category("electronics")
        
        for product in products:
            decision = self.pricing_engine.make_pricing_decision(product.product_id)
            if decision and decision.old_price != decision.new_price:
                self.logger.info(f"Optimized {product.name}: ${decision.old_price} → ${decision.new_price}")
        
        time.sleep(2)
    
    def start_real_time_simulation(self, duration_minutes=5):
        """Start real-time simulation of market activity"""
        try:
            self.logger.info(f"Starting real-time simulation for {duration_minutes} minutes...")
            self.running = True
            
            start_time = datetime.utcnow()
            end_time = start_time + timedelta(minutes=duration_minutes)
            
            while self.running and datetime.utcnow() < end_time:
                # Simulate random market events
                event_type = random.choice(['price_change', 'user_behavior', 'competitor_update'])
                
                if event_type == 'price_change':
                    self._simulate_price_change()
                elif event_type == 'user_behavior':
                    self._simulate_user_behavior()
                elif event_type == 'competitor_update':
                    self._simulate_competitor_update()
                
                time.sleep(random.uniform(5, 15))  # Random interval between events
            
            self.running = False
            self.logger.info("Real-time simulation completed")
            
        except Exception as e:
            self.logger.error(f"Error in real-time simulation: {e}")
    
    def _simulate_price_change(self):
        """Simulate a price change event"""
        products = self.mongo_repo.get_products_by_category("electronics")
        if products:
            product = random.choice(products)
            decision = self.pricing_engine.make_pricing_decision(product.product_id)
            if decision and decision.old_price != decision.new_price:
                self.logger.info(f"Price change: {product.name} ${decision.old_price} → ${decision.new_price}")
    
    def _simulate_user_behavior(self):
        """Simulate user behavior event"""
        products = self.mongo_repo.get_products_by_category("electronics")
        if products:
            product = random.choice(products)
            user_behavior = UserBehavior(
                user_id=f"user_{random.randint(1000, 9999)}",
                product_id=product.product_id,
                action=random.choice(['view', 'add_to_cart', 'purchase']),
                timestamp=datetime.utcnow(),
                session_id=f"session_{random.randint(10000, 99999)}",
                platform=random.choice(list(PlatformType)),
                price_at_time=product.current_price
            )
            self.mongo_repo.insert_user_behavior(user_behavior)
            self.logger.info(f"User behavior: {user_behavior.action} for {product.name}")
    
    def _simulate_competitor_update(self):
        """Simulate competitor price update"""
        products = self.mongo_repo.get_products_by_category("electronics")
        if products:
            product = random.choice(products)
            platform = random.choice(list(PlatformType))
            
            # Random price change
            price_change = random.uniform(-0.1, 0.1)
            new_price = product.current_price * (1 + price_change)
            
            competitor_price = CompetitorPrice(
                platform=platform,
                price=round(new_price, 2),
                timestamp=datetime.utcnow(),
                availability=random.choice([True, False])
            )
            
            self.mongo_repo.insert_competitor_price(competitor_price, product.product_id)
            self.logger.info(f"Competitor update: {platform.value} price for {product.name}: ${new_price:.2f}")
    
    def display_system_status(self):
        """Display current system status"""
        try:
            self.logger.info("=== System Status ===")
            
            # Product count
            collection = self.db_manager.mongo_db[MongoDBCollections.PRODUCTS]
            product_count = collection.count_documents({})
            self.logger.info(f"Total Products: {product_count}")
            
            # Recent pricing decisions
            collection = self.db_manager.mongo_db[MongoDBCollections.PRICING_DECISIONS]
            recent_decisions = collection.count_documents({
                'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=1)}
            })
            self.logger.info(f"Recent Pricing Decisions: {recent_decisions}")
            
            # Active market signals
            collection = self.db_manager.mongo_db[MongoDBCollections.MARKET_SIGNALS]
            active_signals = collection.count_documents({
                'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=24)}
            })
            self.logger.info(f"Active Market Signals: {active_signals}")
            
            # User behavior events
            collection = self.db_manager.mongo_db[MongoDBCollections.USER_BEHAVIOR]
            recent_behavior = collection.count_documents({
                'timestamp': {'$gte': datetime.utcnow() - timedelta(hours=1)}
            })
            self.logger.info(f"Recent User Behavior Events: {recent_behavior}")
            
            self.logger.info("===================")
            
        except Exception as e:
            self.logger.error(f"Error displaying system status: {e}")
    
    def cleanup(self):
        """Cleanup demo resources"""
        try:
            self.running = False
            if self.db_manager:
                self.db_manager.close_connections()
            self.logger.info("Demo cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def main():
    """Main demo function"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    demo = PriceIntelligenceDemo()
    
    try:
        # Initialize demo
        demo.initialize()
        
        # Create sample data
        demo.create_sample_data()
        
        # Display initial status
        demo.display_system_status()
        
        # Run demo scenarios
        demo.run_demo_scenarios()
        
        # Display status after scenarios
        demo.display_system_status()
        
        # Start real-time simulation
        print("\nStarting real-time simulation... Press Ctrl+C to stop")
        demo.start_real_time_simulation(duration_minutes=3)
        
        # Final status
        demo.display_system_status()
        
        print("\nDemo completed! Check the dashboard at http://localhost:8050")
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"Demo error: {e}")
    finally:
        demo.cleanup()


if __name__ == "__main__":
    main() 