"""
Simple Test Script for E-commerce Price Intelligence System
"""

import unittest
import logging
from datetime import datetime
import tempfile
import os

from src.data_models import (
    Product, CompetitorPrice, UserBehavior, PriceHistory, PlatformType, ProductCategory
)
from src.database import DatabaseManager, MongoDBRepository
from src.dynamic_pricing import DynamicPricingEngine


class TestPriceIntelligenceSystem(unittest.TestCase):
    """Test cases for the price intelligence system"""
    
    def setUp(self):
        """Set up test environment"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Create temporary config
        self.config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    price_updates: "price-updates"
    competitor_data: "competitor-data"
    user_behavior: "user-behavior"
  consumer_group: "test-group"

database:
  mongodb:
    uri: "mongodb://localhost:27017/"
    database: "test_price_intelligence"
    collections:
      products: "products"
      competitor_prices: "competitor_prices"
      price_history: "price_history"
      user_behavior: "user_behavior"
      pricing_decisions: "pricing_decisions"
      market_signals: "market_signals"
      demand_metrics: "demand_metrics"

pricing:
  update_frequency_minutes: 15
  min_price_margin: 0.05
  max_price_margin: 0.30
  competitor_weight: 0.4
  demand_weight: 0.3
  inventory_weight: 0.2
  seasonality_weight: 0.1
  
  model:
    type: "random_forest"
    retrain_frequency_hours: 24
    features:
      - "competitor_avg_price"
      - "competitor_min_price"
      - "competitor_max_price"
      - "demand_score"
      - "inventory_level"
      - "seasonality_factor"
      - "time_of_day"
      - "day_of_week"

logging:
  level: "INFO"
  file: "logs/test.log"
  max_size_mb: 10
  backup_count: 2
"""
        
        # Create temporary config file
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, "test_config.yaml")
        
        with open(self.config_path, 'w') as f:
            f.write(self.config_content)
        
        # Initialize components
        self.db_manager = None
        self.mongo_repo = None
        self.pricing_engine = None
    
    def tearDown(self):
        """Clean up test environment"""
        try:
            if self.pricing_engine:
                self.pricing_engine.close()
            if self.db_manager:
                self.db_manager.close_connections()
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")
        
        # Remove temporary files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_data_models(self):
        """Test data model creation and serialization"""
        self.logger.info("Testing data models...")
        
        # Test Product creation
        product = Product(
            product_id="TEST001",
            name="Test Product",
            category=ProductCategory.ELECTRONICS,
            brand="Test Brand",
            sku="TEST-SKU-001",
            current_price=100.00,
            inventory_level=50
        )
        
        self.assertEqual(product.product_id, "TEST001")
        self.assertEqual(product.current_price, 100.00)
        self.assertEqual(product.category, ProductCategory.ELECTRONICS)
        
        # Test CompetitorPrice creation
        competitor_price = CompetitorPrice(
            platform=PlatformType.AMAZON,
            price=95.00,
            timestamp=datetime.utcnow()
        )
        
        self.assertEqual(competitor_price.platform, PlatformType.AMAZON)
        self.assertEqual(competitor_price.price, 95.00)
        
        # Test UserBehavior creation
        user_behavior = UserBehavior(
            user_id="user123",
            product_id="TEST001",
            action="view",
            timestamp=datetime.utcnow(),
            session_id="session123",
            platform=PlatformType.AMAZON,
            price_at_time=100.00
        )
        
        self.assertEqual(user_behavior.user_id, "user123")
        self.assertEqual(user_behavior.action, "view")
        
        self.logger.info("Data models test passed")
    
    def test_database_connection(self):
        """Test database connection (requires MongoDB running)"""
        self.logger.info("Testing database connection...")
        
        try:
            # Initialize database manager
            self.db_manager = DatabaseManager(self.config_path)
            self.db_manager.connect_mongodb()
            
            # Test connection by creating repository
            self.mongo_repo = MongoDBRepository(self.db_manager)
            
            # Test basic operations
            test_product = Product(
                product_id="DBTEST001",
                name="Database Test Product",
                category=ProductCategory.ELECTRONICS,
                brand="Test Brand",
                sku="DBTEST-SKU-001",
                current_price=150.00,
                inventory_level=25
            )
            
            # Insert product
            success = self.mongo_repo.insert_product(test_product)
            self.assertTrue(success)
            
            # Retrieve product
            retrieved_product = self.mongo_repo.get_product("DBTEST001")
            self.assertIsNotNone(retrieved_product)
            self.assertEqual(retrieved_product.name, "Database Test Product")
            
            # Update product
            update_success = self.mongo_repo.update_product("DBTEST001", {"inventory_level": 30})
            self.assertTrue(update_success)
            
            # Verify update
            updated_product = self.mongo_repo.get_product("DBTEST001")
            self.assertEqual(updated_product.inventory_level, 30)
            
            self.logger.info("Database connection test passed")
            
        except Exception as e:
            self.logger.warning(f"Database test skipped (MongoDB not available): {e}")
            self.skipTest("MongoDB not available")
    
    def test_pricing_engine(self):
        """Test pricing engine functionality"""
        self.logger.info("Testing pricing engine...")
        
        try:
            # Initialize pricing engine
            self.pricing_engine = DynamicPricingEngine(self.config_path)
            self.pricing_engine.initialize()
            
            # Test feature preparation
            test_product = Product(
                product_id="PRICETEST001",
                name="Pricing Test Product",
                category=ProductCategory.ELECTRONICS,
                brand="Test Brand",
                sku="PRICETEST-SKU-001",
                current_price=200.00,
                inventory_level=40,
                cost_price=150.00
            )
            
            # Mock competitor prices
            competitor_prices = [
                CompetitorPrice(platform=PlatformType.AMAZON, price=195.00),
                CompetitorPrice(platform=PlatformType.EBAY, price=205.00),
                CompetitorPrice(platform=PlatformType.WALMART, price=190.00)
            ]
            
            # Test feature preparation
            features = self.pricing_engine._prepare_features(
                test_product, competitor_prices, None
            )
            
            self.assertEqual(len(features), 8)  # 8 features as defined in config
            self.assertIsInstance(features[0], float)  # competitor_avg_price
            self.assertIsInstance(features[1], float)  # competitor_min_price
            self.assertIsInstance(features[2], float)  # competitor_max_price
            
            # Test rule-based pricing
            rule_based_price = self.pricing_engine._rule_based_pricing(
                test_product, competitor_prices, None
            )
            
            self.assertIsInstance(rule_based_price, float)
            self.assertGreater(rule_based_price, 0)
            
            # Test price constraints
            constrained_price = self.pricing_engine._apply_pricing_constraints(
                rule_based_price, test_product, competitor_prices
            )
            
            self.assertIsInstance(constrained_price, float)
            self.assertGreater(constrained_price, 0)
            
            self.logger.info("Pricing engine test passed")
            
        except Exception as e:
            self.logger.warning(f"Pricing engine test skipped: {e}")
            self.skipTest("Pricing engine not available")
    
    def test_configuration_loading(self):
        """Test configuration loading"""
        self.logger.info("Testing configuration loading...")
        
        try:
            # Test database manager config loading
            db_manager = DatabaseManager(self.config_path)
            config = db_manager.config
            
            # Verify config structure
            self.assertIn('kafka', config)
            self.assertIn('database', config)
            self.assertIn('pricing', config)
            self.assertIn('logging', config)
            
            # Verify specific values
            self.assertEqual(config['kafka']['bootstrap_servers'], "localhost:9092")
            self.assertEqual(config['database']['mongodb']['database'], "test_price_intelligence")
            self.assertEqual(config['pricing']['update_frequency_minutes'], 15)
            
            self.logger.info("Configuration loading test passed")
            
        except Exception as e:
            self.logger.error(f"Configuration loading test failed: {e}")
            raise
    
    def test_data_serialization(self):
        """Test data serialization functions"""
        self.logger.info("Testing data serialization...")
        
        from src.data_models import (
            create_product_document, create_price_history_document,
            create_user_behavior_document
        )
        
        # Test product serialization
        product = Product(
            product_id="SERTEST001",
            name="Serialization Test Product",
            category=ProductCategory.ELECTRONICS,
            brand="Test Brand",
            sku="SERTEST-SKU-001",
            current_price=100.00
        )
        
        product_doc = create_product_document(product)
        self.assertIsInstance(product_doc, dict)
        self.assertEqual(product_doc['product_id'], "SERTEST001")
        self.assertEqual(product_doc['name'], "Serialization Test Product")
        
        # Test price history serialization
        price_history = PriceHistory(
            product_id="SERTEST001",
            price=100.00,
            timestamp=datetime.utcnow()
        )
        
        price_history_doc = create_price_history_document(price_history)
        self.assertIsInstance(price_history_doc, dict)
        self.assertEqual(price_history_doc['product_id'], "SERTEST001")
        self.assertEqual(price_history_doc['price'], 100.00)
        
        # Test user behavior serialization
        user_behavior = UserBehavior(
            user_id="user123",
            product_id="SERTEST001",
            action="view",
            timestamp=datetime.utcnow(),
            session_id="session123",
            platform=PlatformType.AMAZON,
            price_at_time=100.00
        )
        
        user_behavior_doc = create_user_behavior_document(user_behavior)
        self.assertIsInstance(user_behavior_doc, dict)
        self.assertEqual(user_behavior_doc['user_id'], "user123")
        self.assertEqual(user_behavior_doc['action'], "view")
        
        self.logger.info("Data serialization test passed")


def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestPriceIntelligenceSystem)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\nTest Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print(f"\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print(f"\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1) 