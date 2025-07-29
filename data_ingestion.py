"""
Data Ingestion Layer for E-commerce Price Intelligence System
"""

import logging
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import yaml
import schedule
import threading

from data_models import (
    Product, CompetitorPrice, UserBehavior, PlatformType, ProductCategory
)
from database import DatabaseManager, MongoDBRepository


class DataIngestionManager:
    """Manages data ingestion from multiple sources"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager(config_path)
        self.mongo_repo = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.scrapers = {}
        self.running = False
        
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
    
    def initialize(self):
        """Initialize all components"""
        try:
            # Initialize database connections
            self.db_manager.connect_mongodb()
            self.mongo_repo = MongoDBRepository(self.db_manager)
            
            # Initialize Kafka
            self._initialize_kafka()
            
            # Initialize scrapers
            self._initialize_scrapers()
            
            self.logger.info("Data ingestion manager initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize data ingestion manager: {e}")
            raise
    
    def _initialize_kafka(self):
        """Initialize Kafka producer and consumer"""
        try:
            kafka_config = self.config['kafka']
            
            # Initialize producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                acks='all'
            )
            
            # Initialize consumer
            self.kafka_consumer = KafkaConsumer(
                kafka_config['topics']['price_updates'],
                bootstrap_servers=kafka_config['bootstrap_servers'],
                group_id=kafka_config['consumer_group'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            
            self.logger.info("Kafka connections established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def _initialize_scrapers(self):
        """Initialize web scrapers for different platforms"""
        for platform_config in self.config['platforms']:
            platform_name = platform_config['name']
            self.scrapers[platform_name] = WebScraper(
                platform_config,
                self.config['data_collection']['scraping']
            )
    
    def start_data_collection(self):
        """Start the data collection process"""
        self.running = True
        
        # Start competitor price collection
        self._start_competitor_price_collection()
        
        # Start user behavior simulation
        self._start_user_behavior_simulation()
        
        # Start Kafka consumer
        self._start_kafka_consumer()
        
        self.logger.info("Data collection started")
    
    def stop_data_collection(self):
        """Stop the data collection process"""
        self.running = False
        
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        self.logger.info("Data collection stopped")
    
    def _start_competitor_price_collection(self):
        """Start competitor price collection in background thread"""
        def collect_prices():
            while self.running:
                try:
                    self._collect_competitor_prices()
                    time.sleep(self.config['pricing']['update_frequency_minutes'] * 60)
                except Exception as e:
                    self.logger.error(f"Error in competitor price collection: {e}")
                    time.sleep(60)  # Wait before retrying
        
        thread = threading.Thread(target=collect_prices, daemon=True)
        thread.start()
    
    def _collect_competitor_prices(self):
        """Collect competitor prices from all platforms"""
        products = self._get_products_to_monitor()
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for product in products:
                for platform_name, scraper in self.scrapers.items():
                    future = executor.submit(
                        self._scrape_product_price,
                        scraper,
                        product,
                        platform_name
                    )
                    futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        self._process_competitor_price(result)
                except Exception as e:
                    self.logger.error(f"Error in price scraping: {e}")
    
    def _get_products_to_monitor(self) -> List[Product]:
        """Get products to monitor for price changes"""
        # In a real system, this would query the database
        # For demo purposes, we'll create sample products
        sample_products = [
            Product(
                product_id="PROD001",
                name="iPhone 15 Pro",
                category=ProductCategory.ELECTRONICS,
                brand="Apple",
                sku="IPH15PRO-128",
                current_price=999.99,
                inventory_level=50
            ),
            Product(
                product_id="PROD002",
                name="Samsung Galaxy S24",
                category=ProductCategory.ELECTRONICS,
                brand="Samsung",
                sku="SAMS24-256",
                current_price=899.99,
                inventory_level=30
            ),
            Product(
                product_id="PROD003",
                name="Nike Air Max 270",
                category=ProductCategory.SPORTS,
                brand="Nike",
                sku="NIKE-AM270",
                current_price=150.00,
                inventory_level=100
            )
        ]
        
        # Store sample products in database
        for product in sample_products:
            self.mongo_repo.insert_product(product)
        
        return sample_products
    
    def _scrape_product_price(self, scraper, product: Product, platform_name: str) -> Optional[Dict]:
        """Scrape product price from a specific platform"""
        try:
            price_data = scraper.scrape_product_price(product.name)
            if price_data:
                return {
                    'product_id': product.product_id,
                    'platform': platform_name,
                    'price': price_data['price'],
                    'availability': price_data['availability'],
                    'timestamp': datetime.utcnow().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Error scraping {platform_name} for {product.product_id}: {e}")
        
        return None
    
    def _process_competitor_price(self, price_data: Dict):
        """Process competitor price data"""
        try:
            # Create competitor price object
            competitor_price = CompetitorPrice(
                platform=PlatformType(price_data['platform']),
                price=price_data['price'],
                timestamp=datetime.fromisoformat(price_data['timestamp']),
                availability=price_data['availability']
            )
            
            # Store in database
            self.mongo_repo.insert_competitor_price(
                competitor_price,
                price_data['product_id']
            )
            
            # Send to Kafka
            self._send_to_kafka(
                self.config['kafka']['topics']['competitor_data'],
                price_data['product_id'],
                price_data
            )
            
            self.logger.info(f"Processed competitor price for {price_data['product_id']}")
            
        except Exception as e:
            self.logger.error(f"Error processing competitor price: {e}")
    
    def _start_user_behavior_simulation(self):
        """Start user behavior simulation in background thread"""
        def simulate_behavior():
            while self.running:
                try:
                    self._simulate_user_behavior()
                    time.sleep(30)  # Simulate every 30 seconds
                except Exception as e:
                    self.logger.error(f"Error in user behavior simulation: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=simulate_behavior, daemon=True)
        thread.start()
    
    def _simulate_user_behavior(self):
        """Simulate user behavior data"""
        actions = ['view', 'add_to_cart', 'purchase', 'search']
        platforms = list(PlatformType)
        
        # Simulate multiple user behaviors
        for _ in range(random.randint(5, 15)):
            user_behavior = UserBehavior(
                user_id=f"user_{random.randint(1000, 9999)}",
                product_id=f"PROD00{random.randint(1, 3)}",
                action=random.choice(actions),
                timestamp=datetime.utcnow(),
                session_id=f"session_{random.randint(10000, 99999)}",
                platform=random.choice(platforms),
                price_at_time=random.uniform(50, 1000)
            )
            
            # Store in database
            self.mongo_repo.insert_user_behavior(user_behavior)
            
            # Send to Kafka
            self._send_to_kafka(
                self.config['kafka']['topics']['user_behavior'],
                user_behavior.user_id,
                {
                    'user_id': user_behavior.user_id,
                    'product_id': user_behavior.product_id,
                    'action': user_behavior.action,
                    'timestamp': user_behavior.timestamp.isoformat(),
                    'session_id': user_behavior.session_id,
                    'platform': user_behavior.platform.value,
                    'price_at_time': user_behavior.price_at_time
                }
            )
    
    def _start_kafka_consumer(self):
        """Start Kafka consumer in background thread"""
        def consume_messages():
            for message in self.kafka_consumer:
                if not self.running:
                    break
                
                try:
                    self._process_kafka_message(message)
                except Exception as e:
                    self.logger.error(f"Error processing Kafka message: {e}")
        
        thread = threading.Thread(target=consume_messages, daemon=True)
        thread.start()
    
    def _process_kafka_message(self, message):
        """Process incoming Kafka message"""
        topic = message.topic
        key = message.key
        value = message.value
        
        self.logger.info(f"Received message from topic {topic}: {key}")
        
        if topic == self.config['kafka']['topics']['price_updates']:
            self._handle_price_update(value)
        elif topic == self.config['kafka']['topics']['price_alerts']:
            self._handle_price_alert(value)
    
    def _handle_price_update(self, data: Dict):
        """Handle price update message"""
        try:
            # Update product price in database
            self.mongo_repo.update_product(
                data['product_id'],
                {'current_price': data['new_price']}
            )
            
            self.logger.info(f"Updated price for {data['product_id']}: {data['new_price']}")
            
        except Exception as e:
            self.logger.error(f"Error handling price update: {e}")
    
    def _handle_price_alert(self, data: Dict):
        """Handle price alert message"""
        self.logger.warning(f"Price alert: {data}")
    
    def _send_to_kafka(self, topic: str, key: str, value: Dict):
        """Send message to Kafka topic"""
        try:
            future = self.kafka_producer.send(topic, key=key, value=value)
            future.get(timeout=10)  # Wait for send to complete
        except Exception as e:
            self.logger.error(f"Error sending to Kafka topic {topic}: {e}")


class WebScraper:
    """Web scraper for e-commerce platforms"""
    
    def __init__(self, platform_config: Dict, scraping_config: Dict):
        self.platform_config = platform_config
        self.scraping_config = scraping_config
        self.logger = logging.getLogger(__name__)
        self.driver = None
    
    def scrape_product_price(self, product_name: str) -> Optional[Dict]:
        """Scrape product price from the platform"""
        try:
            # Simulate scraping (in real implementation, this would use actual web scraping)
            # For demo purposes, we'll generate realistic mock data
            
            # Simulate network delay
            time.sleep(self.scraping_config['delay_between_requests'])
            
            # Generate mock price data
            base_price = random.uniform(50, 1000)
            price_variation = random.uniform(-0.2, 0.2)  # ±20% variation
            final_price = base_price * (1 + price_variation)
            
            availability = random.choice([True, True, True, False])  # 75% availability
            
            return {
                'price': round(final_price, 2),
                'availability': availability,
                'platform': self.platform_config['name']
            }
            
        except Exception as e:
            self.logger.error(f"Error scraping {self.platform_config['name']}: {e}")
            return None
    
    def _setup_selenium_driver(self):
        """Setup Selenium WebDriver for dynamic content"""
        if not self.driver:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            self.driver = webdriver.Chrome(options=chrome_options)
    
    def _scrape_with_selenium(self, url: str, selectors: Dict) -> Optional[Dict]:
        """Scrape using Selenium for dynamic content"""
        try:
            self._setup_selenium_driver()
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selectors['price']))
            )
            
            # Extract data
            price_element = self.driver.find_element(By.CSS_SELECTOR, selectors['price'])
            price_text = price_element.text.strip()
            
            # Parse price
            price = self._parse_price(price_text)
            
            return {
                'price': price,
                'availability': True
            }
            
        except Exception as e:
            self.logger.error(f"Error with Selenium scraping: {e}")
            return None
    
    def _parse_price(self, price_text: str) -> float:
        """Parse price from text"""
        try:
            # Remove currency symbols and non-numeric characters
            import re
            price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
            if price_match:
                return float(price_match.group())
            return 0.0
        except:
            return 0.0
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit() 