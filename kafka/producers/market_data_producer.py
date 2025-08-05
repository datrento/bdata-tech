import asyncio
import httpx
import uuid
from datetime import datetime
from base_producer import BaseProducer
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import create_topic_if_not_exists
from config import DATA_API_AGGREGATOR, COLLECTION_INTERVAL, TOPICS, PRODUCTS_MONITORING

class MarketDataProducer(BaseProducer):
    """Producer for market data"""

    def __init__(self):
        super().__init__('market-data')
        self.api_url = DATA_API_AGGREGATOR
        self.collection_interval = COLLECTION_INTERVAL
        self.topics = TOPICS
        self.products_monitoring = PRODUCTS_MONITORING

    def _generate_message_key(self, product_sku: str, timestamp: str=None) -> str:
        """Generate a unique key for the message based on product SKU and timestamp"""
        # Option 1: Product + Timestamp (maintains some ordering)
        # return f"{product_sku}_{timestamp}"
        
        # Option 2: Product + UUID (completely unique)
        return f"{product_sku}_{uuid.uuid4().hex[:8]}"


    def _send_aggregator_prices(self, aggregators: dict, product_sku: str,
                                api_collection_timestamp: str):
        """Send aggregator prices to Kafka"""
        for aggregator, agg_data in aggregators.items():
            if agg_data:
                aggregator_key = f"{product_sku}_{aggregator}_{api_collection_timestamp}"

                aggregator_message = {
                    'api_collection_timestamp': api_collection_timestamp,
                    'data_last_updated': agg_data.get('data_last_updated', api_collection_timestamp),
                    'product_sku': product_sku,
                    'aggregator': aggregator,
                    'available': agg_data['available'],
                    'avg_price': agg_data['avg_price'],
                    'min_price': agg_data['price_range']['min'],
                    'max_price': agg_data['price_range']['max'],
                    'stores_tracked': agg_data['stores_tracked'],
                }

                self.send_message(
                    topic=self.topics['AGGREGATOR_PRICES'],
                    message=aggregator_message,
                    key=aggregator_key
                )
        
    def _send_competitor_prices(self, competitors: dict, product_sku: str,
                                api_collection_timestamp: str):
        """Send competitor prices to Kafka"""
        if not competitors:
            self.logger.warning(f"No competitor data found for {product_sku}")
            return
        for competitor, price_data in competitors.items():
            if price_data:
                competitor_key = f"{product_sku}_{competitor}_{api_collection_timestamp}"

                competitor_message = {
                    'api_collection_timestamp': api_collection_timestamp,
                    'data_timestamp': price_data.get('data_timestamp', api_collection_timestamp),
                    'product_sku': product_sku,
                    'competitor': competitor,
                    'price': price_data['price'],
                    'in_stock': price_data['in_stock'],
                    'source_sku': price_data['sku'],
                }

                self.send_message(
                    topic=self.topics['COMPETITOR_PRICES'],
                    message=competitor_message,
                    key=competitor_key
                )
    async def collect_market_data(self, product_sku: str):
        """Collect market data for a specific product SKU"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.api_url}/{product_sku}")
                
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                self.logger.error(f"Failed to fetch data for {product_sku}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching data for {product_sku}: {e}")
            return None
        
    async def process_product(self, product_sku: str):
        """Process a product and send messages to Kafka"""
        
        self.logger.info(f"Processing product: {product_sku}")

        market_data = await self.collect_market_data(product_sku)

        if not market_data:
            self.logger.warning(f"No market data found for {product_sku}")
            return False
        
        try:
            # Generate a unique key for the message
            api_collection_timestamp = market_data.get('collection_timestamp', datetime.now().isoformat())
            message_key = self._generate_message_key(product_sku)

            # For Raw market data
            self.send_message(
                topic=self.topics['MARKET_DATA_RAW'],
                message={
                    **market_data,
                    'collection_source': 'price_api',
                    'data_type': 'market_pricing'
                },
                key=message_key
            )

            # For competitor price 
            competitors = market_data.get('data_sources', {}).get('competitors', {})
            self._send_competitor_prices(competitors, product_sku, api_collection_timestamp)

            # For price aggregators
            aggregators = market_data.get('data_sources', {}).get('aggregators', {})
            self._send_aggregator_prices(aggregators, product_sku, api_collection_timestamp)

            self.logger.info(f"Successfully processed and sent data for {product_sku}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error processing product {product_sku}: {e}")
            return False

    async def start_producing(self):
        """Start producing messages for all monitored products"""
        self.running = True
        self.logger.info("Starting market data producer...")
        self.logger.info(f"API URL: {self.api_url}")
        self.logger.info(f"Collection interval: {self.collection_interval} seconds")
        self.logger.info(f"Monitoring products: {self.products_monitoring}")


        while self.running:
            try:
                batch_start = datetime.now()
                batch_id = batch_start.strftime("%Y%m%d_%H%M%S")
                successful_count = 0

                self.logger.info(f"Starting batch {batch_id}")

                for product_sku in self.products_monitoring:
                    if await self.process_product(product_sku):
                        successful_count += 1
                
                batch_duration = (datetime.now() - batch_start).total_seconds()
                self.logger.info(f"Batch {batch_id} completed: {successful_count}/{len(self.products_monitoring)} products processed in {batch_duration:.2f} seconds")

                # Wait for the next collection interval
                await asyncio.sleep(self.collection_interval)
            
            except KeyboardInterrupt:
                self.logger.info("Market data producer stopped by user.")
                break
            except Exception as e:
                self.logger.error(f"Error in market data producer: {e}")
                await asyncio.sleep(60)  # Wait before retrying on error

        self.stop()

if __name__ == "__main__":
    def setup_topics():
        """Create necessary Kafka topics if they don't exist"""
        try:
            create_topic_if_not_exists(TOPICS['MARKET_DATA_RAW'])
            create_topic_if_not_exists(TOPICS['COMPETITOR_PRICES'])
            create_topic_if_not_exists(TOPICS['AGGREGATOR_PRICES'])
            return True
        except Exception as e:
            print(f"Error setting up topics: {e}")
            return False

    async def main():
        # create the topics if they don't exist
        setup_topics()
        # Initialize the producer
        producer = MarketDataProducer()

        try:
            await producer.start_producing()
        except Exception as e:
            producer.stop()

    # Main entry point
    if setup_topics():
        asyncio.run(main())
    else:
        print("Failed to set up Kafka topics. Exiting.")
        sys.exit(1)