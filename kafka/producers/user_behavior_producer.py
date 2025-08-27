import asyncio
import uuid
from datetime import datetime
import os
import sys
import httpx

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_producer import BaseProducer
from utils import create_topic_if_not_exists
from config import DATA_API_USER_BEHAVIOR, USER_BEHAVIOR_INTERVAL, TOPICS, PRODUCTS_MONITORING


class UserBehaviorProducer(BaseProducer):
    """Simple synthetic user behavior events producer."""

    def __init__(self):
        super().__init__('user-behavior')
        self.api_url = DATA_API_USER_BEHAVIOR
        self.collection_interval = USER_BEHAVIOR_INTERVAL
        self.products_monitoring = PRODUCTS_MONITORING
        self.topics = TOPICS

    async def collect_user_behavior(self, product_sku: str):
        """Collect user behavior data for a specific product."""
        try:
            timeout = httpx.Timeout(10.0, connect=5.0)  # Set a timeout for the request
            async with httpx.AsyncClient(timeout=timeout) as client:
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

    async def process_user_behavior(self, product_sku: str):
        """Process a product's user behavior event and send messages to Kafka"""

        self.logger.info(f"Processing user behavior for product: {product_sku}")

        user_behavior = await self.collect_user_behavior(product_sku)

        if not user_behavior:
            self.logger.warning(f"No user behavior data found for {product_sku}")
            return False
        
        try:
            # Generate a unique key for the message
            api_collection_timestamp = user_behavior.get('collection_timestamp', datetime.now().astimezone().isoformat())
            message_key = self._generate_message_key(product_sku)
            
            api_collection_ts_ms = int(datetime.fromisoformat(api_collection_timestamp).timestamp() * 1000)
            data_timestamp = user_behavior.get('data_timestamp', api_collection_timestamp)
            data_ts_ms = int(datetime.fromisoformat(data_timestamp).timestamp() * 1000)


            # user behavior data
            self.send_message(
                topic=self.topics['USER_BEHAVIOR'],
                message={
                    **user_behavior['product'],
                    **user_behavior['user_behavior'],
                    'api_collection_timestamp': api_collection_timestamp,
                    'data_ts_ms': data_ts_ms,
                    'api_collection_ts_ms': api_collection_ts_ms,
                    'collection_source': 'user_behavior_api',
                    'data_type': 'user_interaction'
                },
                key=message_key
            )
        except Exception as e:
            self.logger.error(f"Error processing user behavior for {product_sku}: {e}")
            return False

    async def start_producing(self):
        """Start producing messages for all products' user behavior"""
        self.running = True
        self.logger.info("Starting user behavior producer...")
        self.logger.info(f"API URL: {self.api_url}")
        self.logger.info(f"Collection interval: {self.collection_interval} seconds")
        self.logger.info(f"Monitoring products: {self.products_monitoring}")

        while self.running:
            try:
                batch_start = datetime.now()
                batch_id = batch_start.strftime("%Y%m%d_%H%M%S")
                successful_count = 0

                self.logger.info(f"Starting batch {batch_id}")

                process_user_behavior_tasks = [
                    self.process_user_behavior(product_sku) for product_sku in self.products_monitoring
                ]

                # Run all product processing tasks concurrently
                results = await asyncio.gather(*process_user_behavior_tasks, return_exceptions=True)

                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        product_sku = self.products_monitoring[i]
                        self.logger.error(f"Exception processing {product_sku}: {result}")
                    elif result is True:
                        successful_count += 1
                
                batch_duration = (datetime.now() - batch_start).total_seconds()
                self.logger.info(f"Batch {batch_id} completed: {successful_count}/{len(self.products_monitoring)} products processed in {batch_duration:.2f} seconds")

                # Wait for the next collection interval
                await asyncio.sleep(self.collection_interval)
            
            except KeyboardInterrupt:
                self.logger.info("User behavior producer stopped by user.")
                break
            except Exception as e:
                self.logger.error(f"Error in user behavior producer: {e}")
                await asyncio.sleep(60)  # Wait before retrying on error

        self.stop()


if __name__ == "__main__":
    def setup_topics():
        """Create necessary Kafka topics if they don't exist"""
        try:
            create_topic_if_not_exists(TOPICS['USER_BEHAVIOR'])
            return True
        except Exception as e:
            print(f"Error setting up topics: {e}")
            return False

    async def main():
        # Initialize the producer
        producer = UserBehaviorProducer()

        try:
            await producer.start_producing()
        except Exception as e:
            producer.logger.error(f"Error in main producer loop: {e}")
            producer.stop()

    # Main entry point
    if setup_topics():
        asyncio.run(main())
    else:
        print("Failed to set up Kafka topics. Exiting.")
        sys.exit(1)





