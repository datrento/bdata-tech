import asyncio
import httpx
import uuid
from datetime import datetime
from base_producer import BaseProducer
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_API_USER_BEHAVIOR, USER_BEHAVIOR_INTERVAL, TOPICS, PRODUCTS_MONITORING
from utils import create_topic_if_not_exists

class UserBehaviorProducer(BaseProducer):
    """Producer for user behavior data"""

    def __init__(self):
        super().__init__('user-behavior')
        self.api_url = DATA_API_USER_BEHAVIOR
        self.collection_interval = USER_BEHAVIOR_INTERVAL
        self.topic = TOPICS['USER_BEHAVIOR']
        self.products_monitoring = PRODUCTS_MONITORING

    def process_product(self, *args, **kwargs):
        """Dummy implementation to satisfy BaseProducer's abstract method"""
        pass

    def _generate_message_key(self, user_id: str, timestamp: str = None) -> str:
        return f"{user_id}_{uuid.uuid4().hex[:8]}"

    async def collect_user_behavior(self):
        """Collect user behavior data from the API"""
        try:
            for product_sku in self.products_monitoring:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{self.api_url}/user-behavior/{product_sku}")
                if response.status_code == 200:
                    data = response.json()
                    return data
                else:
                    self.logger.error(
                        f"Failed to fetch user behavior data: {response.status_code} - {response.text}")
                    return None

        except Exception as e:
            self.logger.error(f"Error fetching user behavior data: {e}")
            return None

    async def process_user_behavior(self):
        """Process and send user behavior data to Kafka"""
        self.logger.info("Processing user behavior data")
        user_data = await self.collect_user_behavior()
        if not user_data:
            self.logger.warning("No user behavior data found")
            return False
        try:
            timestamp = user_data.get('timestamp', datetime.now().isoformat())
            user_id = user_data.get('user_id', 'unknown')
            message_key = self._generate_message_key(user_id)
            self.send_message(
                topic=self.topic,
                message=user_data,
                key=message_key
            )
            self.logger.info("Successfully sent user behavior data")
            return True
        except Exception as e:
            self.logger.error(f"Error processing user behavior data: {e}")
            return False

    async def start_producing(self):
        self.running = True
        self.logger.info("Starting user behavior producer...")
        self.logger.info(f"API URL: {self.api_url}")
        self.logger.info(
            f"Collection interval: {self.collection_interval} seconds")
        while self.running:
            try:
                await self.process_user_behavior()
                await asyncio.sleep(self.collection_interval)
            except KeyboardInterrupt:
                self.logger.info("User behavior producer stopped by user.")
                break
            except Exception as e:
                self.logger.error(f"Error in user behavior producer: {e}")
                await asyncio.sleep(60)
        self.stop()


if __name__ == "__main__":
    def setup_topics():
        try:
            create_topic_if_not_exists(TOPICS['USER_BEHAVIOR'])
            return True
        except Exception as e:
            print(f"Error setting up topics: {e}")
            return False

    async def main():
        setup_topics()
        producer = UserBehaviorProducer()
        try:
            await producer.start_producing()
        except Exception as e:
            producer.stop()

    if setup_topics():
        asyncio.run(main())
    else:
        print("Failed to set up Kafka topics. Exiting.")
        sys.exit(1)
