import json
import logging
from confluent_kafka import Producer
from datetime import datetime
from abc import ABC, abstractmethod
import time
import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

class BaseProducer(ABC):
    """Abstract base class for Kafka producers."""

    def __init__(self, producer_name: str, custom_config: dict = None) -> None:

        producer_config = {
            **config,
            'client.id': f'{producer_name}-producer',
        }

        if custom_config:
            producer_config.update(custom_config)

        self.producer = Producer(producer_config)
        self.producer_name = producer_name
        self.logger = self._setup_logger()
        self.running = False
        self.message_count = 0

    def _setup_logger(self) -> logging.Logger:
        """Setup logger for the producer."""

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        return logging.getLogger(f'{self.producer_name}Producer')

    def _delivery_callback(self, err, msg) -> None:
        """Callback for message delivery confirmation."""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.message_count += 1
            self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] with key {msg.key()} at offset {msg.offset()}')
    
    def send_message(self, topic: str, message: dict, key: str = None) -> None:
        """ Send message to kafka topic with common enrichment """
        try:
            # Add common producer metadata
            enriched_message = {
                **message,
                'producer_timestamp': datetime.now().isoformat(),
                'producer_id': f'{self.producer_name}-producer',
                'message_id': f'{topic}_{self.message_count}'  # optional unique ID for tracking we can drop later
            }

            self.producer.produce(
                topic=topic,
                value=json.dumps(enriched_message, default=str).encode('utf-8'),
                key=key.encode('utf-8') if key else None,
                callback=self._delivery_callback
            )

            # Trigger delivery reports
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            self.producer.poll(1)

        except Exception as e:
            self.logger.error(f"Error sending message to {topic}: {e}")
            raise

    def stop(self) -> None:
        """Stop the producer gracefully."""
        self.running = False
        self.logger.info("Flushing pending messages ...")
        
        remaining_messages = self.producer.flush(timeout=10)
        if remaining_messages:
            self.logger.warning(f"Some messages were not delivered: {remaining_messages}")
        
        self.logger.info(f"{self.producer_name} Producer stopped. Total messages: {self.message_count}")

    def get_status(self) -> dict:
        """Get the status of the producer."""
        return {
            'producer_name': self.producer_name,
            'is_running': self.running,
            'messages_delivered': self.message_count
        }
    
    @abstractmethod
    async def start_producing(self, product_sku: str) -> None:
        """Abstract method to start producing messages."""
        pass

    @abstractmethod
    async def process_product(self, product_sku: str) -> None:
        """Abstract method to process a product and send messages."""
        pass
