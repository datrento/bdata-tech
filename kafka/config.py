# Kafka producer configuration
# When running in Docker, use the internal network address
# When running locally, use localhost
import os

bootstrap_servers = os.getenv(
    'KAFKA_BOOTSTRAP_SERVER_EXTERNAL', 'localhost:9092')

config = {
    'bootstrap.servers': bootstrap_servers,  # Will use environment variable if set
    'security.protocol': 'PLAINTEXT',        # Using plaintext for local development

    # Producer configuration
    'acks': 'all',                          # Wait for all replicas to acknowledge
    'retries': 3,                           # Retry sending messages up to 3 times
    'batch.size': 16384,                    # Batch size in bytes
    'linger.ms': 5,                         # Wait up to 5ms for more messages
    'compression.type': 'lz4',              # Use LZ4 compression for efficiency
}

# Schema Registry configuration (commented out for local development)
# Uncomment and configure if using Schema Registry
# sr_config = {
#     'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
#     'basic.auth.user.info': os.getenv('SCHEMA_REGISTRY_AUTH', '')
# }

# API Endpoints
BASE_API = os.getenv('DATA_API', 'http://localhost:8000/api')
DATA_API_AGGREGATOR = f"{BASE_API}/aggregator/data"
DATA_API_USER_BEHAVIOR = f"{BASE_API}/user-behavior"

# Collection intervals

COLLECTION_INTERVAL = int(
    os.getenv('COLLECTION_INTERVAL', 300))  # Default to 5 minutes
USER_BEHAVIOR_INTERVAL = int(
    os.getenv('USER_BEHAVIOR_INTERVAL', 60))  # Default to 1 minute

PRODUCTS_MONITORING = [
    "IPHONE-15-PRO-128",
    "MACBOOK-AIR-M2-13",
    "PS5-CONSOLE"
]

# Topics
TOPICS = {
    'MARKET_DATA_RAW': 'market-data-raw',
    'USER_BEHAVIOR': 'user-behavior-events',
    'COMPETITOR_PRICES': 'competitor-prices',
    'AGGREGATOR_PRICES': 'aggregator-prices',
    # Add more topics as needed
}
