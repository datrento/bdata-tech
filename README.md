# E-commerce Price Intelligence System

A comprehensive big data system for automated e-commerce price intelligence, competitor monitoring, and dynamic pricing optimization.

## 🚀 Features

- **Real-time Data Ingestion**: Collect competitor price data from multiple e-commerce platforms
- **Streaming Analytics**: Process data using Apache Kafka and Apache Spark
- **Dynamic Pricing**: ML-powered pricing algorithm that adjusts prices based on market conditions
- **Market Intelligence**: Analyze pricing trends, demand patterns, and competitor behavior
- **Real-time Dashboard**: Interactive visualization of market shifts and pricing decisions
- **Scalable Architecture**: Built with microservices and containerized deployment

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Scrapers  │    │   User Behavior │    │   Price APIs    │
│   (Amazon, eBay,│    │   Tracking      │    │   & Aggregators │
│    Walmart, etc)│    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │      Apache Kafka         │
                    │   (Stream Processing)     │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │      Apache Spark         │
                    │   (Batch & Streaming)     │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │      MongoDB              │
                    │   (Data Storage)          │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Dynamic Pricing Engine  │
                    │   (ML Models)             │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Real-time Dashboard     │
                    │   (Dash + Plotly)         │
                    └───────────────────────────┘
```

## 📋 Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Java 11+ (for Spark)
- 8GB+ RAM (recommended)
- 20GB+ disk space

## 🛠️ Installation

### Option 1: Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ecommerce-price-intelligence
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```

3. **Access the dashboard**
   - Main Dashboard: http://localhost:8050
   - Kafka UI: http://localhost:8080
   - MongoDB: localhost:27017
   - PostgreSQL: localhost:5432

### Option 2: Local Development

1. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install and start MongoDB**
   ```bash
   # On Ubuntu/Debian
   sudo apt-get install mongodb
   sudo systemctl start mongodb
   
   # On macOS
   brew install mongodb-community
   brew services start mongodb-community
   ```

3. **Install and start Kafka**
   ```bash
   # Download Kafka
   wget https://downloads.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz
   tar -xzf kafka_2.13-3.4.1.tgz
   cd kafka_2.13-3.4.1
   
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties &
   
   # Start Kafka
   bin/kafka-server-start.sh config/server.properties &
   ```

4. **Run the application**
   ```bash
   python main.py --demo
   ```

## 📊 System Components

### 1. Data Ingestion Layer
- **Web Scrapers**: Collect competitor prices from Amazon, eBay, Walmart
- **Kafka Producers**: Stream real-time data to Kafka topics
- **User Behavior Tracking**: Monitor customer interactions and demand signals

### 2. Data Processing Layer
- **Apache Spark**: Batch and streaming analytics
- **Price Trend Analysis**: Historical pricing patterns
- **Demand Analytics**: User behavior and conversion metrics
- **Anomaly Detection**: Identify unusual price movements

### 3. Dynamic Pricing Engine
- **ML Models**: XGBoost, Random Forest for price prediction
- **Market Signals**: Real-time alerts for price changes
- **Business Rules**: Margin constraints and competitive positioning
- **A/B Testing**: Validate pricing strategies

### 4. Dashboard & Visualization
- **Real-time Charts**: Price trends, competitor analysis
- **Market Signals**: Alert management and monitoring
- **Pricing Decisions**: Historical decision tracking
- **Performance Metrics**: Revenue impact and ROI analysis

## 🔧 Configuration

The system is configured via `config/config.yaml`:

```yaml
# Kafka Configuration
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    price_updates: "price-updates"
    competitor_data: "competitor-data"
    user_behavior: "user-behavior"

# Database Configuration
database:
  mongodb:
    uri: "mongodb://localhost:27017/"
    database: "price_intelligence"

# Dynamic Pricing
pricing:
  update_frequency_minutes: 15
  min_price_margin: 0.05
  max_price_margin: 0.30
```

## 📈 Usage Examples

### 1. Monitor Competitor Prices
```python
from src.data_ingestion import DataIngestionManager

# Initialize data ingestion
ingestion = DataIngestionManager()
ingestion.initialize()
ingestion.start_data_collection()
```

### 2. Run Dynamic Pricing
```python
from src.dynamic_pricing import DynamicPricingEngine

# Initialize pricing engine
pricing_engine = DynamicPricingEngine()
pricing_engine.initialize()

# Make pricing decision for a product
decision = pricing_engine.make_pricing_decision("PROD001")
```

### 3. Analyze Market Trends
```python
from src.spark_processor import SparkProcessor

# Initialize Spark processor
spark_processor = SparkProcessor()
spark_processor.initialize()

# Run batch analysis
spark_processor.run_batch_analysis()
```

## 📊 Dashboard Features

### Key Metrics
- **Total Products**: Number of monitored products
- **Price Changes**: Daily pricing decisions
- **Competitor Analysis**: Average competitor prices
- **Market Signals**: Active alerts and notifications

### Interactive Charts
- **Price Trends**: Historical price movements
- **Competitor Analysis**: Price distribution by platform
- **Demand Metrics**: User behavior patterns
- **Market Signals**: Real-time alerts

### Data Tables
- **Pricing Decisions**: Recent price changes with reasons
- **Market Signals**: Active alerts and their severity

## 🔍 Monitoring & Alerts

### Market Signals
- **Competitor Price Drops**: When competitors lower prices significantly
- **Demand Surges**: Unusual increase in user interest
- **Inventory Alerts**: Low stock warnings
- **Price Anomalies**: Statistical outliers in pricing

### Alert Configuration
```yaml
alerts:
  price_change_threshold: 0.10  # 10% price change
  competitor_price_drop_threshold: 0.15  # 15% competitor drop
  low_inventory_threshold: 10  # units
```

## 🧪 Testing

### Unit Tests
```bash
pytest tests/ -v
```

### Integration Tests
```bash
pytest tests/integration/ -v
```

### Performance Tests
```bash
python tests/performance/test_throughput.py
```

## 📚 API Documentation

### REST API Endpoints

#### Products
- `GET /api/products` - List all products
- `GET /api/products/{id}` - Get product details
- `POST /api/products` - Create new product
- `PUT /api/products/{id}/price` - Update product price

#### Pricing Decisions
- `GET /api/decisions` - List pricing decisions
- `GET /api/decisions/{id}` - Get decision details
- `POST /api/decisions` - Create manual decision

#### Market Signals
- `GET /api/signals` - List market signals
- `GET /api/signals/{id}` - Get signal details
- `PUT /api/signals/{id}/acknowledge` - Acknowledge signal

## 🚀 Deployment

### Production Deployment

1. **Environment Setup**
   ```bash
   # Set environment variables
   export MONGODB_URI=mongodb://prod-mongo:27017/
   export KAFKA_BOOTSTRAP_SERVERS=prod-kafka:9092
   export SPARK_MASTER=spark://prod-spark-master:7077
   ```

2. **Kubernetes Deployment**
   ```bash
   kubectl apply -f k8s/
   ```

3. **Monitoring Setup**
   ```bash
   # Prometheus metrics
   kubectl apply -f monitoring/
   ```

### Scaling

- **Horizontal Scaling**: Add more Spark executors
- **Vertical Scaling**: Increase memory and CPU resources
- **Database Scaling**: MongoDB replica sets and sharding
- **Kafka Scaling**: Multiple brokers and partitions

## 🔒 Security

### Authentication & Authorization
- JWT-based authentication
- Role-based access control (RBAC)
- API key management for external integrations

### Data Protection
- Encryption at rest and in transit
- PII data anonymization
- GDPR compliance features

### Network Security
- VPC isolation
- Firewall rules
- SSL/TLS encryption

## 📈 Performance Optimization

### Database Optimization
- MongoDB indexes for query performance
- Connection pooling
- Read replicas for analytics

### Spark Optimization
- Memory tuning
- Partition optimization
- Caching strategies

### Caching Strategy
- Redis for session data
- In-memory caching for frequently accessed data
- CDN for static assets

## 🐛 Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   ```bash
   # Check Kafka status
   docker-compose logs kafka
   
   # Test connection
   kafka-console-consumer --bootstrap-server localhost:9092 --topic price-updates
   ```

2. **MongoDB Connection Issues**
   ```bash
   # Check MongoDB status
   docker-compose logs mongodb
   
   # Test connection
   mongo mongodb://localhost:27017/price_intelligence
   ```

3. **Spark Job Failures**
   ```bash
   # Check Spark logs
   docker-compose logs price_intelligence_app
   
   # Monitor Spark UI
   http://localhost:4040
   ```

### Log Analysis
```bash
# View application logs
tail -f logs/price_intelligence.log

# Search for errors
grep "ERROR" logs/price_intelligence.log

# Monitor system resources
docker stats
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run linting
flake8 src/ tests/

# Run type checking
mypy src/

# Run tests with coverage
pytest --cov=src tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Spark for distributed computing
- Apache Kafka for stream processing
- MongoDB for flexible data storage
- Dash and Plotly for interactive visualizations
- Scikit-learn and XGBoost for machine learning

## 📞 Support

For support and questions:
- Create an issue on GitHub
- Email: support@priceintelligence.com
- Documentation: https://docs.priceintelligence.com

---

**Note**: This is a prototype system. For production use, additional security, monitoring, and scalability features should be implemented. 