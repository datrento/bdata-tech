"""
Spark Data Processing Pipeline for E-commerce Price Intelligence System
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, min, max, count, sum, window, expr, 
    lag, lead, when, lit, current_timestamp, to_timestamp,
    date_format, hour, dayofweek, month, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType, ArrayType
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

from data_models import PlatformType, ProductCategory
from database import DatabaseManager, MongoDBRepository


class SparkProcessor:
    """Spark-based data processing for price intelligence analytics"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(__name__)
        self.spark = None
        self.db_manager = DatabaseManager(config_path)
        self.mongo_repo = None
        
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
        """Initialize Spark session and database connections"""
        try:
            # Initialize Spark session
            self._initialize_spark()
            
            # Initialize database connections
            self.db_manager.connect_mongodb()
            self.mongo_repo = MongoDBRepository(self.db_manager)
            
            # Create schemas
            self._create_schemas()
            
            self.logger.info("Spark processor initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark processor: {e}")
            raise
    
    def _initialize_spark(self):
        """Initialize Spark session with configuration"""
        spark_config = self.config['spark']
        
        self.spark = SparkSession.builder \
            .appName(spark_config['app_name']) \
            .master(spark_config['master']) \
            .config("spark.sql.adaptive.enabled", spark_config['config']['spark.sql.adaptive.enabled']) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", spark_config['config']['spark.sql.adaptive.coalescePartitions.enabled']) \
            .config("spark.sql.adaptive.skewJoin.enabled", spark_config['config']['spark.sql.adaptive.skewJoin.enabled']) \
            .config("spark.sql.warehouse.dir", "./spark-warehouse") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        self.logger.info("Spark session created successfully")
    
    def _create_schemas(self):
        """Create Spark schemas for different data types"""
        
        # Competitor prices schema
        self.competitor_prices_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("platform", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("availability", BooleanType(), True),
            StructField("shipping_cost", DoubleType(), True),
            StructField("seller_rating", DoubleType(), True),
            StructField("seller_name", StringType(), True)
        ])
        
        # Price history schema
        self.price_history_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("reason", StringType(), True),
            StructField("competitor_avg_price", DoubleType(), True),
            StructField("demand_score", DoubleType(), True),
            StructField("inventory_level", IntegerType(), True)
        ])
        
        # User behavior schema
        self.user_behavior_schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("action", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("session_id", StringType(), False),
            StructField("platform", StringType(), False),
            StructField("price_at_time", DoubleType(), False),
            StructField("competitor_price_at_time", DoubleType(), True)
        ])
        
        # Products schema
        self.products_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("brand", StringType(), False),
            StructField("sku", StringType(), False),
            StructField("current_price", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("inventory_level", IntegerType(), True),
            StructField("min_price", DoubleType(), True),
            StructField("max_price", DoubleType(), True),
            StructField("cost_price", DoubleType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("tags", ArrayType(StringType()), True)
        ])
    
    def load_data_from_mongodb(self) -> Dict[str, DataFrame]:
        """Load data from MongoDB into Spark DataFrames"""
        try:
            # Load competitor prices
            competitor_prices_df = self._load_competitor_prices()
            
            # Load price history
            price_history_df = self._load_price_history()
            
            # Load user behavior
            user_behavior_df = self._load_user_behavior()
            
            # Load products
            products_df = self._load_products()
            
            return {
                'competitor_prices': competitor_prices_df,
                'price_history': price_history_df,
                'user_behavior': user_behavior_df,
                'products': products_df
            }
            
        except Exception as e:
            self.logger.error(f"Error loading data from MongoDB: {e}")
            raise
    
    def _load_competitor_prices(self) -> DataFrame:
        """Load competitor prices data"""
        collection = self.db_manager.mongo_db[MongoDBCollections.COMPETITOR_PRICES]
        data = list(collection.find({}, {'_id': 0}))
        
        if not data:
            # Create empty DataFrame with schema
            return self.spark.createDataFrame([], self.competitor_prices_schema)
        
        return self.spark.createDataFrame(data, self.competitor_prices_schema)
    
    def _load_price_history(self) -> DataFrame:
        """Load price history data"""
        collection = self.db_manager.mongo_db[MongoDBCollections.PRICE_HISTORY]
        data = list(collection.find({}, {'_id': 0}))
        
        if not data:
            return self.spark.createDataFrame([], self.price_history_schema)
        
        return self.spark.createDataFrame(data, self.price_history_schema)
    
    def _load_user_behavior(self) -> DataFrame:
        """Load user behavior data"""
        collection = self.db_manager.mongo_db[MongoDBCollections.USER_BEHAVIOR]
        data = list(collection.find({}, {'_id': 0}))
        
        if not data:
            return self.spark.createDataFrame([], self.user_behavior_schema)
        
        return self.spark.createDataFrame(data, self.user_behavior_schema)
    
    def _load_products(self) -> DataFrame:
        """Load products data"""
        collection = self.db_manager.mongo_db[MongoDBCollections.PRODUCTS]
        data = list(collection.find({}, {'_id': 0, 'competitor_prices': 0}))
        
        if not data:
            return self.spark.createDataFrame([], self.products_schema)
        
        return self.spark.createDataFrame(data, self.products_schema)
    
    def analyze_competitor_pricing_trends(self, competitor_prices_df: DataFrame) -> DataFrame:
        """Analyze competitor pricing trends"""
        try:
            # Calculate rolling averages and price changes
            window_spec = Window.partitionBy("product_id", "platform") \
                .orderBy("timestamp") \
                .rangeBetween(-timedelta(hours=24), 0)
            
            trends_df = competitor_prices_df \
                .withColumn("price_24h_avg", avg("price").over(window_spec)) \
                .withColumn("price_change_24h", 
                           col("price") - lag("price", 1).over(Window.partitionBy("product_id", "platform").orderBy("timestamp"))) \
                .withColumn("price_change_pct_24h", 
                           (col("price") - lag("price", 1).over(Window.partitionBy("product_id", "platform").orderBy("timestamp"))) / 
                           lag("price", 1).over(Window.partitionBy("product_id", "platform").orderBy("timestamp")) * 100) \
                .withColumn("hour_of_day", hour("timestamp")) \
                .withColumn("day_of_week", dayofweek("timestamp")) \
                .withColumn("month", month("timestamp"))
            
            return trends_df
            
        except Exception as e:
            self.logger.error(f"Error analyzing competitor pricing trends: {e}")
            raise
    
    def calculate_demand_metrics(self, user_behavior_df: DataFrame) -> DataFrame:
        """Calculate demand metrics from user behavior"""
        try:
            # Window for time-based aggregations
            time_window = Window.partitionBy("product_id") \
                .orderBy("timestamp") \
                .rangeBetween(-timedelta(hours=24), 0)
            
            # Calculate demand metrics
            demand_metrics = user_behavior_df \
                .withColumn("hour", hour("timestamp")) \
                .withColumn("day_of_week", dayofweek("timestamp")) \
                .groupBy("product_id", "hour", "day_of_week") \
                .agg(
                    count(when(col("action") == "view", True)).alias("view_count"),
                    count(when(col("action") == "add_to_cart", True)).alias("add_to_cart_count"),
                    count(when(col("action") == "purchase", True)).alias("purchase_count"),
                    avg("price_at_time").alias("avg_price_at_time")
                ) \
                .withColumn("conversion_rate", 
                           when(col("view_count") > 0, col("purchase_count") / col("view_count")).otherwise(0)) \
                .withColumn("cart_to_purchase_rate",
                           when(col("add_to_cart_count") > 0, col("purchase_count") / col("add_to_cart_count")).otherwise(0))
            
            return demand_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating demand metrics: {e}")
            raise
    
    def analyze_price_elasticity(self, price_history_df: DataFrame, user_behavior_df: DataFrame) -> DataFrame:
        """Analyze price elasticity of demand"""
        try:
            # Join price history with user behavior
            elasticity_df = price_history_df \
                .join(user_behavior_df, "product_id") \
                .where(col("user_behavior.timestamp") >= col("price_history.timestamp") - expr("INTERVAL 1 HOUR")) \
                .where(col("user_behavior.timestamp") <= col("price_history.timestamp") + expr("INTERVAL 1 HOUR")) \
                .groupBy("price_history.product_id", "price_history.price") \
                .agg(
                    count("user_behavior.user_id").alias("demand_volume"),
                    avg("user_behavior.price_at_time").alias("avg_competitor_price")
                ) \
                .orderBy("product_id", "price")
            
            # Calculate price elasticity
            window_spec = Window.partitionBy("product_id").orderBy("price")
            
            elasticity_df = elasticity_df \
                .withColumn("prev_price", lag("price", 1).over(window_spec)) \
                .withColumn("prev_demand", lag("demand_volume", 1).over(window_spec)) \
                .withColumn("price_change_pct", 
                           when(col("prev_price").isNotNull(), 
                                (col("price") - col("prev_price")) / col("prev_price") * 100).otherwise(0)) \
                .withColumn("demand_change_pct",
                           when(col("prev_demand").isNotNull(),
                                (col("demand_volume") - col("prev_demand")) / col("prev_demand") * 100).otherwise(0)) \
                .withColumn("price_elasticity",
                           when(col("price_change_pct") != 0, 
                                col("demand_change_pct") / col("price_change_pct")).otherwise(0))
            
            return elasticity_df
            
        except Exception as e:
            self.logger.error(f"Error analyzing price elasticity: {e}")
            raise
    
    def detect_price_anomalies(self, competitor_prices_df: DataFrame) -> DataFrame:
        """Detect price anomalies using statistical methods"""
        try:
            # Calculate statistical measures for each product-platform combination
            stats_df = competitor_prices_df \
                .groupBy("product_id", "platform") \
                .agg(
                    avg("price").alias("mean_price"),
                    expr("stddev(price)").alias("std_price"),
                    min("price").alias("min_price"),
                    max("price").alias("max_price")
                )
            
            # Join with original data to detect anomalies
            anomalies_df = competitor_prices_df \
                .join(stats_df, ["product_id", "platform"]) \
                .withColumn("z_score", 
                           when(col("std_price") > 0, 
                                (col("price") - col("mean_price")) / col("std_price")).otherwise(0)) \
                .withColumn("is_anomaly", 
                           when(abs(col("z_score")) > 2, True).otherwise(False)) \
                .withColumn("anomaly_type",
                           when(col("z_score") > 2, "high_price") \
                           .when(col("z_score") < -2, "low_price") \
                           .otherwise("normal"))
            
            return anomalies_df
            
        except Exception as e:
            self.logger.error(f"Error detecting price anomalies: {e}")
            raise
    
    def generate_pricing_recommendations(self, 
                                       competitor_prices_df: DataFrame,
                                       demand_metrics_df: DataFrame,
                                       products_df: DataFrame) -> DataFrame:
        """Generate pricing recommendations based on analysis"""
        try:
            # Get latest competitor prices
            latest_competitor_prices = competitor_prices_df \
                .groupBy("product_id") \
                .agg(
                    avg("price").alias("avg_competitor_price"),
                    min("price").alias("min_competitor_price"),
                    max("price").alias("max_competitor_price"),
                    count("platform").alias("competitor_count")
                )
            
            # Get latest demand metrics
            latest_demand = demand_metrics_df \
                .groupBy("product_id") \
                .agg(
                    sum("view_count").alias("total_views"),
                    sum("purchase_count").alias("total_purchases"),
                    avg("conversion_rate").alias("avg_conversion_rate")
                )
            
            # Join all data
            recommendations_df = products_df \
                .join(latest_competitor_prices, "product_id", "left") \
                .join(latest_demand, "product_id", "left") \
                .withColumn("competitor_price_gap", 
                           col("current_price") - col("avg_competitor_price")) \
                .withColumn("price_position", 
                           when(col("current_price") < col("min_competitor_price"), "lowest") \
                           .when(col("current_price") > col("max_competitor_price"), "highest") \
                           .otherwise("middle")) \
                .withColumn("demand_score",
                           when(col("total_views") > 100, col("avg_conversion_rate") * 100).otherwise(0)) \
                .withColumn("recommended_price",
                           when(col("price_position") == "highest" and col("demand_score") < 5,
                                col("avg_competitor_price") * 0.95) \
                           .when(col("price_position") == "lowest" and col("demand_score") > 10,
                                 col("avg_competitor_price") * 1.05) \
                           .otherwise(col("current_price"))) \
                .withColumn("price_change_recommended",
                           col("recommended_price") - col("current_price")) \
                .withColumn("recommendation_reason",
                           when(col("price_position") == "highest" and col("demand_score") < 5,
                                "High price with low demand - consider lowering") \
                           .when(col("price_position") == "lowest" and col("demand_score") > 10,
                                 "Low price with high demand - consider raising") \
                           .otherwise("Price is competitive"))
            
            return recommendations_df
            
        except Exception as e:
            self.logger.error(f"Error generating pricing recommendations: {e}")
            raise
    
    def train_pricing_model(self, features_df: DataFrame) -> Any:
        """Train machine learning model for price prediction"""
        try:
            # Prepare features
            feature_cols = self.config['pricing']['model']['features']
            
            # Create feature vector
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # Scale features
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
            
            # Train model
            rf = RandomForestRegressor(
                featuresCol="scaled_features",
                labelCol="current_price",
                numTrees=10,
                maxDepth=5
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler, rf])
            
            # Train model
            model = pipeline.fit(features_df)
            
            self.logger.info("Pricing model trained successfully")
            return model
            
        except Exception as e:
            self.logger.error(f"Error training pricing model: {e}")
            raise
    
    def run_batch_analysis(self):
        """Run complete batch analysis pipeline"""
        try:
            self.logger.info("Starting batch analysis...")
            
            # Load data
            dataframes = self.load_data_from_mongodb()
            
            # Run analyses
            competitor_trends = self.analyze_competitor_pricing_trends(dataframes['competitor_prices'])
            demand_metrics = self.calculate_demand_metrics(dataframes['user_behavior'])
            price_elasticity = self.analyze_price_elasticity(dataframes['price_history'], dataframes['user_behavior'])
            price_anomalies = self.detect_price_anomalies(dataframes['competitor_prices'])
            pricing_recommendations = self.generate_pricing_recommendations(
                dataframes['competitor_prices'],
                demand_metrics,
                dataframes['products']
            )
            
            # Save results
            self._save_analysis_results({
                'competitor_trends': competitor_trends,
                'demand_metrics': demand_metrics,
                'price_elasticity': price_elasticity,
                'price_anomalies': price_anomalies,
                'pricing_recommendations': pricing_recommendations
            })
            
            self.logger.info("Batch analysis completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in batch analysis: {e}")
            raise
    
    def _save_analysis_results(self, results: Dict[str, DataFrame]):
        """Save analysis results to MongoDB"""
        try:
            for analysis_name, df in results.items():
                # Convert to pandas for easier MongoDB storage
                pandas_df = df.toPandas()
                
                # Convert to JSON-serializable format
                records = pandas_df.to_dict('records')
                
                # Store in MongoDB
                collection_name = f"analysis_{analysis_name}"
                collection = self.db_manager.mongo_db[collection_name]
                
                # Clear existing data and insert new
                collection.delete_many({})
                if records:
                    collection.insert_many(records)
                
                self.logger.info(f"Saved {len(records)} records for {analysis_name}")
                
        except Exception as e:
            self.logger.error(f"Error saving analysis results: {e}")
            raise
    
    def close(self):
        """Close Spark session and database connections"""
        if self.spark:
            self.spark.stop()
        if self.db_manager:
            self.db_manager.close_connections()
        self.logger.info("Spark processor closed") 