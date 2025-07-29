"""
Dynamic Pricing Algorithm for E-commerce Price Intelligence System
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import joblib
import json
import yaml
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
from scipy import stats

from data_models import (
    Product, CompetitorPrice, PriceHistory, PricingDecision, 
    MarketSignal, DemandMetrics, PlatformType
)
from database import DatabaseManager, MongoDBRepository


class DynamicPricingEngine:
    """Dynamic pricing engine that adjusts prices based on market conditions"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager(config_path)
        self.mongo_repo = None
        self.pricing_model = None
        self.scaler = None
        self.model_version = "1.0"
        
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
        """Initialize the pricing engine"""
        try:
            # Initialize database connections
            self.db_manager.connect_mongodb()
            self.mongo_repo = MongoDBRepository(self.db_manager)
            
            # Load or train pricing model
            self._load_or_train_model()
            
            self.logger.info("Dynamic pricing engine initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize pricing engine: {e}")
            raise
    
    def _load_or_train_model(self):
        """Load existing model or train a new one"""
        try:
            model_path = "models/pricing_model.pkl"
            scaler_path = "models/scaler.pkl"
            
            # Try to load existing model
            try:
                self.pricing_model = joblib.load(model_path)
                self.scaler = joblib.load(scaler_path)
                self.logger.info("Loaded existing pricing model")
            except FileNotFoundError:
                self.logger.info("No existing model found, training new model...")
                self._train_pricing_model()
                
        except Exception as e:
            self.logger.error(f"Error loading/training model: {e}")
            raise
    
    def _train_pricing_model(self):
        """Train the pricing prediction model"""
        try:
            # Get historical data for training
            training_data = self._prepare_training_data()
            
            if training_data.empty:
                self.logger.warning("No training data available, using default model")
                self._create_default_model()
                return
            
            # Prepare features and target
            feature_cols = self.config['pricing']['model']['features']
            X = training_data[feature_cols].fillna(0)
            y = training_data['optimal_price']
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model based on configuration
            model_type = self.config['pricing']['model']['type']
            
            if model_type == "xgboost":
                self.pricing_model = xgb.XGBRegressor(
                    n_estimators=100,
                    max_depth=6,
                    learning_rate=0.1,
                    random_state=42
                )
            elif model_type == "random_forest":
                self.pricing_model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42
                )
            else:
                self.pricing_model = GradientBoostingRegressor(
                    n_estimators=100,
                    max_depth=6,
                    learning_rate=0.1,
                    random_state=42
                )
            
            # Train model
            self.pricing_model.fit(X_train_scaled, y_train)
            
            # Evaluate model
            y_pred = self.pricing_model.predict(X_test_scaled)
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            self.logger.info(f"Model trained - MAE: {mae:.2f}, MSE: {mse:.2f}, R²: {r2:.3f}")
            
            # Save model
            self._save_model()
            
        except Exception as e:
            self.logger.error(f"Error training pricing model: {e}")
            raise
    
    def _create_default_model(self):
        """Create a default model when no training data is available"""
        self.pricing_model = RandomForestRegressor(n_estimators=10, random_state=42)
        self.scaler = StandardScaler()
        
        # Create dummy data for fitting
        dummy_X = np.random.rand(100, len(self.config['pricing']['model']['features']))
        dummy_y = np.random.uniform(50, 1000, 100)
        
        self.scaler.fit(dummy_X)
        self.pricing_model.fit(self.scaler.transform(dummy_X), dummy_y)
        
        self._save_model()
    
    def _prepare_training_data(self) -> pd.DataFrame:
        """Prepare training data from historical records"""
        try:
            # Get historical pricing decisions
            collection = self.db_manager.mongo_db[MongoDBCollections.PRICING_DECISIONS]
            decisions_data = list(collection.find({}, {'_id': 0}))
            
            if not decisions_data:
                return pd.DataFrame()
            
            decisions_df = pd.DataFrame(decisions_data)
            
            # Get competitor prices for the same time period
            competitor_data = []
            for _, decision in decisions_df.iterrows():
                competitor_prices = self.mongo_repo.get_competitor_prices(
                    decision['product_id'], 
                    hours=24
                )
                
                if competitor_prices:
                    avg_price = np.mean([cp.price for cp in competitor_prices])
                    min_price = np.min([cp.price for cp in competitor_prices])
                    max_price = np.max([cp.price for cp in competitor_prices])
                else:
                    avg_price = min_price = max_price = decision['old_price']
                
                competitor_data.append({
                    'product_id': decision['product_id'],
                    'timestamp': decision['timestamp'],
                    'competitor_avg_price': avg_price,
                    'competitor_min_price': min_price,
                    'competitor_max_price': max_price,
                    'old_price': decision['old_price'],
                    'new_price': decision['new_price'],
                    'optimal_price': decision['new_price']  # Use actual decision as target
                })
            
            if not competitor_data:
                return pd.DataFrame()
            
            training_df = pd.DataFrame(competitor_data)
            
            # Add time-based features
            training_df['timestamp'] = pd.to_datetime(training_df['timestamp'])
            training_df['time_of_day'] = training_df['timestamp'].dt.hour
            training_df['day_of_week'] = training_df['timestamp'].dt.dayofweek
            training_df['month'] = training_df['timestamp'].dt.month
            
            # Add demand features (simplified)
            training_df['demand_score'] = np.random.uniform(0, 1, len(training_df))
            training_df['inventory_level'] = np.random.randint(0, 100, len(training_df))
            training_df['seasonality_factor'] = np.random.uniform(0.8, 1.2, len(training_df))
            
            return training_df
            
        except Exception as e:
            self.logger.error(f"Error preparing training data: {e}")
            return pd.DataFrame()
    
    def _save_model(self):
        """Save the trained model"""
        try:
            import os
            os.makedirs("models", exist_ok=True)
            
            joblib.dump(self.pricing_model, "models/pricing_model.pkl")
            joblib.dump(self.scaler, "models/scaler.pkl")
            
            self.logger.info("Model saved successfully")
            
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
    
    def calculate_optimal_price(self, product_id: str) -> Tuple[float, Dict[str, Any]]:
        """Calculate optimal price for a product"""
        try:
            # Get current product information
            product = self.mongo_repo.get_product(product_id)
            if not product:
                raise ValueError(f"Product {product_id} not found")
            
            # Get competitor prices
            competitor_prices = self.mongo_repo.get_competitor_prices(product_id, hours=24)
            
            # Get demand metrics
            demand_metrics = self._get_demand_metrics(product_id)
            
            # Prepare features for prediction
            features = self._prepare_features(product, competitor_prices, demand_metrics)
            
            # Predict optimal price
            if self.pricing_model and self.scaler:
                features_scaled = self.scaler.transform([features])
                predicted_price = self.pricing_model.predict(features_scaled)[0]
            else:
                predicted_price = self._rule_based_pricing(product, competitor_prices, demand_metrics)
            
            # Apply business rules and constraints
            optimal_price = self._apply_pricing_constraints(
                predicted_price, product, competitor_prices
            )
            
            # Calculate confidence score
            confidence_score = self._calculate_confidence_score(
                product, competitor_prices, demand_metrics
            )
            
            # Prepare factors for decision tracking
            factors = {
                'competitor_avg_price': features[0],
                'competitor_min_price': features[1],
                'competitor_max_price': features[2],
                'demand_score': features[3],
                'inventory_level': features[4],
                'seasonality_factor': features[5],
                'time_of_day': features[6],
                'day_of_week': features[7],
                'predicted_price': predicted_price,
                'confidence_score': confidence_score
            }
            
            return optimal_price, factors
            
        except Exception as e:
            self.logger.error(f"Error calculating optimal price for {product_id}: {e}")
            raise
    
    def _prepare_features(self, product: Product, competitor_prices: List[CompetitorPrice], 
                         demand_metrics: Optional[DemandMetrics]) -> List[float]:
        """Prepare features for price prediction"""
        # Competitor price features
        if competitor_prices:
            competitor_prices_list = [cp.price for cp in competitor_prices]
            competitor_avg_price = np.mean(competitor_prices_list)
            competitor_min_price = np.min(competitor_prices_list)
            competitor_max_price = np.max(competitor_prices_list)
        else:
            competitor_avg_price = competitor_min_price = competitor_max_price = product.current_price
        
        # Demand features
        if demand_metrics:
            demand_score = demand_metrics.conversion_rate
            inventory_level = demand_metrics.purchase_count  # Simplified
        else:
            demand_score = 0.1  # Default low demand
            inventory_level = product.inventory_level
        
        # Time-based features
        now = datetime.utcnow()
        time_of_day = now.hour
        day_of_week = now.weekday()
        
        # Seasonality factor (simplified)
        seasonality_factor = 1.0 + 0.1 * np.sin(2 * np.pi * now.dayofyear / 365)
        
        return [
            competitor_avg_price,
            competitor_min_price,
            competitor_max_price,
            demand_score,
            inventory_level,
            seasonality_factor,
            time_of_day,
            day_of_week
        ]
    
    def _rule_based_pricing(self, product: Product, competitor_prices: List[CompetitorPrice],
                           demand_metrics: Optional[DemandMetrics]) -> float:
        """Rule-based pricing when ML model is not available"""
        if not competitor_prices:
            return product.current_price
        
        competitor_avg = np.mean([cp.price for cp in competitor_prices])
        competitor_min = np.min([cp.price for cp in competitor_prices])
        
        # Base price on competitor average
        base_price = competitor_avg
        
        # Adjust based on demand
        if demand_metrics and demand_metrics.conversion_rate > 0.05:
            # High demand - can charge premium
            base_price *= 1.05
        elif demand_metrics and demand_metrics.conversion_rate < 0.02:
            # Low demand - need to be competitive
            base_price *= 0.95
        
        # Adjust based on inventory
        if product.inventory_level < 10:
            # Low inventory - can charge more
            base_price *= 1.02
        elif product.inventory_level > 100:
            # High inventory - need to move product
            base_price *= 0.98
        
        return base_price
    
    def _apply_pricing_constraints(self, predicted_price: float, product: Product,
                                 competitor_prices: List[CompetitorPrice]) -> float:
        """Apply business rules and constraints to predicted price"""
        pricing_config = self.config['pricing']
        
        # Ensure minimum margin
        min_price = product.cost_price * (1 + pricing_config['min_price_margin'])
        
        # Ensure maximum margin
        max_price = product.cost_price * (1 + pricing_config['max_price_margin'])
        
        # Apply constraints
        constrained_price = max(min_price, min(max_price, predicted_price))
        
        # Ensure price is within reasonable range of competitors
        if competitor_prices:
            competitor_prices_list = [cp.price for cp in competitor_prices]
            competitor_avg = np.mean(competitor_prices_list)
            competitor_std = np.std(competitor_prices_list)
            
            # Don't price more than 2 standard deviations from competitor average
            max_competitive_price = competitor_avg + 2 * competitor_std
            min_competitive_price = max(0, competitor_avg - 2 * competitor_std)
            
            constrained_price = max(min_competitive_price, 
                                  min(max_competitive_price, constrained_price))
        
        return round(constrained_price, 2)
    
    def _calculate_confidence_score(self, product: Product, competitor_prices: List[CompetitorPrice],
                                  demand_metrics: Optional[DemandMetrics]) -> float:
        """Calculate confidence score for the pricing decision"""
        confidence = 0.5  # Base confidence
        
        # Increase confidence with more competitor data
        if len(competitor_prices) >= 3:
            confidence += 0.2
        elif len(competitor_prices) >= 1:
            confidence += 0.1
        
        # Increase confidence with demand data
        if demand_metrics:
            confidence += 0.1
        
        # Increase confidence if we have good inventory data
        if product.inventory_level > 0:
            confidence += 0.1
        
        # Decrease confidence if price change is large
        if abs(product.current_price - self._rule_based_pricing(product, competitor_prices, demand_metrics)) > 50:
            confidence -= 0.1
        
        return min(1.0, max(0.0, confidence))
    
    def _get_demand_metrics(self, product_id: str) -> Optional[DemandMetrics]:
        """Get demand metrics for a product"""
        try:
            # In a real system, this would query aggregated demand metrics
            # For demo purposes, we'll create mock data
            return DemandMetrics(
                product_id=product_id,
                timestamp=datetime.utcnow(),
                view_count=random.randint(10, 100),
                add_to_cart_count=random.randint(1, 20),
                purchase_count=random.randint(0, 5),
                conversion_rate=random.uniform(0.01, 0.1),
                search_volume=random.randint(5, 50),
                competitor_interest=random.uniform(0.1, 0.9),
                seasonality_factor=random.uniform(0.8, 1.2)
            )
        except Exception as e:
            self.logger.error(f"Error getting demand metrics: {e}")
            return None
    
    def make_pricing_decision(self, product_id: str) -> Optional[PricingDecision]:
        """Make a pricing decision for a product"""
        try:
            # Get current product
            product = self.mongo_repo.get_product(product_id)
            if not product:
                self.logger.error(f"Product {product_id} not found")
                return None
            
            # Calculate optimal price
            optimal_price, factors = self.calculate_optimal_price(product_id)
            
            # Check if price change is significant
            price_change_threshold = self.config['alerts']['price_change_threshold']
            price_change_pct = abs(optimal_price - product.current_price) / product.current_price
            
            if price_change_pct < price_change_threshold:
                self.logger.info(f"Price change for {product_id} below threshold ({price_change_pct:.2%})")
                return None
            
            # Create pricing decision
            decision = PricingDecision(
                product_id=product_id,
                old_price=product.current_price,
                new_price=optimal_price,
                timestamp=datetime.utcnow(),
                reason=self._determine_price_change_reason(factors),
                confidence_score=factors['confidence_score'],
                factors=factors,
                algorithm_version=self.model_version
            )
            
            # Store decision
            self.mongo_repo.insert_pricing_decision(decision)
            
            # Update product price
            self.mongo_repo.update_product(product_id, {
                'current_price': optimal_price,
                'updated_at': datetime.utcnow()
            })
            
            # Create market signal if significant change
            if price_change_pct > 0.15:  # 15% change
                self._create_market_signal(product_id, price_change_pct, factors)
            
            self.logger.info(f"Pricing decision made for {product_id}: {product.current_price} -> {optimal_price}")
            
            return decision
            
        except Exception as e:
            self.logger.error(f"Error making pricing decision for {product_id}: {e}")
            return None
    
    def _determine_price_change_reason(self, factors: Dict[str, Any]) -> str:
        """Determine the reason for price change"""
        competitor_avg = factors['competitor_avg_price']
        demand_score = factors['demand_score']
        inventory_level = factors['inventory_level']
        
        if demand_score > 0.05 and inventory_level < 20:
            return "high_demand_low_inventory"
        elif demand_score < 0.02:
            return "low_demand_competitive_pricing"
        elif competitor_avg < factors['old_price'] * 0.9:
            return "competitor_price_drop"
        elif competitor_avg > factors['old_price'] * 1.1:
            return "competitor_price_increase"
        else:
            return "market_adjustment"
    
    def _create_market_signal(self, product_id: str, price_change_pct: float, factors: Dict[str, Any]):
        """Create market signal for significant price changes"""
        signal = MarketSignal(
            signal_id=None,  # Will be auto-generated
            signal_type="significant_price_change",
            product_id=product_id,
            severity=min(1.0, price_change_pct),
            timestamp=datetime.utcnow(),
            description=f"Significant price change detected: {price_change_pct:.1%}",
            data=factors
        )
        
        self.mongo_repo.insert_market_signal(signal)
    
    def run_pricing_cycle(self):
        """Run a complete pricing cycle for all products"""
        try:
            self.logger.info("Starting pricing cycle...")
            
            # Get all products
            products = self._get_all_products()
            
            decisions_made = 0
            for product in products:
                try:
                    decision = self.make_pricing_decision(product.product_id)
                    if decision:
                        decisions_made += 1
                except Exception as e:
                    self.logger.error(f"Error processing product {product.product_id}: {e}")
            
            self.logger.info(f"Pricing cycle completed. Made {decisions_made} pricing decisions.")
            
        except Exception as e:
            self.logger.error(f"Error in pricing cycle: {e}")
            raise
    
    def _get_all_products(self) -> List[Product]:
        """Get all products from database"""
        try:
            collection = self.db_manager.mongo_db[MongoDBCollections.PRODUCTS]
            products_data = list(collection.find({}, {'_id': 0}))
            
            products = []
            for product_data in products_data:
                # Convert to Product object (simplified)
                product = Product(
                    product_id=product_data['product_id'],
                    name=product_data['name'],
                    category=product_data['category'],
                    brand=product_data['brand'],
                    sku=product_data['sku'],
                    current_price=product_data['current_price'],
                    inventory_level=product_data.get('inventory_level', 0),
                    cost_price=product_data.get('cost_price', 0.0)
                )
                products.append(product)
            
            return products
            
        except Exception as e:
            self.logger.error(f"Error getting products: {e}")
            return []
    
    def close(self):
        """Close database connections"""
        if self.db_manager:
            self.db_manager.close_connections()
        self.logger.info("Dynamic pricing engine closed") 