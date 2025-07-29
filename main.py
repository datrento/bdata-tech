"""
Main Application for E-commerce Price Intelligence System
"""

import logging
import time
import signal
import sys
import threading
from datetime import datetime
import argparse
import yaml
import os

from src.data_ingestion import DataIngestionManager
from src.spark_processor import SparkProcessor
from src.dynamic_pricing import DynamicPricingEngine
from src.dashboard import PriceIntelligenceDashboard
from src.database import DatabaseManager


class PriceIntelligenceSystem:
    """Main orchestrator for the E-commerce Price Intelligence System"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Initialize components
        self.data_ingestion = None
        self.spark_processor = None
        self.pricing_engine = None
        self.dashboard = None
        self.db_manager = None
        
        # System state
        self.running = False
        self.components = []
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"Configuration file not found: {config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        log_config = self.config['logging']
        
        # Create logs directory
        os.makedirs(os.path.dirname(log_config['file']), exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_config['file']),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        return logging.getLogger(__name__)
    
    def initialize(self):
        """Initialize all system components"""
        try:
            self.logger.info("Initializing E-commerce Price Intelligence System...")
            
            # Initialize database manager
            self.db_manager = DatabaseManager(self.config_path)
            self.db_manager.connect_mongodb()
            self.components.append(self.db_manager)
            
            # Initialize data ingestion
            self.data_ingestion = DataIngestionManager(self.config_path)
            self.data_ingestion.initialize()
            self.components.append(self.data_ingestion)
            
            # Initialize Spark processor
            self.spark_processor = SparkProcessor(self.config_path)
            self.spark_processor.initialize()
            self.components.append(self.spark_processor)
            
            # Initialize dynamic pricing engine
            self.pricing_engine = DynamicPricingEngine(self.config_path)
            self.pricing_engine.initialize()
            self.components.append(self.pricing_engine)
            
            # Initialize dashboard
            self.dashboard = PriceIntelligenceDashboard(self.config_path)
            self.dashboard.initialize()
            self.components.append(self.dashboard)
            
            self.logger.info("All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize system: {e}")
            self.cleanup()
            raise
    
    def start(self):
        """Start all system components"""
        try:
            self.logger.info("Starting E-commerce Price Intelligence System...")
            self.running = True
            
            # Start data ingestion
            self.logger.info("Starting data ingestion...")
            self.data_ingestion.start_data_collection()
            
            # Start Spark processing in background
            self.logger.info("Starting Spark processing...")
            spark_thread = threading.Thread(target=self._run_spark_processing, daemon=True)
            spark_thread.start()
            
            # Start pricing engine in background
            self.logger.info("Starting pricing engine...")
            pricing_thread = threading.Thread(target=self._run_pricing_engine, daemon=True)
            pricing_thread.start()
            
            # Start dashboard
            self.logger.info("Starting dashboard...")
            dashboard_thread = threading.Thread(target=self._run_dashboard, daemon=True)
            dashboard_thread.start()
            
            self.logger.info("System started successfully")
            self.logger.info("Dashboard available at: http://localhost:8050")
            
            # Keep main thread alive
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, shutting down...")
                self.stop()
                
        except Exception as e:
            self.logger.error(f"Error starting system: {e}")
            self.stop()
            raise
    
    def _run_spark_processing(self):
        """Run Spark processing in background"""
        try:
            while self.running:
                try:
                    self.logger.info("Running Spark batch analysis...")
                    self.spark_processor.run_batch_analysis()
                    
                    # Wait for next analysis cycle
                    time.sleep(3600)  # Run every hour
                    
                except Exception as e:
                    self.logger.error(f"Error in Spark processing: {e}")
                    time.sleep(300)  # Wait 5 minutes before retrying
                    
        except Exception as e:
            self.logger.error(f"Spark processing thread error: {e}")
    
    def _run_pricing_engine(self):
        """Run pricing engine in background"""
        try:
            while self.running:
                try:
                    self.logger.info("Running pricing cycle...")
                    self.pricing_engine.run_pricing_cycle()
                    
                    # Wait for next pricing cycle
                    update_frequency = self.config['pricing']['update_frequency_minutes']
                    time.sleep(update_frequency * 60)
                    
                except Exception as e:
                    self.logger.error(f"Error in pricing engine: {e}")
                    time.sleep(300)  # Wait 5 minutes before retrying
                    
        except Exception as e:
            self.logger.error(f"Pricing engine thread error: {e}")
    
    def _run_dashboard(self):
        """Run dashboard in background"""
        try:
            self.dashboard.start()
        except Exception as e:
            self.logger.error(f"Dashboard error: {e}")
    
    def stop(self):
        """Stop all system components"""
        try:
            self.logger.info("Stopping E-commerce Price Intelligence System...")
            self.running = False
            
            # Stop components in reverse order
            for component in reversed(self.components):
                try:
                    if hasattr(component, 'stop_data_collection'):
                        component.stop_data_collection()
                    elif hasattr(component, 'close'):
                        component.close()
                    elif hasattr(component, 'close_connections'):
                        component.close_connections()
                except Exception as e:
                    self.logger.error(f"Error stopping component {component.__class__.__name__}: {e}")
            
            self.logger.info("System stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping system: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.stop()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def signal_handler(signum, frame):
    """Handle system signals for graceful shutdown"""
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    if hasattr(signal_handler, 'system'):
        signal_handler.system.stop()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='E-commerce Price Intelligence System')
    parser.add_argument('--config', default='config/config.yaml', 
                       help='Path to configuration file')
    parser.add_argument('--demo', action='store_true',
                       help='Run in demo mode with sample data')
    
    args = parser.parse_args()
    
    # Create system instance
    system = PriceIntelligenceSystem(args.config)
    
    # Store reference for signal handler
    signal_handler.system = system
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize system
        system.initialize()
        
        # Start system
        system.start()
        
    except Exception as e:
        print(f"Fatal error: {e}")
        system.cleanup()
        sys.exit(1)
    
    finally:
        system.cleanup()


if __name__ == "__main__":
    main() 