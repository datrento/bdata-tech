import os
from base_job import BaseJob

class PriceMonitoringJob(BaseJob):
    def __init__(self, job_name="Price Monitoring Job"):
        super().__init__(job_name=job_name)

    def setup_tables(self):
        """Create source and sink tables for price monitoring."""

        self.t_env.execute_sql(f"""
            CREATE TABLE competitor_prices (
                api_collection_timestamp STRING,
                data_timestamp STRING,
                product_sku STRING,
                competitor STRING,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                source_sku STRING,
                proc_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'competitor-prices',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = 'price_monitoring_group',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )""")
        
        self.t_env.execute_sql(f"""
            CREATE TABLE price_alerts (
                product_sku STRING,
                competitor_name STRING,
                previous_price DECIMAL(10,2),
                new_price DECIMAL(10,2),
                price_change DECIMAL(10,2),
                percentage_change DECIMAL(5,2),
                alert_type STRING,
                alert_timestamp TIMESTAMP(3)
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'price_alerts',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}'
            )""")

    def run_price_monitoring(self):
        """Fixed price monitoring logic using CTE"""
        
        monitoring_query = """
            INSERT INTO price_alerts
            WITH price_changes AS (
                SELECT 
                    product_sku,
                    competitor,
                    price,
                    LAG(price, 1) OVER (
                        PARTITION BY product_sku, competitor 
                        ORDER BY proc_time
                    ) as previous_price
                FROM competitor_prices
            )
            SELECT 
                product_sku,
                competitor as competitor_name,
                previous_price,
                price as new_price,
                price - previous_price as price_change,
                CAST(
                    (price - previous_price) / previous_price * 100 
                    AS DECIMAL(5,2)
                ) as percentage_change,
                CASE 
                    WHEN price < previous_price THEN 'price_decrease'
                    ELSE 'price_increase'
                END as alert_type,
                CURRENT_TIMESTAMP as alert_timestamp
            FROM price_changes
            WHERE previous_price IS NOT NULL
              AND ABS((price - previous_price) / previous_price * 100) >= 5.0
        """
        
        table_result = self.t_env.execute_sql(monitoring_query)
        print("🔍 Price monitoring started with Table API")
        print("📊 Detecting price changes >= 5%")
        
        return table_result
    
    def run(self):
        """Run the complete job"""
        print("🚀 Starting Price Monitor (Table API Only)")
        
        try:
            self.setup_tables()
            result = self.run_price_monitoring()
            
            print("✅ Job submitted successfully!")
            print("🔄 Running continuously...")
            
            result.wait()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            raise

if __name__ == "__main__":
    job = PriceMonitoringJob()
    job.run()