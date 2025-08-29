import os
from base_job import BaseJob

class PriceMonitoringJob(BaseJob):
    def __init__(self, job_name="Price Monitoring Job"):
        super().__init__(job_name=job_name)
        # Configurable alert threshold (percentage)
        self.alert_percent_threshold = float(os.getenv('ALERT_PERCENT_THRESHOLD', '1.0'))
        self.kafka_group_id = os.getenv('PRICE_MONITORING_KAFKA_GROUP_ID', 'price_monitoring_group')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'competitor-prices')


    def setup_tables(self):
        """Create source and sink tables for price monitoring."""

        # Ensure we replace any previous definition that had TIMESTAMP_LTZ JSON fields
        self.t_env.execute_sql("DROP TABLE IF EXISTS competitor_prices")
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors_dim")

        ## SOURCE TABLES--------------
        self.t_env.execute_sql(f"""
            CREATE TABLE competitor_prices (
                product_sku STRING,
                competitor_name STRING,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                source_sku STRING,
                api_collection_ts_ms BIGINT,
                data_ts_ms BIGINT,
                kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
                event_time AS COALESCE(
                    TO_TIMESTAMP_LTZ(data_ts_ms, 3),
                    TO_TIMESTAMP_LTZ(api_collection_ts_ms, 3),
                    kafka_ts
                ),
                proc_time AS PROCTIME(),
                WATERMARK FOR event_time AS event_time - INTERVAL '10' MINUTE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest-offset',
                'json.ignore-parse-errors' = 'true'
            )""")
        
        # Dimension tables from upsert-kafka topics (materialized by DimsJob)
        self.t_env.execute_sql(f"""
            CREATE TABLE platform_products_dim (
                sku STRING,
                current_price DECIMAL(10, 2),
                price_sensitivity STRING,
                in_stock BOOLEAN,
                is_active BOOLEAN,
                dim_time TIMESTAMP_LTZ(3),
                WATERMARK FOR dim_time AS dim_time - INTERVAL '1' HOUR,
                PRIMARY KEY (sku) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-platform-products',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}-pp-dim',
                'properties.auto.offset.reset' = 'earliest',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

        self.t_env.execute_sql(f"""
            CREATE TABLE external_competitors_dim (
                code STRING,
                id BIGINT,
                name STRING,
                dim_time TIMESTAMP_LTZ(3),
                WATERMARK FOR dim_time AS dim_time - INTERVAL '1' HOUR,
                PRIMARY KEY (code) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-external-competitors',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}-ec-dim',
                'properties.auto.offset.reset' = 'earliest',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

        ## SINK TABLES--------------
        self.t_env.execute_sql(f"""
            CREATE TABLE price_trends (
                product_sku STRING,
                competitor_name STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                avg_price DECIMAL(10,2),
                price_volatility DECIMAL(5,2),
                trend_direction STRING,
                PRIMARY KEY (product_sku, competitor_name, window_start, window_end) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'price_trends',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.interval' = '3s',
                'sink.buffer-flush.max-rows' = '200',
                'sink.parallelism' = '1'
            )
        """)

        self.t_env.execute_sql(f"""
            CREATE TABLE competitor_price_history (
                product_sku STRING,
                competitor_id BIGINT,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                source_sku STRING,
                data_timestamp TIMESTAMP(3),
                collection_timestamp TIMESTAMP(3),
                PRIMARY KEY (product_sku, competitor_id, data_timestamp) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'competitor_price_history',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.parallelism' = '1'
            )
        """)

    def build_competitor_price_history_query(self):
        """Build competitor price history INSERT SQL"""
        history_query = f"""
            INSERT INTO competitor_price_history
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                cp.price,
                cp.in_stock,
                cp.source_sku,
                TO_TIMESTAMP_LTZ(cp.data_ts_ms, 3) AS data_timestamp,
                TO_TIMESTAMP_LTZ(cp.api_collection_ts_ms, 3) AS collection_timestamp
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_sku, competitor_name, data_ts_ms
                        ORDER BY kafka_ts DESC
                    ) AS rn
                FROM competitor_prices
            ) cp
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE cp.data_ts_ms IS NOT NULL AND rn = 1
        """
        return history_query

    def build_trend_queries(self):
        """Build two trend INSERT SQLs: event-time and processing-time."""
        trend_event_query = """
            INSERT INTO price_trends
            WITH w AS (
                SELECT 
                    product_sku,
                    competitor_name,
                    price,
                    event_time,
                    window_start,
                    window_end
                FROM TABLE(
                    TUMBLE(
                        TABLE competitor_prices, 
                        DESCRIPTOR(event_time), 
                        INTERVAL '5' MINUTE
                    )
                )
                -- WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
            ),
            stats AS (
                SELECT 
                    product_sku,
                    competitor_name,
                    window_start,
                    window_end,
                    CAST(AVG(price) AS DECIMAL(10,2)) AS avg_price,
                    CAST(STDDEV_POP(price) AS DECIMAL(5,2)) AS price_volatility,
                    MIN(event_time) AS min_ts,
                    MAX(event_time) AS max_ts
                FROM w
                GROUP BY 
                    product_sku,
                    competitor_name,
                    window_start,
                    window_end
            ),
            ends AS (
                SELECT 
                    s.product_sku,
                    s.competitor_name,
                    s.window_start,
                    s.window_end,
                    s.avg_price,
                    s.price_volatility,
                    MAX(CASE WHEN w.event_time = s.min_ts THEN w.price END) AS first_price,
                    MAX(CASE WHEN w.event_time = s.max_ts THEN w.price END) AS last_price
                FROM stats s
                JOIN w 
                  ON s.product_sku = w.product_sku
                 AND s.competitor_name = w.competitor_name
                 AND s.window_start = w.window_start
                 AND s.window_end = w.window_end
                GROUP BY 
                    s.product_sku,
                    s.competitor_name,
                    s.window_start,
                    s.window_end,
                    s.avg_price,
                    s.price_volatility
            )
            SELECT 
                product_sku,
                competitor_name,
                window_start,
                window_end,
                avg_price,
                price_volatility,
                CASE 
                    WHEN first_price < last_price THEN 'up'
                    WHEN first_price > last_price THEN 'down'
                    ELSE 'stable'
                END AS trend_direction
            FROM ends
        """
        return trend_event_query
    
    def run(self):
        """Run both price monitoring and trend analysis jobs"""
        print("Starting Price Monitor and Trend Analysis")

        try:
            self.setup_tables()
            
            # Build all three INSERTs and run as a single StatementSet job
            print("Building queries and starting a single multi-sink job...")
            trend_event_query= self.build_trend_queries()
            history_query = self.build_competitor_price_history_query()

            stmt_set = self.t_env.create_statement_set()
            stmt_set.add_insert_sql(trend_event_query)
            stmt_set.add_insert_sql(history_query)

            result = stmt_set.execute()
            print("Multi-sink job submitted. Waiting...")
            result.wait()
            
        except Exception as e:
            print(f"Error setting up jobs: {str(e)}")
            raise

if __name__ == "__main__":
    job = PriceMonitoringJob()
    job.run()