import os
import time
from base_job import BaseJob

class AlertsJob(BaseJob):
    def __init__(self, job_name):
        super().__init__(job_name=job_name)

        self.undercut_threshold = float(os.getenv('UNDERCUT_PERCENT_THRESHOLD', '5'))
        self.overpriced_threshold = float(os.getenv('OVERPRICED_PERCENT_THRESHOLD', '5'))
        self.price_increase_24hrs_threshold = float(os.getenv('PRICE_INCREASE_24HRS_THRESHOLD', '10'))
        self.sequential_change_threshold = float(os.getenv('SEQUENTIAL_CHANGE_THRESHOLD', '10'))
        self.kafka_group_id = os.getenv('ALERTS_KAFKA_GROUP_ID', f'alerts_group_{int(time.time())}')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'competitor-prices')
        self.kafka_startup_mode = os.getenv('ALERTS_KAFKA_STARTUP_MODE', 'earliest-offset')
        
    def setup_tables(self):
        self.t_env.execute_sql("DROP TABLE IF EXISTS competitor_prices")
        # CDC dims are produced by DimsJob; alerts only consumes dim topics
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors_dim")

        # Materialized dimension tables in upsert-Kafka
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
        ## SOURCE TABLES------------------

        # Competitor price table
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
                'scan.startup.mode' = '{self.kafka_startup_mode}',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
        """)



        # Price signals sink
        self.t_env.execute_sql(f"""
            CREATE TABLE price_signals (
                product_sku STRING,
                competitor_id BIGINT,
                signal_type STRING,
                platform_price DECIMAL(10, 2),
                competitor_price DECIMAL(10, 2),
                percentage_diff DECIMAL(5, 2),
                recommendation STRING,
                priority SMALLINT,
                in_stock BOOLEAN,
                signal_timestamp TIMESTAMP(3),
                is_active BOOLEAN,
                PRIMARY KEY (product_sku, competitor_id, signal_type) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'price_signals',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.max-rows' = '1',
                'sink.buffer-flush.interval' = '1s'
            )
        """)

    def build_undercut_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                CAST('UNDERCUT' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                cp.price AS competitor_price,
                CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS DECIMAL(5, 2)) AS percentage_diff,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 'URGENT: Competitor undercutting by ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '%'
                    WHEN pp.price_sensitivity = 'medium' THEN 'Consider price adjustment: ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '% undercut'
                    ELSE 'Monitor: ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '% undercut'
                END AS recommendation,
                CAST(CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS SMALLINT) AS priority,
                cp.in_stock,
                CAST(cp.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM competitor_prices cp
            JOIN platform_products_dim AS pp ON cp.product_sku = pp.sku
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE
                cp.price < pp.current_price
        """

    def build_undercut_warmup_query(self):
        # Non-temporal undercut for a short warm-up period to avoid cold-start misses
        return f"""
            INSERT INTO price_signals
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                CAST('UNDERCUT' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                cp.price AS competitor_price,
                CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS DECIMAL(5, 2)) AS percentage_diff,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 'URGENT: Competitor undercutting by ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '%'
                    WHEN pp.price_sensitivity = 'medium' THEN 'Consider price adjustment: ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '% undercut'
                    ELSE 'Monitor: ' || CAST(((pp.current_price - cp.price) / pp.current_price * 100) AS STRING) || '% undercut'
                END AS recommendation,
                CAST(CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS SMALLINT) AS priority,
                cp.in_stock,
                CAST(cp.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM competitor_prices cp
            JOIN platform_products_dim AS pp ON cp.product_sku = pp.sku
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE
                -- Undercut threshold
                cp.price < pp.current_price
                AND ((pp.current_price - cp.price) / pp.current_price * 100) > {self.undercut_threshold}
                -- Stock availability
                AND cp.in_stock = TRUE
                AND pp.in_stock = TRUE
                -- Active products
                AND pp.is_active = TRUE
        """

    def build_overpriced_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                CAST('OVERPRICED' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                cp.price AS competitor_price,
                CAST(((cp.price - pp.current_price) / pp.current_price * 100) AS DECIMAL(5, 2)) AS percentage_diff,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 'Opportunity: We are ' || CAST(((cp.price - pp.current_price) / pp.current_price * 100) AS STRING) || '% cheaper'
                    WHEN pp.price_sensitivity = 'medium' THEN 'Good position: ' || CAST(((cp.price - pp.current_price) / pp.current_price * 100) AS STRING) || '% cheaper'
                    ELSE 'Competitive: ' || CAST(((cp.price - pp.current_price) / pp.current_price * 100) AS STRING) || '% cheaper'
                END AS recommendation,
                CAST(CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 2
                    WHEN pp.price_sensitivity = 'medium' THEN 3
                    ELSE 3
                END AS SMALLINT) AS priority,
                cp.in_stock,
                CAST(cp.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM competitor_prices cp
            JOIN platform_products_dim AS pp ON cp.product_sku = pp.sku
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE
                -- Overpriced threshold
                cp.price > pp.current_price
                AND ((cp.price - pp.current_price) / pp.current_price * 100) > {self.overpriced_threshold}
                -- Stock availability
                AND cp.in_stock = TRUE
                AND pp.in_stock = TRUE
                -- Active products
                AND pp.is_active = TRUE
        """

    def build_stockout_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                CAST('STOCKOUT' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                cp.price AS competitor_price,
                CAST(0 AS DECIMAL(5, 2)) AS percentage_diff,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 'URGENT: Competitor out of stock - opportunity to capture market'
                    WHEN pp.price_sensitivity = 'medium' THEN 'Competitor out of stock - consider inventory management'
                    ELSE 'Competitor out of stock'
                END AS recommendation,
                CAST(CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS SMALLINT) AS priority,
                cp.in_stock,
                CAST(cp.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM competitor_prices cp
            JOIN platform_products_dim AS pp ON cp.product_sku = pp.sku
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE
                -- Stockout
                NOT cp.in_stock
                -- Stock availability
                AND pp.in_stock = TRUE
                -- Active products
                AND pp.is_active = TRUE
        """

    def build_price_increase_24hr_alerts_query(self):
        """
        Build SQL query for detecting price increase alerts by comparing current prices 
        to competitor's historical minimum over the past 24 hours.
        Uses windowed aggregation to track competitor price history in streaming fashion.
        """
        return f"""
            INSERT INTO price_signals
            SELECT 
                cp.product_sku,
                ec.id AS competitor_id,
                CAST('PRICE_INCREASE_24H' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                cp.price AS competitor_price,
                CAST(((cp.price - min_price_24h) / min_price_24h * 100) AS DECIMAL(5, 2)) AS percentage_diff,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 'Opportunity: Competitor raised price ' || CAST(((cp.price - min_price_24h) / min_price_24h * 100) AS STRING) || '% from 24h low'
                    WHEN pp.price_sensitivity = 'medium' THEN 'Consider adjustment: Competitor up ' || CAST(((cp.price - min_price_24h) / min_price_24h * 100) AS STRING) || '% from 24h low'
                    ELSE 'Monitor: Competitor up ' || CAST(((cp.price - min_price_24h) / min_price_24h * 100) AS STRING) || '% from 24h low'
                END AS recommendation,
                CAST(CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 2
                    WHEN pp.price_sensitivity = 'medium' THEN 3
                    ELSE 3
                END AS SMALLINT) AS priority,
                cp.in_stock,
                CAST(cp.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM (
                SELECT 
                    cp.product_sku,
                    cp.competitor_name,
                    cp.price,
                    cp.in_stock,
                    cp.event_time,
                    -- Calculate minimum price over 24-hour window up to current row
                    MIN(cp.price) OVER (
                        PARTITION BY cp.product_sku, cp.competitor_name
                        ORDER BY cp.event_time 
                        RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
                    ) AS min_price_24h
                FROM competitor_prices cp
            ) cp
            JOIN platform_products_dim AS pp ON cp.product_sku = pp.sku
            JOIN external_competitors_dim AS ec ON LOWER(cp.competitor_name) = ec.code
            WHERE
                -- Price increase threshold and exclude very recent events
                min_price_24h IS NOT NULL
                AND cp.price > min_price_24h
                AND ((cp.price - min_price_24h) / min_price_24h * 100) > {self.price_increase_24hrs_threshold}
                -- Stock and active filters
                AND cp.in_stock = TRUE
                AND pp.in_stock = TRUE
                AND pp.is_active = TRUE
                AND cp.event_time < CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
        """

    def build_sequential_change_query(self):
        return f"""
            INSERT INTO price_signals
            WITH price_changes AS (
                SELECT 
                    product_sku,
                    competitor_name,
                    price,
                    in_stock,
                    event_time,
                    LAG(price, 1) OVER (
                        PARTITION BY product_sku, competitor_name
                        ORDER BY event_time
                    ) as previous_price
                FROM competitor_prices
            )
            SELECT 
                pc.product_sku,
                ec.id AS competitor_id,
                CAST('SEQUENTIAL_CHANGE' AS STRING) AS signal_type,
                pp.current_price AS platform_price,
                pc.price AS competitor_price,
                CAST(((pc.price - pc.previous_price) / NULLIF(pc.previous_price, 0) * 100) AS DECIMAL(5,2)) AS percentage_diff,
                CASE 
                    WHEN pc.price < pc.previous_price THEN 'Competitor price decreased'
                    ELSE 'Competitor price increased'
                END AS recommendation,
                CAST(CASE 
                    WHEN ABS((pc.price - pc.previous_price) / NULLIF(pc.previous_price, 0) * 100) >= {self.sequential_change_threshold} THEN 2
                    ELSE 3
                END AS SMALLINT) AS priority,
                pc.in_stock,
                CAST(pc.event_time AS TIMESTAMP(3)) AS signal_timestamp,
                true AS is_active
            FROM price_changes pc
            JOIN external_competitors_dim AS ec ON LOWER(pc.competitor_name) = ec.code
            JOIN platform_products_dim AS pp ON pc.product_sku = pp.sku
            WHERE pc.previous_price IS NOT NULL
              AND ABS((pc.price - pc.previous_price) / NULLIF(pc.previous_price, 0) * 100) >= {self.sequential_change_threshold}
              AND pp.is_active = TRUE
        """

    def run(self):
        print("Starting Alerts job...")
        self.setup_tables()

        stmt_set = self.t_env.create_statement_set()
        
        # Add all signal queries
        stmt_set.add_insert_sql(self.build_undercut_query())
        stmt_set.add_insert_sql(self.build_overpriced_query())
        stmt_set.add_insert_sql(self.build_stockout_query())
        stmt_set.add_insert_sql(self.build_price_increase_24hr_alerts_query())
        stmt_set.add_insert_sql(self.build_sequential_change_query())
        
        result = stmt_set.execute()
        print("Alerts job submitted. Waiting...")
        result.wait()


if __name__ == "__main__":
    job = AlertsJob(job_name="Alerts Job")
    job.run()