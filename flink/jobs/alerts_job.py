import os
from base_job import BaseJob

class AlertsJob(BaseJob):
    def __init__(self, job_name):
        super().__init__(job_name=job_name)

        self.undercut_threshold = float(os.getenv('UNDERCUT_PERCENT_THRESHOLD', '5.0'))
        self.overpriced_threshold = float(os.getenv('OVERPRICED_PERCENT_THRESHOLD', '5.0'))
        self.price_increase_24hrs_threshold = float(os.getenv('PRICE_INCREASE_24HRS_THRESHOLD', '5.0'))
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'undercut_alerts_group')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'competitor-prices')
        
    def setup_tables(self):
        self.t_env.execute_sql("DROP TABLE IF EXISTS competitor_prices")
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors")

        # External competitors
        self.t_env.execute_sql(f"""
            CREATE TABLE external_competitors (
                id BIGINT,
                name STRING,
                code STRING
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'external_competitors',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.parallelism' = '1'
            )
        """)

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
                WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}',
                'properties.auto.offset.reset' = 'earliest',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
        """)

        # Competitor price history table
        self.t_env.execute_sql(f"""
            CREATE TABLE competitor_price_history (
            product_sku STRING,
            competitor_id BIGINT,
            price DECIMAL(10, 2),
            in_stock BOOLEAN,
            source_sku STRING,
            data_timestamp TIMESTAMP(3),
            collection_timestamp TIMESTAMP(3)
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

        # Platform products
        self.t_env.execute_sql(f"""
            CREATE TABLE platform_products (
                sku STRING,
                name STRING,
                category STRING,
                brand STRING,
                cost DECIMAL(10, 2),
                min_viable_price DECIMAL(10, 2),
                current_price DECIMAL(10, 2),
                price_sensitivity STRING,
                in_stock BOOLEAN,
                is_active BOOLEAN,
                PRIMARY KEY (sku) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'platform_products',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.parallelism' = '1'
            )
        """)

        # Price signals sink table
        self.t_env.execute_sql(f"""
            CREATE TABLE price_signals (
            product_sku STRING,
            competitor_id BIGINT,
            signal_type STRING,
            platform_price DECIMAL(10, 2),
            competitor_price DECIMAL(10, 2),
            percentage_diff DECIMAL(5, 2),
            recommendation STRING,
            priority INT,
            in_stock BOOLEAN,
            signal_timestamp TIMESTAMP(3),
            PRIMARY KEY (product_sku, competitor_id, signal_type) NOT ENFORCED
            ) WITH (
            'connector' = 'jdbc',
            'url' = '{self.postgres_url}',
            'table-name' = 'price_signals',
            'driver' = 'org.postgresql.Driver',
            'username' = '{self.postgres_user}',
            'password' = '{self.postgres_password}',
            'sink.parallelism' = '1'
            )
        """)

    def build_undercut_alerts_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT
                cp.product_sku,
                ec.id AS competitor_id,
                'undercut' AS signal_type,
                pp.current_price as platform_price,
                cp.price as competitor_price,
                ((pp.current_price - cp.price) / pp.current_price * 100) as percentage_diff,
                CASE
                    WHEN pp.price_sensitivity = 'high' THEN 'high priority'
                    WHEN pp.price_sensitivity = 'medium' THEN 'medium priority'
                    ELSE 'low priority'
                END AS recommendation,
                CASE 
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS priority,
                cp.in_stock,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS signal_timestamp
            FROM competitor_prices cp
            JOIN platform_products pp ON cp.product_sku = pp.sku
            JOIN external_competitors ec ON LOWER(cp.competitor_name) = LOWER(ec.code)
            WHERE
                --Detect undercut: competitor price is below our platform by X%
                cp.price < pp.current_price
                AND ((pp.current_price - cp.price) / pp.current_price * 100) > {self.undercut_threshold}
                AND cp.in_stock = TRUE
                AND pp.in_stock = TRUE
                AND pp.is_active = TRUE
        """
    
    def build_overpriced_alerts_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT
                cp.product_sku,
                ec.id AS competitor_id,
                'overpriced' AS signal_type,
                pp.current_price as platform_price,
                cp.price as competitor_price,
                ((cp.price - pp.current_price) / pp.current_price * 100) as percentage_diff,
                CASE
                    WHEN pp.price_sensitivity = 'high' THEN 'high priority'
                    WHEN pp.price_sensitivity = 'medium' THEN 'medium priority'
                    ELSE 'low priority'
                END AS recommendation,
                CASE
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS priority,
                cp.in_stock,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS signal_timestamp
            FROM competitor_prices cp
            JOIN platform_products pp ON cp.product_sku = pp.sku
            JOIN external_competitors ec ON LOWER(cp.competitor_name) = LOWER(ec.code)
            WHERE
                -- Detect overpriced: competitor price is above our platform by X%
                cp.price > pp.current_price
                AND ((cp.price - pp.current_price) / pp.current_price * 100) > {self.overpriced_threshold}
                AND cp.in_stock = TRUE
                AND pp.in_stock = TRUE
                AND pp.is_active = TRUE
        """

    def build_stockout_alerts_query(self):
        return f"""
            INSERT INTO price_signals
            SELECT
                cp.product_sku,
                ec.id AS competitor_id,
                'stockout' AS signal_type,
                pp.current_price as platform_price,
                cp.price as competitor_price,
                0.00 as percentage_diff,
                CASE
                    WHEN pp.price_sensitivity = 'high' THEN 'high priority'
                    WHEN pp.price_sensitivity = 'medium' THEN 'medium priority'
                    ELSE 'low priority'
                END AS recommendation,
                CASE
                    WHEN pp.price_sensitivity = 'high' THEN 1
                    WHEN pp.price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS priority,
                cp.in_stock,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS signal_timestamp
            FROM competitor_prices cp
            JOIN platform_products pp ON cp.product_sku = pp.sku
            JOIN external_competitors ec ON LOWER(cp.competitor_name) = LOWER(ec.code)
            WHERE
                -- Detect stockout: competitor is out of stock
                cp.in_stock = FALSE
                AND pp.in_stock = TRUE
                AND pp.is_active = TRUE
        """

    def build_price_increase_24hr_alerts_query(self):
        """
        Build SQL query for detecting price increase alerts. by taking the records between the past 24 hours and 10 minutes of the current record
        Generate alerts when a competitor's current price is significantly higher than the lowest price they've offered in the past 24 hours
        """
        return f"""
            INSERT INTO price_signals
            WITH price_history AS (
                SELECT
                    cp.product_sku,
                    cp.competitor_name,
                    cp.price AS current_price,
                    cp.in_stock,
                    pp.current_price AS platform_price,
                    pp.price_sensitivity,
                    ec.id as competitor_id,
                    -- Get the minimum price over the past 24 hours
                    (
                        SELECT MIN(hist.price)
                        FROM competitor_price_history hist
                        WHERE hist.product_sku = cp.product_sku
                        AND hist.competitor_id = ec.id
                        AND hist.data_timestamp >= (NOW() - INTERVAL '24' HOUR)
                        AND hist.data_timestamp < (NOW() - INTERVAL '10' MINUTE)
                    ) AS min_price_24h
                FROM competitor_prices cp
                JOIN platform_products pp ON cp.product_sku = pp.sku
                JOIN external_competitors ec ON LOWER(cp.competitor_name) = LOWER(ec.code)
                WHERE
                    cp.in_stock = TRUE
                    AND pp.in_stock = TRUE
                    AND pp.is_active = TRUE
            )
            SELECT
                product_sku,
                competitor_id,
                'price_increase_24hrs_min' AS signal_type,
                platform_price,
                current_price as competitor_price,
                ((current_price - min_price_24h) / min_price_24h * 100) as percentage_diff,
                CASE
                    WHEN price_sensitivity = 'high' THEN 'Opportunity to increase our price - competitor raised from 24h low'
                    WHEN price_sensitivity = 'medium' THEN 'Consider price adjustment - competitor raised from 24h low'
                    ELSE 'Monitor market - competitor raised from 24h low'
                END AS recommendation,
                CASE 
                    WHEN price_sensitivity = 'high' THEN 1
                    WHEN price_sensitivity = 'medium' THEN 2
                    ELSE 3
                END AS priority,
                in_stock,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS signal_timestamp
            FROM price_history
            WHERE
                min_price_24h IS NOT NULL
                AND current_price > min_price_24h
                AND ((current_price - min_price_24h) / min_price_24h * 100) > {self.price_increase_24hrs_threshold}
        """
        

    def run(self):
        """ Run undercut alerts job """
        print("Starting undercut alerts job...")

        try:
            self.setup_tables()
            undercut_query = self.build_undercut_alerts_query()
            overpriced_query = self.build_overpriced_alerts_query()
            price_increase_24hr_query = self.build_price_increase_24hr_alerts_query()


            stmt_set = self.t_env.create_statement_set()
            stmt_set.add_insert_sql(undercut_query)
            stmt_set.add_insert_sql(overpriced_query)
            stmt_set.add_insert_sql(price_increase_24hr_query)

            result = stmt_set.execute()
            print("Multi-sink job submitted. Waiting...")
            result.wait()
            print("Multi-sink job completed.")
        except Exception as e:
            print(f"Error occurred while running undercut alerts job: {e}")
            raise

if __name__ == "__main__":
    job = AlertsJob(job_name="Alerts Job")
    job.run()