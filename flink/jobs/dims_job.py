import os
from base_job import BaseJob


class DimsJob(BaseJob):
    def __init__(self, job_name: str):
        super().__init__(job_name=job_name)
        self.platform_products_slot = os.getenv('PLATFORM_PRODUCTS_CDC_SLOT', 'flink_platform_products_dims')
        self.external_competitors_slot = os.getenv('EXTERNAL_COMPETITORS_CDC_SLOT', 'flink_external_competitors_dims')
        self.user_behavior_summary_slot = os.getenv('USER_BEHAVIOR_SUMMARY_CDC_SLOT', 'flink_user_behavior_summary_dims')
        self.price_signals_slot = os.getenv('PRICE_SIGNALS_CDC_SLOT', 'flink_price_signals_dims')

    def setup_tables(self):
        # Drop and recreate dim topics tables (upsert-kafka)
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors")
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_summary_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_summary")
        self.t_env.execute_sql("DROP TABLE IF EXISTS price_signals_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS price_signals")
        # CDC sources from Postgres
        # PLATFORM PRODUCTS CDC SOURCE
        self.t_env.execute_sql(f"""
            CREATE TABLE platform_products (
                sku VARCHAR(100) NOT NULL,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                brand VARCHAR(100),
                cost DECIMAL(10, 2) NOT NULL, -- Cost price to acquire
                min_viable_price DECIMAL(10, 2), -- Minimum viable price
                current_price DECIMAL(10, 2) NOT NULL, -- Actual selling price
                price_sensitivity VARCHAR(50), -- Price sensitivity
                price_elasticity DECIMAL(6, 3), -- Estimated own-price elasticity
                sensitivity_last_updated TIMESTAMP,
                sensitivity_observations INT,
                in_stock BOOLEAN,
                is_active BOOLEAN,
                created_at TIMESTAMP(3),
                updated_at TIMESTAMP(3),
                op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts',
                PRIMARY KEY (sku) NOT ENFORCED
            ) WITH (
                'connector' = 'postgres-cdc',
                'debezium.snapshot.mode' = 'initial',
                'debezium.time.precision.mode' = 'adaptive_time_microseconds',
                'hostname' = '{self.postgres_host}',
                'port' = '{self.postgres_port}',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'database-name' = '{self.postgres_db}',
                'schema-name' = 'public',
                'debezium.publication.name' = 'flink_pub_platform_products_dim',
                'decoding.plugin.name' = 'pgoutput',
                'table-name' = 'platform_products',
                'slot.name' = '{self.platform_products_slot}'
            )
        """)
        # USER BEHAVIOR SUMMARY CDC SOURCE
        self.t_env.execute_sql(f"""
            CREATE TABLE user_behavior_summary (
                product_sku STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                unique_visitors BIGINT,
                page_views BIGINT,
                searches BIGINT,
                cart_additions BIGINT,
                purchases BIGINT, 
                price_comparisons BIGINT,
                search_rate DOUBLE,
                search_cart_rate DOUBLE,
                cart_purchase_rate DOUBLE,
                view_compare_rate DOUBLE,
                avg_dwell_seconds DOUBLE,
                op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts',
                PRIMARY KEY (product_sku, window_start, window_end) NOT ENFORCED
            ) WITH (
                'connector' = 'postgres-cdc',
                'debezium.snapshot.mode' = 'initial',
                'debezium.time.precision.mode' = 'adaptive_time_microseconds',
                'hostname' = '{self.postgres_host}',
                'port' = '{self.postgres_port}',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'database-name' = '{self.postgres_db}',
                'schema-name' = 'public',
                'debezium.publication.name' = 'flink_pub_user_behavior_summary_dim',
                'decoding.plugin.name' = 'pgoutput',
                'table-name' = 'user_behavior_summary',
                'slot.name' = '{self.user_behavior_summary_slot}'
            )
        """)
        # PRICE SIGNALS CDC SOURCE
        self.t_env.execute_sql(f"""
            CREATE TABLE price_signals (
                product_sku STRING,
                competitor_id BIGINT,
                platform_price DECIMAL(10, 2),
                competitor_price DECIMAL(10, 2),
                signal_type STRING,
                percentage_diff DECIMAL(5, 2),
                priority SMALLINT,
                recommendation STRING,
                in_stock BOOLEAN,
                is_active BOOLEAN,
                signal_timestamp TIMESTAMP(3),
                op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts',
                PRIMARY KEY (product_sku, competitor_id, signal_type) NOT ENFORCED
            ) WITH (
                'connector' = 'postgres-cdc',
                'debezium.snapshot.mode' = 'initial',
                'debezium.time.precision.mode' = 'adaptive_time_microseconds',
                'hostname' = '{self.postgres_host}',
                'port' = '{self.postgres_port}',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'database-name' = '{self.postgres_db}',
                'schema-name' = 'public',
                'debezium.publication.name' = 'flink_pub_price_signals_dim',
                'decoding.plugin.name' = 'pgoutput',
                'table-name' = 'price_signals',
                'slot.name' = '{self.price_signals_slot}'
            )
        """)

        # EXTERNAL COMPETITORS CDC SOURCE
        self.t_env.execute_sql(f"""
            CREATE TABLE external_competitors (
                id BIGINT,
                name STRING,
                code STRING,
                created_at TIMESTAMP(3),
                op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts',
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'postgres-cdc',
                'debezium.snapshot.mode' = 'initial',
                'debezium.time.precision.mode' = 'adaptive_time_microseconds',
                'hostname' = '{self.postgres_host}',
                'port' = '{self.postgres_port}',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'database-name' = '{self.postgres_db}',
                'schema-name' = 'public',
                'debezium.publication.name' = 'flink_pub_ec_dims',
                'decoding.plugin.name' = 'pgoutput',
                'table-name' = 'external_competitors',
                'slot.name' = '{self.external_competitors_slot}'
            )
        """)

        # UPSERT-KAFKA DIM TABLES (changelog topics)
        # PLATFORM PRODUCTS DIM
        self.t_env.execute_sql(f"""
            CREATE TABLE platform_products_dim (
                sku STRING,
                current_price DECIMAL(10, 2),
                price_sensitivity STRING,
                in_stock BOOLEAN,
                is_active BOOLEAN,
                dim_time TIMESTAMP_LTZ(3),
                PRIMARY KEY (sku) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-platform-products',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

        # EXTERNAL COMPETITORS DIM
        self.t_env.execute_sql(f"""
            CREATE TABLE external_competitors_dim (
                code STRING,
                id BIGINT,
                name STRING,
                dim_time TIMESTAMP_LTZ(3),
                PRIMARY KEY (code) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-external-competitors',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

        # USER BEHAVIOR SUMMARY DIM
        self.t_env.execute_sql(f"""
            CREATE TABLE user_behavior_summary_dim (
                product_sku STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                unique_visitors BIGINT,
                page_views BIGINT,
                searches BIGINT,
                cart_additions BIGINT,
                purchases BIGINT, 
                price_comparisons BIGINT,
                search_rate DOUBLE,
                search_cart_rate DOUBLE,
                cart_purchase_rate DOUBLE,
                view_compare_rate DOUBLE,
                avg_dwell_seconds DOUBLE,
                op_ts TIMESTAMP_LTZ(3),
                PRIMARY KEY (product_sku, window_start, window_end) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-user-behavior-summary',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

        # PRICE SIGNALS DIM
        self.t_env.execute_sql(f"""
            CREATE TABLE price_signals_dim (
                product_sku STRING,
                competitor_id BIGINT,
                signal_type STRING,
                percentage_diff DECIMAL(5, 2),
                platform_price DECIMAL(10, 2),
                competitor_price DECIMAL(10, 2),
                recommendation STRING,
                priority SMALLINT,
                in_stock BOOLEAN,
                signal_timestamp TIMESTAMP(3),
                is_active BOOLEAN,
                op_ts TIMESTAMP_LTZ(3),
                PRIMARY KEY (product_sku, competitor_id, signal_type) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'dim-price-signals',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'key.format' = 'json',
                'key.json.ignore-parse-errors' = 'true',
                'value.format' = 'json',
                'value.json.fail-on-missing-field' = 'false'
            )
        """)

    def run(self):
        print("Starting Dims materialization job...")
        self.setup_tables()

        stmt_set = self.t_env.create_statement_set()
        stmt_set.add_insert_sql(
            """
            INSERT INTO platform_products_dim
            SELECT sku,
                   current_price,
                   price_sensitivity,
                   in_stock,
                   is_active,
                   COALESCE(CAST(updated_at AS TIMESTAMP_LTZ(3)), CAST(created_at AS TIMESTAMP_LTZ(3)), op_ts) AS dim_time
            FROM platform_products
            """
        )
        stmt_set.add_insert_sql(
            """
            INSERT INTO external_competitors_dim
            SELECT code,
                   id,
                   name,
                   COALESCE(CAST(created_at AS TIMESTAMP_LTZ(3)), op_ts) AS dim_time
            FROM external_competitors
            """
        )
        stmt_set.add_insert_sql(
            """
            INSERT INTO user_behavior_summary_dim
            SELECT product_sku,
                   window_start,
                   window_end,
                   unique_visitors,
                   page_views,
                   searches,
                   cart_additions,
                   purchases,
                   price_comparisons,
                   search_rate,
                   search_cart_rate,
                   cart_purchase_rate,
                   view_compare_rate,
                   avg_dwell_seconds,
                   op_ts
            FROM user_behavior_summary
            """
        )

        stmt_set.add_insert_sql(
            """
            INSERT INTO price_signals_dim
            SELECT product_sku,
                   competitor_id,
                   signal_type,
                   percentage_diff,
                   platform_price,
                   competitor_price,
                   recommendation,
                   priority,
                   in_stock,
                   signal_timestamp,
                   is_active,
                   op_ts
            FROM price_signals
            """
        )
        result = stmt_set.execute()
        print("Dims materialization submitted. Waiting...")
        result.wait()


if __name__ == "__main__":
    job = DimsJob(job_name="Dims Job")
    job.run()


