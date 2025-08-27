import os
from base_job import BaseJob

class ProductSummaryJob(BaseJob):
    def __init__(self, job_name="Product Summary Job"):
        super().__init__(job_name=job_name)
        self.kafka_group_id = os.getenv('SUMMARY_KAFKA_GROUP_ID', 'summary_group')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'competitor-prices')

    def setup_tables(self):
        self.t_env.execute_sql("DROP TABLE IF EXISTS competitor_prices")
        self.t_env.execute_sql("DROP TABLE IF EXISTS platform_products_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS external_competitors_dim")

        # Competitor price stream
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
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true'
            )
        """)

        # Materialized dimension tables from dims_job (same as alerts_job)
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

        self.t_env.execute_sql(f"""
            CREATE TABLE product_market_summary (
                product_sku STRING,
                cheapest_competitor_id BIGINT,
                cheapest_price DECIMAL(10, 2),
                platform_price DECIMAL(10, 2),
                gap_pct DECIMAL(6, 2),
                competitor_count INT,
                in_stock_competitor_count INT,
                summary_timestamp TIMESTAMP(3),
                PRIMARY KEY (product_sku) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'product_market_summary',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.interval' = '1s',
                'sink.buffer-flush.max-rows' = '1',
                'sink.parallelism' = '1'
            )
        """)

    def build_product_market_summary_query(self):
        return f"""
            INSERT INTO product_market_summary
            WITH latest AS (
                SELECT
                    cp.product_sku,
                    cp.competitor_name,
                    cp.price,
                    cp.in_stock,
                    cp.event_time,
                    ROW_NUMBER() OVER (PARTITION BY cp.product_sku, cp.competitor_name ORDER BY cp.event_time DESC) AS rn
                FROM competitor_prices cp
            ),
            fresh AS (
                SELECT product_sku, competitor_name, price, in_stock, event_time
                FROM latest
                WHERE rn = 1
            ),
            priced AS (
                SELECT
                    f.product_sku,
                    LOWER(f.competitor_name) AS competitor_code,
                    f.price,
                    f.in_stock,
                    f.event_time
                FROM fresh f
            ),
            counts AS (
                SELECT
                    p.product_sku,
                    CAST(COUNT(*) AS INT) AS competitor_count,
                    CAST(SUM(CASE WHEN p.in_stock THEN 1 ELSE 0 END) AS INT) AS in_stock_competitor_count
                FROM priced p
                GROUP BY p.product_sku
            ),
            cheapest AS (
                SELECT product_sku, competitor_code, price, event_time
                FROM (
                    SELECT
                        p.product_sku,
                        p.competitor_code,
                        p.price,
                        p.event_time,
                        ROW_NUMBER() OVER (PARTITION BY p.product_sku ORDER BY p.price ASC) AS price_rank
                    FROM priced p
                ) r
                WHERE r.price_rank = 1
            )
            SELECT
                c.product_sku,
                ec.id AS cheapest_competitor_id,
                c.price AS cheapest_price,
                pp.current_price AS platform_price,
                CAST(((pp.current_price - c.price) / NULLIF(pp.current_price, 0) * 100) AS DECIMAL(6,2)) AS gap_pct,
                cnt.competitor_count,
                cnt.in_stock_competitor_count,
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS summary_timestamp
            FROM cheapest c
            JOIN counts cnt ON c.product_sku = cnt.product_sku
            JOIN external_competitors_dim AS ec ON c.competitor_code = ec.code
            JOIN platform_products_dim AS pp ON c.product_sku = pp.sku
        """

    def run(self):
        print("Starting Product Summary job...")
        try:
            self.setup_tables()
            query = self.build_product_market_summary_query()
            result = self.t_env.execute_sql(query)
            print("Product summary job submitted. Waiting...")
            result.wait()
        except Exception as e:
            print(f"Error occurred while running product summary job: {e}")
            raise

if __name__ == "__main__":
    job = ProductSummaryJob()
    job.run()
