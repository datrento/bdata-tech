import os
from base_job import BaseJob


class UserBehaviorJob(BaseJob):
    def __init__(self, job_name="User Behavior Job"):
        super().__init__(job_name=job_name)
        self.kafka_group_id = os.getenv('USER_BEHAVIOR_KAFKA_GROUP_ID', 'user_behavior_group')
        self.kafka_topic = os.getenv('USER_BEHAVIOR_TOPIC', 'user-behavior-events')

    def setup_tables(self):
        # Drop old definitions
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_src")
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_events")
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_summary")

        # Source: Kafka USER_BEHAVIOR
        self.t_env.execute_sql(f"""
            CREATE TABLE user_behavior_src (
                sku STRING,
                title STRING,
                category STRING,
                unique_visitors BIGINT,
                page_views BIGINT,
                dwell_seconds DOUBLE,
                searches BIGINT,
                cart_additions BIGINT,
                purchases BIGINT,
                price_comparisons BIGINT,
                api_collection_ts_ms BIGINT,
                data_ts_ms BIGINT,
                event_time AS TO_TIMESTAMP_LTZ(data_ts_ms, 3),
                collection_source STRING,
                producer_timestamp STRING,
                WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_servers}',
                'properties.group.id' = '{self.kafka_group_id}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true',
                'json.timestamp-format.standard' = 'ISO-8601'
            )
        """)

        # Sink: raw events
        self.t_env.execute_sql(f"""
            CREATE TABLE user_behavior_events (
                product_sku VARCHAR(100) NOT NULL,
                page_views BIGINT NOT NULL,
                unique_visitors BIGINT NOT NULL,
                dwell_seconds DOUBLE NOT NULL,
                searches BIGINT NOT NULL,
                cart_additions BIGINT NOT NULL,
                purchases BIGINT NOT NULL, 
                price_comparisons BIGINT NOT NULL,
                data_timestamp TIMESTAMP NOT NULL,
                collection_timestamp TIMESTAMP NOT NULL
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'user_behavior_events',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.interval' = '3s',
                'sink.buffer-flush.max-rows' = '200'
            )
        """)

        # Sink: hourly summary
        self.t_env.execute_sql(f"""
            CREATE TABLE user_behavior_summary (
                product_sku STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                page_views BIGINT,
                unique_visitors BIGINT,
                searches BIGINT,
                cart_additions BIGINT,
                purchases BIGINT, 
                price_comparisons BIGINT,
                search_rate DOUBLE,
                search_cart_rate DOUBLE,
                cart_purchase_rate DOUBLE,
                view_compare_rate DOUBLE,
                avg_dwell_seconds DOUBLE
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'user_behavior_summary',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.interval' = '3s',
                'sink.buffer-flush.max-rows' = '200'
            )
        """)

    def build_user_behavior_and_summary_queries(self):
        insert_raw = """
            INSERT INTO user_behavior_events
            SELECT
                sku AS product_sku,
                page_views,
                unique_visitors,
                dwell_seconds,
                searches,
                cart_additions,
                purchases,
                price_comparisons,
                CAST(TO_TIMESTAMP_LTZ(data_ts_ms, 3) AS TIMESTAMP) AS data_timestamp,
                CAST(TO_TIMESTAMP_LTZ(api_collection_ts_ms, 3) AS TIMESTAMP) AS collection_timestamp
            FROM user_behavior_src
        """
        
        insert_summary = """
            INSERT INTO user_behavior_summary
            SELECT
                sku AS product_sku,
                window_start,
                window_end,
                CAST(SUM(COALESCE(page_views, 0)) AS BIGINT) AS page_views,
                CAST(SUM(COALESCE(unique_visitors, 0)) AS BIGINT) AS unique_visitors,
                CAST(SUM(COALESCE(searches, 0)) AS BIGINT) AS searches,
                CAST(SUM(COALESCE(cart_additions, 0)) AS BIGINT) AS cart_additions,
                CAST(SUM(COALESCE(purchases, 0)) AS BIGINT) AS purchases,
                CAST(SUM(COALESCE(price_comparisons, 0)) AS BIGINT) AS price_comparisons,
                CASE WHEN SUM(COALESCE(page_views, 0)) > 0 
                     THEN CAST(SUM(COALESCE(searches, 0)) AS DOUBLE) / CAST(SUM(COALESCE(page_views, 0)) AS DOUBLE)
                     ELSE 0 END AS search_rate,
                CASE WHEN SUM(COALESCE(searches, 0)) > 0 
                     THEN CAST(SUM(COALESCE(cart_additions, 0)) AS DOUBLE) / CAST(SUM(COALESCE(searches, 0)) AS DOUBLE)
                     ELSE 0 END AS search_cart_rate,
                CASE WHEN SUM(COALESCE(cart_additions, 0)) > 0 
                     THEN CAST(SUM(COALESCE(purchases, 0)) AS DOUBLE) / CAST(SUM(COALESCE(cart_additions, 0)) AS DOUBLE)
                     ELSE 0 END AS cart_purchase_rate,
                CASE WHEN SUM(COALESCE(page_views, 0)) > 0 
                     THEN CAST(SUM(COALESCE(price_comparisons, 0)) AS DOUBLE) / CAST(SUM(COALESCE(page_views, 0)) AS DOUBLE)
                     ELSE 0 END AS view_compare_rate,
                AVG(dwell_seconds) AS avg_dwell_seconds
            FROM TABLE(
                TUMBLE(TABLE user_behavior_src, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
            )
            GROUP BY sku, window_start, window_end
        """
        return insert_raw, insert_summary

    def run(self):
        print("Starting User Behavior job...")
        self.setup_tables()
        insert_raw, insert_summary = self.build_user_behavior_and_summary_queries()
        stmt_set = self.t_env.create_statement_set()
        stmt_set.add_insert_sql(insert_raw)
        stmt_set.add_insert_sql(insert_summary)
        result = stmt_set.execute()
        print("User Behavior job submitted. Waiting...")
        result.wait()

if __name__ == "__main__":
    job = UserBehaviorJob()
    job.run()



