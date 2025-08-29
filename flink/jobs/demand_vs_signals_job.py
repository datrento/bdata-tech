import os
from base_job import BaseJob

class DemandVsSignalsJob(BaseJob):
    def __init__(self, job_name="Demand vs Signals Job"):
        super().__init__(job_name=job_name)
        self.kafka_group_id = os.getenv('DEMAND_VS_SIGNALS_GROUP_ID', 'demand_vs_signals_group')

    def setup_tables(self):
        # Drop and create sources
        self.t_env.execute_sql("DROP TABLE IF EXISTS user_behavior_summary_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS price_signals_dim")
        self.t_env.execute_sql("DROP TABLE IF EXISTS demand_vs_signals")

        # Source: user_behavior_summary_dim (upsert-kafka)
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
                event_time AS op_ts,
                WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE,
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

        # Source: price_signals_dim (upsert-kafka)
        self.t_env.execute_sql(f"""
            CREATE TABLE price_signals_dim (
                product_sku STRING,
                competitor_id BIGINT,
                platform_price DECIMAL(10,2),
                competitor_price DECIMAL(10,2),
                signal_type STRING,
                percentage_diff DECIMAL(5,2),
                priority SMALLINT,
                signal_timestamp TIMESTAMP(3),
                in_stock BOOLEAN,
                is_active BOOLEAN,
                op_ts TIMESTAMP_LTZ(3),
                event_time AS COALESCE(CAST(signal_timestamp AS TIMESTAMP_LTZ(3)), op_ts),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE,
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

        # Sink: demand vs signals summary (hourly join)
        self.t_env.execute_sql(f"""
            CREATE TABLE demand_vs_signals (
                product_sku STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                engagement_delta DOUBLE,
                undercut_cnt BIGINT,
                abrupt_inc_cnt BIGINT,
                overpriced_cnt BIGINT,
                avg_undercut_gap_pct DECIMAL(5,2),
                avg_abrupt_inc_gap_pct DECIMAL(5,2),
                avg_overpriced_gap_pct DECIMAL(5,2),
                price_position STRING,
                score DOUBLE,
                PRIMARY KEY (product_sku, window_start, window_end) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.postgres_url}',
                'table-name' = 'demand_vs_signals',
                'driver' = 'org.postgresql.Driver',
                'username' = '{self.postgres_user}',
                'password' = '{self.postgres_password}',
                'sink.buffer-flush.interval' = '3s',
                'sink.buffer-flush.max-rows' = '200'
            )
        """)

    def build_insert(self):
        # Window signals to hourly and join to behavior summary on product and window
        # engagement_delta is search_rate delta vs previous hour; score weights recent undercuts more
        return """
            INSERT INTO demand_vs_signals
            WITH signals_hour AS (
                SELECT 
                    product_sku,
                    window_start,
                    window_end,
                    SUM(CASE WHEN UPPER(signal_type) = 'UNDERCUT' THEN 1 ELSE 0 END) AS undercut_cnt,
                    SUM(CASE WHEN UPPER(signal_type) = 'PRICE_INCREASE_24H' THEN 1 ELSE 0 END) AS abrupt_inc_cnt,
                    SUM(CASE WHEN UPPER(signal_type) = 'OVERPRICED' THEN 1 ELSE 0 END) AS overpriced_cnt,
                    AVG(CASE WHEN UPPER(signal_type) = 'UNDERCUT'  THEN ABS(CAST(percentage_diff AS DOUBLE)) END) AS avg_undercut_gap_pct,
                    AVG(CASE WHEN UPPER(signal_type) = 'PRICE_INCREASE_24H'  THEN ABS(CAST(percentage_diff AS DOUBLE)) END) AS avg_abrupt_inc_gap_pct,
                    AVG(CASE WHEN UPPER(signal_type) = 'OVERPRICED'  THEN ABS(CAST(percentage_diff AS DOUBLE)) END) AS avg_overpriced_gap_pct
                FROM TABLE(
                    TUMBLE(TABLE price_signals_dim, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
                )
                GROUP BY product_sku, window_start, window_end
            ),
            behavior AS (
                SELECT 
                    curr.product_sku,
                    curr.window_start,
                    curr.window_end,
                    curr.search_rate,
                    prev_b.search_rate AS prev_search_rate
                FROM user_behavior_summary_dim AS curr
                LEFT JOIN user_behavior_summary_dim AS prev_b
                  ON curr.product_sku = prev_b.product_sku
                 AND prev_b.window_end = curr.window_start
            )
            SELECT 
                b.product_sku,
                b.window_start,
                b.window_end,
                (b.search_rate - COALESCE(b.prev_search_rate, b.search_rate)) AS engagement_delta,
                COALESCE(s.undercut_cnt, 0) AS undercut_cnt,
                COALESCE(s.abrupt_inc_cnt, 0) AS abrupt_inc_cnt,
                COALESCE(s.overpriced_cnt, 0) AS overpriced_cnt,
                COALESCE(s.avg_undercut_gap_pct, 0) AS avg_undercut_gap_pct,
                COALESCE(s.avg_abrupt_inc_gap_pct, 0) AS avg_abrupt_inc_gap_pct,
                COALESCE(s.avg_overpriced_gap_pct, 0) AS avg_overpriced_gap_pct,
                CASE
                    WHEN COALESCE(s.avg_undercut_gap_pct, 0) > 0 THEN 'competitor_cheaper'
                    WHEN COALESCE(s.avg_overpriced_gap_pct, 0) > 0 THEN 'we_cheaper'
                    ELSE 'unknown'
                END AS price_position,
                /* simple score: delta + weighted undercut */
                (b.search_rate - COALESCE(b.prev_search_rate, b.search_rate)) + (COALESCE(s.undercut_cnt,0) * 0.01) AS score
            FROM behavior b
            LEFT JOIN signals_hour s
              ON b.product_sku = s.product_sku
             AND b.window_start = s.window_start
             AND b.window_end = s.window_end
        """

    def run(self):
        print("Starting Demand vs Signals job...")
        self.setup_tables()
        insert_sql = self.build_insert()
        result = self.t_env.execute_sql(insert_sql)
        print("Demand vs Signals job submitted. Waiting...")
        result.wait()


if __name__ == "__main__":
    job = DemandVsSignalsJob()
    job.run()



