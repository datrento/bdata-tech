-- Our platform products table
CREATE TABLE
    IF NOT EXISTS platform_products (
        id BIGSERIAL PRIMARY KEY,
        sku VARCHAR(100) UNIQUE NOT NULL,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        brand VARCHAR(100) NULL,
        cost DECIMAL(10, 2) NOT NULL, -- Cost price to acquire
        min_viable_price DECIMAL(10, 2) NULL, -- Minimum viable price
        current_price DECIMAL(10, 2) NOT NULL, -- Actual selling price
        price_sensitivity VARCHAR(50) NULL, -- Price sensitivity
        price_elasticity DECIMAL(6, 3) NULL, -- Estimated own-price elasticity
        sensitivity_last_updated TIMESTAMP NULL,
        sensitivity_observations INT DEFAULT 0,
        in_stock BOOLEAN DEFAULT TRUE,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Competitors table
CREATE TABLE
    IF NOT EXISTS external_competitors (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        code VARCHAR(100) NOT NULL,
        website VARCHAR(255) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Universal product mapping table
CREATE TABLE
    IF NOT EXISTS product_mapping (
        id BIGSERIAL PRIMARY KEY,
        product_id BIGINT NOT NULL,
        amazon_asin VARCHAR(100) NOT NULL,
        ebay_item_id VARCHAR(100) NOT NULL,
        bestbuy_sku VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES platform_products (id)
    );

-- Competitor price history table (data from Kafka)
CREATE TABLE
    IF NOT EXISTS competitor_price_history (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        competitor_id BIGINT NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        in_stock BOOLEAN DEFAULT TRUE,
        source_sku VARCHAR(100),
        data_timestamp TIMESTAMP NOT NULL,
        collection_timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
        FOREIGN KEY (competitor_id) REFERENCES external_competitors (id),
        UNIQUE (product_sku, competitor_id, data_timestamp)
    );

-- Time-based price trends table
CREATE TABLE
    IF NOT EXISTS price_trends (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        competitor_name VARCHAR(255) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        avg_price DECIMAL(10, 2) NOT NULL,
        price_volatility DECIMAL(5, 2) NOT NULL,
        trend_direction VARCHAR(50) NOT NULL,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
        UNIQUE (product_sku, competitor_name, window_start, window_end)
    );

-- Price signals table for real-time pricing insights
CREATE TABLE
    IF NOT EXISTS price_signals (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        competitor_id BIGINT NOT NULL,
        signal_type VARCHAR(50) NOT NULL, -- e.g., 'undercut', 'price_spike', 'stockout'
        platform_price DECIMAL(10, 2) NOT NULL,
        competitor_price DECIMAL(10, 2) NOT NULL,
        percentage_diff DECIMAL(5, 2) NOT NULL, -- Percentage difference that triggered the signal
        recommendation VARCHAR(255),
        priority SMALLINT DEFAULT 3,
        in_stock BOOLEAN DEFAULT TRUE,
        signal_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
        FOREIGN KEY (competitor_id) REFERENCES external_competitors (id),
        UNIQUE (product_sku, competitor_id, signal_type)
    );

-- Price movements table
CREATE TABLE IF NOT EXISTS price_movements (
  id BIGSERIAL PRIMARY KEY,
  product_sku VARCHAR(100) NOT NULL,
  competitor_id BIGINT NOT NULL,
  previous_price DECIMAL(10,2) NOT NULL,
  new_price DECIMAL(10,2) NOT NULL,
  pct_change DECIMAL(6,2) NOT NULL,
  direction VARCHAR(8) NOT NULL, -- 'up' | 'down'
  in_stock BOOLEAN,
  event_time TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
  FOREIGN KEY (competitor_id) REFERENCES external_competitors (id),
  UNIQUE (product_sku, competitor_id, event_time)
);

-- Aggregator data table (data from Kafka)
CREATE TABLE
    IF NOT EXISTS aggregator_data (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        aggregator_name VARCHAR(255) NOT NULL,
        avg_price DECIMAL(10, 2) NOT NULL,
        min_price DECIMAL(10, 2) NOT NULL,
        max_price DECIMAL(10, 2) NOT NULL,
        stores_tracked INT NOT NULL,
        collection_timestamp TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku)
    );

-- Price-demand observations for elasticity estimation
CREATE TABLE
    IF NOT EXISTS price_demand_observations (
        id BIGSERIAL PRIMARY KEY,
        sku VARCHAR(100) NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        price DECIMAL(10, 2) NOT NULL,
        demand INT NOT NULL
    );

-- Price sensitivity history (time-series of elasticity/sensitivity labels)
CREATE TABLE IF NOT EXISTS price_sensitivity_history (
    id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(100) NOT NULL,
    price_elasticity DOUBLE PRECISION NOT NULL,
    price_sensitivity VARCHAR(50) NOT NULL,
    observations INT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sku) REFERENCES platform_products (sku)
);

-- Product market summary table
CREATE TABLE
    IF NOT EXISTS product_market_summary (
        product_sku VARCHAR(100) PRIMARY KEY,
        cheapest_competitor_id BIGINT NOT NULL,
        cheapest_price DECIMAL(10, 2) NOT NULL,
        platform_price DECIMAL(10, 2) NOT NULL,
        gap_pct DECIMAL(6, 2) NOT NULL,
        competitor_count INT NOT NULL,
        in_stock_competitor_count INT NOT NULL,
        summary_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
        FOREIGN KEY (cheapest_competitor_id) REFERENCES external_competitors (id)
    );

-- Platform product price history table
CREATE TABLE
    IF NOT EXISTS platform_product_price_history (
        id BIGSERIAL PRIMARY KEY,
        sku VARCHAR(100) NOT NULL,
        old_price DECIMAL(10, 2),
        adjusted_price DECIMAL(10, 2) NOT NULL,
        change_type VARCHAR(32),
        changed_by VARCHAR(64),
        source VARCHAR(32),
        reason_code VARCHAR(64),
        change_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        notes TEXT,
        FOREIGN KEY (sku) REFERENCES platform_products (sku)
    );

-- User behavior events table
CREATE TABLE
    IF NOT EXISTS user_behavior_events (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        page_views BIGINT NOT NULL,
        unique_visitors BIGINT NOT NULL,
        dwell_seconds DOUBLE PRECISION NOT NULL,
        searches BIGINT NOT NULL,
        cart_additions BIGINT NOT NULL,
        purchases BIGINT NOT NULL, 
        price_comparisons BIGINT NOT NULL,
        data_timestamp TIMESTAMP NOT NULL,
        collection_timestamp TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku)
    );

-- User behavior summary table
CREATE TABLE
    IF NOT EXISTS user_behavior_summary (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        page_views BIGINT NOT NULL,
        searches BIGINT NOT NULL,
        cart_additions BIGINT NOT NULL,
        purchases BIGINT NOT NULL, 
        price_comparisons BIGINT NOT NULL,
        search_rate DOUBLE PRECISION NOT NULL,
        search_cart_rate DOUBLE PRECISION NOT NULL,
        cart_purchase_rate DOUBLE PRECISION NOT NULL, 
        view_compare_rate DOUBLE PRECISION NOT NULL,
        avg_dwell_seconds DOUBLE PRECISION NOT NULL, 
        unique_visitors BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku)
    );

-- Demand vs Signals table
CREATE TABLE
    IF NOT EXISTS demand_vs_signals (
        id BIGSERIAL NOT NULL,
        product_sku VARCHAR(100) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        engagement_delta DOUBLE PRECISION NOT NULL,
        undercut_cnt BIGINT NOT NULL,
        abrupt_inc_cnt BIGINT NOT NULL,
        overpriced_cnt BIGINT NOT NULL,
        avg_undercut_gap_pct DECIMAL(5, 2) NOT NULL,
        avg_abrupt_inc_gap_pct DECIMAL(5, 2) NOT NULL,
        avg_overpriced_gap_pct DECIMAL(5, 2) NOT NULL,
        price_position VARCHAR(50) NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (product_sku, window_start, window_end)
    );
COMMENT ON COLUMN demand_vs_signals.score IS 'Score is a combination of engagement delta(using search rate as an engagement metric) and undercut count. Formula: engagement_delta + (undercut_cnt * 0.01)';


-- Create indexes for efficient querying
CREATE INDEX idx_price_signals_active ON price_signals (is_active, signal_timestamp DESC)
WHERE
    is_active = TRUE;

CREATE INDEX idx_price_signals_product ON price_signals (product_sku, signal_timestamp DESC);

CREATE INDEX idx_price_signals_type ON price_signals (signal_type, signal_timestamp DESC);

-- Index for better performance
-- Add time-based indexes
-- Performance indexes for faster queries
CREATE INDEX idx_price_trends_time ON price_trends (window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_competitor_price_sku_time ON competitor_price_history (product_sku, collection_timestamp);

-- add index for price demand observations
CREATE INDEX IF NOT EXISTS idx_pdo_sku_time ON price_demand_observations (sku, observed_at DESC);

-- add indexes for the user behaviour, user behavior summary and demand vs price signals
CREATE INDEX idx_user_behavior_events_time ON user_behavior_events (data_timestamp, collection_timestamp);
CREATE INDEX idx_user_behavior_summary_time ON user_behavior_summary (window_start, window_end);
CREATE INDEX idx_demand_vs_signals_time ON demand_vs_signals (window_start, window_end);

-- add indexes for the price movements table
CREATE INDEX IF NOT EXISTS idx_price_movements_sku_time ON price_movements (product_sku, event_time DESC);
-- fast lookups of platform price at time t
CREATE INDEX IF NOT EXISTS idx_ppph_sku_time ON platform_product_price_history (sku, change_timestamp DESC);

-- add index for price sensitivity history
CREATE INDEX IF NOT EXISTS idx_psh_sku_time ON price_sensitivity_history (sku, updated_at DESC);

-- Ensure CDC provides before images for UPDATE/DELETE
ALTER TABLE IF EXISTS public.price_signals REPLICA IDENTITY FULL;
ALTER TABLE IF EXISTS public.platform_products REPLICA IDENTITY FULL;
ALTER TABLE IF EXISTS public.user_behavior_summary REPLICA IDENTITY FULL;
ALTER TABLE IF EXISTS public.price_movements REPLICA IDENTITY FULL;