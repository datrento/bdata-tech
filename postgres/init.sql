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
        collection_timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku),
        FOREIGN KEY (competitor_id) REFERENCES external_competitors (id)
    );

-- Simple price alerts table for Flink
CREATE TABLE
    IF NOT EXISTS price_alerts (
        id BIGSERIAL PRIMARY KEY,
        product_sku VARCHAR(100) NOT NULL,
        competitor_name VARCHAR(255) NOT NULL,
        previous_price DECIMAL(10, 2),
        new_price DECIMAL(10, 2),
        price_change DECIMAL(10, 2),
        percentage_change DECIMAL(5, 2),
        alert_type VARCHAR(50),
        alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku)
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
        FOREIGN KEY (product_sku) REFERENCES platform_products (sku)
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
        FOREIGN KEY (competitor_id) REFERENCES external_competitors (id)
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

CREATE INDEX IF NOT EXISTS idx_price_alerts_unprocessed ON price_alerts (processed, alert_timestamp DESC)
WHERE
    processed = FALSE;

CREATE INDEX IF NOT EXISTS idx_competitor_price_sku_time ON competitor_price_history (product_sku, collection_timestamp);

CREATE INDEX IF NOT EXISTS idx_price_alerts_unprocessed ON price_alerts (processed)
WHERE
    processed = FALSE;

-- Insert platform products
INSERT INTO
    platform_products (
        sku,
        name,
        category,
        brand,
        cost,
        min_viable_price,
        current_price,
        price_sensitivity
    )
VALUES
    (
        'IPHONE-15-PRO-128',
        'iPhone 15 Pro 128GB',
        'Smartphones',
        'Apple',
        850.00,
        900.00,
        950.00,
        'low'
    ),
    (
        'MACBOOK-AIR-M2-13',
        'MacBook Air M2 13-inch',
        'Laptops',
        'Apple',
        950.00,
        1000.00,
        1050.00,
        'low'
    ),
    (
        'PS5-CONSOLE',
        'PlayStation 5 Console',
        'Gaming',
        'Sony',
        400.00,
        450.00,
        500.00,
        'low'
    ) ON CONFLICT (sku) DO NOTHING;

-- Insert external competitors
INSERT INTO
    external_competitors (name, website)
VALUES
    ('Amazon', 'https://www.amazon.com'),
    ('Best Buy', 'https://www.bestbuy.com'),
    ('Ebay', 'https://www.ebay.com'),
    ('Walmart', 'https://www.walmart.com'),
    ('Target', 'https://www.target.com') ON CONFLICT (name) DO NOTHING;