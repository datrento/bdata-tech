-- Users information table
CREATE TABLE
    IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Our platform products table
CREATE TABLE
    IF NOT EXISTS platform_products (
        id BIGSERIAL PRIMARY KEY,
        sku VARCHAR(100) UNIQUE NOT NULL,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        brand VARCHAR(100) NULL,
        current_price DECIMAL(10, 2) NOT NULL, -- current selling price
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

-- Price alerts table (when a competitor changes their price significantly)
CREATE TABLE flink_price_alerts (
    id SERIAL PRIMARY KEY,
    product_sku VARCHAR(100),
    alert_pattern VARCHAR(100), -- 'price_war', 'market_shock', 'opportunity'
    involved_competitors TEXT[], -- Array of competitor names
    confidence_score DECIMAL(3,2),
    recommended_action TEXT,
    alert_timestamp TIMESTAMP
);


-- Add this table to your postgres/init.sql

-- Simple price alerts table for Flink
CREATE TABLE IF NOT EXISTS price_alerts (
    id BIGSERIAL PRIMARY KEY,
    product_sku VARCHAR(100) NOT NULL,
    competitor_name VARCHAR(255) NOT NULL,
    previous_price DECIMAL(10,2),
    new_price DECIMAL(10,2),
    price_change DECIMAL(10,2),
    percentage_change DECIMAL(5,2),
    alert_type VARCHAR(50),
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (product_sku) REFERENCES platform_products(sku)
);

-- Index for better performance
CREATE INDEX IF NOT EXISTS idx_price_alerts_unprocessed 
ON price_alerts(processed, alert_timestamp DESC) 
WHERE processed = FALSE;

-- Keep your existing flink_price_alerts table for complex alerts
-- This new price_alerts table is for simple price change alerts
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

-- Performance indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_competitor_price_sku_time ON competitor_price_history (product_sku, collection_timestamp);

CREATE INDEX IF NOT EXISTS idx_price_alerts_unprocessed ON price_alerts (processed)
WHERE
    processed = FALSE;

-- Insert platform products
INSERT INTO
    platform_products (sku, name, category, brand, current_price)
VALUES
    (
        'IPHONE-15-PRO-128',
        'iPhone 15 Pro 128GB',
        'Smartphones',
        'Apple',
        999.99
    ),
    (
        'MACBOOK-AIR-M2-13',
        'MacBook Air M2 13-inch',
        'Laptops',
        'Apple',
        1199.99
    ),
    (
        'PS5-CONSOLE',
        'PlayStation 5 Console',
        'Gaming',
        'Sony',
        499.99
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