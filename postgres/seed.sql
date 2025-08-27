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
    external_competitors (name, code, website)
VALUES
    ('Amazon', 'amazon', 'https://www.amazon.com'),
    ('Best Buy', 'bestbuy', 'https://www.bestbuy.com'),
    ('Ebay', 'ebay', 'https://www.ebay.com'),
    ('Walmart', 'walmart', 'https://www.walmart.com'),
    ('Target', 'target', 'https://www.target.com') ON CONFLICT (name) DO NOTHING;

INSERT INTO
    platform_product_price_history (
        sku,
        old_price,
        adjusted_price,
        change_type,
        changed_by,
        change_timestamp
    )
VALUES
    (
        'PS5-CONSOLE',
        19.99,
        21.99,
        'manual',
        'admin',
        '2025-08-20 10:15:00'
    ),
    (
        'PS5-CONSOLE',
        18.49,
        19.99,
        'auto',
        'system',
        '2025-08-21 09:00:00'
    ),
    (
        'PS5-CONSOLE',
        29.99,
        27.99,
        'manual',
        'admin',
        '2025-08-21 14:30:00'
    ),
    (
        'PS5-CONSOLE',
        9.99,
        10.99,
        'auto',
        'system',
        '2025-08-22 08:45:00'
    );