CREATE TABLE IF NOT EXISTS finance_orders (
    id SERIAL PRIMARY KEY,
    order_id INT UNIQUE,
    user_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
