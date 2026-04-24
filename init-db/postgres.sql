CREATE TABLE IF NOT EXISTS finance_orders (
    id SERIAL PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
