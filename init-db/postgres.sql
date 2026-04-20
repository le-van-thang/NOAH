CREATE TABLE IF NOT EXISTS finance_orders (
  id SERIAL PRIMARY KEY,
  order_id INT,
  sku VARCHAR(50),
  quantity INT,
  created_at TIMESTAMP DEFAULT now()
);
