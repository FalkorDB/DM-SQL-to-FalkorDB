-- MySQL sample data for end-to-end migration into FalkorDB.
USE mysql_falkordb_sample;

INSERT INTO customers (customer_id, first_name, last_name, email, city, is_deleted, updated_at) VALUES
  (1, 'Alice', 'Nguyen', 'alice@example.com', 'Berlin', 0, '2026-01-10 10:00:00.000000'),
  (2, 'Bob', 'Cohen', 'bob@example.com', 'Tel Aviv', 0, '2026-01-10 10:05:00.000000'),
  (3, 'Carla', 'Meyer', 'carla@example.com', 'London', 0, '2026-01-10 10:10:00.000000');

INSERT INTO products (product_id, sku, name, category, price, is_deleted, updated_at) VALUES
  (101, 'LAP-13-PRO', 'Falcon Laptop 13', 'Laptops', 1299.00, 0, '2026-01-10 10:00:00.000000'),
  (102, 'MOU-WL-01', 'Wireless Mouse', 'Accessories', 49.00, 0, '2026-01-10 10:00:00.000000'),
  (103, 'DOCK-USB-C', 'USB-C Dock', 'Accessories', 149.00, 0, '2026-01-10 10:00:00.000000');

INSERT INTO orders (order_id, customer_id, status, ordered_at, total_amount, is_deleted, updated_at) VALUES
  (5001, 1, 'PAID', '2026-01-11 09:00:00.000000', 1348.00, 0, '2026-01-11 09:00:00.000000'),
  (5002, 2, 'PAID', '2026-01-11 09:30:00.000000', 149.00, 0, '2026-01-11 09:30:00.000000'),
  (5003, 3, 'PENDING', '2026-01-11 10:00:00.000000', 98.00, 0, '2026-01-11 10:00:00.000000');

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, is_deleted, updated_at) VALUES
  (9001, 5001, 101, 1, 1299.00, 0, '2026-01-11 09:00:00.000000'),
  (9002, 5001, 102, 1, 49.00, 0, '2026-01-11 09:00:00.000000'),
  (9003, 5002, 103, 1, 149.00, 0, '2026-01-11 09:30:00.000000'),
  (9004, 5003, 102, 2, 49.00, 0, '2026-01-11 10:00:00.000000');
