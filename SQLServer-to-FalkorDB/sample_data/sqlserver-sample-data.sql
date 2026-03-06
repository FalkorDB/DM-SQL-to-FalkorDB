-- SQL Server sample data for end-to-end migration into FalkorDB.
USE [sqlserver_falkordb_sample];
GO

INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city, is_deleted, updated_at) VALUES
  (1, N'Alice', N'Nguyen', N'alice@example.com', N'Berlin', 0, '2026-01-10T10:00:00.000000'),
  (2, N'Bob', N'Cohen', N'bob@example.com', N'Tel Aviv', 0, '2026-01-10T10:05:00.000000'),
  (3, N'Carla', N'Meyer', N'carla@example.com', N'London', 0, '2026-01-10T10:10:00.000000');

INSERT INTO dbo.products (product_id, sku, name, category, price, is_deleted, updated_at) VALUES
  (101, N'LAP-13-PRO', N'Falcon Laptop 13', N'Laptops', 1299.00, 0, '2026-01-10T10:00:00.000000'),
  (102, N'MOU-WL-01', N'Wireless Mouse', N'Accessories', 49.00, 0, '2026-01-10T10:00:00.000000'),
  (103, N'DOCK-USB-C', N'USB-C Dock', N'Accessories', 149.00, 0, '2026-01-10T10:00:00.000000');

INSERT INTO dbo.orders (order_id, customer_id, status, ordered_at, total_amount, is_deleted, updated_at) VALUES
  (5001, 1, N'PAID', '2026-01-11T09:00:00.000000', 1348.00, 0, '2026-01-11T09:00:00.000000'),
  (5002, 2, N'PAID', '2026-01-11T09:30:00.000000', 149.00, 0, '2026-01-11T09:30:00.000000'),
  (5003, 3, N'PENDING', '2026-01-11T10:00:00.000000', 98.00, 0, '2026-01-11T10:00:00.000000');

INSERT INTO dbo.order_items (order_item_id, order_id, product_id, quantity, unit_price, is_deleted, updated_at) VALUES
  (9001, 5001, 101, 1, 1299.00, 0, '2026-01-11T09:00:00.000000'),
  (9002, 5001, 102, 1, 49.00, 0, '2026-01-11T09:00:00.000000'),
  (9003, 5002, 103, 1, 149.00, 0, '2026-01-11T09:30:00.000000'),
  (9004, 5003, 102, 2, 49.00, 0, '2026-01-11T10:00:00.000000');
GO
