-- Run these statements in the same Livy SQL session referenced by spark.session_id.
-- They create table-like Spark SQL sources from local sample JSON files.

CREATE OR REPLACE TEMP VIEW customers_src AS
SELECT customer_id, name, email, updated_at
FROM json.`file:///workspace/data/customers.json`;

CREATE OR REPLACE TEMP VIEW products_src AS
SELECT product_id, name, category, price, updated_at
FROM json.`file:///workspace/data/products.json`;

CREATE OR REPLACE TEMP VIEW orders_src AS
SELECT order_id, customer_id, product_id, quantity, ordered_at, updated_at
FROM json.`file:///workspace/data/orders.json`;

SELECT count(*) AS customers_count FROM customers_src;
SELECT count(*) AS products_count FROM products_src;
SELECT count(*) AS orders_count FROM orders_src;
