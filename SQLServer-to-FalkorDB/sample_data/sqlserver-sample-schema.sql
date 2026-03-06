-- SQL Server sample schema for end-to-end migration into FalkorDB.
-- Creates a small commerce-like model with incremental and soft-delete fields.

IF DB_ID(N'sqlserver_falkordb_sample') IS NULL
BEGIN
    CREATE DATABASE [sqlserver_falkordb_sample];
END
GO

USE [sqlserver_falkordb_sample];
GO

IF OBJECT_ID(N'dbo.order_items', N'U') IS NOT NULL DROP TABLE dbo.order_items;
IF OBJECT_ID(N'dbo.orders', N'U') IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID(N'dbo.products', N'U') IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID(N'dbo.customers', N'U') IS NOT NULL DROP TABLE dbo.customers;
GO

CREATE TABLE dbo.customers (
  customer_id BIGINT NOT NULL PRIMARY KEY,
  first_name NVARCHAR(100) NOT NULL,
  last_name NVARCHAR(100) NOT NULL,
  email NVARCHAR(255) NOT NULL UNIQUE,
  city NVARCHAR(120) NOT NULL,
  is_deleted BIT NOT NULL CONSTRAINT DF_customers_is_deleted DEFAULT (0),
  updated_at DATETIME2(6) NOT NULL CONSTRAINT DF_customers_updated_at DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.products (
  product_id BIGINT NOT NULL PRIMARY KEY,
  sku NVARCHAR(64) NOT NULL UNIQUE,
  name NVARCHAR(255) NOT NULL,
  category NVARCHAR(120) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  is_deleted BIT NOT NULL CONSTRAINT DF_products_is_deleted DEFAULT (0),
  updated_at DATETIME2(6) NOT NULL CONSTRAINT DF_products_updated_at DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.orders (
  order_id BIGINT NOT NULL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status NVARCHAR(40) NOT NULL,
  ordered_at DATETIME2(6) NOT NULL,
  total_amount DECIMAL(10,2) NOT NULL,
  is_deleted BIT NOT NULL CONSTRAINT DF_orders_is_deleted DEFAULT (0),
  updated_at DATETIME2(6) NOT NULL CONSTRAINT DF_orders_updated_at DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_orders_customer FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
);
GO

CREATE TABLE dbo.order_items (
  order_item_id BIGINT NOT NULL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL,
  is_deleted BIT NOT NULL CONSTRAINT DF_order_items_is_deleted DEFAULT (0),
  updated_at DATETIME2(6) NOT NULL CONSTRAINT DF_order_items_updated_at DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_order_items_order FOREIGN KEY (order_id) REFERENCES dbo.orders(order_id),
  CONSTRAINT FK_order_items_product FOREIGN KEY (product_id) REFERENCES dbo.products(product_id)
);
GO

CREATE INDEX idx_customers_updated_at ON dbo.customers(updated_at);
CREATE INDEX idx_products_updated_at ON dbo.products(updated_at);
CREATE INDEX idx_orders_updated_at ON dbo.orders(updated_at);
CREATE INDEX idx_order_items_updated_at ON dbo.order_items(updated_at);
GO
