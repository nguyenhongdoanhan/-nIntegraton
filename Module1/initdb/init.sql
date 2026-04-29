CREATE DATABASE IF NOT EXISTS inventory_db;
USE inventory_db;

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    quantity INT NOT NULL DEFAULT 0
);

INSERT INTO products (product_id, quantity) VALUES
(101, 10),
(102, 20),
(103, 30),
(104, 40);