CREATE SCHEMA IF NOT EXISTS production;

CREATE TABLE IF NOT EXISTS production.customers AS
SELECT * FROM staging.customers WHERE 1=0;

CREATE TABLE IF NOT EXISTS production.products AS
SELECT * FROM staging.products WHERE 1=0;

CREATE TABLE IF NOT EXISTS production.transactions AS
SELECT * FROM staging.transactions WHERE 1=0;

CREATE TABLE IF NOT EXISTS production.transaction_items AS
SELECT * FROM staging.transaction_items WHERE 1=0;