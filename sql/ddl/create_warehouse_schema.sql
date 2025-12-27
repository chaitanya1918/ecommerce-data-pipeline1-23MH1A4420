CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  date_key INT PRIMARY KEY,
  full_date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT,
  month_name VARCHAR(15),
  day_name VARCHAR(15),
  week_of_year INT,
  is_weekend BOOLEAN
);
