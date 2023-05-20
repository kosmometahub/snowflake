use database PRODUCTION_DATABASE;
create schema sf_schema
CREATE OR REPLACE TABLE customers (
  id              INT NOT NULL,
  last_name       VARCHAR(100) ,
  first_name      VARCHAR(100),
  email           VARCHAR(100),
  company         VARCHAR(100),
  phone           VARCHAR(100),
  address1        STRING,
  address2        STRING,
  city            VARCHAR(100),
  state           VARCHAR(100),
  postal_code     VARCHAR(15),
  country         VARCHAR(50)
);

select * from sf_schema.customers;
LOAN_NUMBER PRIMARY 
LOAN_DIM - 1 TB  10 M 
select * from INFORMATION_SCHEMA.DATABASES 
