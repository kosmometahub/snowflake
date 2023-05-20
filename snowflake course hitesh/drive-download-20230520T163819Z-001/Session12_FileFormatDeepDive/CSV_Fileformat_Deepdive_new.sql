use database PRODUCTION_DATABASE;
use schema sf_schema;
CREATE TABLE Dimcustomers (
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

select * from customers;

list @PUBLIC.EXTERNAL_STG/

TRUNCATE TABLE Dimcustomers

select * from Dimcustomers 


ALTER TABLE Dimcustomers ADD TEST_COLUMN VARCHAR(100);

TRUNCATE TABLE Dimcustomers;
select * from Dimcustomers;

COPY INTO Dimcustomers from @PUBLIC.EXTERNAL_STG/DW/loan_schema/Dimcustomer/10_05_2023/customerdim_10_05_2023_09.csv file_format = CSV_FILEFORMAT FORCE=TRUE;

DESCRIBE FILE FORMAT CSV_FILEFORMAT;

ALTER FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT SET  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

DESCRIBE FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT

ALTER FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT SET ENCODING=UTF16
