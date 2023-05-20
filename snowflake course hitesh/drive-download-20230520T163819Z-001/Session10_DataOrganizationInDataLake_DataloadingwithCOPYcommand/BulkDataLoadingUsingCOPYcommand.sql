--Amazon Simple Storage Service (Amazon S3)

-- A stage in Snowflake is an intermediate space where you can upload the files from


-- create a stage through UI or SQL

--pass your aws accounts ACCESSKEYID and SECRETKEY in the below script

create database PRODUCTION_DATABASE;
create schema sf_schema;

CREATE STAGE "PRODUCTION_DATABASE"."SF_SCHEMA".S3_EXTERNAL_STG URL = 's3://skillscaleraw/DW' 
CREDENTIALS = (AWS_KEY_ID = '********' AWS_SECRET_KEY = '************');

use database production_database;
use  schema sf_schema;

LIST @EXTERNAL_STAGE/DW/loan_schama/dimcustomer/13_03_2023/

-- CREATE FILE FORMAT
CREATE OR REPLACE FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT  COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');
--CREATE FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT_UPDATED  COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');
DESCRIBE FILE FORMAT "PRODUCTION_DATABASE"."SF_SCHEMA".CSV_FILEFORMAT 
CREATE OR REPLACE TABLE customer_dim (
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


select * from customer_dim;  -- before load it will be empty

truncate table customer_dim
--truncate table customers;

-- loads only specific file given in copy command
copy into customer_dim
  from @EXTERNAL_STG/DW/loan_schema/customer_dim/05_21_2022/
  file_format = (format_name = CSV_FILEFORMAT);

---  If you try to load the same file again it wont load again as loadhistory metadata will be maintained for 64 days at table level for copy command
copy into customer_dim
  from @EXTERNAL_STAGE/DW/loan_schama/dimcustomer/13_03_2023/ 
  file_format = (format_name = CSV_FILEFORMAT);
  
-- loads all files under "09_12_2022" folder of the specified path which are not loaded ealier
copy into customer_dim
  from @EXTERNAL_STAGE/DW/loan_schama/dimcustomer/ 
  file_format = (format_name = CSV_FILEFORMAT) FORCE=TRUE;

truncate table customer_dim
  
select * from customer_dim where country like '%,%'
