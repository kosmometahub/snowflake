-- databases
USE ROLE SYSADMIN;

CREATE DATABASE our_first_database;

SHOW DATABASES LIKE 'our_first_database';  --retention_time is set to 1 by default

CREATE OR REPLACE DATABASE production_database 
DATA_RETENTION_TIME_IN_DAYS = 15
COMMENT = 'Critical production database';

use DATABASE production_database 

CREATE TABLE TEST_TBL (NAME VARCHAR(100))  25 
show tables;
ALTER TABLE TEST_TBL SET DATA_RETENTION_TIME_IN_DAYS = 90;
CREATE TABLE TEST_TBL2 (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 5

use database our_first_database

CREATE TABLE TEST_FIRST_TBL (NAME VARCHAR(100))
use database production_database;
show tables;

CREATE OR REPLACE TRANSIENT TABLE TRANSIENT_TABLE (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 1;
CREATE OR REPLACE TRANSIENT TABLE TRANSIENT_TABLE (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 10; -- SQL compilation error: invalid value [10] for parameter 'DATA_RETENTION_TIME_IN_DAYS'
CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 1;

INSERT INTO TEST_TBL values ('Permanent')
INSERT INTO TRANSIENT_TABLE values ('TRANSIENT')
INSERT INTO TEMP_TABLE values ('TEMP')

select * from TEST_TBL;
select * from TRANSIENT_TABLE;
select * from TEMP_TABLE; 

DROP TABLE TEST_TBL;
DROP TABLE TRANSIENT_TABLE;
CREATE  OR REPLACE TABLE TEST3 (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 5

-- in order to set the time travel duration above 1 day, you must have at least the Enterprise license for Snowflake

SHOW DATABASES LIKE 'production_database';

CREATE OR REPLACE TRANSIENT DATABASE trans_database 
DATA_RETENTION_TIME_IN_DAYS = 0
COMMENT = 'Temporary database for ETL processing';

show schemas

CREATE OR REPLACE TABLE TEST_TBL (NAME VARCHAR(100)) DATA_RETENTION_TIME_IN_DAYS = 30

show tables;

CREATE TEMPORARY TABLE TEMP_TBL (NAME VARCHAR(100))  DATA_RETENTION_TIME_IN_DAYS = 30

CREATE OR REPLACE TABLE TEST_TBL2 (NAME VARCHAR(100))

show tables;
SHOW DATABASES LIKE 'transient_database';
-- will show retention_time as zero, indicating that there is no time travel storage for this database, 
-- also the options column would show TRANSIENT as the option
--Transient => no fail-safe storage for the database.

ALTER DATABASE transient_database
SET DATA_RETENTION_TIME_IN_DAYS = 1;

SHOW DATABASES LIKE 'transient_database';

--A schema is a logical grouping of database objects (tables, views, etc.)

--schema
CREATE schem schema_creation;

SHOW SCHEMAS IN DATABASE schema_creation; --check whether the newly created database already has a schema in it (PUBLIC and INFORMATION_SCHEMA)

CREATE SCHEMA custom_schema
COMMENT = 'A new custom schema';

SHOW SCHEMAS LIKE 'custom_schema' IN DATABASE schema_creation ;

use DATABASE production_database
CREATE TRANSIENT SCHEMA transient_data 
DATA_RETENTION_TIME_IN_DAYS = 0
COMMENT = 'Schema containing temporary data used by ETL processes';

DATABASE RETENTION TIME =15 days
SChema  RETENTION TIME =7 days
table RETENTION TIME =3 days

 default retention period will be one day
SHOW SCHEMAS LIKE 'transient_data' IN DATABASE schema_creation ;