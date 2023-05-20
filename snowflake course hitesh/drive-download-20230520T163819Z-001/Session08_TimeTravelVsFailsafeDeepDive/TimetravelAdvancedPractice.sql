------ time travel with TIMESTAMP AT/Before----
CREATE DATABASE TIMETRAVEL_EXERCISE;
CREATE TABLE CUSTOMER AS 
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER
LIMIT 100000;
-- CTAS

--validate that data has successfully been populated
SELECT * FROM CUSTOMER LIMIT 100;
SELECT count(*) FROM CUSTOMER;
--make note of the current time before running an update on the customer table. 
--We will use this time stamp to see the data as it existed before our update. 
SELECT CURRENT_TIMESTAMP; --2022-02-14 18:42:32.641 -0800
 
--run an UPDATE on the customer table.  We will update the email address column for all rows.
UPDATE CUSTOMER SET C_EMAIL_ADDRESS = 'john.doe@gmail.com';
 
--Validate that the email address column has indeed been updated for the whole table.
SELECT DISTINCT C_EMAIL_ADDRESS FROM CUSTOMER;

--use the time travel functionality of Snowflake to view the data as it existed before the update. 
--We will use the timestamp and the AT syntax, to travel back to how the table looked like at or before specific time.
--Replace the time stamp with the timestamp from the previous step
SELECT DISTINCT C_EMAIL_ADDRESS 
FROM CUSTOMER AT 
(TIMESTAMP => '2022-02-14 18:42:32.641 -0800'::timestamp_tz);
 
-- select all rows
SELECT * 
FROM CUSTOMER AT 
(TIMESTAMP => '<time_stamp>'::timestamp_tz);

--if you are not 100% sure of the time when the update was made, you can use the BEFORE syntax and provide an approximate timestamp.

SELECT DISTINCT C_EMAIL_ADDRESS 
FROM CUSTOMER BEFORE 
(TIMESTAMP => '<time_stamp>'::timestamp_tz);

-- figure out the query id of update statement
SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE = 'UPDATE' 
AND EXECUTION_STATUS = 'SUCCESS'
AND DATABASE_NAME = 'TIMETRAVEL_EXERCISE';

---------------------- recover data that's deleted accidentally using STATEMENT------------
--simulate an accidental DELETE
DELETE FROM CUSTOMER;

--Validate that all rows from the table have been deleted. To do so run the following SQL: 
SELECT * FROM CUSTOMER;

--query the query history to identify which query deleted all the rows.
SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE = 'DELETE' 
AND EXECUTION_STATUS = 'SUCCESS'
AND DATABASE_NAME = 'TIMETRAVEL_EXERCISE';

--use the timestamp and the BEFORE syntax, to travel back to how the table looked like before the delete was executed.
--replace the query_id with the appropriate query_id from the above statement
SELECT * FROM CUSTOMER;
create or replace table CUSTOMER as
SELECT *
FROM CUSTOMER BEFORE
(STATEMENT => '01a2516f-0000-2022-0000-0002be9ed971');

select * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
 

--undo the delete by inserting this data back into the table by using time travel. 
INSERT INTO CUSTOMER
SELECT *
FROM CUSTOMER BEFORE
(STATEMENT => '01a2516f-0000-2022-0000-0002be9ed971');

--Validate that the data has been restored
SELECT * FROM CUSTOMER;


----------------- identify_dropped_objects and undrop them ------------------
--drop the customer table from the CUSTOMER schema.

DROP TABLE CUSTOMER; --1 day

create table test(name varchar)
 
--programmatically find out the tables that may have been dropped
--Tables which have been dropped will have a non-NULL date value in the TABLE_DROPPED column.
USE ROLE ACCOUNTADMIN;
SELECT TABLE_DROPPED, TABLE_CATALOG, TABLE_SCHEMA,TABLE_NAME,
ID,CLONE_GROUP_ID, TABLE_CREATED 
FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE TABLE_CATALOG = 'TIMETRAVEL_EXERCISE';

--Let us now restore the dropped table.
UNDROP TABLE CUSTOMER;
 
--Validate that the table is indeed available now
SELECT COUNT(*) FROM CUSTOMER;

SELECT * FROM CUSTOMER;
CREATE SCHEMA SCHEMA1;
show schemas
create table test(name varchar)
create table customers as select * from public.CUSTOMER;
--drop the whole SCHEMA1 schema (will delete all objects under that schema)
DROP SCHEMA SCHEMA1;
 
--Restore the schema (will restore even objects in that schema)
UNDROP SCHEMA SCHEMA1;