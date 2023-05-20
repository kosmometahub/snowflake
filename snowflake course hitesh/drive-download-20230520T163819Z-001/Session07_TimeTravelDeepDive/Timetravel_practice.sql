create or replace database TimeTravelPractise DATA_RETENTION_TIME_IN_DAYS=30;
ALTER DATABASE TimeTravelPractise set DATA_RETENTION_TIME_IN_DAYS=90;

ALTER DATABASE TimeTravelPractise set DATA_RETENTION_TIME_IN_DAYS=120;
/*our edition details will be available in service agreement or we can see it 
when we create reader accounts if we have account admin access*/
show databases like 'TimeTravelPractise';
CREATE or replace TABLE City( CityID INT AUTOINCREMENT PRIMARY KEY,
 Name String(50), Zip String(10), StateCode String(2));
 
show tables

select current_timestamp()
DROP DATABASE TimeTravelPractise ;

UNDROP DATABASE TimeTravelPractise;
select current_timestamp;  --2022-07-03 20:15:32.039 -0700

INSERT INTO City(Name, Zip, StateCode) Values ('Sunnyvale', '94087', 'CA'),
('Cupertino', '95687', 'CA'),
('MountainView', '94017', 'CA'),
('Santa Clara', '95054', 'CA');

select * from City;  


SELECT * FROM city 
AT(timestamp => '2022-07-03 20:15:32.039 -0700'::timestamp_tz);

select current_timestamp;   --2022-07-03 20:18:05.275 -0700

INSERT INTO City(Name, Zip, StateCode) Values ('Bozman', '28732', 'MA');


SELECT * FROM city 
AT(timestamp => '2022-07-03 20:18:05.275 -0700'::timestamp_tz);


select * from City;  


DROP TABLE City;

UNDROP TABLE City;

select * from City

delete from city where cityid=1

--Query data just few min before the insert statements
--By offset → In this example, we select the historical data from a table as of 2 minutes ago:
select * from City at(offset => -60*10); --before 2 min

select * from City before(offset => -60*5);

drop table City

undrop table City

delete from city;

select * from city;
--Query data before a specific statement
--By query statement ID → We can see the Query Statement ID in the history of queries:
INSERT INTO City(Name, Zip, StateCode) Values ('Bozman', '28732', 'MA');

select LAST_QUERY_ID(-3);  --01a24672-0000-2023-0000-0002be9eab2d
show tables;
create or replace table city as select * from city before (statement =>'01a24672-0000-2023-0000-0002be9eab2d'); 
select * from city before (statement =>'01a56512-3200-7baa-0001-38d60001d0fe'); 
select * from city;

--By timestamp
select current_timestamp  --2022-02-13 19:08:32.797 -0800
update city set name = 'Bangalore'  where name='Sunnyvale'
SELECT * FROM city 
AT(timestamp => '2022-02-13 19:08:32.797 -0800'::timestamp_tz);

SELECT * FROM city 
BEFORE(timestamp => '2022-01-12 19:02:44.294 -0800'::timestamp_tz);

DROP TABLE city;

select * from city;

UNDROP TABLE city;

ALTER ACCOUNT SET DATA_RETENTION_TIME_IN_DAYS=1; 

--Data Retention period parameter can be set for Account » Database » Schema » Table

--LAST_QUERY_ID => Returns the ID of a specified query in the current session. 
--If no query is specified, the most recently-executed query is returned.
select LAST_QUERY_ID()

/*Positive numbers start with the first query executed in the session. For example:

LAST_QUERY_ID(1) returns the first query.

LAST_QUERY_ID(2) returns the second query.

LAST_QUERY_ID(6) returns the sixth query.

Etc.

Negative numbers start with the most recently-executed query in the session. For example:

LAST_QUERY_ID(-1) returns the most recently-executed query (equivalent to LAST_QUERY_ID()).

select LAST_QUERY_ID(-2) returns the second most recently-executed query. */


show parameters in account;
select *
    from table(result_scan(last_query_id(-1))) where "key"='DATA_RETENTION_TIME_IN_DAYS'

show parameters;