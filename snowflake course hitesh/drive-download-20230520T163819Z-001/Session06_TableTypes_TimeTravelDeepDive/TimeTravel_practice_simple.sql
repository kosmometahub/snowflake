create or replace database TimeTravelPractise DATA_RETENTION_TIME_IN_DAYS=30;
ALTER DATABASE TimeTravelPractise set DATA_RETENTION_TIME_IN_DAYS=90;

ALTER DATABASE TimeTravelPractise set DATA_RETENTION_TIME_IN_DAYS=120;
/*our edition details will be available in service agreement or we can see it 
when we create reader accounts if we have account admin access*/
show databases like 'TimeTravelPractise';
CREATE or replace TABLE City( CityID INT AUTOINCREMENT PRIMARY KEY,
 Name String(50), Zip String(10), StateCode String(2));
 
show tables

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