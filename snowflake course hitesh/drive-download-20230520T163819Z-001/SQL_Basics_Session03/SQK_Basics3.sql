--Data Definition Language (DDL): changes the structure of the table like creating a table, deleting a table, altering a table
CREATE
ALTER
DROP
TRUNCATE

--CREATE
Syntax: CREATE DATABASE database_name;

CREATE DATABASE HR_DEPT_DB;

CREATE SCHEMA HR_SCHEMA;

use HR_DEPT_DB;
50 
10 HR dept --
12 
20 FInance --
create table empdtl (empid int,empname varchar(50),empsal int)

create table HR_SCHEMA.empdtl (empid int,empname varchar(50),empsal int)

ALTER TABLE empdtl ADD empdesg VARCHAR(50)

DROP TABLE HR_SCHEMA.empdtl;

alter table empdtl  alter column  empname varchar(100)

ALTER TABLE empdtl  ADD EMPADDRESS VARCHAR(50)

ALTER TABLE empdtl  DROP COLUMN EMPADDRESS


/*Data Manipulation Language(DML)
INSERT
UPDATE
DELETE */

INSERT INTO empdtl  VALUES (1,'Kiran',50000,'SSE'),
(2,'Madhu',50000,'SSE');  

select * from empdtl;


insert into empdtl (EMPID,EMPNAME,EMPSAL,EMPDESG)
select 8,'krishna',70000,'TL' union all
select 9,'balu',60000, 'SE' union all
select 56,'hari',40000, 'ASE' union all
select 34,'kish',900000,'MG'
 
INSERT INTO empdtl  VALUES (78)  -- throws error insert list is not specified and inserting data for all columns
INSERT INTO empdtl VALUES (45,'KARTHIK',56577) -- throws error insert list is not specified and inserting data for all columns
INSERT INTO empdtl  (EMPID,EMPNAME,EMPSAL) VALUES (45,'KARTHIK',56577) 

select * from empdtl where empid=1
select * from empdtl where empid!=1
select * from empdtl where empid<>1
select * from empdtl where empsal>50000
select * from empdtl where empdesg=null -- null cant be filtered with =
select * from empdtl where empdesg is null
INSERT INTO empdtl  (EMPID,EMPNAME,EMPSAL,EMPDESG) VALUES (45,'KARTHIK',56577,'')
select * from empdtl where empdesg=''


ALTER TABLE empdtl  ADD EMAILID VARCHAR(50)
UPDATE empdtl  SET EMAILID=EMPNAME+'@skillscaler.net' -- in SQL server
UPDATE empdtl  SET EMAILID=EMPNAME||'@skillscaler.net' --In sanowflake we will use "||" to concatinate instead of +
update empdtl  set empid=3 where empname='KARTHIK'
update empdtl  set empdesg='SE' WHERE EMPID=3
DELETE FROM empdtl  WHERE EMPID=3
select * from empdtl
SELECT * INTO emp_dtl FROM empdtl  ----to create full table structure along with data 
CREATE TABLE emp_dtl as select * from empdtl  --- In snowflake to create full table structure along with data
select * from emp_dtl
select * into emp_dtl3 from empdtl where 5=6  ----it will copy only structure with out data
CREATE TABLE emp_dtl3 like empdtl  --- In snowflake to copy only structure with out data
select * from emp_dtl3
insert into emp_dtl3 select * from emp_dtl /*inserts data into another table with same structure */
INSERT into empdtl (empid,empname,empsal,empdesg) SELECT empid,empname,empsal,empdesg  FROM emp_dtl	 /*inserts specific columns into another table*/
select * from empdtl 
select * from emp_dtl3 
delete from emp_dtl3  -- delete with out filter condition deletes all the data but there are chances to roll back the data that's deleted
--DELETE: it will have entry in transaction log for each and every record so it will be slow
truncate table empdtl  -- delete's all the data in table we cant roll back once delete 
--TRUNCATE:  permanently delete's all the data and it will faster compared to delte statement as it will put single entry in transaction log

drop table empdtl    -- drops the table along with structure i.e table will no longer be there
select * from emp_dtl
--TCL (Transactional Control language)
-- to rollback a transaction begining a transaction is must
BEGIN TRANSACTION;
update emp_dtl set empdesg='TL' where empid=1;
insert into emp_dtl  select 11,'Ram',70000,'TL','ram@skillscaler.net';
DELETE FROM emp_dtl WHERE EMPID =56;
rollback; --(will not affect if executed immediate to that)

-- COMMIT: used to save transactions that we performed before commit
BEGIN TRANSACTION;
update emp_dtl set empdesg='TL' where empid=1;
insert into emp_dtl  select 8,'Ram',70000,'TL','ram@skillscaler.net';
DELETE FROM emp_dtl WHERE EMPID =56;
COMMIT;

select * from emp_dtl WHERE EMPID =56

insert into emp_dtl (empid, empname,empsal, empdesg)
select 81,'ravi',80000,'TL' union all
select 91,'balu',70000, 'SE' union all
select 97,'hari',50000, 'ASE' union all
select 116,'kish',1000000,'MG'

-- ORDER BY CLAUSE : used to show the data either in ascending order or descending order 
-- Default order is ascending
select * from  emp_dtl order by empsal    -- default sorting order ascending
select * from  emp_dtl order by empsal desc, empname asc
select * from  emp_dtl order by empsal asc

-- GROUP BY: used to group the data based on column specified in group by class
select * from emp_dtl
-- LIST distinct values in a column
select distinct empdesg from emp_dtl
select empdesg,SUM(empsal) as sum from emp_dtl group by empdesg
select empdesg,MAX(empsal) as maxsal from emp_dtl group by empdesg
select empdesg,min(empsal) as minsal from emp_dtl group by empdesg
select empdesg,avg(empsal) as avgsal from emp_dtl group by empdesg
select empdesg,count(*) as avgsal from emp_dtl group by empdesg
select empname, empdesg,MAX(empsal) as sum from emp_dtl group by empdesg -- empname -- will through error
select empname, empdesg,SUM(empsal) as sum from emp_dtl group by empdesg, empname
select empdesg,SUM(empsal) as sum from emp_dtl  group by empdesg, empname, empsal
--Having => Its nothing but where class but it is used only for aggregate functions (after group by to filter=> having)
select empdesg,SUM(empsal) as sum from emp_dtl group by empdesg 
select empdesg,SUM(empsal) as sum from emp_dtl group by empdesg having sum(empsal)>50000 order by empdesg
select empdesg,SUM(empsal) as empsal_sum from emp_dtl where empsal<100000  group by empdesg having sum(empsal)>50000 order by empdesg

--where condition, groupby, having and order by is the sequence of using those statements
select * from emp_dtl
select top 7 * from emp_dtl
select top 5 empid,empname from emp_dtl
--select * from emp_dtl limit 5 -- in oracle

--System defined functions readily available to do operations
select * from emp_dtl
-- Aggregate functions
select  SUM(empsal) from emp_dtl
select empdesg, SUM(empsal) from emp_dtl group by empdesg
select MAX(empsal) as max_sal,min(empsal) min_sal  from emp_dtl
select MIN(empsal) from emp_dtl
select AVG(empsal) from emp_dtl
-- count:  used to find number of rows in a specified table

select count(*) from emp_dtl -- gives count of records in your table
select count(empid) from emp_dtl

insert into emp_dtl (empname) values('Ramesh')

select * from emp_dtl

-- STRING FUNCTIONS
select SUBSTRING('hello world',2,6)
select SUBSTRING(empname,1,5) from emp_dtl
select LEN('micro soft')
select empname,LEN(empname) from emp_dtl
select LTRIM('      microsoft        ')
select RTRIM('        microsoft employee     ')
select LTRIM(RTRIM('      microsoft        '))
select REVERSE('microsoft')
select empname, REVERSE(empname),len(empname) from emp_dtl
select REPLACE('ABCD','AB','PQ')
SELECT emailid, REPLACE(EMAILID,'@','$$') FROM EMP_dtl
SELECT emailid, REPLACE(EMAILID,'@','#') FROM EMP_dtl
SELECT empname,LEFT(EMPNAME,3) FROM EMP_dtl
SELECT empname,RIGHT(EMPNAME,2) FROM EMP_dtl
SELECT EMPNAME, UPPER(EMPNAME) FROM EMP_dtl
SELECT EMPNAME,EMAILID, LOWER(EMAILID) FROM EMP_dtl
select REPLICATE(T,5)
select REPEAT('T',4)  -- REPEAT is equivalient of Replicate in snowflake
select STUFF('ABCDEFGHIJK',2,5,'PQ')
select REPLACE('ABCD','AB','PQ')
SELECT CHARINDEX('D','ABCDEFG')
SELECT emailid,CHARINDEX('@',emailid)  FROM EMP_dtl
SELECT COUNT(*) FROM emp_dtl  --TOTAL NO OF records/rows
SELECT COUNT(EMPNAME) FROM EMP  -- count of rows in that column excluding nulls
SELECT COUNT( DISTINCT EMPNAME) FROM EMP_dtl -- unique value count
select * from emp_dtl order by empname
SELECT EMPNAME, count(*) as cnt FROM EMP_dtl group by empname
SELECT EMPNAME, count(*) as cnt FROM EMP_dtl group by empname having count(*)>1
insert into emp_dtl (empname) values('balu')
select * from EMP_dtl
-- Mathematical functions
SELECT CEIL(94.6) --95  -- in snowflake we have to use CEIL instead of CEILING
SELECT CEILING(94.1) --95  -- it wont work in snowflake we have to CEAL instead
SELECT FLOOR(94.6)  --94
SELECT FLOOR(94.9)
SELECT ROUND(94.5647,1)  --94.6000
SELECT ROUND(94.5647,2)  --94.56
SELECT ROUND(94.5667,2)  --94.57
SELECT ROUND(94.5647,3)  --94.5650
LOG, SIN, COS, TAN,EXP,SQRT

select SQRT(4) 
select SQRT(9) 

-- DATE FUNCTIONS
select GETDATE()
--Snowflake specific Date functions
SELECT DATE_PART(YY, GETDATE())
MM-MONTH
DD- DATE
HH- HOURS
MI- MINUTES
SS- SECONDSS
MS- MILLI SECONDS
SELECT DATE_PART(MM, GETDATE())
SELECT DATE_PART(DD, GETDATE())
SELECT DATE_PART(HH, GETDATE())
SELECT DATE_PART(MM, BIRTHDATE) FROM EMP_DTL

SELECT DATEADD(YY,3,GETDATE()) --ADDS 3YEARS TO CURRENT DATE
SELECT DATEADD(YY,-3,GETDATE()) -- substracts 3 years from current date

SELECT DATEDIFF(MM,'2013-12-26','2017-02-27')   --38

SELECT DATEDIFF(YYYY,'1995-01-01','2022-01-01') 
SELECT DATEDIFF(MM,'1995-01-01','2022-03-01') 
SELECT DATEDIFF(DD,'1995-01-01','2022-03-01') 

--SELECT DATENAME(DW,GETDATE())
SELECT DAYNAME(GETDATE())

SELECT MONTHNAME(GETDATE())

/* SQL Server specific
SELECT * FROM sysobjects WHERE type='u'
SELECT * FROM sys.objects WHERE type='u'
select * from sys.tables */

-- Snowflake specific
select * from snowflake.account_usage.tables;
