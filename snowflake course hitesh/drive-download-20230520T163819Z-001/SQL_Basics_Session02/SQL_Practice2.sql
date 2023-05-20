--Data Definition Language (DDL): changes the structure of the table like creating a table, drop a table, altering a table
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

DESCRIBE TABLE empdtl

--create table HR_SCHEMA.empdtl (empid int,empname varchar(50),empsal int)

ALTER TABLE empdtl ADD empdesg VARCHAR(50)

select * from empdtl

---DROP TABLE HR_SCHEMA.empdtl;

alter table empdtl  alter column  empname varchar(100)

ALTER TABLE empdtl  ADD EMPADDRESS VARCHAR(50)

ALTER TABLE empdtl  DROP COLUMN EMPADDRESS

describe table empdtl
select * from empdtl;
/*Data Manipulation Language(DML)
INSERT
UPDATE
DELETE */

INSERT INTO empdtl  VALUES (1,'Kiran',50000,'SSE'),
(2,'Madhu',50000,'SSE');  

select * from empdtl;

CREATE TABLE empdtl

insert into empdtl select EMPID,EMPNAME,EMPSAL,EMPDESG from table2

insert into empdtl (EMPID,EMPNAME,EMPSAL,EMPDESG) 
select 8,'krishna',70000,'TL' union all
select 9,'balu',60000, 'SE' union all
select 56,'hari',40000, 'ASE' union all
select 34,'kish',900000,'MG'
 
--INSERT INTO empdtl  VALUES (78)  -- throws error insert list is not specified and not inserting data for all columns


select * from empdtl
INSERT INTO empdtl VALUES (45,'KARTHIK',56577) -- throws error insert list is not specified and inserting data for all columns
INSERT INTO empdtl  (EMPID,EMPNAME,EMPSAL) VALUES (45,'KARTHIK',56577) 

INSERT INTO empdtl  (EMPNAME,EMPSAL,EMPID) VALUES ('Ravi',56577,51,'TL') 

INSERT INTO empdtl (EMPNAME,EMPSAL,EMPID,EMPDESG) VALUES ('Ravi',56577,51,'TL') 

INSERT INTO empdtl VALUES (45,'KARTHIK',56577,'SE')

select EMPNAME,EMPSAL from empdtl 
select * from empdtl where empid=1
select * from empdtl where empid!=1
select * from empdtl where empid<>1
select * from empdtl where empsal>50000 and empdesg='TL'
select * from empdtl where empdesg=null -- null cant be filtered with =
select * from empdtl where empdesg is null

select * from empdtl where empdesg is not null

INSERT INTO empdtl  (EMPID,EMPNAME,EMPSAL,EMPDESG) VALUES (47,'KARTHIK',56577,'')

select * from empdtl where empdesg='SE'

select * from empdtl where empdesg in ('SE','SSE','TL')

select * from empdtl where empdesg not in ('SE','SSE','TL')


ALTER TABLE empdtl  ADD EMAILID VARCHAR(50)
UPDATE empdtl  SET EMAILID=EMPNAME+'@skillscaler.net' -- in SQL server
UPDATE empdtl  SET EMAILID=EMPNAME||'@skillscaler.net' --In sanowflake we will use "||" to concatinate instead of +
update empdtl  set empid=3 where empname='KARTHIK'
update empdtl  set empdesg='SE', empsal = WHERE EMPID in()



DELETE FROM empdtl  WHERE EMPID=3
select * from empdtl
--SELECT * INTO emp_dtl FROM empdtl  ----to create full table structure along with data 

--CREATE TABLE empdtl_new as select * from empdtl where 1=2
select * from empdtl_new;

CREATE TABLE empdtl5 as select * from empdtl;  --- In snowflake to create full table structure along with data


--select * into emp_dtl3 from empdtl where 5=6  ----it will copy only structure with out data

CREATE TABLE emp_dtl3 like empdtl  --- In snowflake to copy only structure with out data

select * from emp_dtl

select count(*) from empdtl

delete from emp_dtl where empid in (56, 34)

delete from emp_dtl;

insert into emp_dtl3 select * from emp_dtl /*inserts data into another table with same structure */
INSERT into empdtl (empid,empname,empsal,empdesg) SELECT empid,empname,empsal,empdesg  FROM emp_dtl	 /*inserts specific columns into another table*/
select * from empdtl 
select * from emp_dtl3 

delete from emp_dtl3  -- delete with out filter condition deletes all the data but there are chances to roll back the data that's deleted
--DELETE: it will have entry in transaction log for each and every record so it will be slow
truncate table empdtl  -- delete's all the data in table we cant roll back once delete 
--TRUNCATE:  permanently delete's all the data and it will faster compared to delte statement as it will put single entry in transaction log

select * from empdtl

drop table empdtl    -- drops the table along with structure i.e table will no longer be there