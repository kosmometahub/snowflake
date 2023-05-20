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


CREATE SCHEMA LOAN_SCHEMA;

use database hr_dept_db;

use schema LOAN_SCHEMA;

create table loan_dim(loanid int, loanamount int);
create table vechicle_loan(loanid int, loanamount int);

--create table HR_SCHEMA.empdtl (empid int,empname varchar(50),empsal int)

ALTER TABLE empdtl ADD empdesg VARCHAR(50)

select * from hr_schema.empdtl

---DROP TABLE HR_SCHEMA.empdtl;

alter table empdtl  alter column  empname varchar(100)

describe table empdtl;

show tables;

ALTER TABLE empdtl  ADD EMPADDRESS VARCHAR(50)

ALTER TABLE empdtl  DROP COLUMN EMPADDRESS

describe table empdtl
/*Data Manipulation Language(DML)
INSERT
UPDATE
DELETE */

INSERT INTO empdtl  VALUES (1,'Kiran',50000,'SSE'),
(2,'Madhu',50000,'SSE');  

select * from empdtl order by empid limit 3;

select empname,empsal from empdtl;


insert into empdtl (EMPID,EMPNAME,EMPSAL,EMPDESG)
select 8,'krishna',70000,'TL' union all
select 9,'balu',60000, 'SE' union all
select 56,'hari',40000, 'ASE' union all
select 34,'kish',900000,'MG'
 

update empdtl set empdesg='TL', empsal=80000 where empid=1;

delete from empdtl where empid=9;

select * from empdtl