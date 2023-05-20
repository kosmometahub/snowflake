create or replace TABLE EMP_DTL (
	EMPID NUMBER(38,0),
	EMPNAME VARCHAR(100),
	EMPSAL NUMBER(38,0),
	EMPDESG VARCHAR(50),
	EMAILID VARCHAR(50)
);

insert into emp_dtl (empid, empname,empsal, empdesg)
select 81,'ravi',80000,'TL' union all
select 91,'balu',70000, 'SE' union all
select 97,'hari',50000, 'ASE' union all
select 116,'kish',1000000,'MG' union all
select 8,'krishna',70000,'TL' union all
select 9,'Rajesh',60000, 'SE' union all
select 56,'Ramesh',40000, 'ASE' union all
select 34,'Raju',900000,'MG'

insert into emp_dtl (empid, empname,empsal, empdesg)
select 101,'ravindar reddy',80000,'TL' union all
select 101,'Avinash kumar',80000,'TL'






-- ORDER BY CLAUSE : used to show the data either in ascending order or descending order 
-- Default order is ascending
select * from  emp_dtl order by empsal desc
select * from  emp_dtl order by empsal    -- default sorting order ascending
select * from  emp_dtl order by empsal desc, empname desc

select * from  emp_dtl order by empid asc

-- GROUP BY: used to group the data based on column specified in group by class
select * from emp_dtl
delete from emp_dtl where empdesg ='';
-- LIST distinct values in a column

select * from emp_dtl order by empdesg
select distinct empdesg from emp_dtl
select empdesg,SUM(empsal) as sum from emp_dtl group by empdesg
select empdesg,MAX(empsal) as maxsal from emp_dtl group by empdesg
select empdesg,min(empsal) as minsal from emp_dtl group by empdesg
select empdesg,avg(empsal) as avgsal from emp_dtl group by empdesg
select empdesg,count(empid) as desg from emp_dtl group by empdesg
select empname, empdesg,MAX(empsal) as sum from emp_dtl group by empdesg -- empname -- will through error
select empname, empdesg,SUM(empsal) as sum from emp_dtl group by empdesg, empname
select empdesg,SUM(empsal) as sum from emp_dtl  group by empdesg, empname, empsal
--Having => Its nothing but where class but it is used only for aggregate functions (after group by to filter=> having)
select empdesg,SUM(empsal) as sum from emp_dtl  where sum>100000 group by empdesg 
select empdesg,SUM(empsal) as sum from emp_dtl group by empdesg having sum(empsal)>100000 order by empdesg 
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

select * from emp_dtl;
select count(*) from emp_dtl -- gives count of records in your table
select count(empid) from emp_dtl

insert into emp_dtl (empname) values('Ramesh')

select * from emp_dtl

UPDATE emp_dtl  SET EMAILID=EMPNAME||'@skillscaler.net'
describe table emp_dtl
-- STRING FUNCTIONS
select SUBSTRING('hello world',3,5)
select SUBSTRING(empname,1,3) from emp_dtl
select LEN('micro soft')
select empname,LEN(empname) from emp_dtl
select LTRIM('      microsoft        ')
select RTRIM('        microsoft employee     ')
select LTRIM(RTRIM('      microsoft        '))
select REVERSE('microsoft')
select empname, REVERSE(empname),len(empname) from emp_dtl
select REPLACE('ABCD','ABC','P')
SELECT emailid, REPLACE(EMAILID,'@','$') FROM EMP_dtl
SELECT emailid, REPLACE(EMAILID,'@','#') FROM EMP_dtl
SELECT empname,LEFT(EMPNAME,) FROM EMP_dtl
SELECT empname,RIGHT(EMPNAME,2) FROM EMP_dtl
SELECT EMPNAME, UPPER(EMPNAME) FROM EMP_dtl
SELECT EMPNAME,EMAILID, LOWER(EMAILID) as FormatedEmail FROM EMP_dtl
select REPLICATE('T',5)
select REPEAT('T',5)  -- REPEAT is equivalient of Replicate in snowflake
select STUFF('ABCDEFGHIJK',2,5,'PQ')

select CONCAT(SUBSTR('ABCDEFGHIJK',1,(2-1)),'PQ',SUBSTR('ABCDEFGHIJK',(2+5)))
select REPLACE('ABCD','AB','PQ')
SELECT CHARINDEX('D','ABCDEFG')

SELECT CHARINDEX(' ','Ajay Raj')

select SUBSTR('Ajay Raj',1,3)||SUBSTR('Ajay Raj',CHARINDEX(' ','Ajay Raj'),len('Ajay Raj')) from table

select SUBSTR('Ajay Raj',CHARINDEX(' ','Ajay Raj'),len('Ajay Raj'))

select SUBSTR('Ajay Raj',5,8)

select len('Ajay Raj')



select empname, SUBSTR(empname,1,3)||SUBSTR(empname,CHARINDEX(' ',empname),4) from emp_dtl

SELECT emailid,CHARINDEX('@',emailid), LEFT(emailid,CHARINDEX('@',emailid)-1) FROM EMP_dtl
SELECT COUNT(*) FROM emp_dtl  --TOTAL NO OF records/rows
SELECT COUNT(emailid) FROM emp_dtl  -- count of rows in that column excluding nulls
SELECT COUNT(empname) FROM emp_dtl 
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
SELECT ROUND(94.5647,1)  --94.6
SELECT ROUND(94.5647,2)  --94.56
SELECT ROUND(94.5667,2)  --94.57
SELECT ROUND(94.5647,3)  --94.5650
0.001 * 1000000 
LOG, SIN, COS, TAN,EXP,SQRT

select SQRT(4) 
select SQRT(9) 

-- DATE FUNCTIONS
select GETDATE()
--Snowflake specific Date functions
SELECT DATE_PART(YY, GETDATE())
SELECT DATE_PART(MM, GETDATE())
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
SELECT DATEADD(YY,-5,GETDATE()) -- substracts 3 years from current date

SELECT DATEDIFF(MM,'2013-12-26','2017-02-27')   --38

SELECT DATEDIFF(YYYY,'2020-01-01','2022-01-01') 
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


-- Data type conversions
--The CAST() function converts a value (of any type) into a specified datatype.
select * FROM emp_dtl
describe table emp_dtl


SELECT CAST(empid AS VARCHAR) as dc_empid FROM emp_dtl   --INT,FLOAT

SELECT CAST(empid AS DEC(10,2)) as dc_empid, empdesg,SUBSTRING(empname,1,5) sbstr_empname  FROM emp_dtl 
--Using the CAST() function to convert a string to a datetime value example
SELECT CAST('2019-03-14' AS DATETIME) result;
-- Using the CAST() function to convert a decimal to an integer 
SELECT CAST(5.95 AS INT) result;
SELECT CAST(999 AS DEC(3,0)) as derv_decimal
SELECT CAST(1000 AS DEC(3,0)) as derv_decimal -- Arithmetic overflow error converting int to data type numeric.
SELECT CAST(1000 AS DEC(4,0)) as derv_decimal 
SELECT CAST(5.95 AS DEC(3,0)) as derv_decimal

SELECT CAST(5.95 AS DEC(3,2)) as derv_decimal
SELECT CAST(5.95 AS DEC(3,3)) as derv_decimal  --Arithmetic overflow error converting numeric to data type numeric.
SELECT CAST(5.95 AS DEC(4,3)) result;
SELECT CAST(5.95 AS DEC(3,5)) result;
SELECT CAST('2019-03-14' AS DATETIME) result;
select CAST(BIRTHDATE AS DATE) FROM emp_dtl;     --NOT PHYSICALLY 


-- In Snowflake we can create constraints but only NOT NULL constraint will work. other constraints wont work
--CONSTRAINTS: Conditions/restrictions 
--SQL constraints are used to specify rules for data in a table.
1)NOT NULL -- Ensures that a column cannot have a NULL value

CREATE TABLE DIMCUST(CUSTID INT,CUSTNAME VARCHAR(50),CUSTBAL INT)
INSERT INTO DIMCUST(CUSTNAME,CUSTBAL) VALUES('RAJESH', 50000)

select * from DIMCUST
DROP TABLE DIMCUST

CREATE OR REPLACE TABLE DIMCUST(CUSTID INT NOT NULL,CUSTNAME VARCHAR(50),CUSTBAL INT)
INSERT INTO DIMCUST(CUSTNAME,CUSTBAL) VALUES('RAJESH', 50000)
INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL) VALUES(1,'RAJESH', 50000)
INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL) VALUES(null,'Hari', 50000)

/*error
Msg 515, Level 16, State 2, Line 1
Cannot insert the value NULL into column 'CUSTID', table 'hitesh.dbo.DIMCUST'; column does not allow nulls. INSERT fails.
The statement has been terminated.*/
alter TABLE DIMCUST ALTER COLUMN CUSTBAL INT NOT NULL

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL) VALUES(3,'Kiran', null)

SELECT * FROM CUSTdtl

2) UNIQUE -- Ensures that all values in a column are different
ALTER TABLE DIMCUST ADD CUSTNO INT UNIQUE 
-- add unique constraint to existing column
ALTER TABLE DIMCUST ADD CONSTRAINT UX_1 UNIQUE(CUSTID)

TRUNCATE TABLE DIMCUST

--DESCRIBE TABLE DIMCUST

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL,CUSTNO) VALUES(1,'RAJESH', 50000,978)

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL,CUSTNO) VALUES(2,'Raj', 7000,978) --Violation of UNIQUE KEY constraint 'UQ__DIMCUST__3D9DE38B99FEEC85'. Cannot insert duplicate key in object 'dbo.DIMCUST'. The duplicate key value is (978).

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL,CUSTNO) VALUES(2,'Raj', 7000,979)

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL,CUSTNO) VALUES(3,'jazir', 8000,null)

INSERT INTO DIMCUST(CUSTID,CUSTNAME,CUSTBAL,CUSTNO) VALUES(4,'Ravi', 9000,null)
select * from DIMCUST


3) Default -- Sets a default value for a column if no value is specified
create table custdtl(custid int unique, custname varchar(50),
custbranch varchar(50) default 'bangalore')


alter table custdtl add constraint df_123 default 'dummy' for custname

/*alter table t1 alter column c1 drop not null;

alter table t1 modify c2 drop default, c3 set default 'text' ;

alter table t1 alter c4 set data type varchar(50), column c4 drop default;

alter table t1 alter c5 comment '50 character column'; */

insert into custdtl(custid) values (667)
insert into custdtl (custid,custname) values (56,'hari')
select * from custdtl
insert into custdtl (custid,custname) values (100,'kumar')
insert into custdtl values(1,'seenu', 'hyd')
select * from custdtl
insert into custdtl
select 7, 'venki','chennai' union all
select 5, 'raji','mum' union all
select 22, 'muni','pune'
select * from emp_dtl
4) check constraint: Ensures that the values in a column satisfies a specific condition
alter table custdtl add custbal int
alter table custdtl add constraint ck_1 check(custbal>5000)

select * from custdtl

insert into custdtl values(3,'ram', 'hyd', 3000)  -- throws error since we are are entering lesser value for custbal
insert into custdtl values(3,'ram', 'hyd', 5001)
create table custdtl2(custid int, custname varchar(50), custbal int check(custbal>10000))

5) Primary Key: combination of unique & not null
create table pk_test(custid int primary key, custname varchar(50), custbal int)
insert into pk_test(custname,custbal) values('hitesh',565876) --null

select * from pk_test where custid=1 

insert into pk_test(custid,custname,custbal) values(1,'hitesh',565876)
insert into pk_test(custid,custname,custbal) values(2,'kiran',7000)
select * from pk_test

/*error-
Msg 515, Level 16, State 2, Line 1
Cannot insert the value NULL into column 'custid', table 'hitesh.dbo.pk_test'; column does not allow nulls. INSERT fails.
The statement has been terminated.*/
insert into pk_test values(1,'hitesh',565876)
insert into pk_test values(1,'kartik',68778)---duplicate
alter table pk_test alter column custbal int not null

alter table pk_test add constraint pk_56 primary key(custbal) ----only one primary key per table
alter table custdtl alter column custbal int not null   
----can add primary key only if it is nullable column n doesnot contain any dupi=licate
alter table custdtl add constraint pk_56 primary key(custbal)


create table parent (id int primary key, name varchar(50), empsal int)
--Foreign Key : Prevents actions that would destroy links between tables
-- Primary and Foreign references can be created in snowflake but cant be enforced

create table child (id int foreign key references parent(id), branch varchar(50))
insert into child values(1,'hyd') 
----if a value trying to insert into foreign key column in child/reference that value should be in parent/base table
insert into parent values(1,'ravi', 26878) -- while inserting data you will have insert into parent first and then insert into child
insert into child values(1,'hyd')

insert into child values(2,'blr')


select * from parent
delete from parent where id=1
delete from child where id=1  -- while deleting first delete that refernce record/row from child then you can delete from parenr
delete from parent where id=1

select * from child

create table customer(id int, name varchar(50),custbal int);
insert into customer
select 1,'hari',50000 union all
select 2,'krishna',150000 union all
select 4,'ravi',90000 union all
select 5,'seema', 200000 union all
select 6,'Rajesh', 170000 union all
select 7,'Madhu', 70000

create table branch(id int, branch varchar(50),empdesg varchar(50))
select * from customer
select * from branch

insert into branch
select 1,'Bangalore','SE' union all
select 2,'Hyderabad','MG' union all
select 4,'Chennai','TL' union all
select 5,'Kolkatta',' SM' union all
select 8,'Mumbai','AM' union all
select 9,'Mumbai','SE'


/*INNER JOIN: Returns records that have matching values in both tables
LEFT OUTER JOIN/ LEFT JOIN: Returns all records from the left table, and the matched records from the right table
RIGHT OUTER JOIN/RIGHT: Returns all records from the right table, and the matched records from the left table
FULL OUTER JOIN: Returns all records when there is a match in either left or right table 
CROSS JOIN:  cartesian product left table has got m records and right table has got n records it will generate mxn as output
*/

select * from customer
select * from branch
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a inner join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a left join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id as right_id  from customer a right join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id as right_id from customer a full outer join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a cross join branch b

-- if there are duplicates for key fields that were used in joins
insert into customer
select 1,'Kiran',50000 union all
select 1,'Ramesh',50000

insert into branch
select 1,'Hyderabad','SSE'

select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a inner join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a left join branch b on a.id=b.id
select b.id,a.name,a.custbal, b.branch, b.empdesg from customer a right join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id as right_id from customer a full outer join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a cross join branch b

-- insert null values into key fields
insert into customer
select null,'Anil',50000

insert into branch
select null,'Pune','SSE'

select * from customer where id is null
select * from branch
select * from customer where id is null

select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a inner join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a left join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id as right_id  from customer a right join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id as right_id from customer a full outer join branch b on a.id=b.id
select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a cross join branch b

--Non matching records from left table
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id from customer a left join branch b on a.id=b.id where b.id is null
--Non matching records from right table
select a.id,a.name,a.custbal, b.branch, b.empdesg,b.id from customer a right join branch b on a.id=b.id where a.id is null

create table customer_detail (id int, custaddress varchar(500), customermailid varchar(50))

insert into customer_detail
select 1,'PlotN0-20','hari@skillscaler.net' union all
select 2,'PlotN0-30','krishna@skillscaler.net' union all
select 4,'PlotN0-255','ravi@skillscaler.net' union all
select 5,'PlotN0-710','seema@skillscaler.net'



select * from customer_detail

select a.id,a.name,a.custbal, b.branch, b.empdesg from customer a inner join branch b on a.id=b.id

select a.id,a.name,a.custbal, b.branch, b.empdesg,cust_dtl.customermailid,cust_dtl.custaddress from customer a inner join branch b on a.id=b.id
left join customer_detail cust_dtl on a.id=cust_dtl.id

delete from customer_detail where id=5


select * from emp_dtl
union all
select * from emp_dtl2

insert into emp_dtl2 (empid,empname,empsal) values (1,'Kiran',50000),(121,'Ram',5000),(171,'Kiran', 21000)

select empid,empname,empsal from emp_dtl 
union all
select empid,empname,empsal from emp_dtl2

select empid,empname,empsal from emp_dtl
union
select empid,empname,empsal from emp_dtl2 /* will remove duplicates if entire row is repeating*/

select empid,empname from emp_dtl 
union all
select empid,empname from emp_dtl2

select empid,empname from emp_dtl 
union
select empid,empname from emp_dtl2

insert into emp_dtl2 (empid,empname,empsal) values (9,'balu',60000),(8,'Ram',70000)

select empid,empname,empsal from emp_dtl
except
select empid,empname,empsal from emp_dtl2 ----will give you records from a, which r not there in b

select empid,empname,empsal from emp_dtl
intersect
select empid,empname,empsal from emp_dtl2 ----will give you excat matching records between 2 tables

create table dimcustomer(customerkey int, title varchar(50), firstname varchar(50),middlename varchar(50), lastname varchar(50), Gender varchar(50), yearly_income int)
insert into dimcustomer values(1,null,'Rajesh',null,'Kumar','Female',200000),
(2,'Mr',null,null,'Jain','Female',300000),
(3,null,'Lakshmi',null,null,'Male',700000),
(4,null,'Ravi','Kiran',null,'Female',800000),
(5,null,'Jessie',null,null,'Male',800000)

insert into dimcustomer values(1,null,null,'Kiran','Kumar','Female',200000)


select * from dimcustomer
select customerkey, title,isnull(title,'') title_derv, coalesce(firstname,middlename, lastname) as name,gender,yearly_income from dimcustomer
--isnull is not there in snowflake, we can acheive that with coalesce(title,'')
select customerkey,firstname,middlename,lastname, coalesce(firstname,middlename, lastname) as fullname,gender,yearly_income from dimcustomer
select customerkey, coalesce(firstname,middlename, lastname) as name,
case when gender='Male' then 'Female' when gender='Female' then 'Male' end as gender_updated from dimcustomer
--update dimcustomer set gender='Male' where gender='Female'
select customerkey, gender,coalesce(firstname,middlename, lastname) as name,
case when gender='Male' then 'Female' when gender='Female' then 'Male' end as gender_derived from dimcustomer

update dimcustomer set gender=case when gender='Male' then 'Female' when gender='Female' then 'Male' end

select * from dimcustomer

select * from emp_dtl where empid not in (select empid from emp_dtl2) ----same as except but will not allow null values
select * from emp_dtl where empid in (select empid from emp_dtl2)
select * from emp_dtl where empid =1
select * from emp_dtl where empid  in (1,2,3,4)
select * from emp_dtl where empid  not in (1,2,3,4)

select ID,empdesg,
case when empdesg='se' then 'senior engineer'
when empdesg='ase' then 'assistant senior engineer'
when empdesg='tl' then 'team leader'
end as status from branch

select * from branch

create table employee_details(employee_id int,fullname varchar(50),department varchar(100), salary decimal(10,2))

insert into employee_details values(100,'Rajesh Kumar','SALES',70000.00),
(101,'Mahesh Jain','IT',80000.00),
(102,'Kiran Kumar','SALES',100000.00),
(103,'Punith krishna','SALES',110000.00),
(104,'Ramya krishna','IT',60000.00),
(105,'Ravi Kiran','ACCOUNTS',50000.00),
(106,'Sandhya Rani','ACCOUNTS',80000.00),
(107,'Radha krishna','SALES',120000.00)

insert into employee_details values(108,'Ramesh Kumar','SALES',80000.00),(109,'Ravi Kumar','ACCOUNTS',60000.00)

select * from employee_details

select employee_id,fullname,salary,department, RANK() over(order by salary desc) as emp_rank from employee_details
select employee_id,salary,department, DENSE_RANK() over(order by salary desc) as denserank from employee_details
select employee_id,salary, ROW_NUMBER() over(order by salary desc) as RN from employee_details
select employee_id,salary, ROW_NUMBER() over(order by employee_id desc) as RN from employee_details

select * from employee_details order by employee_id
select employee_id,fullname,salary,department, ROW_NUMBER() over(partition by department order by salary desc) as RN from employee_details
select * from (
select employee_id,fullname,salary,department, ROW_NUMBER() over(partition by department order by salary asc) as RN from employee_details)
temp_res where rn=1

-- insert duplicate records
insert into employee_details values
(105,'Ravi Kiran','ACCOUNTS',50000.00),(105,'Ravi Kiran','ACCOUNTS',60000.00),(105,'Ravi Kiran','ACCOUNTS',70000.00)

insert into employee_details values
(105,'Ravi Kiran','ACCOUNTS',50000.00),(102,'Kiran Kumar','SALES',100000.00), (102,'Kiran Kumar','SALES',100000.00)

select employee_id,fullname,salary,department, ROW_NUMBER() over(partition by department,employee_id order by salary desc) as RN from employee_details
select employee_id,salary,department, ROW_NUMBER() over(partition by employee_id order by salary) as Rownumber from employee_details


---- RANK WILL SKIP
-- DENSE Rank will continously provide rank
-- CTE is coman Table expresion
WITH CTE
AS
(select employee_id,salary,department, ROW_NUMBER() over(partition by employee_id order by salary desc) as Rownumber from employee_details)
DELETE FROM CTE WHERE Rownumber>1 --SHOULD EXECUTE ALONG WITH THIS LINE, SEPERATELY CAN'T BE EXECUTED
-- DML operations are not supported after CTE in sniowflake

-- Delete duplicates with CTE+ROW_NUMBER+ INSERT OVERWRITE in snowflake 
insert overwrite into employee_details
WITH CTE
AS
(select employee_id,salary,department, ROW_NUMBER() over(partition by employee_id,salary order by salary desc) as Rownumber from employee_details)
select * from employee_details

/*SYNTAX 
WITH CTE AS(
   SELECT [col1], [col2], [col3], [col4], [col5], [col6], [col7],
       RN = ROW_NUMBER()OVER(PARTITION BY col1 ORDER BY col1)
   FROM dbo.Table1
)
DELETE FROM CTE WHERE RN > 1  */
create view v_test  ---simple view (fetching data from only one table)
as
select * from emp_dtl
select * from v_test ---view will not store data physically,will fetch the data from base table
create view test    ----complex view(fetching data from more than one table)
as select a.empid,a.empname,b.empempsal,b.empdesg from emp_dtl_dtl a full outer join emp_dtl b on a.empid=b.empid
select * from test
