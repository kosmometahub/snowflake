1. Create a Database - sfdb
create database snowflake_exercise;
2. Create a Schema - sf_schema
create schema sf_schema;

3. Create Table Emp
use snowflake_exercise;
use schema sf_schema;
	create or replace table emp(  
	empno    number,  
	ename    varchar(100),  
	job      varchar(25),  
	mgr      number,  
	hiredate varchar(100),  
	sal      number,  
	comm     number,  
	deptno   number  
	)
   STAGE_FILE_FORMAT = ( FORMAT_NAME = 'my_csv_format');
show tables;

4. To Get the Script of Created Object
   select get_ddl(<object type>, '<Object Name>' );
   Select get_ddl('schema','public');
   Select get_ddl('schema','sf_schema');
   Select get_ddl('table','emp');
   Select get_ddl('database','snowflake_exercise');
   
   
5. Create a file Format
 Create  file format my_csv_format type=csv skip_header=1;
 
 describe file format my_csv_format;
6. Create Stage 
   -external named stage
  /*create or replace stage my_external_stg
  storage_integration = storage_s3_integration
  url = 's3://skillsrawbucket/DW/';*/

CREATE OR REPLACE STAGE my_external_stg URL = 's3://skillscaleraw/DW/' 
CREDENTIALS = (AWS_KEY_ID ='AKIAU54YTHYE3F2MTOXM' AWS_SECRET_KEY = 'F3O3SDWewIK6nUEfoic5bMgX70y/7xmaEos6BtQK');

  
  
  --file_format = my_csv_format;
  
 List @my_external_stg/employee/;
 
 
 CREATE STAGE "SNOWFLAKE_EXERCISE"."SF_SCHEMA".EXTERNAL_STG CLONE "SNOWFLAKE_EXERCISE"."PUBLIC"."EXTERNAL_STAGE";

8. Query From Stage
    External Named Stage
    --------------------
    LIST @my_external_stg/employee
   
     Select $1,$2,$3,$4,$5,$7,$8,$6 from @my_external_stg/employee/emp.csv (file_format => 'my_csv_format') limit 5;
   
    Select $1 as empid,$3 as job from @my_external_stg/employee/  limit 5;
    Select $1,$6,$8 from @my_external_stg/employee/emp.csv (file_format => 'my_csv_format') limit 5;

    Internal Named Stage
	-------------
    Select $1,$2,$3 from @my_internal_stage/dataload/emp.csv limit 5;
	Select $1,$2,$3 from @my_internal_stage/dataload/emp.csv.gz (file_format => 'my_csv_format') limit 5;
	User Stage
	-------------
	Select $1,$2,$3 from @~/emp.csv.gz limit 5;

    Table Stage
	-------------
	Select $1,$2,$3 from @~/emp.csv.gz limit 5;	
	
9. Load File into Table if there are no Errors

    copy into emp from @my_internal_stage/dataload/ t; 
    
    copy into emp from @my_stage/emp.csv.gz;   -- stage
    
    copy into emp from @my_stage/emp.csv.gz;   --table
    /*CST If you have defined fileformat at Table level, Stage level and also in copy command
    =>first priority is for the file format that you defined in copy command, next priority is to file format in stage 
    and last priority is to the file format  that you defined in table level*/
select * from emp order by 1;    

select empno, job, ename from emp;

Copy into emp from @my_external_stg/skillscaleraw/DW/employee/emp.csv file_format = my_csv_format FORCE=TRUE;

Copy into emp from @my_external_stg/employee/ file_format = my_csv_format FORCE=TRUE;

CST

Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format;  --Copy executed with 0 files processed.
Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format FORCE=TRUE;

   Copy into emp from @my_external_stg/employee/emp.csv FORCE=TRUE;  --observe the difference
   Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format FORCE=TRUE;
  
   Copy from Stage
   ----------------- 
   truncate table emp;
   select * from emp
     copy into emp from (Select $1,$3,$2,$4,$5,$6,$7,$8 from @my_external_stg/employee/emp.csv) 
	 file_format = 'my_csv_format' FORCE=TRUE;	 
-- OTHER COPY OPTIONS
 copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
 FILES = ('emp.csv','emp_load1.csv','emp_load2.csv')  file_format = 'my_csv_format';	 
 
 copy into emp from (Select $1,$3,$2,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
  pattern='.*emp_load.*[0-9].*.csv' file_format = 'my_csv_format'FORCE=TRUE;
  
  LIST @my_external_stg/employee/ pattern='.*emp_load.*[^0-9].*.csv'
  
  
 copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
  pattern='.*emp_load.*[0-9].*.csv' file_format = 'my_csv_format'FORCE=TRUE;
 
FILE_FORMAT = <file_format_name>

list @my_external_stg/employee pattern='.*emp_load.*[0-9].*.csv'
list @my_external_stg pattern='.*emp.*[0-9].*.csv'

copy into emp from (Select $1, cast($4 as number),$2,$7,$8 from @my_external_stg/employee/) 
  file_format = 'my_csv_format'FORCE=TRUE;
  
copy into emp from @my_external_stg/employee/
  file_format = 'my_csv_format'FORCE=TRUE;
  
  Select $1, cast($4 as number),$2,$7,$8 from @my_external_stg/employee/emp.csv (file_format = 'my_csv_format');
  Select $1,$6,$8 from @my_external_stg/employee/emp.csv (file_format => 'my_csv_format') limit 5;
/*The COPY command supports:

Column reordering, 
column omission
casts using a SELECT statement.

The ENFORCE_LENGTH | TRUNCATECOLUMNS option, which can truncate text strings that exceed the target column length*/
  
copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/emp) 
  file_format = 'my_csv_format'FORCE=TRUE TRUNCATECOLUMNS = TRUE;
  25 
  
copy into emp from @my_external_stg/employee/
  file_format = 'my_csv_format'FORCE=TRUE ;
  
TRUNCATE TABLE emp

select * from emp
	 
10. Validate Data in the File -- No loading -- No Data loding into the table
  copy command with validation_mode -- Return_n_rows, return_errors, return_all_errors;
select * from emp;
truncate table emp;

LIST @~

/*ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT } --
SKIP_FILE_num (e.g. SKIP_FILE_10)
Skip a file when the number of error rows found in the file is equal to or exceeds the specified number.

SKIP_FILE_num% (e.g. SKIP_FILE_10%)
Skip a file when the percentage of error rows found in the file exceeds the specified percentage.
copy into emp from @EXTERNAL_STG/emp_error.csv file_format = my_csv_format*/
truncate table emp;

select * from emp;
copy into emp from @my_external_stg/employee/emp_error_file.csv file_format = my_csv_format ON_ERROR=CONTINUE;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format ON_ERROR=CONTINUE FORCE=TRUE;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format ON_ERROR=ABORT_STATEMENT;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format ON_ERROR=SKIP_FILE;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format ON_ERROR=SKIP_FILE;  --
copy into emp from @my_external_stg/employee/ file_format = my_csv_format ON_ERROR=CONTINUE;


select current_timestamp();
select *
from table(information_schema.copy_history(table_name=>'emp', start_time=> dateadd(hours, -24, current_timestamp())));

10. Validate Data in the File -- No loading -- No Data loding into the table
  copy command with validation_mode -- Return_n_rows, return_errors, return_all_errors;
select * from emp;
truncate table emp;

LIST @~

    /*ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT } --
SKIP_FILE_num (e.g. SKIP_FILE_10)
Skip a file when the number of error rows found in the file is equal to or exceeds the specified number.

SKIP_FILE_num% (e.g. SKIP_FILE_0.01%)
Skip a file when the percentage of error rows found in the file exceeds the specified percentage.
copy into emp from @EXTERNAL_STG/emp_error.csv file_format = my_csv_format*/

truncate table emp

select * from emp
copy into emp from @EXTERNAL_STG/emp_err.csv file_format = my_csv_format on_error=CONTINUE force=true;


copy into emp from @my_external_stg/employee/emp_error_file5.csv file_format = my_csv_format ON_ERROR ='SKIP_FILE_0.1%' FORCE=TRUE;

copy into emp from @my_external_stg/employee/emp_error_file5.csv file_format = my_csv_format ON_ERROR =SKIP_FILE_2 FORCE=TRUE;


copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_all_errors;

copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_errors;

  copy into emp from @my_external_stg/employee/emp_error_file.csv file_format = my_csv_format validation_mode=return_all_errors;
  copy into emp from @my_external_stg/employee/emp_error_file3.csv file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @my_external_stg/employee/emp_error_file3.csv file_format = my_csv_format validation_mode=return_5_rows;
 
  copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_10_rows; -- only first 
  copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_all_errors;
  copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_all_errors; 
  
  copy into emp from @EXTERNAL_STG/emp.csv file_format = my_csv_format
  truncate table emp;
  select * from emp;   --01a17092-0000-2b7e-0000-00007a0d13b1
  select count(*) from emp; --01a17093-0000-2b7e-0000-00007a0d141d
  
  copy into emp from @~/emp.csv.gz file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @%emp/emp.csv.gz file_format = my_csv_format validation_mode=return_all_error;

11. How do we store the erroneous data into a Table with Validation_mode
    --Using Result Scan Table Function we can log the errors
	-- After Executing the Copy Command get the last Query ID and pass it to Result Scan Function
    --01a1708d-0000-2b7e-0000-00007a0d131d
    
select last_query_id(-4);
    
    --select last_query_id();
    Select * from table(result_scan('01a2a032-0000-2115-0002-be9e0001af0e'));
	--Create a Table out of Errored Rows
	select last_query_id();   --01ac5be8-3200-bcfe-0004-0c3e0003f01a
    select last_query_id(-5);   --01ac5be1-3200-bc85-0004-0c3e00037102
    
      select last_query_id();
    
    set quid=last_query_id(-3);  --01ac5bed-3200-bca7-0004-0c3e00038136
    Select $quid from dual;
	Select $quid;
    Create or replace table emp_err_log as select * from table(result_scan($quid));
    select * from emp_err_log
12. Load Data in to the File even if there are errors in data 
    --  Data loding into the table using on_error = Continue
    --copy into table with on_error=continue. If there are errors data gets loaded partially
	 Copy into emp from @my_stage/emp.csv.gz file_format = my_csv_format on_error=continue;
     
     copy into emp from @my_external_stg/employee/emp_error_file5.csv file_format = my_csv_format on_error=CONTINUE;
     
     select * from table(validate(emp, job_id=>'_last'));
     
     create or replace table emp_err_log as select * from table(validate(emp, job_id=>'_last'));
     
     select * from emp_err_log;
     
     set quid=last_query_id(-3);
     select * from table(validate(emp, job_id=>$quid));
     
     select * from table(validate(emp, job_id=>'01a2a5c5-0000-214f-0002-be9e0001e786'));
     select * from table(validate(emp, job_id=> quid));
     
  
	How do we store the erroneous data into table err_log
    --Using validate Function we can log the errors
     create or replace table emp_err_log as 
     select * from table(validate(<table_name>, job_id=><query_id_of_the_copy_command>));
	 
     create or replace table emp_err_log as 
     select * from table(validate(emp, job_id=>'_last'));  -- _last will provide query id of last statement that got executed in this session
     
     select * from emp_err_log;
     
     
     
--Retrieve details about all loading activity in the last hour:

-- information_schema.copy_history table function stores copy history for 14 days including snowpipe copy commands
select *
from table(information_schema.copy_history(table_name=>'emp', start_time=> dateadd(hours, -24, current_timestamp())));

-- information_schema.load_history view stores copy history for 14 days and view does not return the history of data loaded using Snowpipe
select * from information_schema.load_history order by last_load_time desc
  limit 10;

select * from information_schema.load_history order by last_load_time desc
  limit 10;
  
-- snowflake.account_usage.copy_history will have  -- 365 days
select * from snowflake.account_usage.copy_history
  order by last_load_time desc
  limit 10;
  select * from snowflake.account_usage.QUERY_HISTORY;
 ---- Result scan function
    
    select last_query_id(-2);  -- 2nd most recent query_id
Retrieve all values from your second most recent query in the current session:
select * from table(result_scan(last_query_id(-2)));
Retrieve all values from your first query in the current session:
Select last_query_id(5);     -- get first query of current session
select * from table(result_scan(last_query_id(5))); 
desc user SKILLSCALER;

select "property", "value" from table(result_scan(last_query_id())) 
where "property" = 'DEFAULT_ROLE';

--Extract empty tables that are older than 21 days from show command
show tables;
-- Show the tables that are more than 21 days old and that are empty
-- (i.e. tables that I might have forgotten about).

select "database_name", "schema_name", "name" as "table_name", "rows", "created_on"
    from table(result_scan(last_query_id(-3)))
    where "rows" = 6
    and "created_on" < dateadd(day, -21, current_timestamp())
    order by "created_on";
