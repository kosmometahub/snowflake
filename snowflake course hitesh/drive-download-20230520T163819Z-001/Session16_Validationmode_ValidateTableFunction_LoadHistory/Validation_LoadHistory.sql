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

select * from emp

copy into emp from @my_external_stg/employee/emp_error_file5.csv file_format = my_csv_format ON_ERROR ='SKIP_FILE_0.1%' FORCE=TRUE;

copy into emp from @my_external_stg/employee/emp_error_file5.csv file_format = my_csv_format ON_ERROR =SKIP_FILE_2 FORCE=TRUE;


copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_all_errors;

copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_10_rows;

copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_10_rows;

  copy into emp from @my_external_stg/employee/emp_error_file.csv file_format = my_csv_format validation_mode=return_all_errors;
  copy into emp from @my_external_stg/employee/emp_error_file3.csv file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @my_external_stg/employee/emp_error_file3.csv file_format = my_csv_format validation_mode=return_5_rows;
 
  copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format validation_mode=return_10_rows; -- only first error it will return
  copy into emp from @my_external_stg/employee/ file_format = my_csv_format validation_mode=return_errors;
  copy into emp from @my_external_stg/employee/emp_error_file3.csv  file_format = my_csv_format validation_mode=return_all_errors;
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
    
select last_query_id(-4);   --01ac5c14-3200-bc85-0004-0c3e000371ea
    
    --select last_query_id();
    
create or replace table ERROR_LOG as   

INSERT INTO ERROR_LOG
    Select * from table(result_scan('01ac619b-3200-bcfe-0004-0c3e0004706a'));
    
    select * from ERROR_LOG;
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
     insert into emp_error_log
     select error,file,line,category,code,sql_state  from table(validate(emp, job_id=>'_last'));
     
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
from table(information_schema.copy_history(table_name=>'emp', start_time=> dateadd(hours, -48, current_timestamp())));

-- information_schema.load_history view stores copy history for 14 days and view does not return the history of data loaded using Snowpipe
select * from information_schema.load_history order by last_load_time desc
  limit 10;

select * from information_schema.load_history order by last_load_time desc
  limit 10;
  
-- snowflake.account_usage.copy_history will have  -- 365 days
select * from snowflake.account_usage.copy_history
  order by last_load_time desc
  limit 10;
  select * from snowflake.account_usage.QUERY_HISTORY limit 10;
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
show tables ;
-- Show the tables that are more than 21 days old and that are empty
-- (i.e. tables that I might have forgotten about).

select "database_name", "schema_name", "name" as "table_name", "rows", "created_on"
    from table(result_scan(last_query_id(-3)))
    where "rows" = 6
    and "created_on" < dateadd(day, -21, current_timestamp())
    order by "created_on";