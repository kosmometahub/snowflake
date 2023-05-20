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

4. To Get the Script of Created Object
   select get_ddl(<object type>, '<Object Name>' );
   Select get_ddl('schema','public');
   Select get_ddl('schema','sf_schema');
   Select get_ddl('table','emp');
   
5. Create a file Format
 Create or replace file format my_csv_format type=csv skip_header=1;
6. Create Stage 
   Create Stage my_internal_stage; -- with out file format
   Create Stage my_internal_stage file_format=my_csv_format; -- Internal Named stage 
   
   LIST @my_internal_stage;
  --external named stage
  /*create or replace stage my_external_stg
  storage_integration = storage_s3_integration
  url = 's3://skillsrawbucket/DW/';*/

CREATE STAGE my_external_stg URL = 's3://skillscalerraw/' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAQ2KNMXE2TEPA6JVR' AWS_SECRET_KEY = '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');

  
  
  --file_format = my_csv_format;
  
 List @my_external_stg/loan/;
 ALTER STAGE EXTERNAL_STAGE set schema='SF_SCHEMA';
  
 CREATE STAGE "SNOWFLAKE_EXERCISE"."SF_SCHEMA".EXTERNAL_STG CLONE "SNOWFLAKE_EXERCISE"."PUBLIC"."EXTERNAL_STAGE";
8. Query From Stage
    External Named Stage
    --------------------
    LIST @my_external_stg/employee
   
    Select $1 as empid,$3 as job from @my_external_stg/employee/ (file_format => 'my_csv_format') limit 5;
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

    copy into emp from @my_internal_stage/dataload/ file_format = my_csv_format; 
    
    copy into emp from @my_stage/emp.csv.gz;   -- stage
    
    copy into emp from @my_stage/emp.csv.gz;   --table
    /*CST If you have defined fileformat at Table level, Stage level and also in copy command
    =>first priority is for the file format that you defined in copy command, next priority is to file format in stage 
    and last priority is to the file format  that you defined in table level*/
select * from emp;    

Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format;
Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format;  --Copy executed with 0 files processed.
Copy into emp from @my_external_stg/employee/emp.csv file_format = my_csv_format FORCE=TRUE;
   
   Copy from Stage
   ----------------- 
   truncate table emp;
   select * from emp
     copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/emp.csv) 
	 file_format = 'my_csv_format' FORCE=TRUE;	 
-- OTHER COPY OPTIONS
 copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
 FILES = ('emp.csv','emp_load1.csv')  file_format = 'my_csv_format';	 
 
 copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
  pattern='.*emp_load.*[0-9].*.csv' file_format = 'my_csv_format'FORCE=TRUE;
  
  
 copy into emp from (Select $1,$2,$3,$4,$5,$6,$7,$8 from @my_external_stg/employee/) 
  pattern='.*emp_load.*[0-9].*.csv' file_format = 'my_csv_format'FORCE=TRUE;
 
FILE_FORMAT = <file_format_name>

list @my_external_stg pattern='.*emp_load.*[0-9].*.csv'
list @my_external_stg pattern='.*emp.*[0-9].*.csv'