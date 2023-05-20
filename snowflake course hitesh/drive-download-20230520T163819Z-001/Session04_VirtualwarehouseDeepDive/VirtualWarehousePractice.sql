select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."CUSTOMER" limit 10;

CREATE WAREHOUSE REPORTING_WH WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

ALTER WAREHOUSE "REPORTING_WH" SET  AUTO_SUSPEND = 30;

ALTER WAREHOUSE "REPORTING_WH" RESUME;

/* we have set autosuspend as  1 minute for ETL_WH

At 9AM 1st query is executed and it took 2 minutes to complete i,e at 9.02 AM it completed
2nd query started after that and it took 60 seconds i,e it completed at 9.03 AM
Now if no one runs any other query for 1 more minute i.e till 9.04 AM then it goes ETL_WH goes into suspended state
as ETL_WH was not used to run any queries(i.e IDLE) for 1 minute from 9.03 to 9.04 AM*/

ALTER WAREHOUSE "MULTICLUSTER_WH" SET MIN_CLUSTER_COUNT = 2 MAX_CLUSTER_COUNT = 5;

-- create a warehouse with default settings
create warehouse default_wh; 

select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CALL_CENTER";

show warehouses;

-- AUTO SUSPEND we give in seconds 600 seconds=> 10 minutes
CREATE WAREHOUSE SingleCluster_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 30 
AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

-- If we dont run any queries using SingleCluster_WH Warehouse for 30 seconds it will go in suspended state
--As soon as you run any query again using the suspended warehouse it resume immediately if we set AUTO RESUME to true
/*If we resize the warehouse, old queries which are already running will still reuse the same warehouse 
for subsequent queries which are in queue will run with changed warehouse size*/

select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."CUSTOMER"; -- XS - L -- in progress 
select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS";   -- L  -- changed warehouse 
select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CALL_CENTER"; --L

alter warehouse DEFAULT_WH SUSPEND;
alter warehouse DEFAULT_WH RESUME;

ALTER WAREHOUSE "REPORTING_WH" SET WAREHOUSE_SIZE = 'SMALL'

-- ALTER A WAREHOUSE

CREATE WAREHOUSE "MULTICLUSTER_WH" WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 10 SCALING_POLICY = 'STANDARD' COMMENT = '';


MULTICLUSTER_WH warehouse will start with one cluster -- unable to serve all the users (couple of queries in queue) 9 AM  -100 -- 500
add in another cluster --2 clusters (still some more user queries are in queue) --200  -2 minutes  --9.07  AM 300 AM
9.10 AM --200 
9.15 AM

spin in one more cluser if still some queries are in queue -- 300 5 miutes
.
.
.
spin in 10 th cluster  --  