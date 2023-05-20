select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."CUSTOMER" limit 10;

CREATE WAREHOUSE REPORTING_WH WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

ALTER WAREHOUSE "REPORTING_WH" SET  AUTO_SUSPEND = 30;

ALTER WAREHOUSE "REPORTING_WH" RESUME;

/* we have set autosuspend as  1 minute for ETL_WH

At 9AM 1st query is executed and it took 2 minutes to complete i,e at 9.02 AM it completed
2nd query started after that and it took 60 seconds i,e it completed at 9.03 AM
Now if no one runs any other query for 1 more minute i.e till 9.04 AM then it goes ETL_WH goes into suspended state
as ETL_WH was not used to run any queries(i.e IDLE) for 1 minute from 9.03 to 9.04 AM*/
