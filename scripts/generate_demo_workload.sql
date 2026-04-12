-- Run as ACCOUNTADMIN or a role with MODIFY/USAGE on FINOPS_WH.
-- Purpose: create representative query and warehouse metering history in a
-- fresh Snowflake trial account for live validation screenshots.
--
-- Safe to run repeatedly. ACCOUNT_USAGE lag is expected:
-- - METERING_HISTORY and QUERY_HISTORY: usually visible within 45 minutes
-- - WAREHOUSE_METERING_HISTORY: can take up to 3 hours
-- - STORAGE_USAGE: refreshed daily
--
-- Recommended protocol: run this 2-3 times over several days, wait for the
-- ACCOUNT_USAGE latency windows above, then run dbt build --target dev.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_DEV;
USE SCHEMA ANALYTICS;

ALTER WAREHOUSE FINOPS_WH SET WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE TABLE IF NOT EXISTS FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT (
  batch_id VARCHAR,
  created_at TIMESTAMP_NTZ,
  department VARCHAR,
  amount NUMBER(18,2),
  payload VARCHAR
);

INSERT INTO FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT
SELECT
  UUID_STRING() AS batch_id,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS created_at,
  CASE MOD(SEQ4(), 4)
    WHEN 0 THEN 'Analytics'
    WHEN 1 THEN 'Data Engineering'
    WHEN 2 THEN 'Finance'
    ELSE 'Operations'
  END AS department,
  UNIFORM(10, 2500, RANDOM())::NUMBER(18,2) AS amount,
  SHA2(SEQ4()::VARCHAR, 256) AS payload
FROM TABLE(GENERATOR(ROWCOUNT => 25000));

SELECT department, COUNT(*) AS row_count, SUM(amount) AS total_amount
FROM FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT
GROUP BY department
ORDER BY total_amount DESC;

SELECT DATE_TRUNC('hour', created_at) AS created_hour, COUNT(*) AS row_count
FROM FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT
GROUP BY created_hour
ORDER BY created_hour DESC;

ALTER WAREHOUSE FINOPS_WH SET WAREHOUSE_SIZE = SMALL;

SELECT
  department,
  APPROX_COUNT_DISTINCT(payload) AS approx_payloads,
  AVG(amount) AS avg_amount,
  STDDEV(amount) AS stddev_amount
FROM FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT
GROUP BY department
ORDER BY approx_payloads DESC;

CREATE OR REPLACE TEMP TABLE DEMO_WORKLOAD_ROLLUP AS
SELECT
  department,
  DATE_TRUNC('day', created_at) AS created_date,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount
FROM FINOPS_DEV.ANALYTICS.DEMO_WORKLOAD_FACT
GROUP BY department, DATE_TRUNC('day', created_at);

SELECT * FROM DEMO_WORKLOAD_ROLLUP ORDER BY total_amount DESC;

ALTER WAREHOUSE FINOPS_WH SET WAREHOUSE_SIZE = XSMALL;
ALTER WAREHOUSE FINOPS_WH SUSPEND;
