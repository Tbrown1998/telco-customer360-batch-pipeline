-- Purpose:
--   - Create database & schemas
--   - Configure S3 storage integration
--   - Define file format
--   - Create external stage for ETL ingestion

-- ========== 1. DATABASE & SCHEMA SETUP ==========
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS TELCO_DB;

CREATE SCHEMA IF NOT EXISTS TELCO_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS TELCO_DB.DW;

-- ========== 2. STORAGE INTEGRATION (S3 ↔ Snowflake) ==========
-- This integration allows Snowflake to securely access S3
-- using an IAM role (no static credentials)

CREATE OR REPLACE STORAGE INTEGRATION TELCO_PIPELINE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_id>:role/Telco_pipeline_snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://telco-customer360-pipeline/staging/'
  );

-- (Optional) Inspect integration details
-- DESC INTEGRATION TELCO_PIPELINE_INT;

-- ========== 3. FILE FORMAT DEFINITION ==========
-- Parquet is used as the standard interchange format
-- between AWS Glue and Snowflake

CREATE OR REPLACE FILE FORMAT TELCO_DB.STAGING.PARQUET_FORMAT
  TYPE = PARQUET;

-- ========== 4. EXTERNAL STAGE (S3 LANDING ZONE) ==========
USE SCHEMA TELCO_DB.STAGING;

CREATE OR REPLACE STAGE TELCO_PIPELINE_STAGE
  STORAGE_INTEGRATION = TELCO_PIPELINE_INT
  URL = 's3://telco-customer360-pipeline/staging/'
  FILE_FORMAT = PARQUET_FORMAT;

-- ========== 5. VALIDATION ==========
-- Lists files visible to Snowflake via the integration
LIST @TELCO_PIPELINE_STAGE;
