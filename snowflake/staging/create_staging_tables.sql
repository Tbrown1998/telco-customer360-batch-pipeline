USE ROLE TELCO_ETL_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE TELCO_DB;
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE STG_PLANS (
    plan_id            STRING,
    plan_name          STRING,
    plan_category      STRING,
    monthly_price      NUMBER(10,2),
    data_allowance_gb  INTEGER,
    voice_minutes      INTEGER,
    sms_allowance      INTEGER,
    roaming_enabled    BOOLEAN,
    validity_days      INTEGER,
    ingestion_ts       TIMESTAMP
);


CREATE OR REPLACE TABLE STG_CUSTOMERS (
    customer_id          STRING,
    first_name           STRING,
    last_name            STRING,
    email                STRING,
    phone                STRING,
    country              STRING,
    status               STRING,
    plan_id              STRING,
    ingestion_ts         TIMESTAMP
);

CREATE OR REPLACE TABLE STG_ACTIVITY (
    event_id             STRING,
    customer_id          STRING,
    event_type           STRING,
    event_timestamp      TIMESTAMP,
    device_type          STRING,
    network_type         STRING,
    ingestion_ts         TIMESTAMP
);
