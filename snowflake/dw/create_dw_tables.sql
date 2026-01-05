USE ROLE TELCO_ETL_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE TELCO_DB;
USE SCHEMA DW;

CREATE OR REPLACE TABLE DIM_PLAN (
    plan_sk            NUMBER AUTOINCREMENT,
    plan_id            STRING,
    plan_name          STRING,
    plan_category      STRING,
    monthly_price      NUMBER(10,2),
    data_allowance_gb  INTEGER,
    voice_minutes      INTEGER,
    sms_allowance      INTEGER,
    roaming_enabled    BOOLEAN,
    validity_days      INTEGER,
    is_active           BOOLEAN DEFAULT TRUE,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk            NUMBER AUTOINCREMENT,
    customer_id            STRING,
    first_name             STRING,
    last_name              STRING,
    email                  STRING,
    phone                  STRING,
    country                STRING,
    status                 STRING,
    plan_sk                NUMBER,

    effective_start_date   TIMESTAMP,
    effective_end_date     TIMESTAMP,
    is_current             BOOLEAN,

    ingestion_ts           TIMESTAMP
);

CREATE OR REPLACE TABLE FACT_ACTIVITY (
    event_id           STRING,
    customer_id        STRING,
    customer_sk        NUMBER,
    plan_sk            NUMBER,
    event_type         STRING,
    event_timestamp    TIMESTAMP,
    device_type        STRING,
    network_type       STRING,
    ingestion_ts       TIMESTAMP
);