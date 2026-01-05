USE ROLE TELCO_ETL_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE TELCO_DB;
USE SCHEMA DW;

CREATE OR REPLACE PROCEDURE DW.SP_LOAD_DIM_PLAN()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

TRUNCATE TABLE DW.DIM_PLAN;

    INSERT INTO DW.DIM_PLAN (
        plan_id,
        plan_name,
        plan_category,
        monthly_price,
        data_allowance_gb,
        voice_minutes,
        sms_allowance,
        roaming_enabled,
        validity_days,
        is_active,
        updated_at
    )
    SELECT
        plan_id,
        plan_name,
        plan_category,
        monthly_price,
        data_allowance_gb,
        voice_minutes,
        sms_allowance,
        roaming_enabled,
        validity_days,
        TRUE,
        CURRENT_TIMESTAMP
    FROM STAGING.STG_PLANS;

    RETURN 'DIM_PLAN LOAD SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE DW.SP_LOAD_DIM_CUSTOMER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    MERGE INTO DW.DIM_CUSTOMER tgt
    USING (
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.country,
            c.status,
            p.plan_sk,
            c.ingestion_ts
        FROM STAGING.STG_CUSTOMERS c
        LEFT JOIN DW.DIM_PLAN p
            ON c.plan_id = p.plan_id
    ) src
    ON tgt.customer_id = src.customer_id
    AND tgt.is_current = TRUE

    WHEN MATCHED AND (
           NVL(tgt.email, '') <> NVL(src.email, '')
        OR NVL(tgt.phone, '') <> NVL(src.phone, '')
        OR NVL(tgt.status, '') <> NVL(src.status, '')
        OR NVL(tgt.plan_sk, -1) <> NVL(src.plan_sk, -1)
    )
    THEN UPDATE SET
        tgt.effective_end_date = CURRENT_TIMESTAMP,
        tgt.is_current = FALSE;

    INSERT INTO DW.DIM_CUSTOMER (
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country,
        status,
        plan_sk,
        effective_start_date,
        effective_end_date,
        is_current,
        ingestion_ts
    )
    SELECT
        src.customer_id,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        src.country,
        src.status,
        src.plan_sk,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE,
        src.ingestion_ts
    FROM (
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.country,
            c.status,
            p.plan_sk,
            c.ingestion_ts
        FROM STAGING.STG_CUSTOMERS c
        LEFT JOIN DW.DIM_PLAN p
            ON c.plan_id = p.plan_id
    ) src
    LEFT JOIN DW.DIM_CUSTOMER d
        ON src.customer_id = d.customer_id
        AND d.is_current = TRUE
    WHERE d.customer_id IS NULL;

    RETURN 'DIM_CUSTOMER LOAD SUCCESS';

END;
$$;

CREATE OR REPLACE PROCEDURE DW.SP_LOAD_FACT_ACTIVITY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO DW.FACT_ACTIVITY (
        event_id,
        customer_id,
        customer_sk,
        plan_sk,
        event_type,
        event_timestamp,
        device_type,
        network_type,
        ingestion_ts
    )
    SELECT
        a.event_id,
        a.customer_id,
        d.customer_sk,
        d.plan_sk,
        a.event_type,
        a.event_timestamp,
        a.device_type,
        a.network_type,
        a.ingestion_ts
    FROM STAGING.STG_ACTIVITY a
    LEFT JOIN DW.DIM_CUSTOMER d
        ON a.customer_id = d.customer_id
       AND a.event_timestamp >= d.effective_start_date
       AND a.event_timestamp < COALESCE(d.effective_end_date, '9999-12-31')
    LEFT JOIN DW.FACT_ACTIVITY f
        ON a.event_id = f.event_id
    WHERE f.event_id IS NULL;

    RETURN 'FACT_ACTIVITY LOAD SUCCESS';

END;
$$;