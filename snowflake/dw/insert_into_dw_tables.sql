-- ========== DIM_PLAN LOAD ==========
-- Reference dimension: rebuilt on each run
-- ==========

-- Optional truncate if full refresh is required
-- TRUNCATE TABLE DW.DIM_PLAN;

-- Load latest plan definitions from staging
INSERT INTO DW.DIM_PLAN (
    plan_id,
    plan_name,
    plan_category,
    monthly_price,
    data_allowance_gb,
    voice_minutes,
    sms_allowance,
    roaming_enabled,
    validity_days
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
    validity_days
FROM STAGING.STG_PLANS;

-- ========== DIM_CUSTOMER LOAD (SCD TYPE 2) ==========
-- Tracks historical changes to customer attributes
-- ==========

MERGE INTO DW.DIM_CUSTOMER tgt
USING (
    -- Enrich customers with current plan surrogate key
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

-- CASE: Existing customer with attribute change
-- Close out current record
WHEN MATCHED AND (
       NVL(tgt.email, '')   <> NVL(src.email, '')
    OR NVL(tgt.phone, '')   <> NVL(src.phone, '')
    OR NVL(tgt.status, '')  <> NVL(src.status, '')
    OR NVL(tgt.plan_sk, -1) <> NVL(src.plan_sk, -1)
)
THEN UPDATE SET
    tgt.effective_end_date = CURRENT_TIMESTAMP,
    tgt.is_current = FALSE

-- CASE: New customer or new version after change
WHEN NOT MATCHED THEN
INSERT (
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
VALUES (
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
);

-- ========== DATA QUALITY CHECK ==========
-- Validate active customer records
-- ==========

SELECT COUNT(*)
FROM DW.DIM_CUSTOMER
WHERE is_current = TRUE;

-- ========== FACT_ACTIVITY LOAD ==========
-- Idempotent insert (prevents duplicate events)
-- ==========

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

-- Resolve correct customer version at event time
LEFT JOIN DW.DIM_CUSTOMER d
    ON a.customer_id = d.customer_id
   AND a.event_timestamp >= d.effective_start_date
   AND a.event_timestamp < COALESCE(d.effective_end_date, '9999-12-31')

-- Prevent duplicate fact inserts
LEFT JOIN DW.FACT_ACTIVITY f
    ON a.event_id = f.event_id
WHERE f.event_id IS NULL;
