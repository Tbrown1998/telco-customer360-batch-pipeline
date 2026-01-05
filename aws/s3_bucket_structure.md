# S3 Bucket Structure & Data Lifecycle  
## Telco Customer 360 Pipeline

---

## 1. Purpose of This Document

This document explains **how Amazon S3 is used as the data lake backbone** for the Telco Customer 360 pipeline.

When designing the bucket structure, the goals were to:
- clearly represent the **data lifecycle**
- enforce **data quality boundaries**
- enable **safe reprocessing and backfills**
- make ownership and responsibility obvious at each stage

Every folder in the bucket has a **specific purpose** — nothing exists by accident.

---

## 2. S3 Bucket Overview

### Bucket Name

```
s3://telco-customer360-pipeline/
```

This bucket acts as the **system of record** for all data before it enters the warehouse.

---

## 3. High-Level Folder Structure

```
s3://telco-customer360-pipeline/
│
├── raw/
├── archive/
├── rejected/
├── clean/
└── staging/
```

Each top-level folder represents a **distinct data state**.

---

## 4. Visual Data Zone Flow

```mermaid
flowchart LR
    RAW[raw
(Untrusted)]
    ARCH[archive
(Validated)]
    REJ[rejected
(Invalid)]
    CLEAN[clean
(Standardized)]
    STAGE[staging
(Analytics-ready)]

    RAW -->|validate| ARCH
    RAW -->|fail| REJ
    ARCH --> CLEAN
    CLEAN --> STAGE
```

This flow enforces progressive trust as data moves downstream.

---

## 5. Dataset Namespacing

Each dataset is isolated into its own namespace to prevent cross-contamination:

```
customers/
plans/
activity_logs/
```

This structure is repeated consistently across all layers.

---

## 6. RAW Zone — Landing Area

### Path
```
raw/
```

### Purpose
The raw zone is the initial landing area for all incoming files.

This zone is treated as:
- untrusted
- temporary
- write-only

### Characteristics
- Files arrive from upstream systems
- No transformations occur here
- Data should never be read directly by ETL jobs

### Example
```
raw/customers/
raw/plans/
raw/activity_logs/
```

---

## 7. ARCHIVE Zone — Validated Source of Truth

### Path
```
archive/
```

### Purpose
The archive zone holds validated, immutable source data.

Once a file reaches archive:
- it has passed format validation
- it is safe to reprocess
- it becomes part of the audit trail

### Key Rule
Files in archive are **never modified or deleted**.

### Example
```
archive/customers/
archive/plans/
archive/activity_logs/
```

This zone represents the trusted input to ETL.

---

## 8. REJECTED Zone — Failed Ingestion

### Path
```
rejected/
```

### Purpose
The rejected zone captures files that:
- fail format validation
- are malformed
- do not match expected dataset types

### Operational Use
- Alerts are triggered when files land here
- Files can be reviewed manually
- Upstream systems can resend corrected data

### Example
```
rejected/customers/
rejected/plans/
rejected/activity_logs/
```

Rejected data never enters ETL.

---

## 9. CLEAN Zone — Standardized Data

### Path
```
clean/
```

### Purpose
The clean zone contains schema-enforced, standardized Parquet data.

This is the output of **Glue Job A**.

### Characteristics
- Consistent schemas
- Standard data types
- Partitioned for performance
- Still granular (not analytics-modeled)

### Example
```
clean/customers/
clean/plans/
clean/activity_logs/
```

This layer is suitable for reprocessing but not direct analytics.

---

## 10. STAGING Zone — Warehouse Contract

### Path
```
staging/
```

### Purpose
The staging zone represents the formal contract with Snowflake.

Only this layer is exposed to the warehouse.

### Characteristics
- Deduplicated
- Joined where necessary
- Snapshot-style data
- Optimized for COPY operations

### Example
```
staging/customers/
staging/plans/
staging/activity/
```

Snowflake never reads from raw, archive, or clean.

---

## 11. Why This Structure Works

This design provides:
- Clear ownership at every stage
- Protection against bad data
- Deterministic reprocessing
- Easy backfills
- Strong auditability

Each layer answers a simple question:

| Layer | Question |
|-----|---------|
| Raw | What arrived? |
| Archive | What was accepted? |
| Rejected | What failed? |
| Clean | What is structurally correct? |
| Staging | What is analytics-ready? |

---

## 12. Operational Scenarios Supported

This structure supports:
- partial file reprocessing
- schema evolution
- replaying historical days
- isolating ingestion failures
- downstream independence from ingestion logic
