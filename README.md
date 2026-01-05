# Telco Customer - Orchestrated Data Pipeline

## Enterprise-Grade Batch Data Engineering Project (AWS + Snowflake)

---

## Overview

**Telco Customer 360** is a production, end-to-end **batch data pipeline** designed to consolidate customer, subscription, and activity data into an analytics-ready **Customer 360 warehouse**.

The project simulates **Data Engineering** concept of building and operating data pipelines in a medium-to-large telecom organization covering ingestion, validation, orchestration, transformation, historical tracking (SCD Type 2), and warehouse automation.

The final output supports **business analysts and BI tools** with clean, reliable, and historically accurate data.

- **⚙️ Domain:** Data Engineering | Analytics Engineering
- **🧠 Stack:** AWS • Glue • Step Functions • Snowflake • S3

---

## Business Problem

Telecom companies typically store customer information across multiple systems:

- CRM systems (customer demographics)
- Billing systems (subscription plans)
- Network systems (usage and activity logs)

These systems:
- Use **different data formats**
- Update at **different frequencies**
- Do **not preserve history**

This makes it difficult to answer questions like:
- What plan was a customer on when an activity occurred?
- How has customer behavior changed over time?
- Which plans drive the most usage or churn?

This project solves that by building a **Customer 360 warehouse** with full historical tracking.

---

## High-Level Architecture

```mermaid
flowchart LR
    A[External Source Systems] --> B[S3 Raw]
    B --> C[Lambda: File Validation & Routing]
    C -->|Valid| D[S3 Archive]
    C -->|Invalid| E[S3 Rejected]

    D --> F[EventBridge Schedule]
    F --> G[Step Functions]

    G --> H[Glue Job A: Clean & Standardize]
    H --> I[S3 Clean]

    I --> J[Glue Job B: Build Staging]
    J --> K[S3 Staging]

    K --> L[Snowflake External Stage]
    L --> M[Snowflake Stored Procedures]
    M --> N[Dim & Fact Tables]

    N --> O[BI / Analytics Users]
```

---

## 📂 Data Sources

| Dataset | Description | Format |
|------|------------|--------|
| Customers | Demographics & subscription mapping | NDJSON |
| Plans | Subscription plan definitions | CSV |
| Activity Logs | Network usage events | NDJSON |

**Why multiple formats?**  
Real-world pipelines must ingest heterogeneous data from different systems.

---

## 🪣 S3 Data Lake Design

### Bucket
```
s3://telco-customer360-pipeline/
```

### Folder Structure
```
raw/        → Untrusted landing zone
archive/    → Validated, immutable source data
rejected/   → Invalid or corrupt files (alerted)
clean/      → Standardized, schema-enforced data
staging/    → Analytics-ready snapshots
```

### Dataset Namespaces
```
customers/
plans/
activity_logs/
```

### S3 Lifecycle Flow
```mermaid
flowchart LR
    R[raw/] -->|validate| A[archive/]
    R -->|reject| X[rejected/]
    A --> C[clean/]
    C --> S[staging/]
```

**Design principles**
- Clear data lifecycle
- Safe reprocessing & backfills
- Separation of ingestion vs analytics
- Auditability

---

## 🔐 Security & IAM Model

**Principle:** Least Privilege

| Role | Service | Purpose |
|----|-------|--------|
| Lambda Ingestion Role | Lambda | File validation & routing |
| Glue Job Role | AWS Glue | ETL processing |
| Step Functions Role | Step Functions | Orchestration |
| Snowflake IAM Role | Snowflake | Secure S3 access |

**Key Security Practices**
- No shared roles across services
- Explicit trust relationships
- Snowflake assumes AWS IAM role (no credentials stored)

## Trust Relationships

```mermaid
flowchart TB
    AWS[Trusted AWS Services]
    Lambda --> AWS
    Glue --> AWS
    SF --> AWS
    Snowflake --> AWS

    Snowflake -->|STS AssumeRole| IAM[(IAM Role)]
```

Each role:
- trusts only the service that needs it
- does not trust other AWS services by default

---

## High-Level IAM Architecture

```mermaid
flowchart LR
    Lambda[Lambda
Ingestion Validator]
    GlueA[Glue Job A]
    GlueB[Glue Job B]
    SF[Step Functions]
    S3[(Amazon S3)]
    SNS[(Amazon SNS)]
    Snowflake[Snowflake]

    Lambda -->|read/write| S3
    Lambda -->|publish| SNS

    SF -->|start| GlueA
    SF -->|start| GlueB

    GlueA -->|read/write| S3
    GlueB -->|read/write| S3

    Snowflake -->|assume role| S3
```

Each arrow represents a deliberate permission boundary.

---

## 1️⃣ Lambda Ingestion Role

### Role Name
```
telcocustomer360pipeline_lambdafunction
```
![alt text](imgs/lambda_role.png)

### Responsibility
This role allows Lambda to act as a **gatekeeper** at ingestion time:
- validate incoming files
- move files to the correct S3 zone
- notify on failures

---

### IAM Policy - Lambda Ingestion

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3RawArchiveRejectedAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::<BUCKET_NAME>/raw/*",
        "arn:aws:s3:::<BUCKET_NAME>/archive/*",
        "arn:aws:s3:::<BUCKET_NAME>/rejected/*"
      ]
    },
    {
      "Sid": "PublishIngestionAlerts",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:<REGION>:<ACCOUNT_ID>:<SNS_TOPIC_NAME>"
    },
    {
      "Sid": "CloudWatchLogging",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### Explicitly NOT Allowed
- No access to `clean/` or `staging/`
- No Glue permissions
- No Step Functions permissions
- No Snowflake access

---

## 2️⃣ Glue ETL Role (Job A + Job B)

### Role Name
```
telcocustomer360pipeline_gluerole
```

![alt text](imgs/glue_role.png)

### Responsibility
This role supports **data transformation only** and is shared by Glue Job A and Glue Job B.

---

### IAM Policy — Glue ETL

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ETLReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::<BUCKET_NAME>/archive/*",
        "arn:aws:s3:::<BUCKET_NAME>/clean/*",
        "arn:aws:s3:::<BUCKET_NAME>/staging/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogging",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### Explicitly NOT Allowed
- No Step Functions execution
- No Snowflake access
- No access outside ETL S3 zones

---

## 3️⃣ Step Functions Execution Role

### Role Name
```
StepFunctions-telco-customer-state-machine-role-ombadcmyn
```
![alt text](imgs/stepfunction_role.png)

### Responsibility
Allows Step Functions to act as a **pure orchestrator**:
- start Glue jobs
- monitor Glue execution

This role does **not** touch data.

---

### IAM Policy - Step Functions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueJobOrchestration",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJob",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ],
      "Resource": "arn:aws:glue:<REGION>:<ACCOUNT_ID>:job/*"
    },
    {
      "Sid": "CloudWatchLogging",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### Explicitly NOT Allowed
- No S3 access
- No data read/write
- No Snowflake access
---

## 4️⃣ Snowflake Storage Integration Role

### Role Name
```
Telco_pipeline_snowflake_role
```

![alt text](imgs/snowflake_role.png)

### Responsibility
Allows Snowflake to securely read analytics-ready data from S3 via STS.

---

### IAM Policy - Snowflake Integration

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSnowflakeReadStaging",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<BUCKET_NAME>/staging/*"
    },
    {
      "Sid": "AllowSnowflakeListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<BUCKET_NAME>",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["staging/*"]
        }
      }
    }
  ]
}
```

### Explicitly NOT Allowed
- No write or delete permissions
- No access to `raw/`, `archive/`, or `clean/`
- No access to any AWS service besides S3

---

## 5️⃣ IAM Permission Boundary - Visual Summary

```mermaid
flowchart LR
    Lambda[Lambda Role]
    Glue[Glue Role]
    SF[Step Functions Role]
    Snowflake[Snowflake Role]

    RAW[S3 Raw]
    ARCH[S3 Archive]
    CLEAN[S3 Clean]
    STAGE[S3 Staging]
    SNS[(SNS)]

    Lambda --> RAW
    Lambda --> ARCH
    Lambda --> SNS

    Glue --> ARCH
    Glue --> CLEAN
    Glue --> STAGE

    SF --> Glue

    Snowflake --> STAGE
```

---

## Orchestration & Scheduling

### EventBridge
- Time-based batch schedule (e. daily)
- Triggers Step Functions
- Decoupled from file arrival

### Step Functions State Machine
```mermaid
stateDiagram-v2
    [*] --> GlueJobA
    GlueJobA --> GlueJobB
    GlueJobB --> [*]
```

**Why Step Functions?**
- Explicit job sequencing
- Failure visibility
- Retry & monitoring support
- Enterprise-grade orchestration

---

## Glue Job A - Clean & Standardize

**Purpose**
- Convert heterogeneous formats into consistent Parquet
- Enforce schema and data types
- Separate good vs bad records

**Input**
```
archive/
```

**Output**
```
clean/
```

**Responsibilities**
- Schema enforcement
- Type casting
- Basic validation

**Intentionally does NOT**
- Deduplicate
- Join datasets
- Build analytics logic

---

## Glue Job B - Build Staging Snapshots

**Purpose**
Prepare analytics-ready data.

**Input**
```
clean/
```

**Output**
```
staging/
```

**Responsibilities**
- Deduplicate records
- Join customers ↔ plans
- Produce point-in-time activity data

---

## ❄️ Snowflake Warehouse Design

### Database
```
TELCO_DB
```

### Schemas
- STAGING — transient load tables
- DW — analytics layer

### Tables

**STAGING**
- STG_CUSTOMERS
- STG_PLANS
- STG_ACTIVITY

**DIMENSIONS**
- DIM_PLAN (Type 1)
- DIM_CUSTOMER (SCD Type 2)

**FACT**
- FACT_ACTIVITY (append-only, historical)

---

## SCD Type 2 - Customer History

**Tracked changes**
- Plan changes
- Status changes
- Contact updates

**How it works**
- Old record closed with `effective_end_date`
- New record inserted
- `is_current` flag maintained
- Fact table joins by event timestamp

```mermaid
flowchart LR
    STG[STG_CUSTOMERS] --> MERGE[MERGE Logic]
    MERGE --> DIM[DIM_CUSTOMER SCD2]
    ACT[FACT_ACTIVITY] -->|time-based join| DIM
```

✔ Full history preserved  
✔ Past analytics never change  

---

## 🔁 Snowflake Automation

### Stored Procedures
- Load staging
- Load dimensions
- Load facts

### Master Procedure
```
SP_RUN_CUSTOMER360_PIPELINE
```

### Snowflake TASK
- Scheduled execution
- Runs entire warehouse load
- Decoupled from AWS pipeline
- Safe even if upstream data is late

---

## 📊 Downstream Consumers

**Who uses the data**
- Business analysts
- BI tools (Power BI, Tableau)
- Reporting & dashboards

**What they get**
- Clean dimensions
- Historical facts
- Correct customer-plan attribution
- Analytics-ready tables

---

## Key Engineering Decisions

- Batch architecture over streaming (cost & simplicity)
- Step Functions for orchestration
- Snowflake TASK for warehouse automation
- SCD Type 2 for customer history
- No Lambda Snowflake connector (simpler & safer)

---

## End-to-End Flow Summary

```mermaid
flowchart LR
    Source --> S3Raw
    S3Raw --> Lambda
    Lambda --> S3Archive
    S3Archive --> StepFunctions
    StepFunctions --> GlueA
    GlueA --> GlueB
    GlueB --> S3Staging
    S3Staging --> Snowflake
    Snowflake --> Analysts
```
---

# 📫 Contact

## Oluwatosin Amosu Bolaji 
- Data Engineer 
- Buiness Intelligence Analyst
- ETL Developer

#### 🚀 **Always learning. Always building. Data-driven to the core.**  

### 📫 **Let’s connect!**  
- 📩 oluwabolaji60@gmail.com
- 🔗 : [LinkedIn](https://www.linkedin.com/in/oluwatosin-amosu-722b88141)
- 🌐 : [My Portfolio](https://www.datascienceportfol.io/oluwabolaji60) 
- 𝕏 : [Twitter/X](https://x.com/thee_oluwatosin?s=21&t=EqoeQVdQd038wlSUzAtQzw)
- 🔗 : [Medium](https://medium.com/@oluwabolaji60)
- 🔗 : [View my Repositories](https://github.com/Tbrown1998?tab=repositories)