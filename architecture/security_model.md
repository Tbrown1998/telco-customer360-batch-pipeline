# Security Model  
## Telco Customer 360 Orchestrated Data Pipeline

---

## 1. Security Philosophy

The security model follows **least privilege**, **role isolation**, and **explicit trust relationships**.

No service:
- shares credentials
- assumes unnecessary permissions
- has access beyond its responsibility

---

## 2. AWS IAM Role Design

Each AWS service operates under a **dedicated IAM role**.

| Role | Service | Purpose |
|---|---|---|
| Lambda Ingestion Role | AWS Lambda | File validation and routing |
| Glue Job Role | AWS Glue | ETL processing |
| Step Functions Role | Step Functions | Workflow orchestration |
| Snowflake Integration Role | Snowflake | Secure S3 access |

Roles are never reused across services.

---

## 3. Lambda Security

### Permissions
- Read from `raw/`
- Write to `archive/` and `rejected/`
- Publish SNS alerts
- Write CloudWatch logs

### Restrictions
- No access to `clean/` or `staging/`
- No Glue or Snowflake access

---

## 4. Glue Security

### Permissions
- Read from `archive/` and `clean/`
- Write to `clean/` and `staging/`
- Access Glue Data Catalog
- Write logs to CloudWatch

### Execution
- Jobs run under a service role
- No interactive credentials

---

## 5. Step Functions Security

### Permissions
- Start Glue job runs
- Read Glue job metadata
- Emit execution logs

Step Functions cannot read or write data directly.

---

## 6. Snowflake Security Model

### Storage Integration
- Snowflake assumes an AWS IAM role
- Read-only access to `staging/`
- No access to raw or clean layers

### Role-Based Access Control
- ETL role owns schemas and procedures
- Analyst roles have read-only access to `DW`

No Snowflake credentials are stored in AWS.

---

## 7. Schema Ownership & Governance

- ETL role owns:
  - STAGING schema
  - DW schema
- ACCOUNTADMIN is not used for pipeline execution

This enforces proper separation between administration and operations.

---

## 8. Audit & Observability

- CloudWatch logs for AWS services
- Snowflake task history and query history
- SNS alerts for failures

All actions are auditable.

---

## 9. Security Benefits

This model ensures:
- Reduced blast radius
- Clear accountability
- Compliance readiness
- Production-grade isolation

---

## 10. Summary

Security is implemented as a **foundational design element**, not an afterthought.

The pipeline adheres to enterprise security standards across both AWS and Snowflake.

---
