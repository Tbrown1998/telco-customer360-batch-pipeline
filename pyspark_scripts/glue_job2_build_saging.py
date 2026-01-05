import sys
import json
import boto3
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ========== Arguments ===========
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BASE_BUCKET", "SNS_TOPIC_ARN"]
)

JOB_NAME = args["JOB_NAME"]
BASE_BUCKET = args["S3_BASE_BUCKET"].rstrip("/")
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]

# ========== Spark / Glue setup ===========
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(JOB_NAME, args)

sns = boto3.client("sns")

# ========== Paths ===========
CUSTOMERS_SRC = f"{BASE_BUCKET}/clean/customers/"
PLANS_SRC = f"{BASE_BUCKET}/clean/plans/"
ACTIVITY_SRC = f"{BASE_BUCKET}/clean/activity_logs/"

CUSTOMERS_TGT = f"{BASE_BUCKET}/staging/customers/"
ACTIVITY_TGT = f"{BASE_BUCKET}/staging/activity/"
PLANS_TGT = f"{BASE_BUCKET}/staging/plans/"

# ========== Read CLEAN data ===========
customers_df = spark.read.parquet(CUSTOMERS_SRC).alias("c")
plans_df = spark.read.parquet(PLANS_SRC).alias("p")
activity_df = spark.read.parquet(ACTIVITY_SRC).alias("a")

# ========== CUSTOMER STAGING (deduplicated snapshot) ===========
cust_plan_joined = (
    customers_df.join(
        plans_df,
        col("c.plan_id") == col("p.plan_id"),
        how="left"
    )
)

customers_selected = cust_plan_joined.select(
    col("c.customer_id"),
    col("c.first_name"),
    col("c.last_name"),
    col("c.email"),
    col("c.phone"),
    col("c.country"),
    col("c.status"),
    col("c.plan_id"),
    col("p.plan_name"),
    col("p.plan_category"),
    col("p.monthly_price"),
    col("c.ingestion_ts").alias("ingestion_ts")
)

cust_window = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())

customers_staging = (
    customers_selected
    .withColumn("rn", row_number().over(cust_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ========== ACTIVITY STAGING (deduplicated facts) ===========
activity_joined = (
    activity_df.join(
        customers_staging.select("customer_id", "plan_id"),
        on="customer_id",
        how="left"
    )
)

activity_staging = activity_joined.dropDuplicates(["event_id"])

# ========== PLAN STAGING (reference snapshot) ===========
plan_window = Window.partitionBy("plan_id").orderBy(col("p.ingestion_ts").desc())

plans_staging = (
    plans_df
    .withColumn("rn", row_number().over(plan_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ========== Write STAGING (overwrite snapshot) ===========
customers_staging.write.mode("overwrite").parquet(CUSTOMERS_TGT)
activity_staging.write.mode("overwrite").parquet(ACTIVITY_TGT)
plans_staging.write.mode("overwrite").parquet(PLANS_TGT)

# ========== SNS Summary ===========
summary_payload = {
    "job_name": JOB_NAME,
    "run_time_utc": datetime.utcnow().isoformat(),
    "customers_staged": customers_staging.count(),
    "activity_staged": activity_staging.count(),
    "plans_staged": plans_staging.count(),
    "status": "SUCCESS"
}

sns.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="Glue Job B Summary — telco-customer360",
    Message=json.dumps(summary_payload, indent=2)
)

job.commit()
