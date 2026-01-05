import sys
import json
import boto3
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp
)
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ================== Glue Job Arguments ===============
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BASE_BUCKET",
        "SNS_TOPIC_ARN"
    ]
)

JOB_NAME = args["JOB_NAME"]
BASE_BUCKET = args["S3_BASE_BUCKET"].rstrip("/")
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]

# ================== Glue / Spark Setup ===============
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(JOB_NAME, args)

sns = boto3.client("sns")

# ================== S3 Paths===============

CUSTOMERS_SRC = f"{BASE_BUCKET}/archive/customers/*/"
PLANS_SRC = f"{BASE_BUCKET}/archive/plans/*/"
ACTIVITY_SRC = f"{BASE_BUCKET}/archive/activity_logs/*/"

CUSTOMERS_TGT = f"{BASE_BUCKET}/clean/customers/"
PLANS_TGT = f"{BASE_BUCKET}/clean/plans/"
ACTIVITY_TGT = f"{BASE_BUCKET}/clean/activity_logs/"
BAD_RECORDS_TGT = f"{BASE_BUCKET}/clean/bad_records/"

# ================== Schema Setup ===============

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("address", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("registration_channel", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("status", StringType(), True)
])

plans_schema = StructType([
    StructField("plan_id", StringType(), False),
    StructField("plan_name", StringType(), True),
    StructField("monthly_price", DoubleType(), True),
    StructField("data_allowance_gb", IntegerType(), True),
    StructField("voice_minutes", IntegerType(), True),
    StructField("sms_allowance", IntegerType(), True),
    StructField("roaming_enabled", StringType(), True),
    StructField("validity_days", IntegerType(), True),
    StructField("plan_category", StringType(), True)
])

activity_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("payload", StringType(), True)
])

# ================== Read Data ===============
customers_df = spark.read.schema(customers_schema).json(CUSTOMERS_SRC)
plans_df = spark.read.schema(plans_schema).option("header", "true").csv(PLANS_SRC)
activity_df = spark.read.schema(activity_schema).json(ACTIVITY_SRC)

# ================== Rows Count ===============
customers_total = customers_df.count()
plans_total = plans_df.count()
activity_total = activity_df.count()

# ================== Clean & Validate Customers ===============
customers_clean = (
    customers_df
    .withColumn("registration_ts", to_timestamp("registration_date"))
    .withColumn("ingestion_ts", current_timestamp())
    .drop("registration_date")
)

customers_bad = customers_clean.filter(col("customer_id").isNull())
customers_good = customers_clean.filter(col("customer_id").isNotNull()) \
                                 .dropDuplicates(["customer_id"])

# ================== Clean & Validate Plans ===============
plans_clean = plans_df.withColumn("ingestion_ts", current_timestamp())

plans_bad = plans_clean.filter(col("plan_id").isNull())
plans_good = plans_clean.filter(col("plan_id").isNotNull()) \
                         .dropDuplicates(["plan_id"])

# ================== Clean & Validate Activity Logs ===============
activity_clean = (
    activity_df
    .withColumn("event_ts", to_timestamp("event_timestamp"))
    .withColumn("ingestion_ts", current_timestamp())
    .drop("event_timestamp")
)

activity_bad = activity_clean.filter(
    col("event_id").isNull() | col("customer_id").isNull()
)

activity_good = activity_clean.filter(
    col("event_id").isNotNull() & col("customer_id").isNotNull()
).dropDuplicates(["event_id"])

# ================== Row Count (Good / Bad) ===============
customers_good_count = customers_good.count()
customers_bad_count = customers_bad.count()

plans_good_count = plans_good.count()
plans_bad_count = plans_bad.count()

activity_good_count = activity_good.count()
activity_bad_count = activity_bad.count()

# ================== Write Read Output ===============
customers_good.write.mode("append").parquet(CUSTOMERS_TGT)
plans_good.write.mode("append").parquet(PLANS_TGT)
activity_good.write.mode("append").parquet(ACTIVITY_TGT)

customers_bad.write.mode("append").parquet(f"{BAD_RECORDS_TGT}/customers/")
plans_bad.write.mode("append").parquet(f"{BAD_RECORDS_TGT}/plans/")
activity_bad.write.mode("append").parquet(f"{BAD_RECORDS_TGT}/activity_logs/")

# ================== Sns Summary ===============
summary_payload = {
    "job_name": JOB_NAME,
    "run_time_utc": datetime.utcnow().isoformat(),
    "customers": {
        "total": customers_total,
        "good": customers_good_count,
        "bad": customers_bad_count
    },
    "plans": {
        "total": plans_total,
        "good": plans_good_count,
        "bad": plans_bad_count
    },
    "activity_logs": {
        "total": activity_total,
        "good": activity_good_count,
        "bad": activity_bad_count
    },
    "status": "SUCCESS"
}

sns.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="Glue Job A Summary — telco-customer360",
    Message=json.dumps(summary_payload, indent=2)
)

job.commit()