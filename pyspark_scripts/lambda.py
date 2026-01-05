import boto3
import os
import json
import csv
import time
import urllib.parse
from datetime import datetime

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET = os.environ.get("BUCKET")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

RAW_PREFIX = "raw"
ARCHIVE_PREFIX = "archive"
REJECTED_PREFIX = "rejected"

SAMPLE_BYTES = 512 * 1024  # 512 KB
SAMPLE_LINES = 300

# Expected required fields per folder for basic validation
REQUIRED_FIELDS = {
    "customers": ["customer_id"],
    "plans": ["plan_id"],
    "activity_logs": ["event_id", "customer_id"]
}

# ========== Helper: Publish SNS alert ===========
def notify(subject, message):
    if not SNS_TOPIC_ARN:
        return
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(message)
    )

# ========== Helper: Read sample of file ===========
def read_sample(bucket, key):
    # read first SAMPLE_BYTES bytes
    resp = s3.get_object(
        Bucket=bucket,
        Key=key,
        Range=f"bytes=0-{SAMPLE_BYTES-1}"
    )
    return resp["Body"].read()

# ========== Helper: Validate NDJSON ===========
def validate_ndjson(sample_bytes, required):
    decoded = sample_bytes.decode("utf-8", errors="replace")
    lines = decoded.splitlines()

    found = set()
    parsed_count = 0

    for line in lines[:SAMPLE_LINES]:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            parsed_count += 1

            for key in required:
                if key in obj:
                    found.add(key)

        except Exception:
            return False, "invalid_ndjson_format"

    if parsed_count == 0:
        return False, "no_json_lines_detected"

    missing = [k for k in required if k not in found]
    if missing:
        return False, f"missing_fields_{missing}"

    return True, "ok"

# ========== Helper: Validate CSV ===========
def validate_csv(sample_bytes, required):
    decoded = sample_bytes.decode("utf-8", errors="replace")
    rows = decoded.splitlines()

    reader = csv.DictReader(rows)
    header = reader.fieldnames or []

    missing = [k for k in required if k not in header]
    if missing:
        return False, f"missing_columns_{missing}"

    # verify at least one data row exists
    for i, row in enumerate(reader):
        if i > 0:
            return True, "ok"

    return False, "no_data_rows_found"

# ========== Helper: Determine file type from folder ===========
def get_file_type(key):
    """
    key examples:
      raw/customers/file.ndjson        -> customers
      raw/plans/file.csv               -> plans
      raw/activity_logs/file.ndjson    -> activity_logs
    """
    parts = key.split("/")
    if len(parts) < 2:
        return None

    folder = parts[1]  # customers / plans / activity_logs
    return folder if folder in REQUIRED_FIELDS else None

# ========== Helper: Build destination path ===========
def build_destination(prefix, folder, original_key):
    """
    Build something like:
      archive/customers/2025-05-20/filename.json
    """
    filename = original_key.split("/")[-1]
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return f"{prefix}/{folder}/{today}/{filename}"

# ========== Helper: Move file (copy then delete original) ===========
def move_file(bucket, source_key, dest_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=bucket, Key=source_key)

# ========== MAIN HANDLER ===========
def lambda_handler(event, context):

    print("Event:", json.dumps(event))

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"Processing file: s3://{bucket}/{key}")

        # Only process raw files
        if not key.startswith(f"{RAW_PREFIX}/"):
            print("Not a RAW file — skipping.")
            continue

        # Determine file type based on folder
        file_type = get_file_type(key)
        if not file_type:
            reason = "unknown_folder_structure"
            dest_key = build_destination(REJECTED_PREFIX, "_unknown", key)
            move_file(bucket, key, dest_key)
            notify("Rejected file", {"file": key, "reason": reason})
            continue

        required_fields = REQUIRED_FIELDS[file_type]

        # Read sample
        try:
            sample = read_sample(bucket, key)
        except Exception as e:
            reason = f"error_reading_file_{str(e)}"
            dest_key = build_destination(REJECTED_PREFIX, file_type, key)
            move_file(bucket, key, dest_key)
            notify("Rejected file", {"file": key, "reason": reason})
            continue

        # Determine validation approach
        if file_type == "plans":
            ok, reason = validate_csv(sample, required_fields)
        else:
            ok, reason = validate_ndjson(sample, required_fields)

        # Route based on validation
        if ok:
            dest_key = build_destination(ARCHIVE_PREFIX, file_type, key)
            move_file(bucket, key, dest_key)
            print("File archived:", dest_key)

        else:
            dest_key = build_destination(REJECTED_PREFIX, file_type, key)
            move_file(bucket, key, dest_key)

            print("File rejected:", dest_key)
            notify(
                "Rejected file",
                {
                    "file": key,
                    "reason": reason,
                    "moved_to": dest_key
                }
            )

    return {"status": "done"}
