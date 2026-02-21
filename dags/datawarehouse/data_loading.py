import json
import os
import logging

import boto3

logger = logging.getLogger(__name__)


def load_s3_json(s3_path: str) -> dict:
    """Load and return parsed JSON from an S3 path (s3://bucket/key)."""
    bucket_name = os.environ["S3_BUCKET_NAME"]
    s3_key = s3_path.replace(f"s3://{bucket_name}/", "")

    s3_client = boto3.client("s3")

    try:
        logger.info(f"Loading: s3://{bucket_name}/{s3_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Loaded {len(data.get('jobs', []))} jobs from {s3_key}")
        return data

    except s3_client.exceptions.NoSuchKey:
        logger.error(f"File not found: s3://{bucket_name}/{s3_key}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: s3://{bucket_name}/{s3_key}")
        raise
