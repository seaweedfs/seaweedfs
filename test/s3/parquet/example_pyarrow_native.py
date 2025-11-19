#!/usr/bin/env python3
# /// script
# dependencies = [
#     "pyarrow>=22",
#     "boto3>=1.28.0",
# ]
# ///

"""
Simple example of using PyArrow's native S3 filesystem with SeaweedFS.

This is a minimal example demonstrating how to write and read Parquet files
using PyArrow's built-in S3FileSystem without any additional dependencies
like s3fs.

Usage:
    # Set environment variables
    export S3_ENDPOINT_URL=localhost:8333
    export S3_ACCESS_KEY=some_access_key1
    export S3_SECRET_KEY=some_secret_key1
    export BUCKET_NAME=test-parquet-bucket

    # Run the script
    python3 example_pyarrow_native.py
    
    # Or run with uv (if available)
    uv run example_pyarrow_native.py
"""

import os
import secrets

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as pq

# Configuration
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-parquet-bucket")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "localhost:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "some_secret_key1")

# Determine scheme from endpoint
if S3_ENDPOINT_URL.startswith("http://"):
    scheme = "http"
    endpoint = S3_ENDPOINT_URL[7:]
elif S3_ENDPOINT_URL.startswith("https://"):
    scheme = "https"
    endpoint = S3_ENDPOINT_URL[8:]
else:
    scheme = "http"  # Default to http for localhost
    endpoint = S3_ENDPOINT_URL

print(f"Connecting to S3 endpoint: {scheme}://{endpoint}")

# Initialize PyArrow's NATIVE S3 filesystem
s3 = pafs.S3FileSystem(
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    endpoint_override=endpoint,
    scheme=scheme,
    allow_bucket_creation=True,
    allow_bucket_deletion=True,
)

print("✓ Connected to S3 endpoint")


def create_sample_table(num_rows: int = 5) -> pa.Table:
    """Create a sample PyArrow table for testing."""
    return pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(num_rows)], type=pa.string()),
            "value": pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
            "flag": pa.array([i % 2 == 0 for i in range(num_rows)], type=pa.bool_()),
        }
    )


# Create bucket if needed (using boto3)
try:
    import boto3
    from botocore.exceptions import ClientError
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='us-east-1',
    )
    
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"✓ Bucket exists: {BUCKET_NAME}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Creating bucket: {BUCKET_NAME}")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"✓ Bucket created: {BUCKET_NAME}")
        else:
            raise
except ImportError:
    print("Warning: boto3 not available, assuming bucket exists")

# Generate a unique filename
filename = f"{BUCKET_NAME}/dataset-{secrets.token_hex(8)}/test.parquet"

print(f"\nWriting Parquet dataset to: {filename}")

# Write dataset
table = create_sample_table(200_000)
pads.write_dataset(
    table,
    filename,
    filesystem=s3,
    format="parquet",
)

print(f"✓ Wrote {table.num_rows:,} rows")

# Read with pq.read_table
print("\nReading with pq.read_table...")
table_read = pq.read_table(filename, filesystem=s3)
print(f"✓ Read {table_read.num_rows:,} rows")

# Read with pq.ParquetDataset
print("\nReading with pq.ParquetDataset...")
dataset = pq.ParquetDataset(filename, filesystem=s3)
table_dataset = dataset.read()
print(f"✓ Read {table_dataset.num_rows:,} rows")

# Read with pads.dataset
print("\nReading with pads.dataset...")
dataset_pads = pads.dataset(filename, filesystem=s3)
table_pads = dataset_pads.to_table()
print(f"✓ Read {table_pads.num_rows:,} rows")

print("\n✅ All operations completed successfully!")
print(f"\nFile written to: {filename}")
print("You can verify the file using the SeaweedFS S3 API or weed shell")

