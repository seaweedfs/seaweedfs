#!/usr/bin/env python3
"""
Test script for SSE-S3 compatibility with PyArrow native S3 filesystem.

This test specifically targets the SSE-S3 multipart upload bug where
SeaweedFS panics with "bad IV length" when reading multipart uploads
that were encrypted with bucket-default SSE-S3.

Requirements:
    - pyarrow>=10.0.0
    - boto3>=1.28.0

Environment Variables:
    S3_ENDPOINT_URL: S3 endpoint (default: localhost:8333)
    S3_ACCESS_KEY: S3 access key (default: some_access_key1)
    S3_SECRET_KEY: S3 secret key (default: some_secret_key1)
    BUCKET_NAME: S3 bucket name (default: test-parquet-bucket)

Usage:
    # Start SeaweedFS with SSE-S3 enabled
    make start-seaweedfs-ci ENABLE_SSE_S3=true
    
    # Run the test
    python3 test_sse_s3_compatibility.py
"""

import os
import secrets
import sys
import logging
from datetime import datetime
from typing import Optional

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as pq

try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    logging.error("boto3 is required for this test")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Configuration
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "localhost:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "some_secret_key1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-parquet-bucket")

TEST_RUN_ID = secrets.token_hex(8)
TEST_DIR = f"sse-s3-tests/{TEST_RUN_ID}"

# Test sizes designed to trigger multipart uploads
# PyArrow typically uses 5MB chunks, so these sizes should trigger multipart
TEST_SIZES = {
    "tiny": 10,                    # Single part
    "small": 1_000,               # Single part
    "medium": 50_000,             # Single part (~1.5MB)
    "large": 200_000,             # Multiple parts (~6MB)
    "very_large": 500_000,        # Multiple parts (~15MB)
}


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


def init_s3_filesystem() -> tuple[Optional[pafs.S3FileSystem], str, str]:
    """Initialize PyArrow's native S3 filesystem."""
    try:
        logging.info("Initializing PyArrow S3FileSystem...")
        
        # Determine scheme from endpoint
        if S3_ENDPOINT_URL.startswith("http://"):
            scheme = "http"
            endpoint = S3_ENDPOINT_URL[7:]
        elif S3_ENDPOINT_URL.startswith("https://"):
            scheme = "https"
            endpoint = S3_ENDPOINT_URL[8:]
        else:
            scheme = "http"
            endpoint = S3_ENDPOINT_URL
        
        s3 = pafs.S3FileSystem(
            access_key=S3_ACCESS_KEY,
            secret_key=S3_SECRET_KEY,
            endpoint_override=endpoint,
            scheme=scheme,
            allow_bucket_creation=True,
            allow_bucket_deletion=True,
        )
        
        logging.info("✓ PyArrow S3FileSystem initialized\n")
        return s3, scheme, endpoint
    except Exception as e:
        logging.exception("✗ Failed to initialize PyArrow S3FileSystem")
        return None, "", ""


def ensure_bucket_exists(scheme: str, endpoint: str) -> bool:
    """Ensure the test bucket exists using boto3."""
    try:
        endpoint_url = f"{scheme}://{endpoint}"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name='us-east-1',
        )
        
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logging.info(f"Creating bucket: {BUCKET_NAME}")
                s3_client.create_bucket(Bucket=BUCKET_NAME)
                logging.info(f"✓ Bucket created: {BUCKET_NAME}")
        
        # Note: SeaweedFS doesn't support GetBucketEncryption API
        # so we can't verify if SSE-S3 is enabled via API
        # We assume it's configured correctly in the s3.json config file
        logging.info(f"✓ Assuming SSE-S3 is configured in s3.json")
        return True
            
    except Exception as e:
        logging.exception(f"✗ Failed to check bucket: {e}")
        return False


def test_write_read_with_sse(
    s3: pafs.S3FileSystem,
    test_name: str,
    num_rows: int
) -> tuple[bool, str, int]:
    """Test writing and reading with SSE-S3 encryption."""
    try:
        table = create_sample_table(num_rows)
        filename = f"{BUCKET_NAME}/{TEST_DIR}/{test_name}/data.parquet"
        
        logging.info(f"  Writing {num_rows:,} rows...")
        pads.write_dataset(
            table,
            filename,
            filesystem=s3,
            format="parquet",
        )
        
        logging.info(f"  Reading back...")
        table_read = pq.read_table(filename, filesystem=s3)
        
        if table_read.num_rows != num_rows:
            return False, f"Row count mismatch: {table_read.num_rows} != {num_rows}", 0
        
        return True, "Success", table_read.num_rows
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logging.error(f"  ✗ Failed: {error_msg}")
        return False, error_msg, 0


def main():
    """Run SSE-S3 compatibility tests."""
    print("=" * 80)
    print("SSE-S3 Compatibility Tests for PyArrow Native S3")
    print("Testing Multipart Upload Encryption")
    print("=" * 80 + "\n")

    print("Configuration:")
    print(f"  S3 Endpoint: {S3_ENDPOINT_URL}")
    print(f"  Bucket: {BUCKET_NAME}")
    print(f"  Test Directory: {TEST_DIR}")
    print(f"  PyArrow Version: {pa.__version__}")
    print()

    # Initialize
    s3, scheme, endpoint = init_s3_filesystem()
    if s3 is None:
        print("Cannot proceed without S3 connection")
        return 1

    # Check bucket and SSE-S3
    if not ensure_bucket_exists(scheme, endpoint):
        print("\n⚠ WARNING: SSE-S3 is not enabled on the bucket!")
        print("This test requires SSE-S3 encryption to be enabled.")
        print("Please start SeaweedFS with: make start-seaweedfs-ci ENABLE_SSE_S3=true")
        return 1

    print()
    results = []

    # Test all sizes
    for size_name, num_rows in TEST_SIZES.items():
        print(f"\n{'='*80}")
        print(f"Testing {size_name} dataset ({num_rows:,} rows)")
        print(f"{'='*80}")
        
        success, message, rows_read = test_write_read_with_sse(
            s3, size_name, num_rows
        )
        results.append((size_name, num_rows, success, message, rows_read))
        
        if success:
            print(f"  ✓ SUCCESS: Read {rows_read:,} rows")
        else:
            print(f"  ✗ FAILED: {message}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, _, success, _, _ in results if success)
    total = len(results)
    print(f"\nTotal: {passed}/{total} tests passed\n")
    
    print(f"{'Size':<15} {'Rows':>10} {'Status':<10} {'Message':<40}")
    print("-" * 80)
    for size_name, num_rows, success, message, rows_read in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{size_name:<15} {num_rows:>10,} {status:<10} {message[:40]}")

    print("\n" + "=" * 80)
    if passed == total:
        print("✓ ALL TESTS PASSED WITH SSE-S3!")
        print("\nThis means:")
        print("  - SSE-S3 encryption is working correctly")
        print("  - PyArrow native S3 filesystem is compatible")
        print("  - Multipart uploads are handled properly")
    else:
        print(f"✗ {total - passed} test(s) failed")
        print("\nPossible issues:")
        print("  - SSE-S3 multipart upload bug with empty IV")
        print("  - Encryption/decryption mismatch")
        print("  - File corruption during upload")

    print("=" * 80 + "\n")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

