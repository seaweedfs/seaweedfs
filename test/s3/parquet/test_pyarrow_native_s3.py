#!/usr/bin/env python3
"""
Test script for PyArrow's NATIVE S3 filesystem with SeaweedFS.

This test uses PyArrow's built-in S3FileSystem (pyarrow.fs.S3FileSystem)
instead of s3fs, providing a pure PyArrow solution for reading and writing
Parquet files to S3-compatible storage.

Requirements:
    - pyarrow>=10.0.0

Environment Variables:
    S3_ENDPOINT_URL: S3 endpoint (default: localhost:8333)
    S3_ACCESS_KEY: S3 access key (default: some_access_key1)
    S3_SECRET_KEY: S3 secret key (default: some_secret_key1)
    BUCKET_NAME: S3 bucket name (default: test-parquet-bucket)
    TEST_QUICK: Run only small/quick tests (default: 0, set to 1 for quick mode)

Usage:
    # Run with default environment variables
    python3 test_pyarrow_native_s3.py

    # Run with custom environment variables
    S3_ENDPOINT_URL=localhost:8333 \
    S3_ACCESS_KEY=mykey \
    S3_SECRET_KEY=mysecret \
    BUCKET_NAME=mybucket \
    python3 test_pyarrow_native_s3.py
"""

import os
import secrets
import sys
import logging
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

from parquet_test_utils import create_sample_table

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Configuration from environment variables with defaults
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "localhost:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "some_secret_key1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-parquet-bucket")
TEST_QUICK = os.getenv("TEST_QUICK", "0") == "1"

# Create randomized test directory
TEST_RUN_ID = secrets.token_hex(8)
TEST_DIR = f"parquet-native-tests/{TEST_RUN_ID}"

# Test file sizes
TEST_SIZES = {
    "small": 5,
    "large": 200_000,  # This will create multiple row groups
}

# Filter to only small tests if quick mode is enabled
if TEST_QUICK:
    TEST_SIZES = {"small": TEST_SIZES["small"]}
    logging.info("Quick test mode enabled - running only small tests")


def init_s3_filesystem() -> tuple[Optional[pafs.S3FileSystem], str, str]:
    """Initialize PyArrow's native S3 filesystem.
    
    Returns:
        tuple: (S3FileSystem instance, scheme, endpoint)
    """
    try:
        logging.info("Initializing PyArrow S3FileSystem...")
        logging.info(f"  Endpoint: {S3_ENDPOINT_URL}")
        logging.info(f"  Bucket: {BUCKET_NAME}")
        
        # Determine scheme from endpoint
        if S3_ENDPOINT_URL.startswith("http://"):
            scheme = "http"
            endpoint = S3_ENDPOINT_URL[7:]  # Remove http://
        elif S3_ENDPOINT_URL.startswith("https://"):
            scheme = "https"
            endpoint = S3_ENDPOINT_URL[8:]  # Remove https://
        else:
            # Default to http for localhost
            scheme = "http"
            endpoint = S3_ENDPOINT_URL
        
        # Enable bucket creation and deletion for testing
        s3 = pafs.S3FileSystem(
            access_key=S3_ACCESS_KEY,
            secret_key=S3_SECRET_KEY,
            endpoint_override=endpoint,
            scheme=scheme,
            allow_bucket_creation=True,
            allow_bucket_deletion=True,
        )
        
        logging.info("✓ PyArrow S3FileSystem initialized successfully\n")
        return s3, scheme, endpoint
    except Exception:
        logging.exception("✗ Failed to initialize PyArrow S3FileSystem")
        return None, "", ""


def ensure_bucket_exists_boto3(scheme: str, endpoint: str) -> bool:
    """Ensure the test bucket exists using boto3."""
    if not HAS_BOTO3:
        logging.error("boto3 is required for bucket creation")
        return False
    
    try:
        # Create boto3 client
        endpoint_url = f"{scheme}://{endpoint}"
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name='us-east-1',
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                logging.info(f"Creating bucket: {BUCKET_NAME}")
                s3_client.create_bucket(Bucket=BUCKET_NAME)
                logging.info(f"✓ Bucket created: {BUCKET_NAME}")
                return True
            else:
                raise
    except Exception:
        logging.exception("✗ Failed to create/check bucket")
        return False


def ensure_bucket_exists(s3: pafs.S3FileSystem) -> bool:
    """Ensure the test bucket exists using PyArrow's native S3FileSystem."""
    try:
        # Check if bucket exists by trying to list it
        try:
            file_info = s3.get_file_info(BUCKET_NAME)
            if file_info.type == pafs.FileType.Directory:
                logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
                return True
        except OSError as e:
            # OSError typically means bucket not found or network/permission issues
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg or "nosuchbucket" in error_msg:
                logging.debug(f"Bucket '{BUCKET_NAME}' not found, will attempt creation: {e}")
            else:
                # Log other OSErrors (network, auth, etc.) for debugging
                logging.debug(f"Error checking bucket '{BUCKET_NAME}', will attempt creation anyway: {type(e).__name__}: {e}")
        except Exception as e:
            # Catch any other unexpected exceptions and log them
            logging.debug(f"Unexpected error checking bucket '{BUCKET_NAME}', will attempt creation: {type(e).__name__}: {e}")
        
        # Try to create the bucket
        logging.info(f"Creating bucket: {BUCKET_NAME}")
        s3.create_dir(BUCKET_NAME)
        logging.info(f"✓ Bucket created: {BUCKET_NAME}")
        return True
    except Exception:
        logging.exception(f"✗ Failed to create/check bucket '{BUCKET_NAME}' with PyArrow")
        return False


def test_write_and_read(s3: pafs.S3FileSystem, test_name: str, num_rows: int) -> tuple[bool, str]:
    """Test writing and reading a Parquet dataset using PyArrow's native S3 filesystem."""
    try:
        table = create_sample_table(num_rows)
        
        # Write using pads.write_dataset
        filename = f"{BUCKET_NAME}/{TEST_DIR}/{test_name}/data.parquet"
        logging.info(f"  Writing {num_rows:,} rows to {filename}...")
        
        pads.write_dataset(
            table,
            filename,
            filesystem=s3,
            format="parquet",
        )
        logging.info("  ✓ Write completed")
        
        # Test Method 1: Read with pq.read_table
        logging.info("  Reading with pq.read_table...")
        table_read = pq.read_table(filename, filesystem=s3)
        if table_read.num_rows != num_rows:
            return False, f"pq.read_table: Row count mismatch (expected {num_rows}, got {table_read.num_rows})"
        
        # Check schema first
        if not table_read.schema.equals(table.schema):
            return False, f"pq.read_table: Schema mismatch (expected {table.schema}, got {table_read.schema})"
        
        # Sort both tables by 'id' column before comparison to handle potential row order differences
        table_sorted = table.sort_by([('id', 'ascending')])
        table_read_sorted = table_read.sort_by([('id', 'ascending')])
        
        if not table_read_sorted.equals(table_sorted):
            # Provide more detailed error information
            error_details = []
            for col_name in table.column_names:
                col_original = table_sorted.column(col_name)
                col_read = table_read_sorted.column(col_name)
                if not col_original.equals(col_read):
                    error_details.append(f"column '{col_name}' differs")
            return False, f"pq.read_table: Table contents mismatch ({', '.join(error_details)})"
        logging.info(f"  ✓ pq.read_table: {table_read.num_rows:,} rows")
        
        # Test Method 2: Read with pq.ParquetDataset
        logging.info("  Reading with pq.ParquetDataset...")
        dataset = pq.ParquetDataset(filename, filesystem=s3)
        table_dataset = dataset.read()
        if table_dataset.num_rows != num_rows:
            return False, f"pq.ParquetDataset: Row count mismatch (expected {num_rows}, got {table_dataset.num_rows})"
        
        # Sort before comparison
        table_dataset_sorted = table_dataset.sort_by([('id', 'ascending')])
        if not table_dataset_sorted.equals(table_sorted):
            error_details = []
            for col_name in table.column_names:
                col_original = table_sorted.column(col_name)
                col_read = table_dataset_sorted.column(col_name)
                if not col_original.equals(col_read):
                    error_details.append(f"column '{col_name}' differs")
            return False, f"pq.ParquetDataset: Table contents mismatch ({', '.join(error_details)})"
        logging.info(f"  ✓ pq.ParquetDataset: {table_dataset.num_rows:,} rows")
        
        # Test Method 3: Read with pads.dataset
        logging.info("  Reading with pads.dataset...")
        dataset_pads = pads.dataset(filename, filesystem=s3)
        table_pads = dataset_pads.to_table()
        if table_pads.num_rows != num_rows:
            return False, f"pads.dataset: Row count mismatch (expected {num_rows}, got {table_pads.num_rows})"
        
        # Sort before comparison
        table_pads_sorted = table_pads.sort_by([('id', 'ascending')])
        if not table_pads_sorted.equals(table_sorted):
            error_details = []
            for col_name in table.column_names:
                col_original = table_sorted.column(col_name)
                col_read = table_pads_sorted.column(col_name)
                if not col_original.equals(col_read):
                    error_details.append(f"column '{col_name}' differs")
            return False, f"pads.dataset: Table contents mismatch ({', '.join(error_details)})"
        logging.info(f"  ✓ pads.dataset: {table_pads.num_rows:,} rows")
        
        return True, "All read methods passed"
        
    except Exception as exc:
        logging.exception("  ✗ Test failed")
        return False, f"{type(exc).__name__}: {exc}"


def cleanup_test_files(s3: pafs.S3FileSystem) -> None:
    """Clean up test files from S3.
    
    Note: We cannot use s3.delete_dir() directly because SeaweedFS uses implicit
    directories (path prefixes without physical directory objects). PyArrow's
    delete_dir() attempts to delete the directory marker itself, which fails with
    "INTERNAL_FAILURE" on SeaweedFS. Instead, we list and delete files individually,
    letting implicit directories disappear automatically.
    """
    try:
        test_path = f"{BUCKET_NAME}/{TEST_DIR}"
        logging.info(f"Cleaning up test directory: {test_path}")
        
        # List and delete files individually to handle implicit directories
        try:
            file_selector = pafs.FileSelector(test_path, recursive=True)
            files = s3.get_file_info(file_selector)
            
            # Delete files first (not directories)
            for file_info in files:
                if file_info.type == pafs.FileType.File:
                    s3.delete_file(file_info.path)
                    logging.debug(f"  Deleted file: {file_info.path}")
            
            logging.info("✓ Test directory cleaned up")
        except OSError as e:
            # Handle the case where the path doesn't exist or is inaccessible
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                logging.info("✓ Test directory already clean or doesn't exist")
            else:
                raise
    except Exception:
        logging.exception("Failed to cleanup test directory")


def main():
    """Run all tests with PyArrow's native S3 filesystem."""
    print("=" * 80)
    print("PyArrow Native S3 Filesystem Tests for SeaweedFS")
    print("Testing Parquet Files with Multiple Row Groups")
    if TEST_QUICK:
        print("*** QUICK TEST MODE - Small files only ***")
    print("=" * 80 + "\n")

    print("Configuration:")
    print(f"  S3 Endpoint: {S3_ENDPOINT_URL}")
    print(f"  Access Key: {S3_ACCESS_KEY}")
    print(f"  Bucket: {BUCKET_NAME}")
    print(f"  Test Directory: {TEST_DIR}")
    print(f"  Quick Mode: {'Yes (small files only)' if TEST_QUICK else 'No (all file sizes)'}")
    print(f"  PyArrow Version: {pa.__version__}")
    print()

    # Initialize S3 filesystem
    s3, scheme, endpoint = init_s3_filesystem()
    if s3 is None:
        print("Cannot proceed without S3 connection")
        return 1

    # Ensure bucket exists - try PyArrow first, fall back to boto3
    bucket_created = ensure_bucket_exists(s3)
    if not bucket_created:
        logging.info("Trying to create bucket with boto3...")
        bucket_created = ensure_bucket_exists_boto3(scheme, endpoint)
    
    if not bucket_created:
        print("Cannot proceed without bucket")
        return 1

    results = []

    # Test all file sizes
    for size_name, num_rows in TEST_SIZES.items():
        print(f"\n{'='*80}")
        print(f"Testing with {size_name} files ({num_rows:,} rows)")
        print(f"{'='*80}\n")

        test_name = f"{size_name}_test"
        success, message = test_write_and_read(s3, test_name, num_rows)
        results.append((test_name, success, message))
        
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"\n{status}: {message}\n")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    print(f"\nTotal: {passed}/{total} passed\n")

    for test_name, success, message in results:
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}: {message}")

    print("\n" + "=" * 80)
    if passed == total:
        print("✓ ALL TESTS PASSED!")
    else:
        print(f"✗ {total - passed} test(s) failed")

    print("=" * 80 + "\n")

    # Cleanup
    cleanup_test_files(s3)

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

