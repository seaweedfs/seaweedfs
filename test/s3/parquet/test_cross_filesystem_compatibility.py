#!/usr/bin/env python3
"""
Cross-filesystem compatibility tests for PyArrow Parquet files.

This test verifies that Parquet files written using one filesystem implementation
(s3fs or PyArrow native S3) can be correctly read using the other implementation.

Test Matrix:
- Write with s3fs → Read with PyArrow native S3
- Write with PyArrow native S3 → Read with s3fs

Requirements:
    - pyarrow>=22.0.0
    - s3fs>=2024.12.0
    - boto3>=1.40.0

Environment Variables:
    S3_ENDPOINT_URL: S3 endpoint (default: http://localhost:8333)
    S3_ACCESS_KEY: S3 access key (default: some_access_key1)
    S3_SECRET_KEY: S3 secret key (default: some_secret_key1)
    BUCKET_NAME: S3 bucket name (default: test-parquet-bucket)
    TEST_QUICK: Run only small/quick tests (default: 0, set to 1 for quick mode)

Usage:
    # Run with default environment variables
    python3 test_cross_filesystem_compatibility.py

    # Run with custom environment variables
    S3_ENDPOINT_URL=http://localhost:8333 \
    S3_ACCESS_KEY=mykey \
    S3_SECRET_KEY=mysecret \
    BUCKET_NAME=mybucket \
    python3 test_cross_filesystem_compatibility.py
"""

import os
import secrets
import sys
import logging
from typing import Optional, Tuple

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import s3fs

try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

from parquet_test_utils import create_sample_table

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Configuration from environment variables with defaults
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "some_secret_key1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-parquet-bucket")
TEST_QUICK = os.getenv("TEST_QUICK", "0") == "1"

# Create randomized test directory
TEST_RUN_ID = secrets.token_hex(8)
TEST_DIR = f"parquet-cross-fs-tests/{TEST_RUN_ID}"

# Test file sizes
TEST_SIZES = {
    "small": 5,
    "large": 200_000,  # This will create multiple row groups
}

# Filter to only small tests if quick mode is enabled
if TEST_QUICK:
    TEST_SIZES = {"small": TEST_SIZES["small"]}
    logging.info("Quick test mode enabled - running only small tests")


def init_s3fs() -> Optional[s3fs.S3FileSystem]:
    """Initialize s3fs filesystem."""
    try:
        logging.info("Initializing s3fs...")
        fs = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": S3_ENDPOINT_URL},
            key=S3_ACCESS_KEY,
            secret=S3_SECRET_KEY,
            use_listings_cache=False,
        )
        logging.info("✓ s3fs initialized successfully")
        return fs
    except Exception:
        logging.exception("✗ Failed to initialize s3fs")
        return None


def init_pyarrow_s3() -> Tuple[Optional[pafs.S3FileSystem], str, str]:
    """Initialize PyArrow's native S3 filesystem.
    
    Returns:
        tuple: (S3FileSystem instance, scheme, endpoint)
    """
    try:
        logging.info("Initializing PyArrow S3FileSystem...")
        
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
        
        logging.info("✓ PyArrow S3FileSystem initialized successfully")
        return s3, scheme, endpoint
    except Exception:
        logging.exception("✗ Failed to initialize PyArrow S3FileSystem")
        return None, "", ""


def ensure_bucket_exists(s3fs_fs: s3fs.S3FileSystem, pyarrow_s3: pafs.S3FileSystem) -> bool:
    """Ensure the test bucket exists using s3fs."""
    try:
        if not s3fs_fs.exists(BUCKET_NAME):
            logging.info(f"Creating bucket: {BUCKET_NAME}")
            try:
                s3fs_fs.mkdir(BUCKET_NAME)
                logging.info(f"✓ Bucket created: {BUCKET_NAME}")
            except FileExistsError:
                # Bucket was created between the check and mkdir call
                logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
        else:
            logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
        return True
    except Exception:
        logging.exception("✗ Failed to create/check bucket")
        return False


def write_with_s3fs(table: pa.Table, path: str, s3fs_fs: s3fs.S3FileSystem) -> bool:
    """Write Parquet file using s3fs filesystem."""
    try:
        pads.write_dataset(table, path, format="parquet", filesystem=s3fs_fs)
        return True
    except Exception:
        logging.exception("✗ Failed to write with s3fs")
        return False


def write_with_pyarrow_s3(table: pa.Table, path: str, pyarrow_s3: pafs.S3FileSystem) -> bool:
    """Write Parquet file using PyArrow native S3 filesystem."""
    try:
        pads.write_dataset(table, path, format="parquet", filesystem=pyarrow_s3)
        return True
    except Exception:
        logging.exception("✗ Failed to write with PyArrow S3")
        return False


def read_with_s3fs(path: str, s3fs_fs: s3fs.S3FileSystem) -> Tuple[bool, Optional[pa.Table], str]:
    """Read Parquet file using s3fs filesystem with multiple methods."""
    errors = []
    
    # Try pq.read_table
    try:
        table = pq.read_table(path, filesystem=s3fs_fs)
        return True, table, "pq.read_table"
    except Exception as e:
        errors.append(f"pq.read_table: {type(e).__name__}: {e}")
    
    # Try pq.ParquetDataset
    try:
        dataset = pq.ParquetDataset(path, filesystem=s3fs_fs)
        table = dataset.read()
        return True, table, "pq.ParquetDataset"
    except Exception as e:
        errors.append(f"pq.ParquetDataset: {type(e).__name__}: {e}")
    
    # Try pads.dataset
    try:
        dataset = pads.dataset(path, format="parquet", filesystem=s3fs_fs)
        table = dataset.to_table()
        return True, table, "pads.dataset"
    except Exception as e:
        errors.append(f"pads.dataset: {type(e).__name__}: {e}")
    
    return False, None, " | ".join(errors)


def read_with_pyarrow_s3(path: str, pyarrow_s3: pafs.S3FileSystem) -> Tuple[bool, Optional[pa.Table], str]:
    """Read Parquet file using PyArrow native S3 filesystem with multiple methods."""
    errors = []
    
    # Try pq.read_table
    try:
        table = pq.read_table(path, filesystem=pyarrow_s3)
        return True, table, "pq.read_table"
    except Exception as e:
        errors.append(f"pq.read_table: {type(e).__name__}: {e}")
    
    # Try pq.ParquetDataset
    try:
        dataset = pq.ParquetDataset(path, filesystem=pyarrow_s3)
        table = dataset.read()
        return True, table, "pq.ParquetDataset"
    except Exception as e:
        errors.append(f"pq.ParquetDataset: {type(e).__name__}: {e}")
    
    # Try pads.dataset
    try:
        dataset = pads.dataset(path, filesystem=pyarrow_s3)
        table = dataset.to_table()
        return True, table, "pads.dataset"
    except Exception as e:
        errors.append(f"pads.dataset: {type(e).__name__}: {e}")
    
    return False, None, " | ".join(errors)


def verify_table_integrity(original: pa.Table, read: pa.Table) -> Tuple[bool, str]:
    """Verify that read table matches the original table."""
    # Check row count
    if read.num_rows != original.num_rows:
        return False, f"Row count mismatch: expected {original.num_rows}, got {read.num_rows}"
    
    # Check schema
    if not read.schema.equals(original.schema):
        return False, f"Schema mismatch: expected {original.schema}, got {read.schema}"
    
    # Sort both tables by 'id' column before comparison to handle potential row order differences
    original_sorted = original.sort_by([('id', 'ascending')])
    read_sorted = read.sort_by([('id', 'ascending')])
    
    # Check data equality
    if not read_sorted.equals(original_sorted):
        # Provide detailed error information
        error_details = []
        for col_name in original.column_names:
            col_original = original_sorted.column(col_name)
            col_read = read_sorted.column(col_name)
            if not col_original.equals(col_read):
                error_details.append(f"column '{col_name}' differs")
        return False, f"Data mismatch: {', '.join(error_details)}"
    
    return True, "Data verified successfully"


def test_write_s3fs_read_pyarrow(
    test_name: str,
    num_rows: int,
    s3fs_fs: s3fs.S3FileSystem,
    pyarrow_s3: pafs.S3FileSystem
) -> Tuple[bool, str]:
    """Test: Write with s3fs, read with PyArrow native S3."""
    try:
        table = create_sample_table(num_rows)
        path = f"{BUCKET_NAME}/{TEST_DIR}/{test_name}/data.parquet"
        
        # Write with s3fs
        logging.info(f"  Writing {num_rows:,} rows with s3fs to {path}...")
        if not write_with_s3fs(table, path, s3fs_fs):
            return False, "Write with s3fs failed"
        logging.info("  ✓ Write completed")
        
        # Read with PyArrow native S3
        logging.info("  Reading with PyArrow native S3...")
        success, read_table, method = read_with_pyarrow_s3(path, pyarrow_s3)
        if not success:
            return False, f"Read with PyArrow S3 failed: {method}"
        logging.info(f"  ✓ Read {read_table.num_rows:,} rows using {method}")
        
        # Verify data integrity
        verify_success, verify_msg = verify_table_integrity(table, read_table)
        if not verify_success:
            return False, f"Verification failed: {verify_msg}"
        logging.info(f"  ✓ {verify_msg}")
        
        return True, f"s3fs→PyArrow: {method}"
        
    except Exception as e:
        logging.exception("  ✗ Test failed")
        return False, f"{type(e).__name__}: {e}"


def test_write_pyarrow_read_s3fs(
    test_name: str,
    num_rows: int,
    s3fs_fs: s3fs.S3FileSystem,
    pyarrow_s3: pafs.S3FileSystem
) -> Tuple[bool, str]:
    """Test: Write with PyArrow native S3, read with s3fs."""
    try:
        table = create_sample_table(num_rows)
        path = f"{BUCKET_NAME}/{TEST_DIR}/{test_name}/data.parquet"
        
        # Write with PyArrow native S3
        logging.info(f"  Writing {num_rows:,} rows with PyArrow native S3 to {path}...")
        if not write_with_pyarrow_s3(table, path, pyarrow_s3):
            return False, "Write with PyArrow S3 failed"
        logging.info("  ✓ Write completed")
        
        # Read with s3fs
        logging.info("  Reading with s3fs...")
        success, read_table, method = read_with_s3fs(path, s3fs_fs)
        if not success:
            return False, f"Read with s3fs failed: {method}"
        logging.info(f"  ✓ Read {read_table.num_rows:,} rows using {method}")
        
        # Verify data integrity
        verify_success, verify_msg = verify_table_integrity(table, read_table)
        if not verify_success:
            return False, f"Verification failed: {verify_msg}"
        logging.info(f"  ✓ {verify_msg}")
        
        return True, f"PyArrow→s3fs: {method}"
        
    except Exception as e:
        logging.exception("  ✗ Test failed")
        return False, f"{type(e).__name__}: {e}"


def cleanup_test_files(s3fs_fs: s3fs.S3FileSystem) -> None:
    """Clean up test files from S3."""
    try:
        test_path = f"{BUCKET_NAME}/{TEST_DIR}"
        if s3fs_fs.exists(test_path):
            logging.info(f"Cleaning up test directory: {test_path}")
            s3fs_fs.rm(test_path, recursive=True)
            logging.info("✓ Test directory cleaned up")
    except Exception:
        logging.exception("Failed to cleanup test directory")


def main():
    """Run cross-filesystem compatibility tests."""
    print("=" * 80)
    print("Cross-Filesystem Compatibility Tests for PyArrow Parquet")
    print("Testing: s3fs ↔ PyArrow Native S3 Filesystem")
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

    # Initialize both filesystems
    s3fs_fs = init_s3fs()
    if s3fs_fs is None:
        print("Cannot proceed without s3fs connection")
        return 1

    pyarrow_s3, scheme, endpoint = init_pyarrow_s3()
    if pyarrow_s3 is None:
        print("Cannot proceed without PyArrow S3 connection")
        return 1

    print()

    # Ensure bucket exists
    if not ensure_bucket_exists(s3fs_fs, pyarrow_s3):
        print("Cannot proceed without bucket")
        return 1

    print()

    results = []

    # Test all file sizes
    for size_name, num_rows in TEST_SIZES.items():
        print(f"\n{'='*80}")
        print(f"Testing with {size_name} files ({num_rows:,} rows)")
        print(f"{'='*80}\n")

        # Test 1: Write with s3fs, read with PyArrow native S3
        test_name = f"{size_name}_s3fs_to_pyarrow"
        print(f"Test: Write with s3fs → Read with PyArrow native S3")
        success, message = test_write_s3fs_read_pyarrow(
            test_name, num_rows, s3fs_fs, pyarrow_s3
        )
        results.append((test_name, success, message))
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {message}\n")

        # Test 2: Write with PyArrow native S3, read with s3fs
        test_name = f"{size_name}_pyarrow_to_s3fs"
        print(f"Test: Write with PyArrow native S3 → Read with s3fs")
        success, message = test_write_pyarrow_read_s3fs(
            test_name, num_rows, s3fs_fs, pyarrow_s3
        )
        results.append((test_name, success, message))
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {message}\n")

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
        print("✓ ALL CROSS-FILESYSTEM TESTS PASSED!")
        print("\nConclusion: Files written with s3fs and PyArrow native S3 are")
        print("fully compatible and can be read by either filesystem implementation.")
    else:
        print(f"✗ {total - passed} test(s) failed")

    print("=" * 80 + "\n")

    # Cleanup
    cleanup_test_files(s3fs_fs)

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

