#!/usr/bin/env python3
"""
Test script for S3-compatible storage with PyArrow Parquet files.

This script tests different write methods (PyArrow write_dataset vs. pq.write_table to buffer)
combined with different read methods (PyArrow dataset, direct s3fs read, buffered read) to
identify which combinations work with large files that span multiple row groups.

This test specifically addresses issues with large tables using PyArrow where files span
multiple row-groups (default row_group size is around 130,000 rows).

Requirements:
    - pyarrow>=22
    - s3fs>=2024.12.0

Environment Variables:
    S3_ENDPOINT_URL: S3 endpoint (default: http://localhost:8333)
    S3_ACCESS_KEY: S3 access key (default: some_access_key1)
    S3_SECRET_KEY: S3 secret key (default: some_secret_key1)
    BUCKET_NAME: S3 bucket name (default: test-parquet-bucket)
    TEST_QUICK: Run only small/quick tests (default: 0, set to 1 for quick mode)

Usage:
    # Run with default environment variables
    python3 s3_parquet_test.py

    # Run with custom environment variables
    S3_ENDPOINT_URL=http://localhost:8333 \
    S3_ACCESS_KEY=mykey \
    S3_SECRET_KEY=mysecret \
    BUCKET_NAME=mybucket \
    python3 s3_parquet_test.py
"""

import io
import logging
import os
import secrets
import sys
import traceback
from datetime import datetime
from typing import Tuple

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq

try:
    import s3fs
except ImportError:
    logging.error("s3fs not installed. Install with: pip install s3fs")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Error log file
ERROR_LOG_FILE = f"s3_parquet_test_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configuration from environment variables with defaults
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "some_secret_key1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-parquet-bucket")
TEST_QUICK = os.getenv("TEST_QUICK", "0") == "1"

# Create randomized test directory
TEST_RUN_ID = secrets.token_hex(8)
TEST_DIR = f"{BUCKET_NAME}/parquet-tests/{TEST_RUN_ID}"

# Test file sizes
TEST_SIZES = {
    "small": 5,
    "large": 200_000,  # This will create multiple row groups
}

# Filter to only small tests if quick mode is enabled
if TEST_QUICK:
    TEST_SIZES = {"small": TEST_SIZES["small"]}
    logging.info("Quick test mode enabled - running only small tests")


def create_sample_table(num_rows: int = 5) -> pa.Table:
    """Create a sample PyArrow table for testing."""
    return pa.table({
        "id": pa.array(range(num_rows), type=pa.int64()),
        "name": pa.array([f"user_{i}" for i in range(num_rows)], type=pa.string()),
        "value": pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
        "flag": pa.array([i % 2 == 0 for i in range(num_rows)], type=pa.bool_()),
    })


def log_error(operation: str, short_msg: str) -> None:
    """Log error details to file with full traceback."""
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Operation: {operation}\n")
        f.write(f"Time: {datetime.now().isoformat()}\n")
        f.write(f"Message: {short_msg}\n")
        f.write("Full Traceback:\n")
        f.write(traceback.format_exc())
        f.write(f"{'='*80}\n")


def init_s3fs() -> s3fs.S3FileSystem:
    """Initialize and return S3FileSystem."""
    logging.info("Initializing S3FileSystem...")
    logging.info(f"  Endpoint: {S3_ENDPOINT_URL}")
    logging.info(f"  Bucket: {BUCKET_NAME}")
    try:
        fs = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": S3_ENDPOINT_URL},
            key=S3_ACCESS_KEY,
            secret=S3_SECRET_KEY,
            use_listings_cache=False,
        )
        logging.info("✓ S3FileSystem initialized successfully\n")
        return fs
    except Exception:
        logging.exception("✗ Failed to initialize S3FileSystem")
        raise


def ensure_bucket_exists(fs: s3fs.S3FileSystem) -> None:
    """Ensure the test bucket exists."""
    try:
        if not fs.exists(BUCKET_NAME):
            logging.info(f"Creating bucket: {BUCKET_NAME}")
            fs.mkdir(BUCKET_NAME)
            logging.info(f"✓ Bucket created: {BUCKET_NAME}")
        else:
            logging.info(f"✓ Bucket exists: {BUCKET_NAME}")
    except Exception:
        logging.exception("✗ Failed to create/check bucket")
        raise


# Write Methods

def write_with_pads(table: pa.Table, path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str]:
    """Write using pads.write_dataset with filesystem parameter."""
    try:
        pads.write_dataset(table, path, format="parquet", filesystem=fs)
        return True, "pads.write_dataset"
    except Exception as e:
        error_msg = f"pads.write_dataset: {type(e).__name__}"
        log_error("write_with_pads", error_msg)
        return False, error_msg


def write_with_buffer_and_s3fs(table: pa.Table, path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str]:
    """Write using pq.write_table to buffer, then upload via s3fs."""
    try:
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        with fs.open(path, "wb") as f:
            f.write(buffer.read())
        return True, "pq.write_table+s3fs.open"
    except Exception as e:
        error_msg = f"pq.write_table+s3fs.open: {type(e).__name__}"
        log_error("write_with_buffer_and_s3fs", error_msg)
        return False, error_msg


# Read Methods

def get_parquet_files(path: str, fs: s3fs.S3FileSystem) -> list:
    """
    Helper to discover all parquet files for a given path.
    
    Args:
        path: S3 path (file or directory)
        fs: S3FileSystem instance
        
    Returns:
        List of parquet file paths
        
    Raises:
        ValueError: If no parquet files are found in a directory
    """
    if fs.isdir(path):
        # Find all parquet files in the directory
        files = [f for f in fs.ls(path) if f.endswith('.parquet')]
        if not files:
            raise ValueError(f"No parquet files found in directory: {path}")
        return files
    else:
        # Single file path
        return [path]


def read_with_pads_dataset(path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str, int]:
    """Read using pads.dataset - handles both single files and directories."""
    try:
        # pads.dataset() should auto-discover parquet files in the directory
        dataset = pads.dataset(path, format="parquet", filesystem=fs)
        result = dataset.to_table()
        return True, "pads.dataset", result.num_rows
    except Exception as e:
        error_msg = f"pads.dataset: {type(e).__name__}"
        log_error("read_with_pads_dataset", error_msg)
        return False, error_msg, 0


def read_direct_s3fs(path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str, int]:
    """Read directly via s3fs.open() streaming."""
    try:
        # Get all parquet files (handles both single file and directory)
        parquet_files = get_parquet_files(path, fs)
        
        # Read all parquet files and concatenate them
        tables = []
        for file_path in parquet_files:
            with fs.open(file_path, "rb") as f:
                table = pq.read_table(f)
                tables.append(table)
        
        # Concatenate all tables into one
        if len(tables) == 1:
            result = tables[0]
        else:
            result = pa.concat_tables(tables)
        
        return True, "s3fs.open+pq.read_table", result.num_rows
    except Exception as e:
        error_msg = f"s3fs.open+pq.read_table: {type(e).__name__}"
        log_error("read_direct_s3fs", error_msg)
        return False, error_msg, 0


def read_buffered_s3fs(path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str, int]:
    """Read via s3fs.open() into buffer, then pq.read_table."""
    try:
        # Get all parquet files (handles both single file and directory)
        parquet_files = get_parquet_files(path, fs)
        
        # Read all parquet files and concatenate them
        tables = []
        for file_path in parquet_files:
            with fs.open(file_path, "rb") as f:
                buffer = io.BytesIO(f.read())
            buffer.seek(0)
            table = pq.read_table(buffer)
            tables.append(table)
        
        # Concatenate all tables into one
        if len(tables) == 1:
            result = tables[0]
        else:
            result = pa.concat_tables(tables)
        
        return True, "s3fs.open+BytesIO+pq.read_table", result.num_rows
    except Exception as e:
        error_msg = f"s3fs.open+BytesIO+pq.read_table: {type(e).__name__}"
        log_error("read_buffered_s3fs", error_msg)
        return False, error_msg, 0


def read_with_parquet_dataset(path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str, int]:
    """Read using pq.ParquetDataset - designed for directories."""
    try:
        # ParquetDataset is specifically designed to handle directories
        dataset = pq.ParquetDataset(path, filesystem=fs)
        result = dataset.read()
        return True, "pq.ParquetDataset", result.num_rows
    except Exception as e:
        error_msg = f"pq.ParquetDataset: {type(e).__name__}"
        log_error("read_with_parquet_dataset", error_msg)
        return False, error_msg, 0


def read_with_pq_read_table(path: str, fs: s3fs.S3FileSystem) -> Tuple[bool, str, int]:
    """Read using pq.read_table with filesystem parameter."""
    try:
        # pq.read_table() with filesystem should handle directories
        result = pq.read_table(path, filesystem=fs)
        return True, "pq.read_table+filesystem", result.num_rows
    except Exception as e:
        error_msg = f"pq.read_table+filesystem: {type(e).__name__}"
        log_error("read_with_pq_read_table", error_msg)
        return False, error_msg, 0


def test_combination(
    fs: s3fs.S3FileSystem,
    test_name: str,
    write_func,
    read_func,
    num_rows: int,
) -> Tuple[bool, str]:
    """Test a specific write/read combination."""
    table = create_sample_table(num_rows=num_rows)
    path = f"{TEST_DIR}/{test_name}/data.parquet"

    # Write
    write_ok, write_msg = write_func(table, path, fs)
    if not write_ok:
        return False, f"WRITE_FAIL: {write_msg}"

    # Read
    read_ok, read_msg, rows_read = read_func(path, fs)
    if not read_ok:
        return False, f"READ_FAIL: {read_msg}"

    # Verify
    if rows_read != num_rows:
        return False, f"DATA_MISMATCH: expected {num_rows}, got {rows_read}"

    return True, f"{write_msg} + {read_msg}"


def cleanup_test_files(fs: s3fs.S3FileSystem) -> None:
    """Clean up test files from S3."""
    try:
        if fs.exists(TEST_DIR):
            logging.info(f"Cleaning up test directory: {TEST_DIR}")
            fs.rm(TEST_DIR, recursive=True)
            logging.info("✓ Test directory cleaned up")
    except Exception as e:
        logging.warning(f"Failed to cleanup test directory: {e}")


def main():
    """Run all write/read method combinations."""
    print("=" * 80)
    print("Write/Read Method Combination Tests for S3-Compatible Storage")
    print("Testing PyArrow Parquet Files with Multiple Row Groups")
    if TEST_QUICK:
        print("*** QUICK TEST MODE - Small files only ***")
    print("=" * 80 + "\n")

    print("Configuration:")
    print(f"  S3 Endpoint: {S3_ENDPOINT_URL}")
    print(f"  Bucket: {BUCKET_NAME}")
    print(f"  Test Directory: {TEST_DIR}")
    print(f"  Quick Mode: {'Yes (small files only)' if TEST_QUICK else 'No (all file sizes)'}")
    print()

    try:
        fs = init_s3fs()
        ensure_bucket_exists(fs)
    except Exception as e:
        print(f"Cannot proceed without S3 connection: {e}")
        return 1

    # Define all write methods
    write_methods = [
        ("pads", write_with_pads),
        ("buffer+s3fs", write_with_buffer_and_s3fs),
    ]

    # Define all read methods
    read_methods = [
        ("pads.dataset", read_with_pads_dataset),
        ("pq.ParquetDataset", read_with_parquet_dataset),
        ("pq.read_table", read_with_pq_read_table),
        ("s3fs+direct", read_direct_s3fs),
        ("s3fs+buffered", read_buffered_s3fs),
    ]

    results = []

    # Test all combinations for each file size
    for size_name, num_rows in TEST_SIZES.items():
        print(f"\n{'='*80}")
        print(f"Testing with {size_name} files ({num_rows:,} rows)")
        print(f"{'='*80}\n")
        print(f"{'Write Method':<20} | {'Read Method':<20} | {'Result':<40}")
        print("-" * 85)

        for write_name, write_func in write_methods:
            for read_name, read_func in read_methods:
                test_name = f"{size_name}_{write_name}_{read_name}"
                success, message = test_combination(
                    fs, test_name, write_func, read_func, num_rows
                )
                results.append((test_name, success, message))
                status = "✓ PASS" if success else "✗ FAIL"
                print(f"{write_name:<20} | {read_name:<20} | {status}: {message[:35]}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    print(f"\nTotal: {passed}/{total} passed\n")

    # Group results by file size
    for size_name in TEST_SIZES.keys():
        size_results = [r for r in results if size_name in r[0]]
        size_passed = sum(1 for _, success, _ in size_results if success)
        print(f"{size_name.upper()}: {size_passed}/{len(size_results)} passed")

    print("\n" + "=" * 80)
    if passed == total:
        print("✓ ALL TESTS PASSED!")
    else:
        print(f"✗ {total - passed} test(s) failed")
        print("\nFailing combinations:")
        for name, success, message in results:
            if not success:
                parts = name.split("_")
                size = parts[0]
                write = parts[1]
                read = "_".join(parts[2:])
                print(f"  - {size:6} | {write:15} | {read:20} -> {message[:50]}")

    print("=" * 80 + "\n")
    print(f"Error details logged to: {ERROR_LOG_FILE}")
    print("=" * 80 + "\n")

    # Cleanup
    cleanup_test_files(fs)

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

