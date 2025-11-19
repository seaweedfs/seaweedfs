#!/usr/bin/env python3
"""
Test script to verify the implicit directory fix for s3fs compatibility.

This test verifies that:
1. Implicit directory markers (0-byte objects with children) return 404 on HEAD
2. s3fs correctly identifies them as directories via LIST fallback
3. PyArrow can read datasets created with write_dataset()

The fix makes SeaweedFS behave like AWS S3 and improves s3fs compatibility.
"""

import io
import logging
import os
import sys
import traceback

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import s3fs
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "some_access_key1")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "some_secret_key1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test-implicit-dir")

def create_sample_table(num_rows: int = 1000) -> pa.Table:
    """Create a sample PyArrow table."""
    return pa.table({
        'id': pa.array(range(num_rows), type=pa.int64()),
        'value': pa.array([f'value_{i}' for i in range(num_rows)], type=pa.string()),
        'score': pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
    })

def setup_s3():
    """Set up S3 clients."""
    # s3fs client
    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL},
        use_ssl=False
    )
    
    # boto3 client for raw S3 operations
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        use_ssl=False
    )
    
    return fs, s3_client

def test_implicit_directory_head_behavior(fs, s3_client):
    """Test that HEAD on implicit directory markers returns 404."""
    logger.info("\n" + "="*80)
    logger.info("TEST 1: Implicit Directory HEAD Behavior")
    logger.info("="*80)
    
    test_path = f"{BUCKET_NAME}/test_implicit_dir"
    
    # Clean up any existing data
    try:
        fs.rm(test_path, recursive=True)
    except:
        pass
    
    # Create a dataset using PyArrow (creates implicit directory)
    logger.info(f"Creating dataset at: {test_path}")
    table = create_sample_table(1000)
    pads.write_dataset(table, test_path, filesystem=fs, format='parquet')
    
    # List what was created
    logger.info("\nFiles created:")
    files = fs.ls(test_path, detail=True)
    for f in files:
        logger.info(f"  {f['name']} - size: {f['size']} bytes, type: {f['type']}")
    
    # Test HEAD request on the directory marker (without trailing slash)
    logger.info(f"\nTesting HEAD on: {test_path}")
    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key='test_implicit_dir')
        logger.info(f"  HEAD response: {response['ResponseMetadata']['HTTPStatusCode']}")
        logger.info(f"  Content-Length: {response.get('ContentLength', 'N/A')}")
        logger.info(f"  Content-Type: {response.get('ContentType', 'N/A')}")
        logger.warning("  ⚠️  Expected 404, but got 200 - fix may not be working")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info("  ✓ HEAD returned 404 (expected - implicit directory)")
            return True
        else:
            logger.error(f"  ✗ Unexpected error: {e}")
            return False

def test_s3fs_directory_detection(fs):
    """Test that s3fs correctly detects the directory."""
    logger.info("\n" + "="*80)
    logger.info("TEST 2: s3fs Directory Detection")
    logger.info("="*80)
    
    test_path = f"{BUCKET_NAME}/test_implicit_dir"
    
    # Test s3fs.info()
    logger.info(f"\nTesting s3fs.info('{test_path}'):")
    try:
        info = fs.info(test_path)
        logger.info(f"  Type: {info.get('type', 'N/A')}")
        logger.info(f"  Size: {info.get('size', 'N/A')}")
        
        if info.get('type') == 'directory':
            logger.info("  ✓ s3fs correctly identified as directory")
            return True
        else:
            logger.warning(f"  ⚠️  s3fs identified as: {info.get('type')}")
            return False
    except Exception as e:
        logger.error(f"  ✗ Error: {e}")
        return False

def test_s3fs_isdir(fs):
    """Test that s3fs.isdir() works correctly."""
    logger.info("\n" + "="*80)
    logger.info("TEST 3: s3fs.isdir() Method")
    logger.info("="*80)
    
    test_path = f"{BUCKET_NAME}/test_implicit_dir"
    
    logger.info(f"\nTesting s3fs.isdir('{test_path}'):")
    try:
        is_dir = fs.isdir(test_path)
        logger.info(f"  Result: {is_dir}")
        
        if is_dir:
            logger.info("  ✓ s3fs.isdir() correctly returned True")
            return True
        else:
            logger.warning("  ⚠️  s3fs.isdir() returned False")
            return False
    except Exception as e:
        logger.error(f"  ✗ Error: {e}")
        return False

def test_pyarrow_dataset_read(fs):
    """Test that PyArrow can read the dataset."""
    logger.info("\n" + "="*80)
    logger.info("TEST 4: PyArrow Dataset Read")
    logger.info("="*80)
    
    test_path = f"{BUCKET_NAME}/test_implicit_dir"
    
    logger.info(f"\nReading dataset from: {test_path}")
    try:
        ds = pads.dataset(test_path, filesystem=fs, format='parquet')
        table = ds.to_table()
        logger.info(f"  ✓ Successfully read {len(table)} rows")
        logger.info(f"  Columns: {table.column_names}")
        return True
    except Exception as e:
        logger.error(f"  ✗ Failed to read dataset: {e}")
        traceback.print_exc()
        return False

def test_explicit_directory_marker(fs, s3_client):
    """Test that explicit directory markers (with trailing slash) still work."""
    logger.info("\n" + "="*80)
    logger.info("TEST 5: Explicit Directory Marker (with trailing slash)")
    logger.info("="*80)
    
    # Create an explicit directory marker
    logger.info(f"\nCreating explicit directory: {BUCKET_NAME}/explicit_dir/")
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='explicit_dir/',
            Body=b'',
            ContentType='httpd/unix-directory'
        )
        logger.info("  ✓ Created explicit directory marker")
    except Exception as e:
        logger.error(f"  ✗ Failed to create: {e}")
        return False
    
    # Test HEAD with trailing slash
    logger.info(f"\nTesting HEAD on: {BUCKET_NAME}/explicit_dir/")
    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key='explicit_dir/')
        logger.info(f"  ✓ HEAD returned 200 (expected for explicit directory)")
        logger.info(f"  Content-Type: {response.get('ContentType', 'N/A')}")
        return True
    except ClientError as e:
        logger.error(f"  ✗ HEAD failed: {e}")
        return False

def test_empty_file_not_directory(fs, s3_client):
    """Test that legitimate empty files are not treated as directories."""
    logger.info("\n" + "="*80)
    logger.info("TEST 6: Empty File (not a directory)")
    logger.info("="*80)
    
    # Create an empty file with text/plain mime type
    logger.info(f"\nCreating empty file: {BUCKET_NAME}/empty.txt")
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='empty.txt',
            Body=b'',
            ContentType='text/plain'
        )
        logger.info("  ✓ Created empty file")
    except Exception as e:
        logger.error(f"  ✗ Failed to create: {e}")
        return False
    
    # Test HEAD
    logger.info(f"\nTesting HEAD on: {BUCKET_NAME}/empty.txt")
    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key='empty.txt')
        logger.info(f"  ✓ HEAD returned 200 (expected for empty file)")
        logger.info(f"  Content-Type: {response.get('ContentType', 'N/A')}")
        
        # Verify s3fs doesn't think it's a directory
        info = fs.info(f"{BUCKET_NAME}/empty.txt")
        if info.get('type') == 'file':
            logger.info("  ✓ s3fs correctly identified as file")
            return True
        else:
            logger.warning(f"  ⚠️  s3fs identified as: {info.get('type')}")
            return False
    except Exception as e:
        logger.error(f"  ✗ Error: {e}")
        return False

def main():
    """Run all tests."""
    logger.info("="*80)
    logger.info("Implicit Directory Fix Test Suite")
    logger.info("="*80)
    logger.info(f"Endpoint: {S3_ENDPOINT_URL}")
    logger.info(f"Bucket: {BUCKET_NAME}")
    logger.info("="*80)
    
    # Set up S3 clients
    fs, s3_client = setup_s3()
    
    # Create bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"\n✓ Created bucket: {BUCKET_NAME}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
            logger.info(f"\n✓ Bucket already exists: {BUCKET_NAME}")
        else:
            logger.error(f"\n✗ Failed to create bucket: {e}")
            return 1
    
    # Run tests
    results = []
    
    results.append(("Implicit Directory HEAD", test_implicit_directory_head_behavior(fs, s3_client)))
    results.append(("s3fs Directory Detection", test_s3fs_directory_detection(fs)))
    results.append(("s3fs.isdir() Method", test_s3fs_isdir(fs)))
    results.append(("PyArrow Dataset Read", test_pyarrow_dataset_read(fs)))
    results.append(("Explicit Directory Marker", test_explicit_directory_marker(fs, s3_client)))
    results.append(("Empty File Not Directory", test_empty_file_not_directory(fs, s3_client)))
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {name}")
    
    logger.info("="*80)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("="*80)
    
    if passed == total:
        logger.info("\n🎉 All tests passed! The implicit directory fix is working correctly.")
        return 0
    else:
        logger.warning(f"\n⚠️  {total - passed} test(s) failed. The fix may not be fully working.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

