#!/usr/bin/env python3
"""Debug script to understand what pads.write_dataset creates."""

import sys
import pyarrow as pa
import pyarrow.dataset as pads
import s3fs

# Create a simple test table
table = pa.table({'id': [1, 2, 3], 'value': [1.0, 2.0, 3.0]})

# Initialize S3 filesystem
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'http://localhost:8333'},
    key='some_access_key1',
    secret='some_secret_key1',
    use_listings_cache=False,
)

# Create bucket
if not fs.exists('test-bucket'):
    fs.mkdir('test-bucket')

# Write with pads.write_dataset
test_path = 's3://test-bucket/test-write-simple/'
print(f"Writing to: {test_path}")
print(f"Table schema: {table.schema}")
print(f"Table rows: {table.num_rows}")

try:
    pads.write_dataset(table, test_path, format='parquet', filesystem=fs)
    print("\n✓ Write succeeded")
    
    # List all files recursively
    print(f"\nListing all files recursively under {test_path}:")
    import os
    base_path = 'test-bucket/test-write-simple'
    def list_recursive(path, indent=0):
        try:
            items = fs.ls(path, detail=False)
            for item in items:
                is_dir = fs.isdir(item)
                item_name = item.split('/')[-1] if '/' in item else item
                if is_dir:
                    print(f"{'  ' * indent}📁 {item_name}/")
                    list_recursive(item, indent + 1)
                else:
                    # Get file size
                    try:
                        info = fs.info(item)
                        size = info.get('size', 0)
                        print(f"{'  ' * indent}📄 {item_name} ({size} bytes)")
                    except:
                        print(f"{'  ' * indent}📄 {item_name}")
        except Exception as e:
            print(f"{'  ' * indent}Error listing {path}: {e}")

    list_recursive(base_path)
    
    # Try to read back with different methods
    print(f"\n\nTrying to read back using different methods:")
    
    # Method 1: pads.dataset with the same path
    print(f"\n1. pads.dataset('{test_path}'):")
    try:
        dataset = pads.dataset(test_path, format='parquet', filesystem=fs)
        result = dataset.to_table()
        print(f"   ✓ Success: {result.num_rows} rows")
    except Exception as e:
        print(f"   ✗ Failed: {e}")
    
    # Method 2: pads.dataset with the dir containing parquet files
    print(f"\n2. pads.dataset without trailing slash:")
    test_path_no_slash = 's3://test-bucket/test-write-simple'
    try:
        dataset = pads.dataset(test_path_no_slash, format='parquet', filesystem=fs)
        result = dataset.to_table()
        print(f"   ✓ Success: {result.num_rows} rows")
    except Exception as e:
        print(f"   ✗ Failed: {e}")

except Exception as e:
    import traceback
    print(f"✗ Error: {e}")
    traceback.print_exc()
    sys.exit(1)
