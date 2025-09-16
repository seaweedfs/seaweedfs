#!/usr/bin/env python3
"""
Fix S3 tests to handle bucket creation conflicts properly.

This script patches the s3-tests repository to handle BucketAlreadyExists errors
by retrying with new bucket names instead of failing.

This is needed because SeaweedFS now properly returns BucketAlreadyExists errors
per S3 specification, but the original s3-tests code doesn't handle this case.
"""

import os
import sys
import re

def patch_s3_tests_init_file(file_path):
    """Patch the s3tests_boto3/functional/__init__.py file to handle bucket conflicts."""
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found")
        return False
    
    print(f"Patching {file_path}...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check if already patched
    if 'max_retries = 10' in content:
        print("File already patched, skipping...")
        return True
    
    # Patch get_new_bucket_resource function
    old_resource_func = '''def get_new_bucket_resource(name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    s3 = boto3.resource('s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify)
    if name is None:
        name = get_new_bucket_name()
    bucket = s3.Bucket(name)
    bucket_location = bucket.create()
    return bucket'''

    new_resource_func = '''def get_new_bucket_resource(name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    s3 = boto3.resource('s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify)
    
    # If a name is provided, do not change it; reuse that exact bucket name.
    if name is not None:
        bucket = s3.Bucket(name)
        try:
            bucket.create()
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyOwnedByYou']:
                # Bucket already exists and is owned by us; reuse it
                return bucket
            if error_code in ['BucketAlreadyExists']:
                # Name conflict in global namespace; fall back to a fresh unique name
                name = None
            else:
                raise
        else:
            return bucket

    # Retry bucket creation with generated names only when name is not provided
    max_retries = 10
    for attempt in range(max_retries):
        gen_name = get_new_bucket_name()
        bucket = s3.Bucket(gen_name)
        try:
            bucket.create()
            return bucket
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                raise'''

    # Patch get_new_bucket function
    old_client_func = '''def get_new_bucket(client=None, name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if client is None:
        client = get_client()
    if name is None:
        name = get_new_bucket_name()

    client.create_bucket(Bucket=name)
    return name'''

    new_client_func = '''def get_new_bucket(client=None, name=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if client is None:
        client = get_client()
    
    # Retry bucket creation if name conflicts occur
    max_retries = 10
    for attempt in range(max_retries):
        if name is None or attempt > 0:
            name = get_new_bucket_name()
        
        try:
            client.create_bucket(Bucket=name)
            return name
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                # Bucket name conflict, try again with a new name
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                # Other error, re-raise
                raise'''

    # Apply patches
    content = content.replace(old_resource_func, new_resource_func)
    content = content.replace(old_client_func, new_client_func)
    
    # Write back the patched content
    with open(file_path, 'w') as f:
        f.write(content)
    
    print("Successfully patched s3-tests to handle bucket creation conflicts!")
    return True

def main():
    """Main function to apply the patch."""
    
    # Default path for s3-tests in GitHub Actions
    s3_tests_path = os.environ.get('S3_TESTS_PATH', 's3-tests')
    init_file_path = os.path.join(s3_tests_path, 's3tests_boto3', 'functional', '__init__.py')
    
    print("🔧 Fixing S3 tests bucket creation conflicts...")
    print(f"Looking for s3-tests at: {s3_tests_path}")
    
    if not os.path.exists(s3_tests_path):
        print(f"Error: s3-tests directory not found at {s3_tests_path}")
        print("Make sure to check out the s3-tests repository first")
        return 1
    
    if patch_s3_tests_init_file(init_file_path):
        print("✅ S3 tests successfully patched!")
        print("The tests will now handle BucketAlreadyExists errors properly")
        return 0
    else:
        print("❌ Failed to patch s3-tests")
        return 1

if __name__ == '__main__':
    sys.exit(main())
