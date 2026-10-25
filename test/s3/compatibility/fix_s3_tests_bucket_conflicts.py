#!/usr/bin/env python3
"""
Patch Ceph s3-tests helpers to avoid bucket name mismatches and make bucket
creation idempotent when a fixed bucket name is provided.

Why:
- Some tests call get_new_bucket() to get a name, then call
  get_new_bucket_resource(name=<that name>) which unconditionally calls
  CreateBucket again. If the bucket already exists, boto3 raises a
  ClientError. We want to treat that as idempotent and reuse the bucket.
- We must NOT silently generate a different bucket name when a name is
  explicitly provided, otherwise subsequent test steps still reference the
  original string and read from the wrong (empty) bucket.

What this does:
- get_new_bucket_resource(name=...):
  - Try to create the exact bucket name.
  - If error code is BucketAlreadyOwnedByYou OR BucketAlreadyExists, simply
    reuse and return the bucket object for that SAME name.
  - Only when name is None, generate a new unique name with retries.
- get_new_bucket(client=None, name=None):
  - If name is None, generate unique names with retries until creation
    succeeds, and return the actual name string to the caller.

This keeps bucket names consistent across the test helper calls and prevents
404s or KeyErrors later in the tests that depend on that bucket name.
"""

import os
import sys


def patch_s3_tests_init_file(file_path: str) -> bool:
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found")
        return False

    print(f"Patching {file_path}...")
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # If already patched, skip
    if "max_retries = 10" in content and "BucketAlreadyOwnedByYou" in content and "BucketAlreadyExists" in content:
        print("Already patched. Skipping.")
        return True

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

    from botocore.exceptions import ClientError

    # If a name is provided, do not change it. Reuse that exact bucket name.
    if name is not None:
        bucket = s3.Bucket(name)
        try:
            bucket.create()
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                # Treat as idempotent create for an explicitly provided name.
                # We must not change the name or tests will read from the wrong bucket.
                return bucket
            # Other errors should surface
            raise
        else:
            return bucket

    # Only generate unique names when no name was provided
    max_retries = 10
    for attempt in range(max_retries):
        gen_name = get_new_bucket_name()
        bucket = s3.Bucket(gen_name)
        try:
            bucket.create()
            return bucket
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyExists', 'BucketAlreadyOwnedByYou'):
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                raise'''

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

    from botocore.exceptions import ClientError

    # If a name is provided, just try to create it once and fall back to idempotent reuse
    if name is not None:
        try:
            client.create_bucket(Bucket=name)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                return name
            raise
        else:
            return name

    # Otherwise, generate a unique name with retries and return the actual name string
    max_retries = 10
    for attempt in range(max_retries):
        gen_name = get_new_bucket_name()
        try:
            client.create_bucket(Bucket=gen_name)
            return gen_name
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyExists', 'BucketAlreadyOwnedByYou'):
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                raise'''

    updated = content
    updated = updated.replace(old_resource_func, new_resource_func)
    updated = updated.replace(old_client_func, new_client_func)

    if updated == content:
        print("Patterns not found; appending override implementations to end of file.")
        append_patch = '''

# --- SeaweedFS override start ---
from botocore.exceptions import ClientError as _Sw_ClientError


# Idempotent create for provided name; generate unique only when no name given
# Keep the bucket name stable when provided by the caller

def _sw_get_new_bucket_resource(name=None):
    s3 = boto3.resource('s3',
                        aws_access_key_id=config.main_access_key,
                        aws_secret_access_key=config.main_secret_key,
                        endpoint_url=config.default_endpoint,
                        use_ssl=config.default_is_secure,
                        verify=config.default_ssl_verify)
    if name is not None:
        bucket = s3.Bucket(name)
        try:
            bucket.create()
        except _Sw_ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                return bucket
            raise
        else:
            return bucket
    # name not provided: generate unique
    max_retries = 10
    for attempt in range(max_retries):
        gen_name = get_new_bucket_name()
        bucket = s3.Bucket(gen_name)
        try:
            bucket.create()
            return bucket
        except _Sw_ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyExists', 'BucketAlreadyOwnedByYou'):
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                raise


from botocore.exceptions import ClientError as _Sw2_ClientError


def _sw_get_new_bucket(client=None, name=None):
    if client is None:
        client = get_client()
    if name is not None:
        try:
            client.create_bucket(Bucket=name)
        except _Sw2_ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                return name
            raise
        else:
            return name
    max_retries = 10
    for attempt in range(max_retries):
        gen_name = get_new_bucket_name()
        try:
            client.create_bucket(Bucket=gen_name)
            return gen_name
        except _Sw2_ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            if code in ('BucketAlreadyExists', 'BucketAlreadyOwnedByYou'):
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create unique bucket after {max_retries} attempts")
                continue
            else:
                raise

# Override original helper functions
get_new_bucket_resource = _sw_get_new_bucket_resource
get_new_bucket = _sw_get_new_bucket
# --- SeaweedFS override end ---
'''
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(append_patch)
        print("Appended override implementations.")
        return True

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(updated)

    print("Successfully patched s3-tests helpers.")
    return True


def main() -> int:
    s3_tests_path = os.environ.get("S3_TESTS_PATH", "s3-tests")
    init_file_path = os.path.join(s3_tests_path, "s3tests", "functional", "__init__.py")
    print("Applying s3-tests patch for bucket creation idempotency...")
    print(f"Target repo path: {s3_tests_path}")
    if not os.path.exists(s3_tests_path):
        print(f"Warning: s3-tests directory not found at {s3_tests_path}")
        print("Skipping patch - directory structure may have changed in the upstream repository")
        return 0  # Return success to not break CI
    if not os.path.exists(init_file_path):
        print(f"Warning: Target file {init_file_path} not found")
        print("This may indicate the s3-tests repository structure has changed.")
        print("Skipping patch - tests may still work without it")
        return 0  # Return success to not break CI
    ok = patch_s3_tests_init_file(init_file_path)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())


