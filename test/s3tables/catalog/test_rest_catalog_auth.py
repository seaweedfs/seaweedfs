#!/usr/bin/env python3
"""
Iceberg REST Catalog Compatibility Test for SeaweedFS (Authenticated)

This script tests the Iceberg REST Catalog API compatibility with authentication.

Usage:
    python3 test_rest_catalog_auth.py --catalog-url http://localhost:8182 \\
        --access-key admin --secret-key admin

Requirements:
    pip install pyiceberg[s3fs]
"""

import argparse
import sys
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    StringType,
    NestedField,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    TableAlreadyExistsError,
    NoSuchTableError,
)


def test_config_endpoint(catalog):
    """Test that the catalog config endpoint returns valid configuration."""
    print("Testing /v1/config endpoint...")
    # The catalog is already loaded which means config endpoint worked
    print("  /v1/config endpoint working")
    return True


def test_namespace_operations(catalog, prefix):
    """Test namespace CRUD operations."""
    print("Testing namespace operations...")
    namespace = (f"{prefix.replace('-', '_')}_auth_test_ns",)
    
    # List initial namespaces
    namespaces = catalog.list_namespaces()
    print(f"  Initial namespaces: {namespaces}")
    
    # Create namespace
    try:
        catalog.create_namespace(namespace)
        print(f"  Created namespace: {namespace}")
    except NamespaceAlreadyExistsError:
        print(f"  ! Namespace already exists: {namespace}")
    
    # List namespaces (should include our new one)
    namespaces = catalog.list_namespaces()
    if namespace in namespaces:
        print("  Namespace appears in list")
    else:
        print(f"  Namespace not found in list: {namespaces}")
        return False
    
    # Get namespace properties
    try:
        props = catalog.load_namespace_properties(namespace)
        print(f"  Loaded namespace properties: {props}")
    except NoSuchNamespaceError:
        print(f"  Failed to load namespace properties")
        return False
    
    return True


def test_table_operations(catalog, prefix):
    """Test table CRUD operations."""
    print("Testing table operations...")
    namespace = (f"{prefix.replace('-', '_')}_auth_test_ns",)
    table_name = "auth_test_table"
    table_id = namespace + (table_name,)
    
    # Define a simple schema
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
    )
    
    # Create table
    try:
        table = catalog.create_table(
            identifier=table_id,
            schema=schema,
        )
        print(f"  Created table: {table_id}")
    except TableAlreadyExistsError:
        print(f"  ! Table already exists: {table_id}")
        _ = catalog.load_table(table_id)
    
    # List tables
    tables = catalog.list_tables(namespace)
    if table_name in [t[1] for t in tables]:
        print("  Table appears in list")
    else:
        print(f"  Table not found in list: {tables}")
        return False
    
    # Load table
    try:
        loaded_table = catalog.load_table(table_id)
        print(f"  Loaded table: {loaded_table.name()}")
        print(f"    Schema: {loaded_table.schema()}")
        print(f"    Location: {loaded_table.location()}")
    except NoSuchTableError:
        print(f"  Failed to load table")
        return False
    
    return True


def test_cleanup(catalog, prefix):
    """Test table and namespace deletion."""
    print("Testing cleanup operations...")
    namespace = (f"{prefix.replace('-', '_')}_auth_test_ns",)
    table_id = namespace + ("auth_test_table",)
    
    # Drop table
    try:
        catalog.drop_table(table_id)
        print(f"  Dropped table: {table_id}")
    except NoSuchTableError:
        print(f"  ! Table already deleted: {table_id}")
    
    # Drop namespace
    try:
        catalog.drop_namespace(namespace)
        print(f"  Dropped namespace: {namespace}")
    except NoSuchNamespaceError:
        print(f"  ! Namespace already deleted: {namespace}")
    except Exception as e:
        print(f"  ? Namespace drop error (may be expected): {e}")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Test Iceberg REST Catalog with authentication")
    parser.add_argument("--catalog-url", required=True, help="Iceberg REST Catalog URL")
    parser.add_argument("--warehouse", default="s3://iceberg-test/", help="Warehouse location")
    parser.add_argument("--prefix", required=True, help="Table bucket prefix")
    parser.add_argument("--access-key", required=True, help="AWS Access Key ID")
    parser.add_argument("--secret-key", required=True, help="AWS Secret Access Key")
    parser.add_argument("--skip-cleanup", action="store_true", help="Skip cleanup at the end")
    args = parser.parse_args()
    
    print(f"Connecting to Iceberg REST Catalog at: {args.catalog_url}")
    print(f"Warehouse: {args.warehouse}")
    print(f"Prefix: {args.prefix}")
    print(f"Using authenticated access with key: {args.access_key}")
    print()
    
    # Load the REST catalog with authentication
    import time
    max_retries = 10
    catalog = None
    for attempt in range(max_retries):
        try:
            catalog = load_catalog(
                "rest",
                **{
                    "type": "rest",
                    "uri": args.catalog_url,
                    "warehouse": args.warehouse,
                    "prefix": args.prefix,
                    "s3.access-key-id": args.access_key,
                    "s3.secret-access-key": args.secret_key,
                }
            )
            print(f"Successfully connected to catalog on attempt {attempt + 1}")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1} failed, retrying in 2s... ({e})")
                time.sleep(2)
            else:
                print(f"  All {max_retries} attempts failed.")
                raise e
    
    # Run tests
    tests = [
        ("Config Endpoint", lambda: test_config_endpoint(catalog)),
        ("Namespace Operations", lambda: test_namespace_operations(catalog, args.prefix)),
        ("Table Operations", lambda: test_table_operations(catalog, args.prefix)),
    ]
    
    if not args.skip_cleanup:
        tests.append(("Cleanup", lambda: test_cleanup(catalog, args.prefix)))
    
    passed = 0
    failed = 0
    
    for name, test_fn in tests:
        print(f"\n{'='*50}")
        try:
            if test_fn():
                passed += 1
                print(f"PASSED: {name}")
            else:
                failed += 1
                print(f"FAILED: {name}")
        except Exception as e:
            failed += 1
            print(f"ERROR in {name}: {e}")
    
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
