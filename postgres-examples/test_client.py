#!/usr/bin/env python3
"""
Test client for SeaweedFS PostgreSQL protocol support.

This script demonstrates how to connect to SeaweedFS using standard PostgreSQL
libraries and execute various types of queries.

Requirements:
    pip install psycopg2-binary

Usage:
    python test_client.py
    python test_client.py --host localhost --port 5432 --user seaweedfs --database default
"""

import sys
import argparse
import time
import traceback

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Error: psycopg2 not found. Install with: pip install psycopg2-binary")
    sys.exit(1)


def test_connection(host, port, user, database, password=None):
    """Test basic connection to SeaweedFS PostgreSQL server."""
    print(f"🔗 Testing connection to {host}:{port}/{database} as user '{user}'")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database,
            'connect_timeout': 10
        }
        
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        print("✅ Connection successful!")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✅ Basic query successful: {result}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_system_queries(host, port, user, database, password=None):
    """Test PostgreSQL system queries."""
    print("\n🔧 Testing PostgreSQL system queries...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        system_queries = [
            ("Version", "SELECT version()"),
            ("Current Database", "SELECT current_database()"),
            ("Current User", "SELECT current_user"),
            ("Server Encoding", "SELECT current_setting('server_encoding')"),
            ("Client Encoding", "SELECT current_setting('client_encoding')"),
        ]
        
        for name, query in system_queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                print(f"  ✅ {name}: {result[0]}")
            except Exception as e:
                print(f"  ❌ {name}: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ System queries failed: {e}")


def test_schema_queries(host, port, user, database, password=None):
    """Test schema and metadata queries."""
    print("\n📊 Testing schema queries...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        schema_queries = [
            ("Show Databases", "SHOW DATABASES"),
            ("Show Tables", "SHOW TABLES"),
            ("List Schemas", "SELECT 'public' as schema_name"),
        ]
        
        for name, query in schema_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                print(f"  ✅ {name}: Found {len(results)} items")
                for row in results[:3]:  # Show first 3 results
                    print(f"    - {dict(row)}")
                if len(results) > 3:
                    print(f"    ... and {len(results) - 3} more")
            except Exception as e:
                print(f"  ❌ {name}: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Schema queries failed: {e}")


def test_data_queries(host, port, user, database, password=None):
    """Test data queries on actual topics."""
    print("\n📝 Testing data queries...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # First, try to get available tables/topics
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if not tables:
            print("  ℹ️  No tables/topics found for data testing")
            cursor.close()
            conn.close()
            return
            
        # Test with first available table
        table_name = tables[0][0] if tables[0] else 'test_topic'
        print(f"  📋 Testing with table: {table_name}")
        
        test_queries = [
            (f"Count records in {table_name}", f"SELECT COUNT(*) FROM \"{table_name}\""),
            (f"Sample data from {table_name}", f"SELECT * FROM \"{table_name}\" LIMIT 3"),
            (f"System columns from {table_name}", f"SELECT _timestamp_ns, _key, _source FROM \"{table_name}\" LIMIT 3"),
            (f"Describe {table_name}", f"DESCRIBE \"{table_name}\""),
        ]
        
        for name, query in test_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                
                if "COUNT" in query.upper():
                    count = results[0][0] if results else 0
                    print(f"  ✅ {name}: {count} records")
                elif "DESCRIBE" in query.upper():
                    print(f"  ✅ {name}: {len(results)} columns")
                    for row in results[:5]:  # Show first 5 columns
                        print(f"    - {dict(row)}")
                else:
                    print(f"  ✅ {name}: {len(results)} rows")
                    for row in results:
                        print(f"    - {dict(row)}")
                        
            except Exception as e:
                print(f"  ❌ {name}: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Data queries failed: {e}")


def test_prepared_statements(host, port, user, database, password=None):
    """Test prepared statements."""
    print("\n📝 Testing prepared statements...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Test parameterized query
        try:
            cursor.execute("SELECT %s as param1, %s as param2", ("hello", 42))
            result = cursor.fetchone()
            print(f"  ✅ Prepared statement: {result}")
        except Exception as e:
            print(f"  ❌ Prepared statement: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Prepared statements test failed: {e}")


def test_transaction_support(host, port, user, database, password=None):
    """Test transaction support (should be no-op for read-only)."""
    print("\n🔄 Testing transaction support...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        transaction_commands = [
            "BEGIN",
            "SELECT 1 as in_transaction", 
            "COMMIT",
            "SELECT 1 as after_commit",
        ]
        
        for cmd in transaction_commands:
            try:
                cursor.execute(cmd)
                if "SELECT" in cmd:
                    result = cursor.fetchone()
                    print(f"  ✅ {cmd}: {result}")
                else:
                    print(f"  ✅ {cmd}: OK")
            except Exception as e:
                print(f"  ❌ {cmd}: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Transaction test failed: {e}")


def test_performance(host, port, user, database, password=None, iterations=10):
    """Test query performance."""
    print(f"\n⚡ Testing performance ({iterations} iterations)...")
    
    try:
        conn_params = {
            'host': host,
            'port': port,
            'user': user,
            'database': database
        }
        if password:
            conn_params['password'] = password
            
        times = []
        
        for i in range(iterations):
            start_time = time.time()
            
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            elapsed = time.time() - start_time
            times.append(elapsed)
            
            if i < 3:  # Show first 3 iterations
                print(f"  Iteration {i+1}: {elapsed:.3f}s")
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"  ✅ Performance results:")
        print(f"    - Average: {avg_time:.3f}s")
        print(f"    - Min: {min_time:.3f}s") 
        print(f"    - Max: {max_time:.3f}s")
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Test SeaweedFS PostgreSQL Protocol")
    parser.add_argument("--host", default="localhost", help="PostgreSQL server host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL server port")
    parser.add_argument("--user", default="seaweedfs", help="PostgreSQL username")
    parser.add_argument("--password", help="PostgreSQL password")
    parser.add_argument("--database", default="default", help="PostgreSQL database")
    parser.add_argument("--skip-performance", action="store_true", help="Skip performance tests")
    
    args = parser.parse_args()
    
    print("🧪 SeaweedFS PostgreSQL Protocol Test Client")
    print("=" * 50)
    
    # Test basic connection first
    if not test_connection(args.host, args.port, args.user, args.database, args.password):
        print("\n❌ Basic connection failed. Cannot continue with other tests.")
        sys.exit(1)
    
    # Run all tests
    try:
        test_system_queries(args.host, args.port, args.user, args.database, args.password)
        test_schema_queries(args.host, args.port, args.user, args.database, args.password)
        test_data_queries(args.host, args.port, args.user, args.database, args.password)
        test_prepared_statements(args.host, args.port, args.user, args.database, args.password)
        test_transaction_support(args.host, args.port, args.user, args.database, args.password)
        
        if not args.skip_performance:
            test_performance(args.host, args.port, args.user, args.database, args.password)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error during testing: {e}")
        traceback.print_exc()
        sys.exit(1)
    
    print("\n🎉 All tests completed!")
    print("\nTo use SeaweedFS with PostgreSQL tools:")
    print(f"  psql -h {args.host} -p {args.port} -U {args.user} -d {args.database}")
    print(f"  Connection string: postgresql://{args.user}@{args.host}:{args.port}/{args.database}")


if __name__ == "__main__":
    main()
