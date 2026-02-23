# SeaweedFS PostgreSQL Protocol Test Suite

This directory contains a comprehensive Docker Compose test setup for the SeaweedFS PostgreSQL wire protocol implementation.

## Overview

The test suite includes:
- **SeaweedFS Cluster**: Full SeaweedFS server with MQ broker and agent
- **PostgreSQL Server**: SeaweedFS PostgreSQL wire protocol server
- **MQ Data Producer**: Creates realistic test data across multiple topics and namespaces
- **PostgreSQL Test Client**: Comprehensive Go client testing all functionality
- **Interactive Tools**: psql CLI access for manual testing

## Quick Start

### 1. Run Complete Test Suite (Automated)
```bash
./run-tests.sh all
```

This will automatically:
1. Start SeaweedFS and PostgreSQL servers
2. Create test data in multiple MQ topics
3. Run comprehensive PostgreSQL client tests
4. Show results

### 2. Manual Step-by-Step Testing
```bash
# Start the services
./run-tests.sh start

# Create test data
./run-tests.sh produce

# Run automated tests
./run-tests.sh test

# Connect with psql for interactive testing
./run-tests.sh psql
```

### 3. Interactive PostgreSQL Testing
```bash
# Connect with psql
./run-tests.sh psql

# Inside psql session:
postgres=> SHOW DATABASES;
postgres=> \c analytics;
postgres=> SHOW TABLES;
postgres=> SELECT COUNT(*) FROM user_events;
postgres=> SELECT COUNT(*) FROM user_events;
postgres=> \q
```

## Test Data Structure

The producer creates realistic test data across multiple namespaces:

### Analytics Namespace
- **`user_events`** (1000 records): User interaction events
  - Fields: id, user_id, user_type, action, status, amount, timestamp, metadata
  - User types: premium, standard, trial, enterprise
  - Actions: login, logout, purchase, view, search, click, download

- **`system_logs`** (500 records): System operation logs
  - Fields: id, level, service, message, error_code, timestamp
  - Levels: debug, info, warning, error, critical
  - Services: auth-service, payment-service, user-service, etc.

- **`metrics`** (800 records): System metrics
  - Fields: id, name, value, tags, timestamp
  - Metrics: cpu_usage, memory_usage, disk_usage, request_latency, etc.

### E-commerce Namespace
- **`product_views`** (1200 records): Product interaction data
  - Fields: id, product_id, user_id, category, price, view_count, timestamp
  - Categories: electronics, books, clothing, home, sports, automotive

- **`user_events`** (600 records): E-commerce specific user events

### Logs Namespace
- **`application_logs`** (2000 records): Application logs
- **`error_logs`** (300 records): Error-specific logs with 4xx/5xx error codes

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   PostgreSQL     │    │   SeaweedFS     │
│   Clients       │◄──►│   Wire Protocol  │◄──►│   SQL Engine    │
│   (psql, Go)    │    │   Server         │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                         │
                              ▼                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   Session        │    │   MQ Broker     │
                       │   Management     │    │   & Topics      │
                       └──────────────────┘    └─────────────────┘
```

## Services

### SeaweedFS Server
- **Ports**: 9333 (master), 8888 (filer), 8333 (S3), 8085 (volume), 9533 (metrics), 26777→16777 (MQ agent), 27777→17777 (MQ broker)
- **Features**: Full MQ broker, S3 API, filer, volume server
- **Data**: Persistent storage in Docker volume
- **Health Check**: Cluster status endpoint

### PostgreSQL Server  
- **Port**: 5432 (standard PostgreSQL port)
- **Protocol**: Full PostgreSQL 3.0 wire protocol
- **Authentication**: Trust mode (no password for testing)
- **Features**: Real-time MQ topic discovery, database context switching

### MQ Producer
- **Purpose**: Creates realistic test data
- **Topics**: 7 topics across 3 namespaces
- **Data Types**: JSON messages with varied schemas
- **Volume**: ~4,400 total records with realistic distributions

### Test Client
- **Language**: Go with standard `lib/pq` PostgreSQL driver
- **Tests**: 8 comprehensive test categories
- **Coverage**: System info, discovery, queries, aggregations, context switching

## Available Commands

```bash
./run-tests.sh start      # Start services
./run-tests.sh produce    # Create test data  
./run-tests.sh test       # Run client tests
./run-tests.sh psql       # Interactive psql
./run-tests.sh logs       # Show service logs
./run-tests.sh status     # Service status
./run-tests.sh stop       # Stop services
./run-tests.sh clean      # Complete cleanup
./run-tests.sh all        # Full automated test
```

## Test Categories

### 1. System Information
- PostgreSQL version compatibility
- Current user and database
- Server settings and encoding

### 2. Database Discovery
- `SHOW DATABASES` - List MQ namespaces
- Dynamic namespace discovery from filer

### 3. Table Discovery
- `SHOW TABLES` - List topics in current namespace
- Real-time topic discovery

### 4. Data Queries
- Basic `SELECT * FROM table` queries
- Sample data retrieval and display
- Column information

### 5. Aggregation Queries
- `COUNT(*)`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
- Aggregation operations
- Statistical analysis

### 6. Database Context Switching
- `USE database` commands
- Session isolation testing
- Cross-namespace queries

### 7. System Columns
- `_timestamp_ns`, `_key`, `_source` access
- MQ metadata exposure

### 8. Complex Queries
- `WHERE` clauses with comparisons
- `LIMIT`
- Multi-condition filtering

## Expected Results

After running the complete test suite, you should see:

```
=== Test Results ===
✅ Test PASSED: System Information
✅ Test PASSED: Database Discovery  
✅ Test PASSED: Table Discovery
✅ Test PASSED: Data Queries
✅ Test PASSED: Aggregation Queries
✅ Test PASSED: Database Context Switching
✅ Test PASSED: System Columns
✅ Test PASSED: Complex Queries

Test Results: 8/8 tests passed
🎉 All tests passed!
```

## Manual Testing Examples

### Connect with psql
```bash
./run-tests.sh psql
```

### Basic Exploration
```sql
-- Check system information
SELECT version();
SELECT current_user, current_database();

-- Discover data structure  
SHOW DATABASES;
\c analytics;
SHOW TABLES;
DESCRIBE user_events;
```

### Data Analysis
```sql
-- Basic queries
SELECT COUNT(*) FROM user_events;
SELECT * FROM user_events LIMIT 5;

-- Aggregations
SELECT 
    COUNT(*) as events,
    AVG(amount) as avg_amount
FROM user_events 
WHERE amount IS NOT NULL;

-- Time-based analysis
SELECT 
    COUNT(*) as count
FROM user_events 
WHERE status = 'active';
```

### Cross-Namespace Analysis
```sql
-- Switch between namespaces
USE ecommerce;
SELECT COUNT(*) FROM product_views;

USE logs;  
SELECT COUNT(*) FROM application_logs;
```

## Troubleshooting

### Services Not Starting
```bash
# Check service status
./run-tests.sh status

# View logs
./run-tests.sh logs seaweedfs
./run-tests.sh logs postgres-server
```

### No Test Data
```bash
# Recreate test data
./run-tests.sh produce

# Check producer logs
./run-tests.sh logs mq-producer
```

### Connection Issues
```bash
# Test PostgreSQL server health
docker-compose exec postgres-server nc -z localhost 5432

# Test SeaweedFS health
curl http://localhost:9333/cluster/status
```

### Clean Restart
```bash
# Complete cleanup and restart
./run-tests.sh clean
./run-tests.sh all
```

## Development

### Modifying Test Data
Edit `producer.go` to change:
- Data schemas and volume
- Topic names and namespaces
- Record generation logic

### Adding Tests
Edit `client.go` to add new test functions:
```go
func testNewFeature(db *sql.DB) error {
    // Your test implementation
    return nil
}

// Add to tests slice in main()
{"New Feature", testNewFeature},
```

### Custom Queries
Use the interactive psql session:
```bash
./run-tests.sh psql
```

## Production Considerations

This test setup demonstrates:
- **Real MQ Integration**: Actual topic discovery and data access
- **Universal PostgreSQL Compatibility**: Works with any PostgreSQL client
- **Production-Ready Features**: Authentication, session management, error handling
- **Scalable Architecture**: Direct SQL engine integration, no translation overhead

The test validates that SeaweedFS can serve as a drop-in PostgreSQL replacement for read-only analytics workloads on MQ data.
