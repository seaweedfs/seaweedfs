# SQL Query Engine Feature, Dev, and Test Plan

This document outlines the plan for adding SQL querying support to SeaweedFS, focusing on reading and analyzing data from Message Queue (MQ) topics.

## Feature Plan

**1. Goal**

To provide a SQL querying interface for SeaweedFS, enabling analytics on existing MQ topics. This enables:
- Basic querying with SELECT, WHERE, aggregations on MQ topics
- Schema discovery and metadata operations (SHOW DATABASES, SHOW TABLES, DESCRIBE)
- In-place analytics on Parquet-stored messages without data movement

**2. Key Features**

*   **Schema Discovery and Metadata:**
    *   `SHOW DATABASES` - List all MQ namespaces
    *   `SHOW TABLES` - List all topics in a namespace  
    *   `DESCRIBE table_name` - Show topic schema details
    *   Automatic schema detection from existing Parquet data
*   **Basic Query Engine:**
    *   `SELECT` support with `WHERE`, `LIMIT`, `OFFSET`
    *   Aggregation functions: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
    *   Temporal queries with timestamp-based filtering
*   **User Interfaces:**
    *   New CLI command `weed sql` with interactive shell mode
    *   Optional: Web UI for query execution and result visualization
*   **Output Formats:**
    *   JSON (default), CSV, Parquet for result sets
    *   Streaming results for large queries
    *   Pagination support for result navigation

## Development Plan



**3. Data Source Integration**

*   **MQ Topic Connector (Primary):**
    *   Build on existing `weed/mq/logstore/read_parquet_to_log.go`
    *   Implement efficient Parquet scanning with predicate pushdown
    *   Support schema evolution and backward compatibility
    *   Handle partition-based parallelism for scalable queries
*   **Schema Registry Integration:**
    *   Extend `weed/mq/schema/schema.go` for SQL metadata operations
    *   Read existing topic schemas for query planning
    *   Handle schema evolution during query execution

**4. API & CLI Integration**

*   **CLI Command:**
    *   New `weed sql` command with interactive shell mode (similar to `weed shell`)
    *   Support for script execution and result formatting
    *   Connection management for remote SeaweedFS clusters
*   **gRPC API:**
    *   Add SQL service to existing MQ broker gRPC interface
    *   Enable efficient query execution with streaming results

## Example Usage Scenarios

**Scenario 1: Schema Discovery and Metadata**
```sql
-- List all namespaces (databases)
SHOW DATABASES;

-- List topics in a namespace
USE my_namespace;
SHOW TABLES;

-- View topic structure and discovered schema
DESCRIBE user_events;
```

**Scenario 2: Data Querying**
```sql
-- Basic filtering and projection
SELECT user_id, event_type, timestamp 
FROM user_events 
WHERE timestamp > 1640995200000 
LIMIT 100;

-- Aggregation queries  
SELECT COUNT(*) as event_count
FROM user_events 
WHERE timestamp >= 1640995200000;

-- More aggregation examples
SELECT MAX(timestamp), MIN(timestamp) 
FROM user_events;
```

**Scenario 3: Analytics & Monitoring**
```sql
-- Basic analytics
SELECT COUNT(*) as total_events
FROM user_events 
WHERE timestamp >= 1640995200000;

-- Simple monitoring
SELECT AVG(response_time) as avg_response
FROM api_logs
WHERE timestamp >= 1640995200000;

## Architecture Overview

```
SQL Query Flow:
                                  1. Parse SQL        2. Plan & Optimize      3. Execute Query
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Client    │    │  SQL Parser  │    │  Query Planner  │    │   Execution  │
│    (CLI)    │──→ │ PostgreSQL   │──→ │   & Optimizer   │──→ │    Engine    │
│             │    │ (Custom)     │    │                 │    │              │
└─────────────┘    └──────────────┘    └─────────────────┘    └──────────────┘
                                               │                       │
                                               │ Schema Lookup         │ Data Access
                                               ▼                       ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    Schema Catalog                            │
                    │  • Namespace → Database mapping                            │
                    │  • Topic → Table mapping                                  │
                    │  • Schema version management                              │
                    └─────────────────────────────────────────────────────────────┘
                                                                        ▲
                                                                        │ Metadata
                                                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MQ Storage Layer                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    ▲    │
│  │   Topic A   │  │   Topic B   │  │   Topic C   │  │     ...     │    │    │
│  │ (Parquet)   │  │ (Parquet)   │  │ (Parquet)   │  │ (Parquet)   │    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
└──────────────────────────────────────────────────────────────────────────│──┘
                                                                          │
                                                                     Data Access
```


## Success Metrics

*   **Feature Completeness:** Support for all specified SELECT operations and metadata commands
*   **Performance:** 
    *   **Simple SELECT queries**: < 100ms latency for single-table queries with up to 3 WHERE predicates on ≤ 100K records
    *   **Complex queries**: < 1s latency for queries involving aggregations (COUNT, SUM, MAX, MIN) on ≤ 1M records
    *   **Time-range queries**: < 500ms for timestamp-based filtering on ≤ 500K records within 24-hour windows
*   **Scalability:** Handle topics with millions of messages efficiently
