# SQL Query Engine Feature, Dev, and Test Plan

This document outlines the plan for adding comprehensive SQL support to SeaweedFS, focusing on schema-tized Message Queue (MQ) topics with full DDL and DML capabilities, plus S3 objects querying.

## Feature Plan

**1. Goal**

To provide a full-featured SQL interface for SeaweedFS, treating schema-tized MQ topics as database tables with complete DDL/DML support. This enables:
- Database-like operations on MQ topics (CREATE TABLE, ALTER TABLE, DROP TABLE)
- Advanced querying with SELECT, WHERE, JOIN, aggregations
- Schema management and metadata operations (SHOW DATABASES, SHOW TABLES)
- In-place analytics on Parquet-stored messages without data movement

**2. Key Features**

*   **Schema-tized Topic Management (Priority 1):**
    *   `SHOW DATABASES` - List all MQ namespaces
    *   `SHOW TABLES` - List all topics in a namespace
    *   `CREATE TABLE topic_name (field1 INT, field2 STRING, ...)` - Create new MQ topic with schema
    *   `ALTER TABLE topic_name ADD COLUMN field3 BOOL` - Modify topic schema (with versioning)
    *   `DROP TABLE topic_name` - Delete MQ topic
    *   `DESCRIBE table_name` - Show topic schema details
*   **Advanced Query Engine (Priority 1):**
    *   Full `SELECT` support with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
    *   Aggregation functions: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`, `GROUP BY`
    *   Join operations between topics (leveraging Parquet columnar format)
    *   Window functions and advanced analytics
    *   Temporal queries with timestamp-based filtering
*   **S3 Select (Priority 2):**
    *   Support for querying objects in standard data formats (CSV, JSON, Parquet)
    *   Queries executed directly on storage nodes to minimize data transfer
*   **User Interfaces:**
    *   New API endpoint `/sql` for HTTP-based SQL execution
    *   New CLI command `weed sql` with interactive shell mode
    *   Optional: Web UI for query execution and result visualization
*   **Output Formats:**
    *   JSON (default), CSV, Parquet for result sets
    *   Streaming results for large queries
    *   Pagination support for result navigation

## Development Plan

**1. Scaffolding & Dependencies**

*   **SQL Parser:** **POSTGRESQL-ONLY IMPLEMENTATION**
    *   **Current Implementation:** Custom lightweight PostgreSQL parser (pure Go, no CGO)
    *   **Design Decision:** PostgreSQL-only dialect support for optimal wire protocol compatibility
        *   **Identifier Quoting:** Uses PostgreSQL double quotes (`"identifiers"`)
        *   **String Concatenation:** Supports PostgreSQL `||` operator
        *   **System Functions:** Full support for PostgreSQL system catalogs and functions
    *   **Architecture Benefits:**
        *   **No CGO Dependencies:** Avoids build complexity and cross-platform issues
        *   **Wire Protocol Alignment:** Perfect compatibility with PostgreSQL clients
        *   **Lightweight:** Custom parser focused on SeaweedFS SQL feature subset
        *   **Extensible:** Easy to add new PostgreSQL syntax support as needed
    *   **Error Handling:** Typed errors map to standard PostgreSQL error codes
    *   **Parser Location:** `weed/query/engine/engine.go` (ParseSQL function)
*   **Project Structure:** 
    *   Extend existing `weed/query/` package for SQL execution engine
    *   Create `weed/query/engine/` for query planning and execution
    *   Create `weed/query/metadata/` for schema catalog management
    *   Integration point in `weed/mq/` for topic-to-table mapping

**2. SQL Engine Architecture**

*   **Schema Catalog:**
    *   Leverage existing `weed/mq/schema/` infrastructure
    *   Map MQ namespaces to "databases" and topics to "tables"
    *   Store schema metadata with version history
    *   Handle schema evolution and migration
*   **Query Planner:**
    *   Parse SQL statements using custom PostgreSQL parser
    *   Create optimized execution plans leveraging Parquet columnar format
    *   Push-down predicates to storage layer for efficient filtering
    *   Optimize aggregations using Parquet column statistics
    *   Support time-based filtering with intelligent time range extraction
*   **Query Executor:**
    *   Utilize existing `weed/mq/logstore/` for Parquet reading
    *   Implement streaming execution for large result sets
    *   Support parallel processing across topic partitions
    *   Handle schema evolution during query execution

**3. Data Source Integration**

*   **MQ Topic Connector (Primary):**
    *   Build on existing `weed/mq/logstore/read_parquet_to_log.go`
    *   Implement efficient Parquet scanning with predicate pushdown
    *   Support schema evolution and backward compatibility
    *   Handle partition-based parallelism for scalable queries
*   **Schema Registry Integration:**
    *   Extend `weed/mq/schema/schema.go` for SQL metadata operations
    *   Implement DDL operations that modify underlying MQ topic schemas
    *   Version control for schema changes with migration support
*   **S3 Connector (Secondary):**
    *   Reading data from S3 objects with CSV, JSON, and Parquet parsers
    *   Efficient streaming for large files with columnar optimizations

**4. API & CLI Integration**

*   **HTTP API Endpoint:**
    *   Add `/sql` endpoint to Filer server following existing patterns in `weed/server/filer_server.go`
    *   Support both POST (for queries) and GET (for metadata operations)
    *   Include query result pagination and streaming
    *   Authentication and authorization integration
*   **CLI Command:**
    *   New `weed sql` command with interactive shell mode (similar to `weed shell`)
    *   Support for script execution and result formatting
    *   Connection management for remote SeaweedFS clusters
*   **gRPC API:**
    *   Add SQL service to existing MQ broker gRPC interface
    *   Enable efficient query execution with streaming results

## Example Usage Scenarios

**Scenario 1: Topic Management**
```sql
-- List all namespaces (databases)
SHOW DATABASES;

-- List topics in a namespace
USE my_namespace;
SHOW TABLES;

-- Create a new topic with schema
CREATE TABLE user_events (
    user_id INT,
    event_type STRING,
    timestamp BIGINT,
    metadata STRING
);

-- Modify topic schema
ALTER TABLE user_events ADD COLUMN session_id STRING;

-- View topic structure
DESCRIBE user_events;
```

**Scenario 2: Data Querying**
```sql
-- Basic filtering and projection
SELECT user_id, event_type, timestamp 
FROM user_events 
WHERE timestamp > 1640995200000 
ORDER BY timestamp DESC 
LIMIT 100;

-- Aggregation queries
SELECT event_type, COUNT(*) as event_count
FROM user_events 
WHERE timestamp >= 1640995200000
GROUP BY event_type;

-- Cross-topic joins
SELECT u.user_id, u.event_type, p.product_name
FROM user_events u
JOIN product_catalog p ON u.product_id = p.id
WHERE u.event_type = 'purchase';
```

**Scenario 3: Analytics & Monitoring**
```sql
-- Time-series analysis
SELECT 
    DATE_TRUNC('hour', FROM_UNIXTIME(timestamp/1000)) as hour,
    COUNT(*) as events_per_hour
FROM user_events 
WHERE timestamp >= 1640995200000
GROUP BY hour
ORDER BY hour;

-- Real-time monitoring
SELECT event_type, AVG(response_time) as avg_response
FROM api_logs
WHERE timestamp >= UNIX_TIMESTAMP() - 3600
GROUP BY event_type
HAVING avg_response > 1000;
```

## Architecture Overview

```
SQL Query Flow:
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Client    │    │  SQL Parser  │    │  Query Planner  │    │   Execution  │
│ (CLI/HTTP)  │──→ │ PostgreSQL   │──→ │   & Optimizer   │──→ │    Engine    │
│             │    │ (Custom)     │    │                 │    │              │
└─────────────┘    └──────────────┘    └─────────────────┘    └──────────────┘
                                               │                       │
                                               ▼                       │
                    ┌─────────────────────────────────────────────────┐│
                    │            Schema Catalog                        ││
                    │  • Namespace → Database mapping                ││
                    │  • Topic → Table mapping                       ││
                    │  • Schema version management                   ││
                    └─────────────────────────────────────────────────┘│
                                                                        │
                                                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MQ Storage Layer                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Topic A   │  │   Topic B   │  │   Topic C   │  │     ...     │         │
│  │ (Parquet)   │  │ (Parquet)   │  │ (Parquet)   │  │ (Parquet)   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

**1. SQL-to-MQ Mapping Strategy:**
*   MQ Namespaces ↔ SQL Databases
*   MQ Topics ↔ SQL Tables
*   Topic Partitions ↔ Table Shards (transparent to users)
*   Schema Fields ↔ Table Columns

**2. Schema Evolution Handling:**
*   Maintain schema version history in topic metadata
*   Support backward-compatible queries across schema versions
*   Automatic type coercion where possible
*   Clear error messages for incompatible changes

**3. Query Optimization:**
*   Leverage Parquet columnar format for projection pushdown
*   Use topic partitioning for parallel query execution
*   Implement predicate pushdown to minimize data scanning
*   Cache frequently accessed schema metadata

**4. PostgreSQL Implementation Strategy:**
*   **Architecture:** Custom PostgreSQL parser + PostgreSQL wire protocol = perfect compatibility
*   **Benefits:** No dialect translation needed, direct PostgreSQL syntax support
*   **Supported Features:** 
    *   Native PostgreSQL identifier quoting (`"identifiers"`)
    *   PostgreSQL string concatenation (`||` operator)
    *   System queries (`version()`, `BEGIN`, `COMMIT`) with proper responses
    *   PostgreSQL error codes mapped from typed errors
*   **Error Handling:** Typed error system with proper PostgreSQL error code mapping
*   **Parser Features:** Lightweight, focused on SeaweedFS SQL subset, easily extensible

**5. Transaction Semantics:**
*   DDL operations (CREATE/ALTER/DROP) are atomic per topic
*   SELECT queries provide read-consistent snapshots
*   No cross-topic transactions initially (future enhancement)

**6. Performance Considerations:**
*   Prioritize read performance over write consistency
*   Leverage MQ's natural partitioning for parallel queries
*   Use Parquet metadata for query optimization
*   Implement connection pooling and query caching

## Implementation Phases

**Phase 1: Core SQL Infrastructure (Weeks 1-3)** ✅ COMPLETED
1. Implemented custom PostgreSQL parser for optimal compatibility (no CGO dependencies)
2. Created `weed/query/engine/` package with comprehensive SQL execution framework
3. Implemented metadata catalog mapping MQ topics to SQL tables
4. Added `SHOW DATABASES`, `SHOW TABLES`, `DESCRIBE` commands with full PostgreSQL compatibility

**Phase 2: DDL Operations (Weeks 4-5)**
1. `CREATE TABLE` → Create MQ topic with schema
2. `ALTER TABLE` → Modify topic schema with versioning
3. `DROP TABLE` → Delete MQ topic
4. Schema validation and migration handling

**Phase 3: Query Engine (Weeks 6-8)**
1. `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
2. Aggregation functions and `GROUP BY`
3. Basic joins between topics
4. Predicate pushdown to Parquet layer

**Phase 4: API & CLI Integration (Weeks 9-10)**
1. HTTP `/sql` endpoint implementation
2. `weed sql` CLI command with interactive mode
3. Result streaming and pagination
4. Error handling and query optimization

## Test Plan

**1. Unit Tests**

*   **SQL Parser Tests:** Validate parsing of all supported DDL/DML statements
*   **Schema Mapping Tests:** Test topic-to-table conversion and metadata operations
*   **Query Planning Tests:** Verify optimization and predicate pushdown logic
*   **Execution Engine Tests:** Test query execution with various data patterns
*   **Edge Cases:** Malformed queries, schema evolution, concurrent operations

**2. Integration Tests**

*   **End-to-End Workflow:** Complete SQL operations against live SeaweedFS cluster
*   **Schema Evolution:** Test backward compatibility during schema changes
*   **Multi-Topic Joins:** Validate cross-topic query performance and correctness
*   **Large Dataset Tests:** Performance validation with GB-scale Parquet data
*   **Concurrent Access:** Multiple SQL sessions operating simultaneously

**3. Performance & Security Testing**

*   **Query Performance:** Benchmark latency for various query patterns
*   **Memory Usage:** Monitor resource consumption during large result sets
*   **Scalability Tests:** Performance across multiple partitions and topics
*   **SQL Injection Prevention:** Security validation of parser and execution engine
*   **Fuzz Testing:** Automated testing with malformed SQL inputs

## Success Metrics

*   **Feature Completeness:** Support for all specified DDL/DML operations
*   **Performance:** Query latency < 100ms for simple selects, < 1s for complex joins
*   **Scalability:** Handle topics with millions of messages efficiently
*   **Reliability:** 99.9% success rate for valid SQL operations
*   **Usability:** Intuitive SQL interface matching standard database expectations
