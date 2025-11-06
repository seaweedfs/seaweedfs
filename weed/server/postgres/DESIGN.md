# PostgreSQL Wire Protocol Support for SeaweedFS

## Overview

This design adds native PostgreSQL wire protocol support to SeaweedFS, enabling compatibility with all PostgreSQL clients, tools, and drivers without requiring custom implementations.

## Benefits

### Universal Compatibility
- **Standard PostgreSQL Clients**: psql, pgAdmin, Adminer, etc.
- **JDBC/ODBC Drivers**: Use standard PostgreSQL drivers
- **BI Tools**: Tableau, Power BI, Grafana, Superset with native PostgreSQL connectors
- **ORMs**: Hibernate, ActiveRecord, Django ORM, etc.
- **Programming Languages**: Native PostgreSQL libraries in Python (psycopg2), Node.js (pg), Go (lib/pq), etc.

### Enterprise Integration
- **Existing Infrastructure**: Drop-in replacement for PostgreSQL in read-only scenarios
- **Migration Path**: Easy transition from PostgreSQL-based analytics
- **Tool Ecosystem**: Leverage entire PostgreSQL ecosystem

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   PostgreSQL     │    │   SeaweedFS     │
│   Clients       │◄──►│   Protocol       │◄──►│   SQL Engine    │
│   (psql, etc.)  │    │   Server         │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Authentication │
                       │   & Session Mgmt │
                       └──────────────────┘
```

## Core Components

### 1. PostgreSQL Wire Protocol Handler

```go
// PostgreSQL message types
const (
    PG_MSG_STARTUP         = 0x00  // Startup message
    PG_MSG_QUERY           = 'Q'   // Simple query
    PG_MSG_PARSE           = 'P'   // Parse (prepared statement)
    PG_MSG_BIND            = 'B'   // Bind parameters
    PG_MSG_EXECUTE         = 'E'   // Execute prepared statement
    PG_MSG_DESCRIBE        = 'D'   // Describe statement/portal
    PG_MSG_CLOSE           = 'C'   // Close statement/portal
    PG_MSG_FLUSH           = 'H'   // Flush
    PG_MSG_SYNC            = 'S'   // Sync
    PG_MSG_TERMINATE       = 'X'   // Terminate connection
    PG_MSG_PASSWORD        = 'p'   // Password message
)

// PostgreSQL response types
const (
    PG_RESP_AUTH_OK        = 'R'   // Authentication OK
    PG_RESP_AUTH_REQ       = 'R'   // Authentication request
    PG_RESP_BACKEND_KEY    = 'K'   // Backend key data
    PG_RESP_PARAMETER      = 'S'   // Parameter status
    PG_RESP_READY          = 'Z'   // Ready for query
    PG_RESP_COMMAND        = 'C'   // Command complete
    PG_RESP_DATA_ROW       = 'D'   // Data row
    PG_RESP_ROW_DESC       = 'T'   // Row description
    PG_RESP_PARSE_COMPLETE = '1'   // Parse complete
    PG_RESP_BIND_COMPLETE  = '2'   // Bind complete
    PG_RESP_CLOSE_COMPLETE = '3'   // Close complete
    PG_RESP_ERROR          = 'E'   // Error response
    PG_RESP_NOTICE         = 'N'   // Notice response
)
```

### 2. Session Management

```go
type PostgreSQLSession struct {
    conn             net.Conn
    reader           *bufio.Reader
    writer           *bufio.Writer
    authenticated    bool
    username         string
    database         string
    parameters       map[string]string
    preparedStmts    map[string]*PreparedStatement
    portals          map[string]*Portal
    transactionState TransactionState
    processID        uint32
    secretKey        uint32
}

type PreparedStatement struct {
    name       string
    query      string
    paramTypes []uint32
    fields     []FieldDescription
}

type Portal struct {
    name       string
    statement  string
    parameters [][]byte
    suspended  bool
}
```

### 3. SQL Translation Layer

```go
type PostgreSQLTranslator struct {
    dialectMap map[string]string
}

// Translates PostgreSQL-specific SQL to SeaweedFS SQL
func (t *PostgreSQLTranslator) TranslateQuery(pgSQL string) (string, error) {
    // Handle PostgreSQL-specific syntax:
    // - SELECT version() -> SELECT 'SeaweedFS 1.0'
    // - SELECT current_database() -> SELECT 'default'
    // - SELECT current_user -> SELECT 'seaweedfs'
    // - \d commands -> SHOW TABLES/DESCRIBE equivalents
    // - PostgreSQL system catalogs -> SeaweedFS equivalents
}
```

### 4. Data Type Mapping

```go
var PostgreSQLTypeMap = map[string]uint32{
    "TEXT":      25,   // PostgreSQL TEXT type
    "VARCHAR":   1043, // PostgreSQL VARCHAR type
    "INTEGER":   23,   // PostgreSQL INTEGER type
    "BIGINT":    20,   // PostgreSQL BIGINT type
    "FLOAT":     701,  // PostgreSQL FLOAT8 type
    "BOOLEAN":   16,   // PostgreSQL BOOLEAN type
    "TIMESTAMP": 1114, // PostgreSQL TIMESTAMP type
    "JSON":      114,  // PostgreSQL JSON type
}

func SeaweedToPostgreSQLType(seaweedType string) uint32 {
    if pgType, exists := PostgreSQLTypeMap[strings.ToUpper(seaweedType)]; exists {
        return pgType
    }
    return 25 // Default to TEXT
}
```

## Protocol Implementation

### 1. Connection Flow

```
Client                          Server
  │                              │
  ├─ StartupMessage ────────────►│
  │                              ├─ AuthenticationOk
  │                              ├─ ParameterStatus (multiple)
  │                              ├─ BackendKeyData
  │                              └─ ReadyForQuery
  │                              │
  ├─ Query('SELECT 1') ─────────►│
  │                              ├─ RowDescription
  │                              ├─ DataRow
  │                              ├─ CommandComplete
  │                              └─ ReadyForQuery
  │                              │
  ├─ Parse('stmt1', 'SELECT $1')►│
  │                              └─ ParseComplete
  ├─ Bind('portal1', 'stmt1')───►│
  │                              └─ BindComplete  
  ├─ Execute('portal1')─────────►│
  │                              ├─ DataRow (multiple)
  │                              └─ CommandComplete
  ├─ Sync ──────────────────────►│
  │                              └─ ReadyForQuery
  │                              │
  ├─ Terminate ─────────────────►│
  │                              └─ [Connection closed]
```

### 2. Authentication

```go
type AuthMethod int

const (
    AuthTrust AuthMethod = iota
    AuthPassword
    AuthMD5
    AuthSASL
)

func (s *PostgreSQLServer) handleAuthentication(session *PostgreSQLSession) error {
    switch s.authMethod {
    case AuthTrust:
        return s.sendAuthenticationOk(session)
    case AuthPassword:
        return s.handlePasswordAuth(session)
    case AuthMD5:
        return s.handleMD5Auth(session)
    default:
        return fmt.Errorf("unsupported auth method")
    }
}
```

### 3. Query Processing

```go
func (s *PostgreSQLServer) handleSimpleQuery(session *PostgreSQLSession, query string) error {
    // 1. Translate PostgreSQL SQL to SeaweedFS SQL
    translatedQuery, err := s.translator.TranslateQuery(query)
    if err != nil {
        return s.sendError(session, err)
    }
    
    // 2. Execute using existing SQL engine
    result, err := s.sqlEngine.ExecuteSQL(context.Background(), translatedQuery)
    if err != nil {
        return s.sendError(session, err)
    }
    
    // 3. Send results in PostgreSQL format
    err = s.sendRowDescription(session, result.Columns)
    if err != nil {
        return err
    }
    
    for _, row := range result.Rows {
        err = s.sendDataRow(session, row)
        if err != nil {
            return err
        }
    }
    
    return s.sendCommandComplete(session, fmt.Sprintf("SELECT %d", len(result.Rows)))
}
```

## System Catalogs Support

PostgreSQL clients expect certain system catalogs. We'll implement views for key ones:

```sql
-- pg_tables equivalent
SELECT 
    'default' as schemaname,
    table_name as tablename,
    'seaweedfs' as tableowner,
    NULL as tablespace,
    false as hasindexes,
    false as hasrules,
    false as hastriggers
FROM information_schema.tables;

-- pg_database equivalent  
SELECT 
    database_name as datname,
    'seaweedfs' as datdba,
    'UTF8' as encoding,
    'C' as datcollate,
    'C' as datctype
FROM information_schema.schemata;

-- pg_version equivalent
SELECT 'SeaweedFS 1.0 (PostgreSQL 14.0 compatible)' as version;
```

## Configuration

### Server Configuration
```go
type PostgreSQLServerConfig struct {
    Host         string
    Port         int
    Database     string
    AuthMethod   AuthMethod
    Users        map[string]string // username -> password
    TLSConfig    *tls.Config
    MaxConns     int
    IdleTimeout  time.Duration
}
```

### Client Connection String
```bash
# Standard PostgreSQL connection strings work
psql "host=localhost port=5432 dbname=default user=seaweedfs"
PGPASSWORD=secret psql -h localhost -p 5432 -U seaweedfs -d default

# JDBC URL
jdbc:postgresql://localhost:5432/default?user=seaweedfs&password=secret
```

## Command Line Interface

```bash
# Start PostgreSQL protocol server
weed db -port=5432 -auth=trust
weed db -port=5432 -auth=password -users="admin:secret;readonly:pass"
weed db -port=5432 -tls-cert=server.crt -tls-key=server.key

# Configuration options
-host=localhost              # Listen host
-port=5432                   # PostgreSQL standard port
-auth=trust|password|md5     # Authentication method
-users=user:pass;user2:pass2 # User credentials (password/md5 auth) - use semicolons to separate users
-database=default            # Default database name
-max-connections=100         # Maximum concurrent connections
-idle-timeout=1h             # Connection idle timeout
-tls-cert=""                 # TLS certificate file
-tls-key=""                  # TLS private key file
```

## Client Compatibility Testing

### Essential Clients
- **psql**: PostgreSQL command line client
- **pgAdmin**: Web-based administration tool  
- **DBeaver**: Universal database tool
- **DataGrip**: JetBrains database IDE

### Programming Language Drivers
- **Python**: psycopg2, asyncpg
- **Java**: PostgreSQL JDBC driver
- **Node.js**: pg, node-postgres
- **Go**: lib/pq, pgx
- **.NET**: Npgsql

### BI Tools
- **Grafana**: PostgreSQL data source
- **Superset**: PostgreSQL connector
- **Tableau**: PostgreSQL native connector
- **Power BI**: PostgreSQL connector

## Implementation Plan

1. **Phase 1**: Basic wire protocol and simple queries
2. **Phase 2**: Extended query protocol (prepared statements)
3. **Phase 3**: System catalog views
4. **Phase 4**: Advanced features (transactions, notifications)
5. **Phase 5**: Performance optimization and caching

## Limitations

### Read-Only Access
- INSERT/UPDATE/DELETE operations not supported
- Returns appropriate error messages for write operations

### Partial SQL Compatibility
- Subset of PostgreSQL SQL features
- SeaweedFS-specific limitations apply

### System Features
- No stored procedures/functions
- No triggers or constraints
- No user-defined types
- Limited transaction support (mostly no-op)

## Security Considerations

### Authentication
- Support for trust, password, and MD5 authentication
- TLS encryption support
- User access control

### SQL Injection Prevention
- Prepared statements with parameter binding
- Input validation and sanitization
- Query complexity limits

## Performance Optimizations

### Connection Pooling
- Configurable maximum connections
- Connection reuse and idle timeout
- Memory efficient session management

### Query Caching  
- Prepared statement caching
- Result set caching for repeated queries
- Metadata caching

### Protocol Efficiency
- Binary result format support
- Batch query processing
- Streaming large result sets

This design provides a comprehensive PostgreSQL wire protocol implementation that makes SeaweedFS accessible to the entire PostgreSQL ecosystem while maintaining compatibility and performance.
