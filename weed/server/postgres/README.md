# PostgreSQL Wire Protocol Package

This package implements PostgreSQL wire protocol support for SeaweedFS, enabling universal compatibility with PostgreSQL clients, tools, and applications.

## Package Structure

```
weed/server/postgres/
├── README.md           # This documentation
├── server.go          # Main PostgreSQL server implementation  
├── protocol.go        # Wire protocol message handlers
├── translator.go      # SQL translation layer
├── DESIGN.md          # Architecture and design documentation
└── IMPLEMENTATION.md  # Complete implementation guide
```

## Core Components

### `server.go`
- **PostgreSQLServer**: Main server structure with connection management
- **PostgreSQLSession**: Individual client session handling  
- **PostgreSQLServerConfig**: Server configuration options
- **Authentication System**: Trust, password, and MD5 authentication
- **TLS Support**: Encrypted connections with custom certificates
- **Connection Pooling**: Resource management and cleanup

### `protocol.go` 
- **Wire Protocol Implementation**: Full PostgreSQL 3.0 protocol support
- **Message Handlers**: Startup, query, parse/bind/execute sequences
- **Response Generation**: Row descriptions, data rows, command completion
- **Data Type Mapping**: SeaweedFS to PostgreSQL type conversion
- **Error Handling**: PostgreSQL-compliant error responses

### `translator.go`
- **SQL Translation**: PostgreSQL to SeaweedFS SQL conversion
- **System Query Emulation**: version(), current_database(), current_user
- **Meta-Command Support**: psql commands (\d, \dt, \l, \q)
- **System Catalog Emulation**: pg_tables, pg_database, information_schema
- **Transaction Commands**: BEGIN/COMMIT/ROLLBACK (no-op for read-only)

## Usage

### Import the Package
```go
import "github.com/seaweedfs/seaweedfs/weed/server/postgres"
```

### Create and Start Server
```go
config := &postgres.PostgreSQLServerConfig{
    Host:        "localhost",
    Port:        5432,
    AuthMethod:  postgres.AuthMD5,
    Users:       map[string]string{"admin": "secret"},
    Database:    "default",
    MaxConns:    100,
    IdleTimeout: time.Hour,
}

server, err := postgres.NewPostgreSQLServer(config, "localhost:9333")
if err != nil {
    return err
}

err = server.Start()
if err != nil {
    return err
}

// Server is now accepting PostgreSQL connections
```

## Authentication Methods

The package supports three authentication methods:

### Trust Authentication
```go
AuthMethod: postgres.AuthTrust
```
- No password required
- Suitable for development/testing
- Not recommended for production

### Password Authentication  
```go
AuthMethod: postgres.AuthPassword,
Users: map[string]string{"user": "password"}
```
- Clear text password transmission
- Simple but less secure
- Requires TLS for production use

### MD5 Authentication
```go  
AuthMethod: postgres.AuthMD5,
Users: map[string]string{"user": "password"}
```
- Secure hashed authentication with salt
- **Recommended for production**
- Compatible with all PostgreSQL clients

## TLS Configuration

Enable TLS encryption for secure connections:

```go
cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    return err
}

config.TLSConfig = &tls.Config{
    Certificates: []tls.Certificate{cert},
}
```

## Client Compatibility

This implementation is compatible with:

### Command Line Tools
- `psql` - PostgreSQL command line client
- `pgcli` - Enhanced command line with auto-completion
- Database IDEs (DataGrip, DBeaver)

### Programming Languages
- **Python**: psycopg2, asyncpg
- **Java**: PostgreSQL JDBC driver
- **JavaScript**: pg (node-postgres)
- **Go**: lib/pq, pgx
- **.NET**: Npgsql
- **PHP**: pdo_pgsql
- **Ruby**: pg gem

### BI Tools
- Tableau (native PostgreSQL connector)
- Power BI (PostgreSQL data source)
- Grafana (PostgreSQL plugin)
- Apache Superset

## Supported SQL Operations

### Data Queries
```sql
SELECT * FROM topic_name;
SELECT id, message FROM topic_name WHERE condition;
SELECT COUNT(*) FROM topic_name;
SELECT MIN(id), MAX(id), AVG(amount) FROM topic_name;
```

### Schema Information
```sql
SHOW DATABASES;
SHOW TABLES; 
DESCRIBE topic_name;
DESC topic_name;
```

### System Information
```sql
SELECT version();
SELECT current_database();
SELECT current_user;
```

### System Columns
```sql
SELECT id, message, _timestamp_ns, _key, _source FROM topic_name;
```

## Configuration Options

### Server Configuration
- **Host/Port**: Server binding address and port
- **Authentication**: Method and user credentials  
- **Database**: Default database/namespace name
- **Connections**: Maximum concurrent connections
- **Timeouts**: Idle connection timeout
- **TLS**: Certificate and encryption settings

### Performance Tuning
- **Connection Limits**: Prevent resource exhaustion
- **Idle Timeout**: Automatic cleanup of unused connections
- **Memory Management**: Efficient session handling
- **Query Streaming**: Large result set support

## Error Handling

The package provides PostgreSQL-compliant error responses:

- **Connection Errors**: Authentication failures, network issues
- **SQL Errors**: Invalid syntax, missing tables
- **Resource Errors**: Connection limits, timeouts
- **Security Errors**: Permission denied, invalid credentials

## Development and Testing

### Unit Tests
Run PostgreSQL package tests:
```bash
go test ./weed/server/postgres
```

### Integration Testing  
Use the provided Python test client:
```bash
python postgres-examples/test_client.py --host localhost --port 5432
```

### Manual Testing
Connect with psql:
```bash
psql -h localhost -p 5432 -U seaweedfs -d default
```

## Documentation

- **DESIGN.md**: Complete architecture and design overview
- **IMPLEMENTATION.md**: Detailed implementation guide
- **postgres-examples/**: Client examples and test scripts
- **Command Documentation**: `weed postgres -help`

## Security Considerations

### Production Deployment
- Use MD5 or stronger authentication
- Enable TLS encryption
- Configure appropriate connection limits
- Monitor for suspicious activity
- Use strong passwords
- Implement proper firewall rules

### Access Control
- Create dedicated read-only users
- Use principle of least privilege
- Monitor connection patterns
- Log authentication attempts

This package provides enterprise-grade PostgreSQL compatibility, enabling seamless integration of SeaweedFS with the entire PostgreSQL ecosystem.
