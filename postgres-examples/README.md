# SeaweedFS PostgreSQL Protocol Examples

This directory contains examples demonstrating how to connect to SeaweedFS using the PostgreSQL wire protocol.

## Starting the PostgreSQL Server

```bash
# Start with trust authentication (no password required)
weed postgres -port=5432 -master=localhost:9333

# Start with password authentication
weed postgres -port=5432 -auth=password -users="admin:secret;readonly:view123"

# Start with MD5 authentication (more secure)
weed postgres -port=5432 -auth=md5 -users="user1:pass1;user2:pass2"

# Start with TLS encryption
weed postgres -port=5432 -tls-cert=server.crt -tls-key=server.key

# Allow connections from any host
weed postgres -host=0.0.0.0 -port=5432
```

## Client Connections

### psql Command Line

```bash
# Basic connection (trust auth)
psql -h localhost -p 5432 -U seaweedfs -d default

# With password
PGPASSWORD=secret psql -h localhost -p 5432 -U admin -d default

# Connection string format
psql "postgresql://admin:secret@localhost:5432/default"

# Connection string with parameters
psql "host=localhost port=5432 dbname=default user=admin password=secret"
```

### Programming Languages

#### Python (psycopg2)
```python
import psycopg2

# Connect to SeaweedFS
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="seaweedfs", 
    database="default"
)

# Execute queries
cursor = conn.cursor()
cursor.execute("SELECT * FROM my_topic LIMIT 10")

for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

#### Java JDBC
```java
import java.sql.*;

public class SeaweedFSExample {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/default";
        
        Connection conn = DriverManager.getConnection(url, "seaweedfs", "");
        Statement stmt = conn.createStatement();
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM my_topic LIMIT 10");
        while (rs.next()) {
            System.out.println("ID: " + rs.getLong("id"));
            System.out.println("Message: " + rs.getString("message"));
        }
        
        rs.close();
        stmt.close(); 
        conn.close();
    }
}
```

#### Go (lib/pq)
```go
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/lib/pq"
)

func main() {
    db, err := sql.Open("postgres", 
        "host=localhost port=5432 user=seaweedfs dbname=default sslmode=disable")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    rows, err := db.Query("SELECT * FROM my_topic LIMIT 10")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var message string
        err := rows.Scan(&id, &message)
        if err != nil {
            panic(err)
        }
        fmt.Printf("ID: %d, Message: %s\n", id, message)
    }
}
```

#### Node.js (pg)
```javascript
const { Client } = require('pg');

const client = new Client({
    host: 'localhost',
    port: 5432,
    user: 'seaweedfs',
    database: 'default',
});

async function query() {
    await client.connect();
    
    const result = await client.query('SELECT * FROM my_topic LIMIT 10');
    console.log(result.rows);
    
    await client.end();
}

query().catch(console.error);
```

## SQL Operations

### Basic Queries
```sql
-- List databases
SHOW DATABASES;

-- List tables (topics)
SHOW TABLES;

-- Describe table structure
DESCRIBE my_topic;
-- or use the shorthand: DESC my_topic;

-- Basic select
SELECT * FROM my_topic;

-- With WHERE clause
SELECT id, message FROM my_topic WHERE id > 1000;

-- With LIMIT
SELECT * FROM my_topic LIMIT 100;
```

### Aggregations
```sql
-- Count records
SELECT COUNT(*) FROM my_topic;

-- Multiple aggregations
SELECT 
    COUNT(*) as total_messages,
    MIN(id) as min_id,
    MAX(id) as max_id,
    AVG(amount) as avg_amount
FROM my_topic;

-- Aggregations with WHERE
SELECT COUNT(*) FROM my_topic WHERE status = 'active';
```

### System Columns
```sql
-- Access system columns
SELECT 
    id,
    message, 
    _timestamp_ns as timestamp,
    _key as partition_key,
    _source as data_source
FROM my_topic;

-- Filter by timestamp
SELECT * FROM my_topic 
WHERE _timestamp_ns > 1640995200000000000
LIMIT 10;
```

### PostgreSQL System Queries
```sql
-- Version information
SELECT version();

-- Current database
SELECT current_database();

-- Current user
SELECT current_user;

-- Server settings
SELECT current_setting('server_version');
SELECT current_setting('server_encoding');
```

## psql Meta-Commands

```sql
-- List tables
\d
\dt

-- List databases  
\l

-- Describe specific table
\d my_topic
\dt my_topic

-- List schemas
\dn

-- Help
\h
\?

-- Quit
\q
```

## Database Tools Integration

### DBeaver
1. Create New Connection → PostgreSQL
2. Settings:
   - **Host**: localhost
   - **Port**: 5432
   - **Database**: default
   - **Username**: seaweedfs (or configured user)
   - **Password**: (if using password auth)

### pgAdmin
1. Add New Server
2. Connection tab:
   - **Host**: localhost
   - **Port**: 5432
   - **Username**: seaweedfs
   - **Database**: default

### DataGrip
1. New Data Source → PostgreSQL
2. Configure:
   - **Host**: localhost
   - **Port**: 5432
   - **User**: seaweedfs
   - **Database**: default

### Grafana
1. Add Data Source → PostgreSQL
2. Configuration:
   - **Host**: localhost:5432
   - **Database**: default
   - **User**: seaweedfs
   - **SSL Mode**: disable

## BI Tools

### Tableau
1. Connect to Data → PostgreSQL
2. Server: localhost
3. Port: 5432
4. Database: default
5. Username: seaweedfs

### Power BI
1. Get Data → Database → PostgreSQL
2. Server: localhost
3. Database: default
4. Username: seaweedfs

## Connection Pooling

### Java (HikariCP)
```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/default");
config.setUsername("seaweedfs");
config.setMaximumPoolSize(10);

HikariDataSource dataSource = new HikariDataSource(config);
```

### Python (connection pooling)
```python
from psycopg2 import pool

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 20,
    host="localhost",
    port=5432,
    user="seaweedfs",
    database="default"
)

conn = connection_pool.getconn()
# Use connection
connection_pool.putconn(conn)
```

## Security Best Practices

### Use TLS Encryption
```bash
# Generate self-signed certificate for testing
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes

# Start with TLS
weed postgres -tls-cert=server.crt -tls-key=server.key
```

### Use MD5 Authentication
```bash
# More secure than password auth
weed postgres -auth=md5 -users="admin:secret123;readonly:view456"
```

### Limit Connections
```bash
# Limit concurrent connections
weed postgres -max-connections=50 -idle-timeout=30m
```

## Troubleshooting

### Connection Issues
```bash
# Test connectivity
telnet localhost 5432

# Check if server is running
ps aux | grep "weed postgres"

# Check logs for errors
tail -f /var/log/seaweedfs/postgres.log
```

### Common Errors

**"Connection refused"**
- Ensure PostgreSQL server is running
- Check host/port configuration
- Verify firewall settings

**"Authentication failed"**
- Check username/password
- Verify auth method configuration
- Ensure user is configured in server

**"Database does not exist"**
- Use correct database name (default: 'default')
- Check available databases: `SHOW DATABASES`

**"Permission denied"**
- Check user permissions
- Verify authentication method
- Use correct credentials

## Performance Tips

1. **Use LIMIT clauses** for large result sets
2. **Filter with WHERE clauses** to reduce data transfer
3. **Use connection pooling** for multi-threaded applications
4. **Close resources properly** (connections, statements, result sets)
5. **Use prepared statements** for repeated queries

## Monitoring

### Connection Statistics
```sql
-- Current connections (if supported)
SELECT COUNT(*) FROM pg_stat_activity;

-- Server version
SELECT version();

-- Current settings
SELECT name, setting FROM pg_settings WHERE name LIKE '%connection%';
```

### Query Performance
```sql
-- Use EXPLAIN for query plans (if supported)
EXPLAIN SELECT * FROM my_topic WHERE id > 1000;
```

This PostgreSQL protocol support makes SeaweedFS accessible to the entire PostgreSQL ecosystem, enabling seamless integration with existing tools, applications, and workflows.
