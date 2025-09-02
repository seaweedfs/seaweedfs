# SeaweedFS JDBC Driver

A JDBC driver for connecting to SeaweedFS SQL engine, enabling standard Java applications and BI tools to query SeaweedFS MQ topics using SQL.

## Features

- **Standard JDBC Interface**: Compatible with any Java application or tool that supports JDBC
- **SQL Query Support**: Execute SELECT queries on SeaweedFS MQ topics
- **Aggregation Functions**: Support for COUNT, SUM, AVG, MIN, MAX operations
- **System Columns**: Access to `_timestamp_ns`, `_key`, `_source` system columns
- **Database Tools**: Works with DBeaver, IntelliJ DataGrip, and other database tools
- **BI Tools**: Compatible with Tableau, Power BI, and other business intelligence tools
- **Read-Only Access**: Secure read-only access to your SeaweedFS data

## Quick Start

### 1. Start SeaweedFS JDBC Server

First, start the SeaweedFS JDBC server:

```bash
# Start JDBC server on default port 8089
weed jdbc

# Or with custom configuration
weed jdbc -port=8090 -host=0.0.0.0 -master=master-server:9333
```

### 2. Add JDBC Driver to Your Project

#### Maven

```xml
<dependency>
    <groupId>com.seaweedfs</groupId>
    <artifactId>seaweedfs-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

#### Gradle

```gradle
implementation 'com.seaweedfs:seaweedfs-jdbc:1.0.0'
```

### 3. Connect and Query

```java
import java.sql.*;

public class SeaweedFSExample {
    public static void main(String[] args) throws SQLException {
        // JDBC URL format: jdbc:seaweedfs://host:port/database
        String url = "jdbc:seaweedfs://localhost:8089/default";
        
        // Connect to SeaweedFS
        Connection conn = DriverManager.getConnection(url);
        
        // Execute queries
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM my_topic LIMIT 10");
        
        // Process results
        while (rs.next()) {
            System.out.println("ID: " + rs.getLong("id"));
            System.out.println("Message: " + rs.getString("message"));
            System.out.println("Timestamp: " + rs.getTimestamp("_timestamp_ns"));
        }
        
        // Clean up
        rs.close();
        stmt.close();
        conn.close();
    }
}
```

## JDBC URL Format

```
jdbc:seaweedfs://host:port/database[?property=value&...]
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | localhost | SeaweedFS JDBC server hostname |
| `port` | 8089 | SeaweedFS JDBC server port |
| `database` | default | Database/namespace name |
| `connectTimeout` | 30000 | Connection timeout in milliseconds |
| `socketTimeout` | 0 | Socket timeout in milliseconds (0 = infinite) |

### Examples

```java
// Basic connection
"jdbc:seaweedfs://localhost:8089/default"

// Custom host and port
"jdbc:seaweedfs://seaweed-server:9000/production"

// With query parameters
"jdbc:seaweedfs://localhost:8089/default?connectTimeout=5000&socketTimeout=30000"
```

## Supported SQL Operations

### SELECT Queries
```sql
-- Basic select
SELECT * FROM topic_name;

-- With WHERE clause
SELECT id, message FROM topic_name WHERE id > 1000;

-- With LIMIT
SELECT * FROM topic_name ORDER BY _timestamp_ns DESC LIMIT 100;
```

### Aggregation Functions
```sql
-- Count records
SELECT COUNT(*) FROM topic_name;

-- Aggregations
SELECT 
    COUNT(*) as total_messages,
    MIN(id) as min_id,
    MAX(id) as max_id,
    AVG(amount) as avg_amount
FROM topic_name;
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
FROM topic_name;
```

### Schema Information
```sql
-- List databases
SHOW DATABASES;

-- List tables in current database
SHOW TABLES;

-- Describe table structure
DESCRIBE topic_name;
-- or
DESC topic_name;
```

## Database Tool Integration

### DBeaver

1. Download and install DBeaver
2. Create new connection → Generic JDBC
3. Settings:
   - **URL**: `jdbc:seaweedfs://localhost:8089/default`
   - **Driver Class**: `com.seaweedfs.jdbc.SeaweedFSDriver`
   - **Libraries**: Add `seaweedfs-jdbc-1.0.0.jar`

### IntelliJ DataGrip

1. Open DataGrip
2. Add New Data Source → Generic
3. Configure:
   - **URL**: `jdbc:seaweedfs://localhost:8089/default`
   - **Driver**: Add `seaweedfs-jdbc-1.0.0.jar`
   - **Driver Class**: `com.seaweedfs.jdbc.SeaweedFSDriver`

### Tableau

1. Connect to Data → More... → Generic JDBC
2. Configure:
   - **URL**: `jdbc:seaweedfs://localhost:8089/default`
   - **Driver Path**: Path to `seaweedfs-jdbc-1.0.0.jar`
   - **Class Name**: `com.seaweedfs.jdbc.SeaweedFSDriver`

## Advanced Usage

### Connection Pooling

```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:seaweedfs://localhost:8089/default");
config.setMaximumPoolSize(10);

HikariDataSource dataSource = new HikariDataSource(config);
Connection conn = dataSource.getConnection();
```

### PreparedStatements

```java
String sql = "SELECT * FROM topic_name WHERE id > ? AND created_date > ?";
PreparedStatement stmt = conn.prepareStatement(sql);
stmt.setLong(1, 1000);
stmt.setTimestamp(2, Timestamp.valueOf("2024-01-01 00:00:00"));

ResultSet rs = stmt.executeQuery();
while (rs.next()) {
    // Process results
}
```

### Metadata Access

```java
DatabaseMetaData metadata = conn.getMetaData();

// Get database information
System.out.println("Database: " + metadata.getDatabaseProductName());
System.out.println("Version: " + metadata.getDatabaseProductVersion());
System.out.println("Driver: " + metadata.getDriverName());

// Get table information
ResultSet tables = metadata.getTables(null, null, null, null);
while (tables.next()) {
    System.out.println("Table: " + tables.getString("TABLE_NAME"));
}
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/seaweedfs/seaweedfs.git
cd seaweedfs/jdbc-driver

# Build with Maven
mvn clean package

# Run tests
mvn test

# Install to local repository
mvn install
```

## Configuration

### Server-Side Configuration

The JDBC server supports the following command-line options:

```bash
weed jdbc -help
  -host string
        JDBC server host (default "localhost")
  -master string
        SeaweedFS master server address (default "localhost:9333")
  -port int
        JDBC server port (default 8089)
```

### Client-Side Configuration

Connection properties can be set via URL parameters or Properties object:

```java
Properties props = new Properties();
props.setProperty("connectTimeout", "10000");
props.setProperty("socketTimeout", "30000");

Connection conn = DriverManager.getConnection(
    "jdbc:seaweedfs://localhost:8089/default", props);
```

## Performance Tips

1. **Use LIMIT clauses**: Always limit result sets for large topics
2. **Filter early**: Use WHERE clauses to reduce data transfer
3. **Connection pooling**: Use connection pools for multi-threaded applications
4. **Batch operations**: Use batch statements for multiple queries
5. **Close resources**: Always close ResultSets, Statements, and Connections

## Limitations

- **Read-Only**: SeaweedFS JDBC driver only supports SELECT operations
- **No Transactions**: Transaction support is not available
- **Single Table**: Joins between tables are not supported
- **Limited SQL**: Only basic SQL SELECT syntax is supported

## Troubleshooting

### Connection Issues

```bash
# Test JDBC server connectivity
telnet localhost 8089

# Check SeaweedFS master connectivity
weed shell
> cluster.status
```

### Common Errors

**Error: "Connection refused"**
- Ensure JDBC server is running on the specified host/port
- Check firewall settings

**Error: "No suitable driver found"**
- Verify JDBC driver is in classpath
- Ensure correct driver class name: `com.seaweedfs.jdbc.SeaweedFSDriver`

**Error: "Topic not found"**
- Verify topic exists in SeaweedFS
- Check database/namespace name in connection URL

## Contributing

Contributions are welcome! Please see the main SeaweedFS repository for contribution guidelines.

## License

This JDBC driver is part of SeaweedFS and is licensed under the Apache License 2.0.

## Support

- **Documentation**: [SeaweedFS Wiki](https://github.com/seaweedfs/seaweedfs/wiki)
- **Issues**: [GitHub Issues](https://github.com/seaweedfs/seaweedfs/issues)
- **Discussions**: [GitHub Discussions](https://github.com/seaweedfs/seaweedfs/discussions)
- **Chat**: [SeaweedFS Slack](https://join.slack.com/t/seaweedfs/shared_invite/...)
