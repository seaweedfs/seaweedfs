# SeaweedFS Hadoop3 Client

Hadoop FileSystem implementation for SeaweedFS, compatible with Hadoop 3.x.

## Building

```bash
mvn clean install
```

## Testing

This project includes two types of tests:

### 1. Configuration Tests (No SeaweedFS Required)

These tests verify configuration handling and initialization logic without requiring a running SeaweedFS instance:

```bash
mvn test -Dtest=SeaweedFileSystemConfigTest
```

### 2. Integration Tests (Requires SeaweedFS)

These tests verify actual FileSystem operations against a running SeaweedFS instance.

#### Prerequisites

1. Start SeaweedFS with default ports:
   ```bash
   # Terminal 1: Start master
   weed master
   
   # Terminal 2: Start volume server
   weed volume -master=localhost:9333
   
   # Terminal 3: Start filer
   weed filer -master=localhost:9333
   ```

2. Verify services are running:
   - Master: http://localhost:9333
   - Filer HTTP: http://localhost:8888
   - Filer gRPC: localhost:18888

#### Running Integration Tests

```bash
# Enable integration tests
export SEAWEEDFS_TEST_ENABLED=true

# Run all tests
mvn test

# Run specific test
mvn test -Dtest=SeaweedFileSystemTest
```

### Test Configuration

Integration tests can be configured via environment variables or system properties:

- `SEAWEEDFS_TEST_ENABLED`: Set to `true` to enable integration tests (default: false)
- Tests use these default connection settings:
  - Filer Host: localhost
  - Filer HTTP Port: 8888
  - Filer gRPC Port: 18888

### Running Tests with Custom Configuration

To test against a different SeaweedFS instance, modify the test code or use Hadoop configuration:

```java
conf.set("fs.seaweed.filer.host", "your-host");
conf.setInt("fs.seaweed.filer.port", 8888);
conf.setInt("fs.seaweed.filer.port.grpc", 18888);
```

## Test Coverage

The test suite covers:

- **Configuration & Initialization**
  - URI parsing and configuration
  - Default values
  - Configuration overrides
  - Working directory management

- **File Operations**
  - Create files
  - Read files
  - Write files
  - Append to files
  - Delete files

- **Directory Operations**
  - Create directories
  - List directory contents
  - Delete directories (recursive and non-recursive)

- **Metadata Operations**
  - Get file status
  - Set permissions
  - Set owner/group
  - Rename files and directories

## Usage in Hadoop

1. Copy the built JAR to your Hadoop classpath:
   ```bash
   cp target/seaweedfs-hadoop3-client-*.jar $HADOOP_HOME/share/hadoop/common/lib/
   ```

2. Configure `core-site.xml`:
   ```xml
   <configuration>
     <property>
       <name>fs.seaweedfs.impl</name>
       <value>seaweed.hdfs.SeaweedFileSystem</value>
     </property>
     <property>
       <name>fs.seaweed.filer.host</name>
       <value>localhost</value>
     </property>
     <property>
       <name>fs.seaweed.filer.port</name>
       <value>8888</value>
     </property>
     <property>
       <name>fs.seaweed.filer.port.grpc</name>
       <value>18888</value>
     </property>
     <!-- Optional: Replication configuration with three priority levels:
          1) If set to non-empty value (e.g. "001") - uses that value
          2) If set to empty string "" - uses SeaweedFS filer's default replication
          3) If not configured (property not present) - uses HDFS replication parameter
     -->
     <!-- <property>
       <name>fs.seaweed.replication</name>
       <value>001</value>
     </property> -->
   </configuration>
   ```

3. Use SeaweedFS with Hadoop commands:
   ```bash
   hadoop fs -ls seaweedfs://localhost:8888/
   hadoop fs -mkdir seaweedfs://localhost:8888/test
   hadoop fs -put local.txt seaweedfs://localhost:8888/test/
   ```

## Continuous Integration

For CI environments, tests can be run in two modes:

1. **Configuration Tests Only** (default, no SeaweedFS required):
   ```bash
   mvn test -Dtest=SeaweedFileSystemConfigTest
   ```

2. **Full Integration Tests** (requires SeaweedFS):
   ```bash
   # Start SeaweedFS in CI environment
   # Then run:
   export SEAWEEDFS_TEST_ENABLED=true
   mvn test
   ```

## Troubleshooting

### Tests are skipped

If you see "Skipping test - SEAWEEDFS_TEST_ENABLED not set":
```bash
export SEAWEEDFS_TEST_ENABLED=true
```

### Connection refused errors

Ensure SeaweedFS is running and accessible:
```bash
curl http://localhost:8888/
```

### gRPC errors

Verify the gRPC port is accessible:
```bash
# Should show the port is listening
netstat -an | grep 18888
```

## Contributing

When adding new features, please include:
1. Configuration tests (no SeaweedFS required)
2. Integration tests (with SEAWEEDFS_TEST_ENABLED guard)
3. Documentation updates

