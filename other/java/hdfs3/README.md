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

### Secured filer

With `fs.seaweed.volume.server.access` set to `filerProxy`, chunk reads and writes go to the
filer's HTTP port, so they need the filer's HTTP credentials. The client signs a filer JWT
when it has the filer's `jwt.filer_signing` key (writes) or
`jwt.filer_signing.read` key (reads), and otherwise sends the `[filer.basic_auth]`
credentials. These credentials are sent only to the filer, never to volume servers. gRPC mTLS
comes from the `[grpc]` section as before, but it does not cover these HTTP chunk requests:
to keep the credentials off plain HTTP, serve the filer HTTP port over HTTPS (`[https.filer]`
on the filer) and enable `[https.client]` here.

`security.toml` (read from `./`, `~/.seaweedfs/` or `/etc/seaweedfs/`), using the filer's
own values:

```toml
[jwt.filer_signing]
key = "<filer write key>"

[jwt.filer_signing.read]
key = "<filer read key>"

# or, for a filer with Basic Auth
[filer.basic_auth]
user = "<user>"
password = "<password>"

[grpc]
ca = "/etc/seaweedfs/ca.crt"

[grpc.client]
cert = "/etc/seaweedfs/client.crt"
key = "/etc/seaweedfs/client.key"

[https.client]
enabled = true
ca = "/etc/seaweedfs/ca.crt"
cert = "/etc/seaweedfs/client.crt"
key = "/etc/seaweedfs/client.key"
```

The same values can be set in `core-site.xml`. Each option overrides only its own
`security.toml` value (for example, a password alone keeps the `security.toml` user), and
applies only to that filesystem. The secrets can also come from a Hadoop credential provider:

```xml
<property>
  <name>fs.seaweed.volume.server.access</name>
  <value>filerProxy</value>
</property>
<property>
  <name>fs.seaweed.filer.jwt.signing.key</name>
  <value>filer write key</value>
</property>
<property>
  <name>fs.seaweed.filer.jwt.signing.read.key</name>
  <value>filer read key</value>
</property>
<!-- token lifetimes, defaults 10 and 60 seconds:
     fs.seaweed.filer.jwt.signing.expires.after.seconds
     fs.seaweed.filer.jwt.signing.read.expires.after.seconds -->
<!-- or Basic Auth:
     fs.seaweed.filer.basic.auth.user
     fs.seaweed.filer.basic.auth.password -->
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

