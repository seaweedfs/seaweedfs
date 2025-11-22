# SeaweedFS Spark Integration Tests

Comprehensive integration tests for Apache Spark with SeaweedFS HDFS client.

## Overview

This test suite validates that Apache Spark works correctly with SeaweedFS as the storage backend, covering:

- **Data I/O**: Reading and writing data in various formats (Parquet, CSV, JSON)
- **Spark SQL**: Complex SQL queries, joins, aggregations, and window functions
- **Partitioning**: Partitioned writes and partition pruning
- **Performance**: Large dataset operations

## Prerequisites

### 1. Running SeaweedFS

Start SeaweedFS with default ports:

```bash
# Terminal 1: Start master
weed master

# Terminal 2: Start volume server
weed volume -mserver=localhost:9333

# Terminal 3: Start filer
weed filer -master=localhost:9333
```

Verify services are running:
- Master: http://localhost:9333
- Filer HTTP: http://localhost:8888
- Filer gRPC: localhost:18888

### 2. Java and Maven

- Java 8 or higher
- Maven 3.6 or higher

### 3. Apache Spark (for standalone execution)

Download and extract Apache Spark 3.5.0:

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=$(pwd)/spark-3.5.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

## Building

```bash
mvn clean package
```

This creates:
- Test JAR: `target/seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar`
- Fat JAR (with dependencies): `target/original-seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar`

## Running Integration Tests

### Quick Test

Run all integration tests (requires running SeaweedFS):

```bash
# Enable integration tests
export SEAWEEDFS_TEST_ENABLED=true

# Run all tests
mvn test
```

### Run Specific Test

```bash
export SEAWEEDFS_TEST_ENABLED=true

# Run only read/write tests
mvn test -Dtest=SparkReadWriteTest

# Run only SQL tests
mvn test -Dtest=SparkSQLTest
```

### Custom SeaweedFS Configuration

If your SeaweedFS is running on a different host or port:

```bash
export SEAWEEDFS_TEST_ENABLED=true
export SEAWEEDFS_FILER_HOST=my-seaweedfs-host
export SEAWEEDFS_FILER_PORT=8888
export SEAWEEDFS_FILER_GRPC_PORT=18888

mvn test
```

### Skip Tests

By default, tests are skipped if `SEAWEEDFS_TEST_ENABLED` is not set:

```bash
mvn test  # Tests will be skipped with message
```

## Running the Example Application

### Local Mode

Run the example application in Spark local mode:

```bash
spark-submit \
  --class seaweed.spark.SparkSeaweedFSExample \
  --master local[2] \
  --conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
  --conf spark.hadoop.fs.seaweed.filer.host=localhost \
  --conf spark.hadoop.fs.seaweed.filer.port=8888 \
  --conf spark.hadoop.fs.seaweed.filer.port.grpc=18888 \
  --conf spark.hadoop.fs.seaweed.replication="" \
  target/seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar \
  seaweedfs://localhost:8888/spark-example-output
```

### Cluster Mode

For production Spark clusters:

```bash
spark-submit \
  --class seaweed.spark.SparkSeaweedFSExample \
  --master spark://master-host:7077 \
  --deploy-mode cluster \
  --conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
  --conf spark.hadoop.fs.seaweed.filer.host=seaweedfs-filer \
  --conf spark.hadoop.fs.seaweed.filer.port=8888 \
  --conf spark.hadoop.fs.seaweed.filer.port.grpc=18888 \
  --conf spark.hadoop.fs.seaweed.replication=001 \
  --conf spark.executor.instances=4 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  target/seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar \
  seaweedfs://seaweedfs-filer:8888/spark-output
```

## Configuration

### SeaweedFS Configuration Options

Configure Spark to use SeaweedFS through Hadoop configuration:

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `spark.hadoop.fs.seaweedfs.impl` | FileSystem implementation class | - | `seaweed.hdfs.SeaweedFileSystem` |
| `spark.hadoop.fs.seaweed.filer.host` | SeaweedFS filer hostname | `localhost` | `seaweedfs-filer` |
| `spark.hadoop.fs.seaweed.filer.port` | SeaweedFS filer HTTP port | `8888` | `8888` |
| `spark.hadoop.fs.seaweed.filer.port.grpc` | SeaweedFS filer gRPC port | `18888` | `18888` |
| `spark.hadoop.fs.seaweed.replication` | Replication strategy | (uses HDFS default) | `001`, `""` (filer default) |
| `spark.hadoop.fs.seaweed.buffer.size` | Buffer size for I/O | `4MB` | `8388608` |

### Replication Configuration Priority

1. **Non-empty value** (e.g., `001`) - uses that specific replication
2. **Empty string** (`""`) - uses SeaweedFS filer's default replication
3. **Not configured** - uses Hadoop/Spark's replication parameter

## Test Coverage

### SparkReadWriteTest

- ✓ Write and read Parquet files
- ✓ Write and read CSV files with headers
- ✓ Write and read JSON files
- ✓ Partitioned data writes with partition pruning
- ✓ Append mode operations
- ✓ Large dataset handling (10,000+ rows)

### SparkSQLTest

- ✓ Create tables and run SELECT queries
- ✓ Aggregation queries (GROUP BY, SUM, AVG)
- ✓ JOIN operations between datasets
- ✓ Window functions (RANK, PARTITION BY)

## Continuous Integration

### GitHub Actions

A GitHub Actions workflow is configured at `.github/workflows/spark-integration-tests.yml` that automatically:
- Runs on push/PR to `master`/`main` when Spark or HDFS code changes
- Starts SeaweedFS in Docker
- Runs all integration tests
- Runs the example application
- Uploads test reports
- Can be triggered manually via workflow_dispatch

The workflow includes two jobs:
1. **spark-tests**: Runs all integration tests (10 tests)
2. **spark-example**: Runs the example Spark application

View the workflow status in the GitHub Actions tab of the repository.

### CI-Friendly Test Execution

```bash
# In CI environment
./scripts/start-seaweedfs.sh  # Start SeaweedFS in background
export SEAWEEDFS_TEST_ENABLED=true
mvn clean test
./scripts/stop-seaweedfs.sh   # Cleanup
```

### Docker-Based Testing

Use docker-compose for isolated testing:

```bash
docker-compose up -d seaweedfs
export SEAWEEDFS_TEST_ENABLED=true
mvn test
docker-compose down
```

## Troubleshooting

### Tests are Skipped

**Symptom**: Tests show "Skipping test - SEAWEEDFS_TEST_ENABLED not set"

**Solution**:
```bash
export SEAWEEDFS_TEST_ENABLED=true
mvn test
```

### Connection Refused Errors

**Symptom**: `java.net.ConnectException: Connection refused`

**Solution**:
1. Verify SeaweedFS is running:
   ```bash
   curl http://localhost:8888/
   ```

2. Check if ports are accessible:
   ```bash
   netstat -an | grep 8888
   netstat -an | grep 18888
   ```

### ClassNotFoundException: seaweed.hdfs.SeaweedFileSystem

**Symptom**: Spark cannot find the SeaweedFS FileSystem implementation

**Solution**:
1. Ensure the SeaweedFS HDFS client is in your classpath
2. For spark-submit, add the JAR:
   ```bash
   spark-submit --jars /path/to/seaweedfs-hadoop3-client-*.jar ...
   ```

### Out of Memory Errors

**Symptom**: `java.lang.OutOfMemoryError: Java heap space`

**Solution**:
```bash
mvn test -DargLine="-Xmx4g"
```

For spark-submit:
```bash
spark-submit --driver-memory 4g --executor-memory 4g ...
```

### gRPC Version Conflicts

**Symptom**: `java.lang.NoSuchMethodError` related to gRPC

**Solution**: Ensure consistent gRPC versions. The project uses Spark 3.5.0 compatible versions.

## Performance Tips

1. **Increase buffer size** for large files:
   ```bash
   --conf spark.hadoop.fs.seaweed.buffer.size=8388608
   ```

2. **Use appropriate replication** based on your cluster:
   ```bash
   --conf spark.hadoop.fs.seaweed.replication=001
   ```

3. **Enable partition pruning** by partitioning data on commonly filtered columns

4. **Use columnar formats** (Parquet) for better performance

## Additional Examples

### PySpark with SeaweedFS

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkSeaweedFS") \
    .config("spark.hadoop.fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem") \
    .config("spark.hadoop.fs.seaweed.filer.host", "localhost") \
    .config("spark.hadoop.fs.seaweed.filer.port", "8888") \
    .config("spark.hadoop.fs.seaweed.filer.port.grpc", "18888") \
    .getOrCreate()

# Write data
df = spark.range(1000)
df.write.parquet("seaweedfs://localhost:8888/pyspark-output")

# Read data
df_read = spark.read.parquet("seaweedfs://localhost:8888/pyspark-output")
df_read.show()
```

### Scala with SeaweedFS

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("ScalaSeaweedFS")
  .config("spark.hadoop.fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem")
  .config("spark.hadoop.fs.seaweed.filer.host", "localhost")
  .config("spark.hadoop.fs.seaweed.filer.port", "8888")
  .config("spark.hadoop.fs.seaweed.filer.port.grpc", "18888")
  .getOrCreate()

// Write data
val df = spark.range(1000)
df.write.parquet("seaweedfs://localhost:8888/scala-output")

// Read data
val dfRead = spark.read.parquet("seaweedfs://localhost:8888/scala-output")
dfRead.show()
```

## Contributing

When adding new tests:

1. Extend `SparkTestBase` for new test classes
2. Use `skipIfTestsDisabled()` in test methods
3. Clean up test data in tearDown
4. Add documentation to this README
5. Ensure tests work in CI environment

## License

Same as SeaweedFS project.

