# FoundationDB Filer Store

This package provides a FoundationDB-based filer store for SeaweedFS, offering ACID transactions and horizontal scalability.

## Features

- **ACID Transactions**: Strong consistency guarantees with full ACID properties
- **Horizontal Scalability**: Automatic data distribution across multiple nodes
- **High Availability**: Built-in fault tolerance and automatic failover
- **Efficient Directory Operations**: Optimized for large directory listings
- **Key-Value Support**: Full KV operations for metadata storage
- **Compression**: Automatic compression for large entry chunks

## Installation

### Prerequisites

1. **FoundationDB Server**: Install and configure a FoundationDB cluster
2. **FoundationDB Client Libraries**: Install libfdb_c client libraries
3. **Go Build Tags**: Use the `foundationdb` build tag when compiling

### Building SeaweedFS with FoundationDB Support

```bash
go build -tags foundationdb -o weed
```

## Configuration

### Basic Configuration

Add the following to your `filer.toml`:

```toml
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
api_version = 740
timeout = "5s"
max_retry_delay = "1s"
directory_prefix = "seaweedfs"
```

### Configuration Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `enabled` | Enable FoundationDB filer store | `false` | Yes |
| `cluster_file` | Path to FDB cluster file | `/etc/foundationdb/fdb.cluster` | Yes |
| `api_version` | FoundationDB API version | `740` | No |
| `timeout` | Operation timeout duration | `5s` | No |
| `max_retry_delay` | Maximum retry delay | `1s` | No |
| `directory_prefix` | Directory prefix for organization | `seaweedfs` | No |

### Path-Specific Configuration

For path-specific filer stores:

```toml
[foundationdb.backup]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
directory_prefix = "seaweedfs_backup"
location = "/backup"
```

## Environment Variables

Configure via environment variables:

```bash
export WEED_FOUNDATIONDB_ENABLED=true
export WEED_FOUNDATIONDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
export WEED_FOUNDATIONDB_API_VERSION=740
export WEED_FOUNDATIONDB_TIMEOUT=5s
export WEED_FOUNDATIONDB_MAX_RETRY_DELAY=1s
export WEED_FOUNDATIONDB_DIRECTORY_PREFIX=seaweedfs
```

## FoundationDB Cluster Setup

### Single Node (Development)

```bash
# Start FoundationDB server
foundationdb start

# Initialize database
fdbcli --exec 'configure new single ssd'
```

### Multi-Node Cluster (Production)

1. **Install FoundationDB** on all nodes
2. **Configure cluster file** (`/etc/foundationdb/fdb.cluster`)
3. **Initialize cluster**:
   ```bash
   fdbcli --exec 'configure new double ssd'
   ```

### Docker Setup

Use the provided docker-compose.yml in `test/foundationdb/`:

```bash
cd test/foundationdb
make setup
```

## Performance Considerations

### Optimal Configuration

- **API Version**: Use the latest stable API version (720+)
- **Directory Structure**: Use logical directory prefixes to isolate different SeaweedFS instances
- **Transaction Size**: Keep transactions under 10MB (FDB limit)
- **Batch Operations**: Use transactions for multiple related operations

### Monitoring

Monitor FoundationDB cluster status:

```bash
fdbcli --exec 'status'
fdbcli --exec 'status details'
```

### Scaling

FoundationDB automatically handles:
- Data distribution across nodes
- Load balancing
- Automatic failover
- Storage node addition/removal

## Testing

### Unit Tests

```bash
cd weed/filer/foundationdb
go test -tags foundationdb -v
```

### Integration Tests

```bash
cd test/foundationdb
make test
```

### End-to-End Tests

```bash
cd test/foundationdb
make test-e2e
```

## Troubleshooting

### Common Issues

1. **Connection Failures**:
   - Verify cluster file path
   - Check FoundationDB server status
   - Validate network connectivity

2. **Transaction Conflicts**:
   - Reduce transaction scope
   - Implement retry logic
   - Check for concurrent operations

3. **Performance Issues**:
   - Monitor cluster health
   - Check data distribution
   - Optimize directory structure

### Debug Information

Enable verbose logging:

```bash
weed -v=2 server -filer
```

Check FoundationDB status:

```bash
fdbcli --exec 'status details'
```

## Security

### Network Security

- Configure TLS for FoundationDB connections
- Use firewall rules to restrict access
- Monitor connection attempts

### Data Encryption

- Enable encryption at rest in FoundationDB
- Use encrypted connections
- Implement proper key management

## Limitations

- Maximum transaction size: 10MB
- Single transaction timeout: configurable (default 5s)
- API version compatibility required
- Requires FoundationDB cluster setup

## Support

For issues specific to the FoundationDB filer store:
1. Check FoundationDB cluster status
2. Verify configuration settings
3. Review SeaweedFS logs with verbose output
4. Test with minimal reproduction case

For FoundationDB-specific issues, consult the [FoundationDB documentation](https://apple.github.io/foundationdb/).
