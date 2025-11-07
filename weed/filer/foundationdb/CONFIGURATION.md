# FoundationDB Filer Store Configuration Reference

This document provides comprehensive configuration options for the FoundationDB filer store.

## Configuration Methods

### 1. Configuration File (filer.toml)

```toml
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
api_version = 740
timeout = "5s"
max_retry_delay = "1s"
directory_prefix = "seaweedfs"
```

### 2. Environment Variables

All configuration options can be set via environment variables with the `WEED_FOUNDATIONDB_` prefix:

```bash
export WEED_FOUNDATIONDB_ENABLED=true
export WEED_FOUNDATIONDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
export WEED_FOUNDATIONDB_API_VERSION=720
export WEED_FOUNDATIONDB_TIMEOUT=5s
export WEED_FOUNDATIONDB_MAX_RETRY_DELAY=1s
export WEED_FOUNDATIONDB_DIRECTORY_PREFIX=seaweedfs
```

### 3. Command Line Arguments

While not directly supported, configuration can be specified via config files passed to the `weed` command.

## Configuration Options

### Basic Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable the FoundationDB filer store |
| `cluster_file` | string | `/etc/foundationdb/fdb.cluster` | Path to FoundationDB cluster file |
| `api_version` | integer | `740` | FoundationDB API version to use |

### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timeout` | duration | `5s` | Transaction timeout duration |
| `max_retry_delay` | duration | `1s` | Maximum delay between retries |

### Storage Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `directory_prefix` | string | `seaweedfs` | Directory prefix for key organization |

## Configuration Examples

### Development Environment

```toml
[foundationdb]
enabled = true
cluster_file = "/var/fdb/config/fdb.cluster"
api_version = 740
timeout = "10s"
max_retry_delay = "2s"
directory_prefix = "seaweedfs_dev"
```

### Production Environment

```toml
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
api_version = 740
timeout = "30s"
max_retry_delay = "5s"  
directory_prefix = "seaweedfs_prod"
```

### High-Performance Setup

```toml
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
api_version = 740
timeout = "60s"
max_retry_delay = "10s"
directory_prefix = "sw"  # Shorter prefix for efficiency
```

### Path-Specific Configuration

Configure different FoundationDB settings for different paths:

```toml
# Default configuration
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
directory_prefix = "seaweedfs_main"

# Backup path with different prefix
[foundationdb.backup]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
directory_prefix = "seaweedfs_backup"
location = "/backup"
timeout = "120s"

# Archive path with extended timeouts
[foundationdb.archive]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
directory_prefix = "seaweedfs_archive"
location = "/archive"
timeout = "300s"
max_retry_delay = "30s"
```

## Configuration Validation

### Required Settings

The following settings are required for FoundationDB to function:

1. `enabled = true`
2. `cluster_file` must point to a valid FoundationDB cluster file
3. `api_version` must match your FoundationDB installation

### Validation Rules

- `api_version` must be between 600 and 740
- `timeout` must be a valid duration string (e.g., "5s", "30s", "2m")
- `max_retry_delay` must be a valid duration string
- `cluster_file` must exist and be readable
- `directory_prefix` must not be empty

### Error Handling

Invalid configurations will result in startup errors:

```
FATAL: Failed to initialize store for foundationdb: invalid timeout duration
FATAL: Failed to initialize store for foundationdb: failed to open FoundationDB database
FATAL: Failed to initialize store for foundationdb: cluster file not found
```

## Performance Tuning

### Timeout Configuration

| Use Case | Timeout | Max Retry Delay | Notes |
|----------|---------|-----------------|-------|
| Interactive workloads | 5s | 1s | Fast response times |
| Batch processing | 60s | 10s | Handle large operations |
| Archive operations | 300s | 30s | Very large data sets |

### Connection Pool Settings

FoundationDB automatically manages connection pooling. No additional configuration needed.

### Directory Organization

Use meaningful directory prefixes to organize data:

```toml
# Separate environments
directory_prefix = "prod_seaweedfs"      # Production
directory_prefix = "staging_seaweedfs"   # Staging
directory_prefix = "dev_seaweedfs"       # Development

# Separate applications
directory_prefix = "app1_seaweedfs"      # Application 1
directory_prefix = "app2_seaweedfs"      # Application 2
```

## Security Configuration

### Cluster File Security

Protect the FoundationDB cluster file:

```bash
# Set proper permissions
sudo chown root:seaweedfs /etc/foundationdb/fdb.cluster
sudo chmod 640 /etc/foundationdb/fdb.cluster
```

### Network Security

FoundationDB supports TLS encryption. Configure in the cluster file:

```
description:cluster_id@tls(server1:4500,server2:4500,server3:4500)
```

### Access Control

Use FoundationDB's built-in access control mechanisms when available.

## Monitoring Configuration

### Health Check Settings

Configure health check timeouts appropriately:

```toml
[foundationdb]
enabled = true
timeout = "10s"  # Reasonable timeout for health checks
```

### Logging Configuration

Enable verbose logging for troubleshooting:

```bash
# Start SeaweedFS with debug logs
WEED_FOUNDATIONDB_ENABLED=true weed -v=2 server -filer
```

## Migration Configuration

### From Other Filer Stores

When migrating from other filer stores:

1. Configure both stores temporarily
2. Use path-specific configuration for gradual migration
3. Migrate data using SeaweedFS tools

```toml
# During migration - keep old store for reads
[leveldb2]
enabled = true
dir = "/old/filer/data"

# New writes go to FoundationDB
[foundationdb.migration]
enabled = true
location = "/new"
cluster_file = "/etc/foundationdb/fdb.cluster"
```

## Backup Configuration

### Metadata Backup Strategy

```toml
# Main storage
[foundationdb]
enabled = true
directory_prefix = "seaweedfs_main"

# Backup storage (different cluster recommended)
[foundationdb.backup]
enabled = true
cluster_file = "/etc/foundationdb/backup_fdb.cluster"
directory_prefix = "seaweedfs_backup"
location = "/backup"
```

## Container Configuration

### Docker Environment Variables

```bash
# Docker environment
WEED_FOUNDATIONDB_ENABLED=true
WEED_FOUNDATIONDB_CLUSTER_FILE=/var/fdb/config/fdb.cluster
WEED_FOUNDATIONDB_API_VERSION=720
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seaweedfs-config
data:
  filer.toml: |
    [foundationdb]
    enabled = true
    cluster_file = "/var/fdb/config/cluster_file"
    api_version = 740
    timeout = "30s"
    max_retry_delay = "5s"
    directory_prefix = "k8s_seaweedfs"
```

## Troubleshooting Configuration

### Debug Configuration

```toml
[foundationdb]
enabled = true
cluster_file = "/etc/foundationdb/fdb.cluster"
timeout = "60s"        # Longer timeouts for debugging
max_retry_delay = "10s"
directory_prefix = "debug_seaweedfs"
```

### Test Configuration

```toml
[foundationdb]
enabled = true
cluster_file = "/tmp/fdb.cluster"  # Test cluster
timeout = "5s"
directory_prefix = "test_seaweedfs"
```

## Configuration Best Practices

### 1. Environment Separation

Use different directory prefixes for different environments:
- Production: `prod_seaweedfs`
- Staging: `staging_seaweedfs` 
- Development: `dev_seaweedfs`

### 2. Timeout Settings

- Interactive: 5-10 seconds
- Batch: 30-60 seconds
- Archive: 120-300 seconds

### 3. Cluster File Management

- Use absolute paths for cluster files
- Ensure proper file permissions
- Keep backup copies of cluster files

### 4. Directory Naming

- Use descriptive prefixes
- Include environment/application identifiers
- Keep prefixes reasonably short for efficiency

### 5. Error Handling

- Configure appropriate timeouts
- Monitor retry patterns
- Set up alerting for configuration errors

## Configuration Testing

### Validation Script

```bash
#!/bin/bash
# Test FoundationDB configuration

# Check cluster file
if [ ! -f "$WEED_FOUNDATIONDB_CLUSTER_FILE" ]; then
    echo "ERROR: Cluster file not found: $WEED_FOUNDATIONDB_CLUSTER_FILE"
    exit 1
fi

# Test connection
fdbcli -C "$WEED_FOUNDATIONDB_CLUSTER_FILE" --exec 'status' > /dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Cannot connect to FoundationDB cluster"
    exit 1
fi

echo "Configuration validation passed"
```

### Integration Testing

```bash
# Test configuration with SeaweedFS
cd test/foundationdb
make check-env
make test-unit
```
