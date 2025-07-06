# Credential Store Integration

This document shows how the credential store has been integrated into SeaweedFS's S3 API and IAM API components.

## Quick Start

1. **Generate credential configuration:**
   ```bash
   weed scaffold -config=credential -output=.
   ```

2. **Edit credential.toml** to enable your preferred store (filer_etc is enabled by default)

3. **Start S3 API server** - it will automatically load credential.toml:
   ```bash
   weed s3 -filer=localhost:8888
   ```

## Integration Overview

The credential store provides a pluggable backend for storing S3 identities and credentials, supporting:
- **Filer-based storage** (filer_etc) - Uses existing filer storage (default)
- **SQLite** - Local database storage
- **PostgreSQL** - Shared database for multiple servers
- **Memory** - In-memory storage for testing

## Configuration

### Using credential.toml

Generate the configuration template:
```bash
weed scaffold -config=credential
```

This creates a `credential.toml` file with all available options. The filer_etc store is enabled by default:

```toml
# Filer-based credential store (default, uses existing filer storage)
[credential.filer_etc]
enabled = true

# SQLite credential store (recommended for single-node deployments)
[credential.sqlite]
enabled = false
file = "/var/lib/seaweedfs/credentials.db"

# PostgreSQL credential store (recommended for multi-node deployments)
[credential.postgres]
enabled = false
hostname = "localhost"
port = 5432
username = "seaweedfs"
password = "your_password"
database = "seaweedfs"

# Memory credential store (for testing only, data is lost on restart)
[credential.memory]
enabled = false
```

The credential.toml file is automatically loaded from these locations (in priority order):
- `./credential.toml`
- `$HOME/.seaweedfs/credential.toml`
- `/etc/seaweedfs/credential.toml`

### Server Configuration

Both S3 API and IAM API servers automatically load credential.toml during startup. No additional configuration is required.

## Usage Examples

### Filer-based Store (Default)

```toml
[credential.filer_etc]
enabled = true
```

This uses the existing filer storage and is compatible with current deployments.

### SQLite Store

```toml
[credential.sqlite]
enabled = true
file = "/var/lib/seaweedfs/credentials.db"
table_prefix = "sw_"
```

### PostgreSQL Store

```toml
[credential.postgres]
enabled = true
hostname = "localhost"
port = 5432
username = "seaweedfs"
password = "your_password"
database = "seaweedfs"
schema = "public"
sslmode = "disable"
table_prefix = "sw_"
connection_max_idle = 10
connection_max_open = 100
connection_max_lifetime_seconds = 3600
```

### Memory Store (Testing)

```toml
[credential.memory]
enabled = true
```

## Environment Variables

All credential configuration can be overridden with environment variables:

```bash
# Override PostgreSQL password
export WEED_CREDENTIAL_POSTGRES_PASSWORD=secret

# Override SQLite file path
export WEED_CREDENTIAL_SQLITE_FILE=/custom/path/credentials.db

# Override PostgreSQL hostname
export WEED_CREDENTIAL_POSTGRES_HOSTNAME=db.example.com

# Enable/disable stores
export WEED_CREDENTIAL_FILER_ETC_ENABLED=true
export WEED_CREDENTIAL_SQLITE_ENABLED=false
```

Rules:
- Prefix with `WEED_CREDENTIAL_`
- Convert to uppercase
- Replace `.` with `_`

## Implementation Details

Components automatically load credential configuration during startup:

```go
// Server initialization
if credConfig, err := credential.LoadCredentialConfiguration(); err == nil && credConfig != nil {
    credentialManager, err := credential.NewCredentialManager(
        credConfig.Store,
        credConfig.Config,
        credConfig.Prefix,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to initialize credential manager: %v", err)
    }
    // Use credential manager for operations
}
```

## Benefits

1. **Easy Configuration** - Generate template with `weed scaffold -config=credential`
2. **Pluggable Storage** - Switch between filer_etc, SQLite, PostgreSQL without code changes
3. **Backward Compatibility** - Filer-based storage works with existing deployments
4. **Scalability** - Database stores support multiple concurrent servers
5. **Performance** - Database access can be faster than file-based storage
6. **Testing** - Memory store simplifies unit testing
7. **Environment Override** - All settings can be overridden with environment variables

## Error Handling

When a credential store is configured, it must initialize successfully or the server will fail to start:

```go
if credConfig != nil {
    credentialManager, err = credential.NewCredentialManager(...)
    if err != nil {
        return nil, fmt.Errorf("failed to initialize credential manager: %v", err)
    }
}
```

This ensures explicit configuration - if you configure a credential store, it must work properly. 