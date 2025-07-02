# SeaweedFS Credential Storage Interface

This package provides a pluggable interface for storing user credentials in SeaweedFS. It supports multiple storage backends including the current file-based approach and database backends.

## Available Stores

### Filer Etc Store (`file`)
The default implementation that stores credentials in SeaweedFS files at `/etc/iam/identity.json`. This is the current approach and maintains backward compatibility.

### SQLite Store (`sqlite`) 
Stores credentials in a local SQLite database. Useful for single-node deployments or when you want local credential storage.

### PostgreSQL Store (`postgres`)
Stores credentials in a PostgreSQL database. Suitable for production deployments with multiple nodes.

## Configuration

### Filer Etc Store Configuration
```toml
[credential]
store = "file"
filer = "localhost:8888"
```

### SQLite Store Configuration
```toml
[credential]
store = "sqlite"
dbFile = "/path/to/seaweedfs_credentials.db"
```

### PostgreSQL Store Configuration
```toml
[credential]
store = "postgres"
hostname = "localhost"
port = 5432
username = "seaweedfs"
password = "password"
database = "seaweedfs"
schema = "public"
sslmode = "disable"
connection_max_idle = 10
connection_max_open = 100
connection_max_lifetime_seconds = 3600
```

## Usage

### Admin Server Integration
The admin server automatically uses the credential manager when initialized:

```go
// Initialize admin server
adminServer := dash.NewAdminServer(masterAddress, templateFS)

// Initialize credential manager
config := util.LoadConfiguration("seaweedfs", false)
err := adminServer.InitializeCredentialManager("postgres", config, "credential.")
if err != nil {
    log.Fatalf("Failed to initialize credential manager: %v", err)
}
```

### Direct Usage
You can also use the credential manager directly:

```go
import "github.com/seaweedfs/seaweedfs/weed/credential"

// Create credential manager
config := util.LoadConfiguration("seaweedfs", false)
cm, err := credential.NewCredentialManager("sqlite", config, "credential.")
if err != nil {
    log.Fatalf("Failed to create credential manager: %v", err)
}
defer cm.Shutdown()

// Create a user
identity := &iam_pb.Identity{
    Name: "testuser",
    Actions: []string{"Read", "Write"},
    Account: &iam_pb.Account{
        Id: "123456789012",
        DisplayName: "Test User",
        EmailAddress: "test@example.com",
    },
    Credentials: []*iam_pb.Credential{
        {
            AccessKey: "AKIAIOSFODNN7EXAMPLE",
            SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        },
    },
}

err = cm.CreateUser(context.Background(), identity)
if err != nil {
    log.Printf("Failed to create user: %v", err)
}
```

## Migration

### From Filer Etc Storage to Database
1. Export existing credentials from filer_etc storage
2. Configure the new database store
3. Import credentials to the database
4. Update configuration to use the new store

### Backward Compatibility
The system maintains backward compatibility by falling back to the legacy file-based approach when no credential manager is configured.

## Database Schema

### SQLite/PostgreSQL Schema
```sql
CREATE TABLE users (
    username VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255),
    account_data TEXT,
    actions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE credentials (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    access_key VARCHAR(255) UNIQUE NOT NULL,
    secret_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE
);

CREATE INDEX idx_credentials_username ON credentials(username);
CREATE INDEX idx_credentials_access_key ON credentials(access_key);
```

## Adding New Store Implementations

To add a new credential store implementation:

1. Implement the `CredentialStore` interface
2. Add the store to the `Stores` slice in the `init()` function
3. Add configuration documentation

Example:
```go
type MyStore struct {
    // your fields
}

func (store *MyStore) GetName() string {
    return "mystore"
}

func (store *MyStore) Initialize(configuration util.Configuration, prefix string) error {
    // initialization logic
    return nil
}

// Implement other interface methods...

func init() {
    credential.Stores = append(credential.Stores, &MyStore{})
}
``` 