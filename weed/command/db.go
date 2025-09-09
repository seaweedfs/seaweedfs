package command

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/server/postgres"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	dbOptions DBOptions
)

type DBOptions struct {
	host        *string
	port        *int
	masterAddr  *string
	authMethod  *string
	users       *string
	database    *string
	maxConns    *int
	idleTimeout *string
	tlsCert     *string
	tlsKey      *string
}

func init() {
	cmdDB.Run = runDB // break init cycle
	dbOptions.host = cmdDB.Flag.String("host", "localhost", "Database server host")
	dbOptions.port = cmdDB.Flag.Int("port", 5432, "Database server port")
	dbOptions.masterAddr = cmdDB.Flag.String("master", "localhost:9333", "SeaweedFS master server address")
	dbOptions.authMethod = cmdDB.Flag.String("auth", "trust", "Authentication method: trust, password, md5")
	dbOptions.users = cmdDB.Flag.String("users", "", "User credentials for auth (JSON format '{\"user1\":\"pass1\",\"user2\":\"pass2\"}' or file '@/path/to/users.json')")
	dbOptions.database = cmdDB.Flag.String("database", "default", "Default database name")
	dbOptions.maxConns = cmdDB.Flag.Int("max-connections", 100, "Maximum concurrent connections per server")
	dbOptions.idleTimeout = cmdDB.Flag.String("idle-timeout", "1h", "Connection idle timeout")
	dbOptions.tlsCert = cmdDB.Flag.String("tls-cert", "", "TLS certificate file path")
	dbOptions.tlsKey = cmdDB.Flag.String("tls-key", "", "TLS private key file path")
}

var cmdDB = &Command{
	UsageLine: "db -port=5432 -master=<master_server>",
	Short:     "start a PostgreSQL-compatible database server for SQL queries",
	Long: `Start a PostgreSQL wire protocol compatible database server that provides SQL query access to SeaweedFS.

This database server enables any PostgreSQL client, tool, or application to connect to SeaweedFS
and execute SQL queries against MQ topics. It implements the PostgreSQL wire protocol for maximum
compatibility with the existing PostgreSQL ecosystem.

Examples:

	# Start database server on default port 5432
	weed db
	
	# Start with MD5 authentication using JSON format (recommended)
	weed db -auth=md5 -users='{"admin":"secret","readonly":"view123"}'
	
	# Start with complex passwords using JSON format
	weed db -auth=md5 -users='{"admin":"pass;with;semicolons","user":"password:with:colons"}'
	
	# Start with credentials from JSON file (most secure)
	weed db -auth=md5 -users="@/etc/seaweedfs/users.json"
	
	# Start with custom port and master
	weed db -port=5433 -master=master1:9333
	
	# Allow connections from any host
	weed db -host=0.0.0.0 -port=5432
	
	# Start with TLS encryption
	weed db -tls-cert=server.crt -tls-key=server.key

Client Connection Examples:

	# psql command line client
	psql "host=localhost port=5432 dbname=default user=seaweedfs"
	psql -h localhost -p 5432 -U seaweedfs -d default
	
	# With password
	PGPASSWORD=secret psql -h localhost -p 5432 -U admin -d default
	
	# Connection string
	psql "postgresql://admin:secret@localhost:5432/default"

Programming Language Examples:

	# Python (psycopg2)
	import psycopg2
	conn = psycopg2.connect(
		host="localhost", port=5432, 
		user="seaweedfs", database="default"
	)
	
	# Java JDBC
	String url = "jdbc:postgresql://localhost:5432/default";
	Connection conn = DriverManager.getConnection(url, "seaweedfs", "");
	
	# Go (lib/pq)
	db, err := sql.Open("postgres", "host=localhost port=5432 user=seaweedfs dbname=default sslmode=disable")
	
	# Node.js (pg)
	const client = new Client({
		host: 'localhost', port: 5432,
		user: 'seaweedfs', database: 'default'
	});

Supported SQL Operations:
	- SELECT queries on MQ topics
	- DESCRIBE/DESC table_name commands
	- EXPLAIN query execution plans
	- SHOW DATABASES/TABLES commands  
	- Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
	- WHERE clauses with filtering
	- System columns (_timestamp_ns, _key, _source)
	- Basic PostgreSQL system queries (version(), current_database(), current_user)

Authentication Methods:
	- trust: No authentication required (default)
	- password: Clear text password authentication
	- md5: MD5 password authentication

User Credential Formats:
	- JSON format: '{"user1":"pass1","user2":"pass2"}' (supports any special characters)
	- File format: "@/path/to/users.json" (JSON file)
	
	Note: JSON format supports passwords with semicolons, colons, and any other special characters.
	      File format is recommended for production to keep credentials secure.

Compatible Tools:
	- psql (PostgreSQL command line client)
	- Any PostgreSQL JDBC/ODBC compatible tool

Security Features:
	- Multiple authentication methods
	- TLS encryption support
	- Read-only access (no data modification)

Performance Features:
	- Fast path aggregation optimization (COUNT, MIN, MAX without WHERE clauses)
	- Hybrid data scanning (parquet files + live logs)
	- PostgreSQL wire protocol
	- Query result streaming

`,
}

func runDB(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	// Validate options
	if *dbOptions.masterAddr == "" {
		fmt.Fprintf(os.Stderr, "Error: master address is required\n")
		return false
	}

	// Parse authentication method
	authMethod, err := parseAuthMethod(*dbOptions.authMethod)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return false
	}

	// Parse user credentials
	users, err := parseUsers(*dbOptions.users, authMethod)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return false
	}

	// Parse idle timeout
	idleTimeout, err := time.ParseDuration(*dbOptions.idleTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing idle timeout: %v\n", err)
		return false
	}

	// Validate port number
	if err := validatePortNumber(*dbOptions.port); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return false
	}

	// Setup TLS if requested
	var tlsConfig *tls.Config
	if *dbOptions.tlsCert != "" && *dbOptions.tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(*dbOptions.tlsCert, *dbOptions.tlsKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading TLS certificates: %v\n", err)
			return false
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// Create server configuration
	config := &postgres.PostgreSQLServerConfig{
		Host:        *dbOptions.host,
		Port:        *dbOptions.port,
		AuthMethod:  authMethod,
		Users:       users,
		Database:    *dbOptions.database,
		MaxConns:    *dbOptions.maxConns,
		IdleTimeout: idleTimeout,
		TLSConfig:   tlsConfig,
	}

	// Create database server
	dbServer, err := postgres.NewPostgreSQLServer(config, *dbOptions.masterAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating database server: %v\n", err)
		return false
	}

	// Print startup information
	fmt.Printf("Starting SeaweedFS Database Server...\n")
	fmt.Printf("Host: %s\n", *dbOptions.host)
	fmt.Printf("Port: %d\n", *dbOptions.port)
	fmt.Printf("Master: %s\n", *dbOptions.masterAddr)
	fmt.Printf("Database: %s\n", *dbOptions.database)
	fmt.Printf("Auth Method: %s\n", *dbOptions.authMethod)
	fmt.Printf("Max Connections: %d\n", *dbOptions.maxConns)
	fmt.Printf("Idle Timeout: %s\n", *dbOptions.idleTimeout)
	if tlsConfig != nil {
		fmt.Printf("TLS: Enabled\n")
	} else {
		fmt.Printf("TLS: Disabled\n")
	}
	if len(users) > 0 {
		fmt.Printf("Users: %d configured\n", len(users))
	}

	fmt.Printf("\nDatabase Connection Examples:\n")
	fmt.Printf("  psql -h %s -p %d -U seaweedfs -d %s\n", *dbOptions.host, *dbOptions.port, *dbOptions.database)
	if len(users) > 0 {
		// Show first user as example
		for username := range users {
			fmt.Printf("  psql -h %s -p %d -U %s -d %s\n", *dbOptions.host, *dbOptions.port, username, *dbOptions.database)
			break
		}
	}
	fmt.Printf("  postgresql://%s:%d/%s\n", *dbOptions.host, *dbOptions.port, *dbOptions.database)

	fmt.Printf("\nSupported Operations:\n")
	fmt.Printf("  - SELECT queries on MQ topics\n")
	fmt.Printf("  - DESCRIBE/DESC table_name\n")
	fmt.Printf("  - EXPLAIN query execution plans\n")
	fmt.Printf("  - SHOW DATABASES/TABLES\n")
	fmt.Printf("  - Aggregations: COUNT, SUM, AVG, MIN, MAX\n")
	fmt.Printf("  - System columns: _timestamp_ns, _key, _source\n")
	fmt.Printf("  - Basic PostgreSQL system queries\n")

	fmt.Printf("\nReady for database connections!\n\n")

	// Start the server
	err = dbServer.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting database server: %v\n", err)
		return false
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	fmt.Printf("\nReceived shutdown signal, stopping database server...\n")

	// Create context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop the server with timeout
	done := make(chan error, 1)
	go func() {
		done <- dbServer.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error stopping database server: %v\n", err)
			return false
		}
		fmt.Printf("Database server stopped successfully\n")
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "Timeout waiting for database server to stop\n")
		return false
	}

	return true
}

// parseAuthMethod parses the authentication method string
func parseAuthMethod(method string) (postgres.AuthMethod, error) {
	switch strings.ToLower(method) {
	case "trust":
		return postgres.AuthTrust, nil
	case "password":
		return postgres.AuthPassword, nil
	case "md5":
		return postgres.AuthMD5, nil
	default:
		return postgres.AuthTrust, fmt.Errorf("unsupported auth method '%s'. Supported: trust, password, md5", method)
	}
}

// parseUsers parses the user credentials string with support for secure formats only
// Supported formats:
// 1. JSON format: {"username":"password","username2":"password2"}
// 2. File format: /path/to/users.json or @/path/to/users.json
func parseUsers(usersStr string, authMethod postgres.AuthMethod) (map[string]string, error) {
	users := make(map[string]string)

	if usersStr == "" {
		// No users specified
		if authMethod != postgres.AuthTrust {
			return nil, fmt.Errorf("users must be specified when auth method is not 'trust'")
		}
		return users, nil
	}

	// Trim whitespace
	usersStr = strings.TrimSpace(usersStr)

	// Determine format and parse accordingly
	if strings.HasPrefix(usersStr, "{") && strings.HasSuffix(usersStr, "}") {
		// JSON format
		return parseUsersJSON(usersStr, authMethod)
	}

	// Check if it's a file path (with or without @ prefix) before declaring invalid format
	filePath := strings.TrimPrefix(usersStr, "@")
	if _, err := os.Stat(filePath); err == nil {
		// File format
		return parseUsersFile(usersStr, authMethod) // Pass original string to preserve @ handling
	}

	// Invalid format
	return nil, fmt.Errorf("invalid user credentials format. Use JSON format '{\"user\":\"pass\"}' or file format '@/path/to/users.json' or 'path/to/users.json'. Legacy semicolon-separated format is no longer supported")
}

// parseUsersJSON parses user credentials from JSON format
func parseUsersJSON(jsonStr string, authMethod postgres.AuthMethod) (map[string]string, error) {
	var users map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &users); err != nil {
		return nil, fmt.Errorf("invalid JSON format for users: %v", err)
	}

	// Validate users
	for username, password := range users {
		if username == "" {
			return nil, fmt.Errorf("empty username in JSON user specification")
		}
		if authMethod != postgres.AuthTrust && password == "" {
			return nil, fmt.Errorf("empty password for user '%s' with auth method", username)
		}
	}

	return users, nil
}

// parseUsersFile parses user credentials from a JSON file
func parseUsersFile(filePath string, authMethod postgres.AuthMethod) (map[string]string, error) {
	// Remove @ prefix if present
	filePath = strings.TrimPrefix(filePath, "@")

	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read users file '%s': %v", filePath, err)
	}

	contentStr := strings.TrimSpace(string(content))

	// File must contain JSON format
	if !strings.HasPrefix(contentStr, "{") || !strings.HasSuffix(contentStr, "}") {
		return nil, fmt.Errorf("users file '%s' must contain JSON format: {\"user\":\"pass\"}. Legacy formats are no longer supported", filePath)
	}

	// Parse as JSON
	return parseUsersJSON(contentStr, authMethod)
}

// validatePortNumber validates that the port number is reasonable
func validatePortNumber(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("port number must be between 1 and 65535, got %d", port)
	}
	if port < 1024 {
		fmt.Fprintf(os.Stderr, "Warning: port number %d may require root privileges\n", port)
	}
	return nil
}
