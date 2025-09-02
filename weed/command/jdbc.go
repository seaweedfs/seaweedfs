package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	jdbcOptions JdbcOptions
)

type JdbcOptions struct {
	host       *string
	port       *int
	masterAddr *string
}

func init() {
	cmdJdbc.Run = runJdbc // break init cycle
	jdbcOptions.host = cmdJdbc.Flag.String("host", "localhost", "JDBC server host")
	jdbcOptions.port = cmdJdbc.Flag.Int("port", 8089, "JDBC server port")
	jdbcOptions.masterAddr = cmdJdbc.Flag.String("master", "localhost:9333", "SeaweedFS master server address")
}

var cmdJdbc = &Command{
	UsageLine: "jdbc -port=8089 -master=<master_server>",
	Short:     "start a JDBC server for SQL queries",
	Long: `Start a JDBC server that provides SQL query access to SeaweedFS.
	
This JDBC server allows standard JDBC clients and tools to connect to SeaweedFS
and execute SQL queries against MQ topics. It implements a subset of the JDBC
protocol for compatibility with most database tools and applications.

Examples:

	# Start JDBC server on default port 8089
	weed jdbc
	
	# Start on custom port with specific master
	weed jdbc -port=8090 -master=master1:9333
	
	# Allow connections from any host
	weed jdbc -host=0.0.0.0 -port=8089

Clients can then connect using JDBC URL:
	jdbc:seaweedfs://hostname:port/database

Supported SQL operations:
	- SELECT queries on MQ topics
	- DESCRIBE/DESC commands
	- SHOW DATABASES/TABLES commands
	- Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
	- WHERE clauses with filtering
	- System columns (_timestamp_ns, _key, _source)

Compatible with:
	- Standard JDBC tools (DBeaver, IntelliJ DataGrip, etc.)
	- Business Intelligence tools (Tableau, Power BI, etc.)
	- Java applications using JDBC drivers
	- SQL reporting tools

`,
}

func runJdbc(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	// Validate options
	if *jdbcOptions.masterAddr == "" {
		fmt.Fprintf(os.Stderr, "Error: master address is required\n")
		return false
	}

	// Create JDBC server
	jdbcServer, err := weed_server.NewJDBCServer(*jdbcOptions.host, *jdbcOptions.port, *jdbcOptions.masterAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating JDBC server: %v\n", err)
		return false
	}

	// Start the server
	fmt.Printf("Starting SeaweedFS JDBC Server...\n")
	fmt.Printf("Host: %s\n", *jdbcOptions.host)
	fmt.Printf("Port: %d\n", *jdbcOptions.port)
	fmt.Printf("Master: %s\n", *jdbcOptions.masterAddr)
	fmt.Printf("\nJDBC URL: jdbc:seaweedfs://%s:%d/default\n", *jdbcOptions.host, *jdbcOptions.port)
	fmt.Printf("\nSupported operations:\n")
	fmt.Printf("  - SELECT queries on MQ topics\n")
	fmt.Printf("  - DESCRIBE/DESC table_name\n")
	fmt.Printf("  - SHOW DATABASES\n")
	fmt.Printf("  - SHOW TABLES\n")
	fmt.Printf("  - Aggregations: COUNT, SUM, AVG, MIN, MAX\n")
	fmt.Printf("  - System columns: _timestamp_ns, _key, _source\n")
	fmt.Printf("\nReady for JDBC connections!\n\n")

	err = jdbcServer.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting JDBC server: %v\n", err)
		return false
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	fmt.Printf("\nReceived shutdown signal, stopping JDBC server...\n")

	// Create context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop the server with timeout
	done := make(chan error, 1)
	go func() {
		done <- jdbcServer.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error stopping JDBC server: %v\n", err)
			return false
		}
		fmt.Printf("JDBC server stopped successfully\n")
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "Timeout waiting for JDBC server to stop\n")
		return false
	}

	return true
}
