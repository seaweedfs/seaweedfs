package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type DashboardOptions struct {
	port          *int
	masters       *string
	filer         *string
	tlsCertPath   *string
	tlsKeyPath    *string
	sessionSecret *string
	metricsServer *string
	adminUser     *string
	adminPassword *string
}

func init() {
	cmdDashboard.Run = runDashboard // break init cycle
}

var cmdDashboard = &Command{
	UsageLine: "dashboard -port=9999 -masters=localhost:9333 -filer=localhost:8888",
	Short:     "start SeaweedFS web dashboard",
	Long: `Start a web dashboard for SeaweedFS cluster management.

  The dashboard provides a modern web interface for:
  - Cluster topology visualization and monitoring
  - Volume management and operations
  - File browser and management
  - System metrics and performance monitoring
  - Configuration management
  - Maintenance operations

  Example Usage:
    weed dashboard -port=9999 -masters="master1:9333,master2:9333" -filer="filer:8888"
    weed dashboard -port=443 -tlsCert=/etc/ssl/dashboard.crt -tlsKey=/etc/ssl/dashboard.key

  Authentication:
    - If adminPassword is not set, the dashboard runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword
    - Sessions are secured with sessionSecret (auto-generated if not provided)

  Security:
    - Use HTTPS in production by providing TLS certificates
    - Set strong adminPassword for production deployments
    - Configure firewall rules to restrict dashboard access

`,
}

func runDashboard(cmd *Command, args []string) bool {
	dashboardOptions := DashboardOptions{}
	dashboardOptions.port = cmdDashboard.Flag.Int("port", 9999, "dashboard server port")
	dashboardOptions.masters = cmdDashboard.Flag.String("masters", "localhost:9333", "comma-separated master servers")
	dashboardOptions.filer = cmdDashboard.Flag.String("filer", "localhost:8888", "filer server address")
	dashboardOptions.tlsCertPath = cmdDashboard.Flag.String("tlsCert", "", "path to TLS certificate file")
	dashboardOptions.tlsKeyPath = cmdDashboard.Flag.String("tlsKey", "", "path to TLS private key file")
	dashboardOptions.sessionSecret = cmdDashboard.Flag.String("sessionSecret", "", "session encryption secret (auto-generated if empty)")
	dashboardOptions.metricsServer = cmdDashboard.Flag.String("metricsServer", "", "metrics server address for reporting")
	dashboardOptions.adminUser = cmdDashboard.Flag.String("adminUser", "admin", "dashboard admin username")
	dashboardOptions.adminPassword = cmdDashboard.Flag.String("adminPassword", "", "dashboard admin password (if empty, auth is disabled)")

	if err := cmdDashboard.Flag.Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		return false
	}

	// Validate required parameters
	if *dashboardOptions.masters == "" {
		fmt.Println("Error: masters parameter is required")
		fmt.Println("Usage: weed dashboard -masters=master1:9333,master2:9333")
		return false
	}

	if *dashboardOptions.filer == "" {
		fmt.Println("Error: filer parameter is required")
		fmt.Println("Usage: weed dashboard -filer=filer:8888")
		return false
	}

	// Validate TLS configuration
	if (*dashboardOptions.tlsCertPath != "" && *dashboardOptions.tlsKeyPath == "") ||
		(*dashboardOptions.tlsCertPath == "" && *dashboardOptions.tlsKeyPath != "") {
		fmt.Println("Error: Both tlsCert and tlsKey must be provided for TLS")
		return false
	}

	// Security warnings
	if *dashboardOptions.adminPassword == "" {
		fmt.Println("WARNING: Dashboard is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}

	if *dashboardOptions.tlsCertPath == "" {
		fmt.Println("WARNING: Dashboard is running without TLS encryption!")
		fmt.Println("         Use -tlsCert and -tlsKey for production use")
	}

	fmt.Printf("Starting SeaweedFS Dashboard on port %d\n", *dashboardOptions.port)
	fmt.Printf("Masters: %s\n", *dashboardOptions.masters)
	fmt.Printf("Filer: %s\n", *dashboardOptions.filer)
	if *dashboardOptions.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (user: %s)\n", *dashboardOptions.adminUser)
	} else {
		fmt.Printf("Authentication: Disabled\n")
	}
	if *dashboardOptions.tlsCertPath != "" {
		fmt.Printf("TLS: Enabled\n")
	} else {
		fmt.Printf("TLS: Disabled\n")
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)
		cancel()
	}()

	// Start the dashboard server
	// Note: This would need to be implemented in the main dashboard package
	// For now, this shows the integration pattern
	err := startDashboardServer(ctx, dashboardOptions)
	if err != nil {
		fmt.Printf("Dashboard server error: %v\n", err)
		return false
	}

	fmt.Println("Dashboard server stopped")
	return true
}

// startDashboardServer would start the actual dashboard server
// This is a placeholder for the integration
func startDashboardServer(ctx context.Context, options DashboardOptions) error {
	// This would call the actual dashboard main function
	// dashboard.Main(options) or similar

	// For demonstration, just wait for context cancellation
	<-ctx.Done()
	return nil
}

// GetDashboardOptions returns the dashboard command options for testing
func GetDashboardOptions() *DashboardOptions {
	return &DashboardOptions{}
}
