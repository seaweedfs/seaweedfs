package command

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/handlers"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	a AdminOptions
)

type AdminOptions struct {
	port          *int
	masters       *string
	adminUser     *string
	adminPassword *string
	dataDir       *string
}

func init() {
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.masters = cmdAdmin.Flag.String("masters", "localhost:9333", "comma-separated master servers")
	a.dataDir = cmdAdmin.Flag.String("dataDir", "", "directory to store admin configuration and data files")

	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -masters=localhost:9333 [-dataDir=/path/to/data]",
	Short:     "start SeaweedFS web admin interface",
	Long: `Start a web admin interface for SeaweedFS cluster management.

  The admin interface provides a modern web interface for:
  - Cluster topology visualization and monitoring
  - Volume management and operations
  - File browser and management
  - System metrics and performance monitoring
  - Configuration management
  - Maintenance operations

  The admin interface automatically discovers filers from the master servers.
  A gRPC server for worker connections runs on HTTP port + 10000.

  Example Usage:
    weed admin -port=23646 -masters="master1:9333,master2:9333"
    weed admin -port=23646 -masters="localhost:9333" -dataDir="/var/lib/seaweedfs-admin"
    weed admin -port=23646 -masters="localhost:9333" -dataDir="~/seaweedfs-admin"

  Data Directory:
    - If dataDir is specified, admin configuration and maintenance data is persisted
    - The directory will be created if it doesn't exist
    - Configuration files are stored in JSON format for easy editing
    - Without dataDir, all configuration is kept in memory only

  Authentication:
    - If adminPassword is not set, the admin interface runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword
    - Sessions are secured with auto-generated session keys

  Security Configuration:
    - The admin server reads TLS configuration from security.toml
    - Configure [https.admin] section in security.toml for HTTPS support
    - If https.admin.key is set, the server will start in TLS mode
    - If https.admin.ca is set, mutual TLS authentication is enabled
    - Set strong adminPassword for production deployments
    - Configure firewall rules to restrict admin interface access

  security.toml Example:
    [https.admin]
    cert = "/etc/ssl/admin.crt"
    key = "/etc/ssl/admin.key"
    ca = "/etc/ssl/ca.crt"     # optional, for mutual TLS

  Worker Communication:
    - Workers connect via gRPC on HTTP port + 10000
    - Workers use [grpc.admin] configuration from security.toml
    - TLS is automatically used if certificates are configured
    - Workers fall back to insecure connections if TLS is unavailable

  Configuration File:
    - The security.toml file is read from ".", "$HOME/.seaweedfs/", 
      "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order
    - Generate example security.toml: weed scaffold -config=security

`,
}

func runAdmin(cmd *Command, args []string) bool {
	// Load security configuration
	util.LoadSecurityConfiguration()

	// Validate required parameters
	if *a.masters == "" {
		fmt.Println("Error: masters parameter is required")
		fmt.Println("Usage: weed admin -masters=master1:9333,master2:9333")
		return false
	}

	// Security warnings
	if *a.adminPassword == "" {
		fmt.Println("WARNING: Admin interface is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}

	fmt.Printf("Starting SeaweedFS Admin Interface on port %d\n", *a.port)
	fmt.Printf("Masters: %s\n", *a.masters)
	fmt.Printf("Filers will be discovered automatically from masters\n")
	if *a.dataDir != "" {
		fmt.Printf("Data Directory: %s\n", *a.dataDir)
	} else {
		fmt.Printf("Data Directory: Not specified (configuration will be in-memory only)\n")
	}
	if *a.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (user: %s)\n", *a.adminUser)
	} else {
		fmt.Printf("Authentication: Disabled\n")
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

	// Start the admin server
	err := startAdminServer(ctx, a)
	if err != nil {
		fmt.Printf("Admin server error: %v\n", err)
		return false
	}

	fmt.Println("Admin server stopped")
	return true
}

// startAdminServer starts the actual admin server
func startAdminServer(ctx context.Context, options AdminOptions) error {
	// Set Gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Session store - always auto-generate session key
	sessionKeyBytes := make([]byte, 32)
	_, err := rand.Read(sessionKeyBytes)
	if err != nil {
		return fmt.Errorf("failed to generate session key: %v", err)
	}
	store := cookie.NewStore(sessionKeyBytes)
	r.Use(sessions.Sessions("admin-session", store))

	// Static files - serve from filesystem
	staticPath := filepath.Join("weed", "admin", "static")
	if _, err := os.Stat(staticPath); err == nil {
		r.Static("/static", staticPath)
	} else {
		log.Printf("Warning: Static files not found at %s", staticPath)
	}

	// Create data directory if specified
	var dataDir string
	if *options.dataDir != "" {
		// Expand tilde (~) to home directory
		expandedDir, err := expandHomeDir(*options.dataDir)
		if err != nil {
			return fmt.Errorf("failed to expand dataDir path %s: %v", *options.dataDir, err)
		}
		dataDir = expandedDir

		// Show path expansion if it occurred
		if dataDir != *options.dataDir {
			fmt.Printf("Expanded dataDir: %s -> %s\n", *options.dataDir, dataDir)
		}

		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory %s: %v", dataDir, err)
		}
		fmt.Printf("Data directory created/verified: %s\n", dataDir)
	}

	// Create admin server
	adminServer := dash.NewAdminServer(*options.masters, nil, dataDir)

	// Show discovered filers
	filers := adminServer.GetAllFilers()
	if len(filers) > 0 {
		fmt.Printf("Discovered filers: %s\n", strings.Join(filers, ", "))
	} else {
		fmt.Printf("No filers discovered from masters\n")
	}

	// Start worker gRPC server for worker connections
	err = adminServer.StartWorkerGrpcServer(*options.port)
	if err != nil {
		return fmt.Errorf("failed to start worker gRPC server: %v", err)
	}

	// Set up cleanup for gRPC server
	defer func() {
		if stopErr := adminServer.StopWorkerGrpcServer(); stopErr != nil {
			log.Printf("Error stopping worker gRPC server: %v", stopErr)
		}
	}()

	// Create handlers and setup routes
	adminHandlers := handlers.NewAdminHandlers(adminServer)
	adminHandlers.SetupRoutes(r, *options.adminPassword != "", *options.adminUser, *options.adminPassword)

	// Server configuration
	addr := fmt.Sprintf(":%d", *options.port)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Start server
	go func() {
		log.Printf("Starting SeaweedFS Admin Server on port %d", *options.port)

		// start http or https server with security.toml
		var (
			clientCertFile,
			certFile,
			keyFile string
		)
		useTLS := false
		useMTLS := false

		if viper.GetString("https.admin.key") != "" {
			useTLS = true
			certFile = viper.GetString("https.admin.cert")
			keyFile = viper.GetString("https.admin.key")
		}

		if viper.GetString("https.admin.ca") != "" {
			useMTLS = true
			clientCertFile = viper.GetString("https.admin.ca")
		}

		if useMTLS {
			server.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
		}

		if useTLS {
			log.Printf("Starting SeaweedFS Admin Server with TLS on port %d", *options.port)
			err = server.ListenAndServeTLS(certFile, keyFile)
		} else {
			err = server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			log.Printf("Failed to start server: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	log.Println("Shutting down admin server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("admin server forced to shutdown: %v", err)
	}

	return nil
}

// GetAdminOptions returns the admin command options for testing
func GetAdminOptions() *AdminOptions {
	return &AdminOptions{}
}

// expandHomeDir expands the tilde (~) in a path to the user's home directory
func expandHomeDir(path string) (string, error) {
	if path == "" {
		return path, nil
	}

	if !strings.HasPrefix(path, "~") {
		return path, nil
	}

	// Get current user
	currentUser, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("failed to get current user: %v", err)
	}

	// Handle different tilde patterns
	if path == "~" {
		return currentUser.HomeDir, nil
	}

	if strings.HasPrefix(path, "~/") {
		return filepath.Join(currentUser.HomeDir, path[2:]), nil
	}

	// Handle ~username/ patterns
	if strings.HasPrefix(path, "~") {
		parts := strings.SplitN(path[1:], "/", 2)
		username := parts[0]

		targetUser, err := user.Lookup(username)
		if err != nil {
			return "", fmt.Errorf("user %s not found: %v", username, err)
		}

		if len(parts) == 1 {
			return targetUser.HomeDir, nil
		}
		return filepath.Join(targetUser.HomeDir, parts[1]), nil
	}

	return path, nil
}
