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

	"github.com/seaweedfs/seaweedfs/weed/admin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/handlers"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc" // Register filer_etc credential store
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	a AdminOptions
)

type AdminOptions struct {
	port             *int
	grpcPort         *int
	master           *string
	masters          *string // deprecated, for backward compatibility
	pluginEnabled    *bool
	adminUser        *string
	adminPassword    *string
	readOnlyUser     *string
	readOnlyPassword *string
	dataDir          *string
	icebergPort      *int
}

func init() {
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.grpcPort = cmdAdmin.Flag.Int("port.grpc", 0, "gRPC server port for worker connections (default: http port + 10000)")
	a.master = cmdAdmin.Flag.String("master", "localhost:9333", "comma-separated master servers")
	a.masters = cmdAdmin.Flag.String("masters", "", "comma-separated master servers (deprecated, use -master instead)")
	a.pluginEnabled = cmdAdmin.Flag.Bool("plugin.enabled", false, "enable plugin on worker gRPC server")
	a.dataDir = cmdAdmin.Flag.String("dataDir", "", "directory to store admin configuration and data files")

	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
	a.readOnlyUser = cmdAdmin.Flag.String("readOnlyUser", "", "read-only user username (optional, for view-only access)")
	a.readOnlyPassword = cmdAdmin.Flag.String("readOnlyPassword", "", "read-only user password (optional, for view-only access; requires adminPassword to be set)")
	a.icebergPort = cmdAdmin.Flag.Int("iceberg.port", 8181, "Iceberg REST Catalog port (0 to hide in UI)")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -master=localhost:9333 [-port.grpc=33646] [-dataDir=/path/to/data] [-plugin.enabled=true]",
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
  A gRPC server for worker connections runs on the configured gRPC port (default: HTTP port + 10000).

  Example Usage:
    weed admin -port=23646 -master="master1:9333,master2:9333"
    weed admin -port=23646 -master="localhost:9333" -dataDir="/var/lib/seaweedfs-admin"
    weed admin -port=23646 -port.grpc=33646 -master="localhost:9333" -dataDir="~/seaweedfs-admin"
    weed admin -port=9900 -port.grpc=19900 -master="localhost:9333"

  Data Directory:
    - If dataDir is specified, admin configuration and maintenance data is persisted
    - The directory will be created if it doesn't exist
    - Configuration files are stored in JSON format for easy editing
    - Without dataDir, all configuration is kept in memory only

  Authentication:
    - If adminPassword is not set, the admin interface runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword (full access)
    - Optional read-only access: set readOnlyUser and readOnlyPassword for view-only access
    - Read-only users can view cluster status and configurations but cannot make changes
    - IMPORTANT: When read-only credentials are configured, adminPassword MUST also be set
    - This ensures an admin account exists to manage and authorize read-only access
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

  Plugin:
    - Disabled by default and enabled via -plugin.enabled=true
    - Registers plugin.proto gRPC service on the same worker gRPC port
    - External workers connect with: weed plugin.worker -admin=<admin_host:admin_port>
    - Persists plugin metadata under dataDir/plugin when dataDir is configured

  Configuration File:
    - The security.toml file is read from ".", "$HOME/.seaweedfs/", 
      "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order
    - Generate example security.toml: weed scaffold -config=security

`,
}

func runAdmin(cmd *Command, args []string) bool {
	// Load security configuration
	util.LoadSecurityConfiguration()

	// Backward compatibility: if -masters is provided, use it
	if *a.masters != "" {
		*a.master = *a.masters
	}

	// Validate required parameters
	if *a.master == "" {
		fmt.Println("Error: master parameter is required")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Validate that master string can be parsed
	masterAddresses := pb.ServerAddresses(*a.master).ToAddresses()
	if len(masterAddresses) == 0 {
		fmt.Println("Error: no valid master addresses found")
		fmt.Println("Usage: weed admin -master=master1:9333,master2:9333")
		return false
	}

	// Security validation: prevent empty username when password is set
	if *a.adminPassword != "" && *a.adminUser == "" {
		fmt.Println("Error: -adminUser cannot be empty when -adminPassword is set")
		return false
	}
	if *a.readOnlyPassword != "" && *a.readOnlyUser == "" {
		fmt.Println("Error: -readOnlyUser is required when -readOnlyPassword is set")
		return false
	}
	// Security validation: prevent username conflicts between admin and read-only users
	if *a.adminUser != "" && *a.readOnlyUser != "" && *a.adminUser == *a.readOnlyUser {
		fmt.Println("Error: -adminUser and -readOnlyUser must be different when both are configured")
		return false
	}
	// Security validation: admin password is required for read-only user
	if *a.readOnlyPassword != "" && *a.adminPassword == "" {
		fmt.Println("Error: -adminPassword must be set when -readOnlyPassword is configured")
		return false
	}

	// Set default gRPC port if not specified
	if *a.grpcPort == 0 {
		*a.grpcPort = *a.port + 10000
	}

	// Security warnings
	if *a.adminPassword == "" {
		fmt.Println("WARNING: Admin interface is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}

	fmt.Printf("Starting SeaweedFS Admin Interface on port %d\n", *a.port)
	fmt.Printf("Worker gRPC server will run on port %d\n", *a.grpcPort)
	fmt.Printf("Masters: %s\n", *a.master)
	fmt.Printf("Filers will be discovered automatically from masters\n")
	if *a.dataDir != "" {
		fmt.Printf("Data Directory: %s\n", *a.dataDir)
	} else {
		fmt.Printf("Data Directory: Not specified (configuration will be in-memory only)\n")
	}
	if *a.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (admin user: %s)\n", *a.adminUser)
		if *a.readOnlyPassword != "" {
			fmt.Printf("Read-only access: Enabled (read-only user: %s)\n", *a.readOnlyUser)
		}
	} else {
		fmt.Printf("Authentication: Disabled\n")
	}
	fmt.Printf("Plugin: %v\n", *a.pluginEnabled)

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

	// Start the admin server with all masters (UI enabled by default)
	err := startAdminServer(ctx, a, true, *a.icebergPort)
	if err != nil {
		fmt.Printf("Admin server error: %v\n", err)
		return false
	}

	fmt.Println("Admin server stopped")
	return true
}

// startAdminServer starts the actual admin server
func startAdminServer(ctx context.Context, options AdminOptions, enableUI bool, icebergPort int) error {
	// Set Gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	r := gin.New()
	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		if param.StatusCode == 200 {
			return ""
		}
		return fmt.Sprintf("[GIN] %v | %3d | %13v | %15s | %-7s %s\n%s",
			param.TimeStamp.Format("2006/01/02 - 15:04:05"),
			param.StatusCode,
			param.Latency,
			param.ClientIP,
			param.Method,
			param.Path,
			param.ErrorMessage,
		)
	}), gin.Recovery())

	// Create data directory first if specified (needed for session key storage)
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

	// Detect TLS configuration to set Secure cookie flag
	cookieSecure := viper.GetString("https.admin.key") != ""

	// Session store - load or generate session key
	sessionKeyBytes, err := loadOrGenerateSessionKey(dataDir)
	if err != nil {
		return fmt.Errorf("failed to get session key: %w", err)
	}
	store := cookie.NewStore(sessionKeyBytes)

	// Configure session options to ensure cookies are properly saved
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   3600 * 24,    // 24 hours
		HttpOnly: true,         // Prevent JavaScript access
		Secure:   cookieSecure, // Set based on actual TLS configuration
		SameSite: http.SameSiteLaxMode,
	})

	r.Use(sessions.Sessions("admin-session", store))

	// Static files - serve from embedded filesystem
	staticFS, err := admin.GetStaticFS()
	if err != nil {
		log.Printf("Warning: Failed to load embedded static files: %v", err)
	} else {
		r.StaticFS("/static", http.FS(staticFS))
	}

	// Create admin server
	pluginEnabled := options.pluginEnabled != nil && *options.pluginEnabled
	adminServer := dash.NewAdminServer(*options.master, nil, dataDir, icebergPort, pluginEnabled)

	// Show discovered filers
	filers := adminServer.GetAllFilers()
	if len(filers) > 0 {
		fmt.Printf("Discovered filers: %s\n", strings.Join(filers, ", "))
	} else {
		fmt.Printf("No filers discovered from masters\n")
	}

	// Start worker gRPC server for worker connections
	err = adminServer.StartWorkerGrpcServer(*options.grpcPort)
	if err != nil {
		return fmt.Errorf("failed to start worker gRPC server: %w", err)
	}

	// Set up cleanup for gRPC server
	defer func() {
		if stopErr := adminServer.StopWorkerGrpcServer(); stopErr != nil {
			log.Printf("Error stopping worker gRPC server: %v", stopErr)
		}
	}()

	// Create handlers and setup routes
	authRequired := *options.adminPassword != ""
	adminHandlers := handlers.NewAdminHandlers(adminServer)
	adminHandlers.SetupRoutes(r, authRequired, *options.adminUser, *options.adminPassword, *options.readOnlyUser, *options.readOnlyPassword, enableUI)

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
		return fmt.Errorf("admin server forced to shutdown: %w", err)
	}

	adminServer.Shutdown()

	return nil
}

// GetAdminOptions returns the admin command options for testing
func GetAdminOptions() *AdminOptions {
	return &AdminOptions{}
}

// loadOrGenerateSessionKey loads an existing session key from dataDir or generates a new one
func loadOrGenerateSessionKey(dataDir string) ([]byte, error) {
	const sessionKeyLength = 32
	if dataDir == "" {
		// No persistence, generate random key
		log.Println("No dataDir specified, generating ephemeral session key")
		key := make([]byte, sessionKeyLength)
		_, err := rand.Read(key)
		return key, err
	}

	sessionKeyPath := filepath.Join(dataDir, ".session_key")

	// Try to load existing key
	if data, err := os.ReadFile(sessionKeyPath); err == nil {
		if len(data) == sessionKeyLength {
			log.Printf("Loaded persisted session key from %s", sessionKeyPath)
			return data, nil
		}
		log.Printf("Warning: Invalid session key file (expected %d bytes, got %d), generating new key", sessionKeyLength, len(data))
	} else if !os.IsNotExist(err) {
		log.Printf("Warning: Failed to read session key from %s: %v. A new key will be generated.", sessionKeyPath, err)
	}

	// Generate new key
	key := make([]byte, sessionKeyLength)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	// Save key for future use
	if err := os.WriteFile(sessionKeyPath, key, 0600); err != nil {
		log.Printf("Warning: Failed to persist session key: %v", err)
	} else {
		log.Printf("Generated and persisted new session key to %s", sessionKeyPath)
	}

	return key, nil
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
		return "", fmt.Errorf("failed to get current user: %w", err)
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
