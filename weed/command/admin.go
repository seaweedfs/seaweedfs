package command

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/handlers"
)

var (
	a AdminOptions
)

type AdminOptions struct {
	port          *int
	masters       *string
	tlsCertPath   *string
	tlsKeyPath    *string
	adminUser     *string
	adminPassword *string
}

func init() {
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.masters = cmdAdmin.Flag.String("masters", "localhost:9333", "comma-separated master servers")
	a.tlsCertPath = cmdAdmin.Flag.String("tlsCert", "", "path to TLS certificate file")
	a.tlsKeyPath = cmdAdmin.Flag.String("tlsKey", "", "path to TLS private key file")

	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -masters=localhost:9333",
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

  Example Usage:
    weed admin -port=23646 -masters="master1:9333,master2:9333"
    weed admin -port=443 -tlsCert=/etc/ssl/admin.crt -tlsKey=/etc/ssl/admin.key

  Authentication:
    - If adminPassword is not set, the admin interface runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword
    - Sessions are secured with auto-generated session keys

  Security:
    - Use HTTPS in production by providing TLS certificates
    - Set strong adminPassword for production deployments
    - Configure firewall rules to restrict admin interface access

`,
}

func runAdmin(cmd *Command, args []string) bool {
	// Validate required parameters
	if *a.masters == "" {
		fmt.Println("Error: masters parameter is required")
		fmt.Println("Usage: weed admin -masters=master1:9333,master2:9333")
		return false
	}

	// Validate TLS configuration
	if (*a.tlsCertPath != "" && *a.tlsKeyPath == "") ||
		(*a.tlsCertPath == "" && *a.tlsKeyPath != "") {
		fmt.Println("Error: Both tlsCert and tlsKey must be provided for TLS")
		return false
	}

	// Security warnings
	if *a.adminPassword == "" {
		fmt.Println("WARNING: Admin interface is running without authentication!")
		fmt.Println("         Set -adminPassword for production use")
	}

	if *a.tlsCertPath == "" {
		fmt.Println("WARNING: Admin interface is running without TLS encryption!")
		fmt.Println("         Use -tlsCert and -tlsKey for production use")
	}

	fmt.Printf("Starting SeaweedFS Admin Interface on port %d\n", *a.port)
	fmt.Printf("Masters: %s\n", *a.masters)
	fmt.Printf("Filers will be discovered automatically from masters\n")
	if *a.adminPassword != "" {
		fmt.Printf("Authentication: Enabled (user: %s)\n", *a.adminUser)
	} else {
		fmt.Printf("Authentication: Disabled\n")
	}
	if *a.tlsCertPath != "" {
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

	// Create admin server
	adminServer := dash.NewAdminServer(*options.masters, nil)

	// Show discovered filers
	filers := adminServer.GetAllFilers()
	if len(filers) > 0 {
		fmt.Printf("Discovered filers: %s\n", strings.Join(filers, ", "))
	} else {
		fmt.Printf("No filers discovered from masters\n")
	}

	// Create handlers and setup routes
	adminHandlers := handlers.NewAdminHandlers(adminServer)
	adminHandlers.SetupRoutes(r, *options.adminPassword != "", *options.adminUser, *options.adminPassword)

	// Server configuration
	addr := fmt.Sprintf(":%d", *options.port)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// TLS configuration
	if *options.tlsCertPath != "" && *options.tlsKeyPath != "" {
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	// Start server
	go func() {
		log.Printf("Starting SeaweedFS Admin Server on port %d", *options.port)

		var err error
		if *options.tlsCertPath != "" && *options.tlsKeyPath != "" {
			log.Printf("Using TLS with cert: %s, key: %s", *options.tlsCertPath, *options.tlsKeyPath)
			err = server.ListenAndServeTLS(*options.tlsCertPath, *options.tlsKeyPath)
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
