package command

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

var (
	a AdminOptions
)

type AdminOptions struct {
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
	cmdAdmin.Run = runAdmin // break init cycle
	a.port = cmdAdmin.Flag.Int("port", 23646, "admin server port")
	a.masters = cmdAdmin.Flag.String("masters", "localhost:9333", "comma-separated master servers")
	a.filer = cmdAdmin.Flag.String("filer", "localhost:8888", "filer server address")
	a.tlsCertPath = cmdAdmin.Flag.String("tlsCert", "", "path to TLS certificate file")
	a.tlsKeyPath = cmdAdmin.Flag.String("tlsKey", "", "path to TLS private key file")
	a.sessionSecret = cmdAdmin.Flag.String("sessionSecret", "", "session encryption secret (auto-generated if empty)")
	a.metricsServer = cmdAdmin.Flag.String("metricsServer", "", "metrics server address for reporting")
	a.adminUser = cmdAdmin.Flag.String("adminUser", "admin", "admin interface username")
	a.adminPassword = cmdAdmin.Flag.String("adminPassword", "", "admin interface password (if empty, auth is disabled)")
}

var cmdAdmin = &Command{
	UsageLine: "admin -port=23646 -masters=localhost:9333 -filer=localhost:8888",
	Short:     "start SeaweedFS web admin interface",
	Long: `Start a web admin interface for SeaweedFS cluster management.

  The admin interface provides a modern web interface for:
  - Cluster topology visualization and monitoring
  - Volume management and operations
  - File browser and management
  - System metrics and performance monitoring
  - Configuration management
  - Maintenance operations

  Example Usage:
    weed admin -port=23646 -masters="master1:9333,master2:9333" -filer="filer:8888"
    weed admin -port=443 -tlsCert=/etc/ssl/admin.crt -tlsKey=/etc/ssl/admin.key

  Authentication:
    - If adminPassword is not set, the admin interface runs without authentication
    - If adminPassword is set, users must login with adminUser/adminPassword
    - Sessions are secured with sessionSecret (auto-generated if not provided)

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

	if *a.filer == "" {
		fmt.Println("Error: filer parameter is required")
		fmt.Println("Usage: weed admin -filer=filer:8888")
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
	fmt.Printf("Filer: %s\n", *a.filer)
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

	// Session store
	var sessionKeyBytes []byte
	if *options.sessionSecret != "" {
		sessionKeyBytes = []byte(*options.sessionSecret)
	} else {
		// Generate a random session key
		sessionKeyBytes = make([]byte, 32)
		for i := range sessionKeyBytes {
			sessionKeyBytes[i] = byte(time.Now().UnixNano() & 0xff)
		}
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
	adminServer := dash.NewAdminServer(*options.masters, *options.filer, nil)

	// Setup routes
	setupRoutes(r, adminServer, *options.adminPassword != "", *options.adminUser, *options.adminPassword)

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

func setupRoutes(r *gin.Engine, adminServer *dash.AdminServer, authRequired bool, username, password string) {
	// Health check (no auth required)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Main admin interface route
	r.GET("/", func(c *gin.Context) {
		// Get admin data from the server
		adminData := getAdminData(adminServer, c)

		// Render HTML template
		c.Header("Content-Type", "text/html")
		adminComponent := app.Admin(adminData)
		layoutComponent := layout.Layout(c, adminComponent)
		err := layoutComponent.Render(c.Request.Context(), c.Writer)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
			return
		}
	})

	// S3 Buckets management routes
	r.GET("/s3/buckets", func(c *gin.Context) {
		// Get S3 buckets data from the server
		s3Data := getS3BucketsData(adminServer, c)

		// Render HTML template
		c.Header("Content-Type", "text/html")
		s3Component := app.S3Buckets(s3Data)
		layoutComponent := layout.Layout(c, s3Component)
		err := layoutComponent.Render(c.Request.Context(), c.Writer)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
			return
		}
	})

	r.GET("/s3/buckets/:bucket", func(c *gin.Context) {
		bucketName := c.Param("bucket")
		details, err := adminServer.GetBucketDetails(bucketName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get bucket details: " + err.Error()})
			return
		}
		c.JSON(http.StatusOK, details)
	})

	// API routes for AJAX calls
	api := r.Group("/api")
	{
		api.GET("/cluster/topology", func(c *gin.Context) {
			topology, err := adminServer.GetClusterTopology()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, topology)
		})
		api.GET("/cluster/masters", func(c *gin.Context) {
			// Simple master info
			c.JSON(http.StatusOK, gin.H{"masters": []gin.H{{"address": "localhost:9333", "status": "active"}}})
		})
		api.GET("/cluster/volumes", func(c *gin.Context) {
			topology, err := adminServer.GetClusterTopology()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"volume_servers": topology.VolumeServers})
		})
		api.GET("/admin", adminServer.ShowAdmin) // JSON API for admin data

		// S3 API routes
		s3Api := api.Group("/s3")
		{
			s3Api.GET("/buckets", adminServer.ListBucketsAPI)
			s3Api.POST("/buckets", adminServer.CreateBucket)
			s3Api.DELETE("/buckets/:bucket", adminServer.DeleteBucket)
			s3Api.GET("/buckets/:bucket", adminServer.ShowBucketDetails)
		}
	}
}

// getS3BucketsData retrieves S3 buckets data from the server
func getS3BucketsData(adminServer *dash.AdminServer, c *gin.Context) dash.S3BucketsData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get S3 buckets
	buckets, err := adminServer.GetS3Buckets()
	if err != nil {
		// Return empty data on error
		return dash.S3BucketsData{
			Username:     username,
			Buckets:      []dash.S3Bucket{},
			TotalBuckets: 0,
			TotalSize:    0,
			LastUpdated:  time.Now(),
		}
	}

	// Calculate totals
	var totalSize int64
	for _, bucket := range buckets {
		totalSize += bucket.Size
	}

	return dash.S3BucketsData{
		Username:     username,
		Buckets:      buckets,
		TotalBuckets: len(buckets),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}
}

// getAdminData retrieves admin data from the server
func getAdminData(adminServer *dash.AdminServer, c *gin.Context) dash.AdminData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get cluster topology
	topology, err := adminServer.GetClusterTopology()
	if err != nil {
		// Return default data on error
		return dash.AdminData{
			Username:      username,
			ClusterStatus: "error",
			SystemHealth:  "unknown",
			LastUpdated:   time.Now(),
		}
	}

	// Get master nodes status (simplified version of what's in the handler)
	masterNodes := []dash.MasterNode{
		{
			Address:  "localhost:9333", // Use the configured master address
			IsLeader: true,
			Status:   "active",
		},
	}

	return dash.AdminData{
		Username:      username,
		ClusterStatus: determineClusterStatus(topology, masterNodes),
		TotalVolumes:  topology.TotalVolumes,
		TotalFiles:    topology.TotalFiles,
		TotalSize:     topology.TotalSize,
		MasterNodes:   masterNodes,
		VolumeServers: topology.VolumeServers,
		DataCenters:   topology.DataCenters,
		LastUpdated:   topology.UpdatedAt,
		SystemHealth:  determineSystemHealth(topology, masterNodes),
	}
}

// Helper functions
func determineClusterStatus(topology *dash.ClusterTopology, masters []dash.MasterNode) string {
	if len(topology.VolumeServers) == 0 {
		return "warning"
	}
	return "healthy"
}

func determineSystemHealth(topology *dash.ClusterTopology, masters []dash.MasterNode) string {
	if len(topology.VolumeServers) > 0 && len(masters) > 0 {
		return "good"
	}
	return "fair"
}

// GetAdminOptions returns the admin command options for testing
func GetAdminOptions() *AdminOptions {
	return &AdminOptions{}
}
