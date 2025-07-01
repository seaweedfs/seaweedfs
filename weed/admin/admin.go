package main

import (
	"context"
	"crypto/tls"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

//go:embed static/* view/*
var adminFS embed.FS

func main() {
	var (
		port         = flag.Int("port", 23646, "Port to run the admin server on")
		host         = flag.String("host", "localhost", "Host to bind the admin server to")
		sessionKey   = flag.String("sessionKey", "", "Session encryption key (32 bytes, random if not provided)")
		tlsCert      = flag.String("tlsCert", "", "Path to TLS certificate file")
		tlsKey       = flag.String("tlsKey", "", "Path to TLS key file")
		master       = flag.String("master", "localhost:9333", "SeaweedFS master server address")
		authRequired = flag.Bool("auth", false, "Enable authentication")
		username     = flag.String("username", "admin", "Admin username (only used if auth is enabled)")
		password     = flag.String("password", "", "Admin password (only used if auth is enabled)")
		help         = flag.Bool("help", false, "Show help")
	)

	flag.Parse()

	if *help {
		fmt.Println("SeaweedFS Admin Server")
		fmt.Println()
		flag.PrintDefaults()
		return
	}

	// Set Gin mode
	gin.SetMode(gin.ReleaseMode)

	// Create router
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Session store
	var sessionKeyBytes []byte
	if *sessionKey != "" {
		sessionKeyBytes = []byte(*sessionKey)
	} else {
		// Generate a random session key
		sessionKeyBytes = make([]byte, 32)
		for i := range sessionKeyBytes {
			sessionKeyBytes[i] = byte(time.Now().UnixNano() & 0xff)
		}
	}
	store := cookie.NewStore(sessionKeyBytes)
	r.Use(sessions.Sessions("admin-session", store))

	// Static files
	staticFS, err := fs.Sub(adminFS, "static")
	if err != nil {
		log.Fatal("Failed to create static filesystem:", err)
	}
	r.StaticFS("/static", http.FS(staticFS))

	// Templates
	viewFS, err := fs.Sub(adminFS, "view")
	if err != nil {
		log.Fatal("Failed to create view filesystem:", err)
	}

	// Create admin server
	adminServer := dash.NewAdminServer(*master, http.FS(viewFS))

	// Setup routes
	setupRoutes(r, adminServer, *authRequired, *username, *password)

	// Server configuration
	addr := fmt.Sprintf("%s:%d", *host, *port)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// TLS configuration
	if *tlsCert != "" && *tlsKey != "" {
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	// Start server
	go func() {
		log.Printf("Starting SeaweedFS Admin Server on %s", addr)

		var err error
		if *tlsCert != "" && *tlsKey != "" {
			log.Printf("Using TLS with cert: %s, key: %s", *tlsCert, *tlsKey)
			err = server.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			log.Fatal("Failed to start server:", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down admin server...")

	// Give outstanding requests 30 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Admin server forced to shutdown:", err)
	}

	log.Println("Admin server exited")
}

func setupRoutes(r *gin.Engine, adminServer *dash.AdminServer, authRequired bool, username, password string) {
	// Health check (no auth required)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	if authRequired {
		// Auth routes
		auth := r.Group("/")
		auth.GET("/login", adminServer.ShowLogin)
		auth.POST("/login", adminServer.HandleLogin(username, password))
		auth.POST("/logout", adminServer.HandleLogout)

		// Protected routes
		protected := r.Group("/")
		protected.Use(dash.RequireAuth())

		// Admin routes
		protected.GET("/", adminServer.ShowAdmin)
		protected.GET("/admin", adminServer.ShowAdmin)
		protected.GET("/overview", adminServer.ShowAdmin)

		// Cluster management
		cluster := protected.Group("/cluster")
		{
			cluster.GET("/topology", adminServer.GetClusterTopologyHandler)
			cluster.GET("/masters", adminServer.GetMasters)
			cluster.GET("/volumes", adminServer.GetVolumeServers)
			cluster.POST("/volumes/assign", adminServer.AssignVolume)
		}

		// Volume management
		volumes := protected.Group("/volumes")
		{
			volumes.GET("/", adminServer.ListVolumes)
			volumes.POST("/create", adminServer.CreateVolume)
			volumes.DELETE("/:id", adminServer.DeleteVolume)
			volumes.POST("/:id/replicate", adminServer.ReplicateVolume)
		}

		// File browser
		files := protected.Group("/filer")
		{
			files.GET("/*path", adminServer.BrowseFiles)
			files.POST("/upload", adminServer.UploadFile)
			files.DELETE("/*path", adminServer.DeleteFile)
		}

		// Metrics
		metrics := protected.Group("/metrics")
		{
			metrics.GET("/", adminServer.ShowMetrics)
			metrics.GET("/data", adminServer.GetMetricsData)
		}

		// Maintenance
		maintenance := protected.Group("/maintenance")
		{
			maintenance.POST("/gc", adminServer.TriggerGC)
			maintenance.POST("/compact", adminServer.CompactVolumes)
			maintenance.GET("/status", adminServer.GetMaintenanceStatus)
		}
	} else {
		// No auth required - all routes are public
		r.GET("/", adminServer.ShowAdmin)
		r.GET("/admin", adminServer.ShowAdmin)
		r.GET("/overview", adminServer.ShowAdmin)

		// Cluster management
		cluster := r.Group("/cluster")
		{
			cluster.GET("/topology", adminServer.GetClusterTopologyHandler)
			cluster.GET("/masters", adminServer.GetMasters)
			cluster.GET("/volumes", adminServer.GetVolumeServers)
			cluster.POST("/volumes/assign", adminServer.AssignVolume)
		}

		// Volume management
		volumes := r.Group("/volumes")
		{
			volumes.GET("/", adminServer.ListVolumes)
			volumes.POST("/create", adminServer.CreateVolume)
			volumes.DELETE("/:id", adminServer.DeleteVolume)
			volumes.POST("/:id/replicate", adminServer.ReplicateVolume)
		}

		// File browser
		files := r.Group("/filer")
		{
			files.GET("/*path", adminServer.BrowseFiles)
			files.POST("/upload", adminServer.UploadFile)
			files.DELETE("/*path", adminServer.DeleteFile)
		}

		// Metrics
		metrics := r.Group("/metrics")
		{
			metrics.GET("/", adminServer.ShowMetrics)
			metrics.GET("/data", adminServer.GetMetricsData)
		}

		// Maintenance
		maintenance := r.Group("/maintenance")
		{
			maintenance.POST("/gc", adminServer.TriggerGC)
			maintenance.POST("/compact", adminServer.CompactVolumes)
			maintenance.GET("/status", adminServer.GetMaintenanceStatus)
		}
	}
}
