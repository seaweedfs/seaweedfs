package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
)

// AdminHandlers contains all the HTTP handlers for the admin interface
type AdminHandlers struct {
	adminServer     *dash.AdminServer
	authHandlers    *AuthHandlers
	clusterHandlers *ClusterHandlers
}

// NewAdminHandlers creates a new instance of AdminHandlers
func NewAdminHandlers(adminServer *dash.AdminServer) *AdminHandlers {
	authHandlers := NewAuthHandlers(adminServer)
	clusterHandlers := NewClusterHandlers(adminServer)
	return &AdminHandlers{
		adminServer:     adminServer,
		authHandlers:    authHandlers,
		clusterHandlers: clusterHandlers,
	}
}

// SetupRoutes configures all the routes for the admin interface
func (h *AdminHandlers) SetupRoutes(r *gin.Engine, authRequired bool, username, password string) {
	// Health check (no auth required)
	r.GET("/health", h.HealthCheck)

	if authRequired {
		// Authentication routes (no auth required)
		r.GET("/login", h.authHandlers.ShowLogin)
		r.POST("/login", h.authHandlers.HandleLogin(username, password))
		r.GET("/logout", h.authHandlers.HandleLogout)

		// Protected routes group
		protected := r.Group("/")
		protected.Use(dash.RequireAuth())

		// Main admin interface routes
		protected.GET("/", h.ShowDashboard)
		protected.GET("/admin", h.ShowDashboard)

		// S3 Buckets management routes
		protected.GET("/s3/buckets", h.ShowS3Buckets)
		protected.GET("/s3/buckets/:bucket", h.ShowBucketDetails)

		// Cluster management routes
		protected.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)
		protected.GET("/cluster/volumes", h.clusterHandlers.ShowClusterVolumes)
		protected.GET("/cluster/collections", h.clusterHandlers.ShowClusterCollections)

		// API routes for AJAX calls
		api := protected.Group("/api")
		{
			api.GET("/cluster/topology", h.clusterHandlers.GetClusterTopology)
			api.GET("/cluster/masters", h.clusterHandlers.GetMasters)
			api.GET("/cluster/volumes", h.clusterHandlers.GetVolumeServers)
			api.GET("/admin", h.adminServer.ShowAdmin) // JSON API for admin data

			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
			}
		}
	} else {
		// No authentication required - all routes are public
		r.GET("/", h.ShowDashboard)
		r.GET("/admin", h.ShowDashboard)

		// S3 Buckets management routes
		r.GET("/s3/buckets", h.ShowS3Buckets)
		r.GET("/s3/buckets/:bucket", h.ShowBucketDetails)

		// Cluster management routes
		r.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)
		r.GET("/cluster/volumes", h.clusterHandlers.ShowClusterVolumes)
		r.GET("/cluster/collections", h.clusterHandlers.ShowClusterCollections)

		// API routes for AJAX calls
		api := r.Group("/api")
		{
			api.GET("/cluster/topology", h.clusterHandlers.GetClusterTopology)
			api.GET("/cluster/masters", h.clusterHandlers.GetMasters)
			api.GET("/cluster/volumes", h.clusterHandlers.GetVolumeServers)
			api.GET("/admin", h.adminServer.ShowAdmin) // JSON API for admin data

			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
			}
		}
	}
}

// HealthCheck returns the health status of the admin interface
func (h *AdminHandlers) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

// ShowDashboard renders the main admin dashboard
func (h *AdminHandlers) ShowDashboard(c *gin.Context) {
	// Get admin data from the server
	adminData := h.getAdminData(c)

	// Render HTML template
	c.Header("Content-Type", "text/html")
	adminComponent := app.Admin(adminData)
	layoutComponent := layout.Layout(c, adminComponent)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowS3Buckets renders the S3 buckets management page
func (h *AdminHandlers) ShowS3Buckets(c *gin.Context) {
	// Get S3 buckets data from the server
	s3Data := h.getS3BucketsData(c)

	// Render HTML template
	c.Header("Content-Type", "text/html")
	s3Component := app.S3Buckets(s3Data)
	layoutComponent := layout.Layout(c, s3Component)
	err := layoutComponent.Render(c.Request.Context(), c.Writer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to render template: " + err.Error()})
		return
	}
}

// ShowBucketDetails returns detailed information about a specific bucket
func (h *AdminHandlers) ShowBucketDetails(c *gin.Context) {
	bucketName := c.Param("bucket")
	details, err := h.adminServer.GetBucketDetails(bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get bucket details: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, details)
}

// getS3BucketsData retrieves S3 buckets data from the server
func (h *AdminHandlers) getS3BucketsData(c *gin.Context) dash.S3BucketsData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get S3 buckets
	buckets, err := h.adminServer.GetS3Buckets()
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

// getAdminData retrieves admin data from the server (now uses consolidated method)
func (h *AdminHandlers) getAdminData(c *gin.Context) dash.AdminData {
	username := c.GetString("username")

	// Use the consolidated GetAdminData method from AdminServer
	adminData, err := h.adminServer.GetAdminData(username)
	if err != nil {
		// Return default data when services are not available
		if username == "" {
			username = "admin"
		}

		masterNodes := []dash.MasterNode{
			{
				Address:  "localhost:9333",
				IsLeader: true,
				Status:   "unreachable",
			},
		}

		return dash.AdminData{
			Username:      username,
			ClusterStatus: "warning",
			TotalVolumes:  0,
			TotalFiles:    0,
			TotalSize:     0,
			MasterNodes:   masterNodes,
			VolumeServers: []dash.VolumeServer{},
			FilerNodes:    []dash.FilerNode{},
			DataCenters:   []dash.DataCenter{},
			LastUpdated:   time.Now(),
			SystemHealth:  "poor",
		}
	}

	return adminData
}

// Helper functions
func (h *AdminHandlers) determineClusterStatus(topology *dash.ClusterTopology, masters []dash.MasterNode) string {
	if len(topology.VolumeServers) == 0 {
		return "warning"
	}
	return "healthy"
}

func (h *AdminHandlers) determineSystemHealth(topology *dash.ClusterTopology, masters []dash.MasterNode) string {
	if len(topology.VolumeServers) > 0 && len(masters) > 0 {
		return "good"
	}
	return "fair"
}
