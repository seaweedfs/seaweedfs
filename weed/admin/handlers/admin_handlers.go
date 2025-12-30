package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// AdminHandlers contains all the HTTP handlers for the admin interface
type AdminHandlers struct {
	adminServer            *dash.AdminServer
	authHandlers           *AuthHandlers
	clusterHandlers        *ClusterHandlers
	fileBrowserHandlers    *FileBrowserHandlers
	userHandlers           *UserHandlers
	policyHandlers         *PolicyHandlers
	maintenanceHandlers    *MaintenanceHandlers
	mqHandlers             *MessageQueueHandlers
	serviceAccountHandlers *ServiceAccountHandlers
}

// NewAdminHandlers creates a new instance of AdminHandlers
func NewAdminHandlers(adminServer *dash.AdminServer) *AdminHandlers {
	authHandlers := NewAuthHandlers(adminServer)
	clusterHandlers := NewClusterHandlers(adminServer)
	fileBrowserHandlers := NewFileBrowserHandlers(adminServer)
	userHandlers := NewUserHandlers(adminServer)
	policyHandlers := NewPolicyHandlers(adminServer)
	maintenanceHandlers := NewMaintenanceHandlers(adminServer)
	mqHandlers := NewMessageQueueHandlers(adminServer)
	serviceAccountHandlers := NewServiceAccountHandlers(adminServer)
	return &AdminHandlers{
		adminServer:            adminServer,
		authHandlers:           authHandlers,
		clusterHandlers:        clusterHandlers,
		fileBrowserHandlers:    fileBrowserHandlers,
		userHandlers:           userHandlers,
		policyHandlers:         policyHandlers,
		maintenanceHandlers:    maintenanceHandlers,
		mqHandlers:             mqHandlers,
		serviceAccountHandlers: serviceAccountHandlers,
	}
}

// SetupRoutes configures all the routes for the admin interface
func (h *AdminHandlers) SetupRoutes(r *gin.Engine, authRequired bool, adminUser, adminPassword, readOnlyUser, readOnlyPassword string) {
	// Health check (no auth required)
	r.GET("/health", h.HealthCheck)

	// Prometheus metrics endpoint (no auth required)
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(stats.Gather, promhttp.HandlerOpts{})))

	// Favicon route (no auth required) - redirect to static version
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/static/favicon.ico")
	})

	if authRequired {
		// Authentication routes (no auth required)
		r.GET("/login", h.authHandlers.ShowLogin)
		r.POST("/login", h.authHandlers.HandleLogin(adminUser, adminPassword, readOnlyUser, readOnlyPassword))
		r.GET("/logout", h.authHandlers.HandleLogout)

		// Protected routes group
		protected := r.Group("/")
		protected.Use(dash.RequireAuth())

		// Main admin interface routes
		protected.GET("/", h.ShowDashboard)
		protected.GET("/admin", h.ShowDashboard)

		// Object Store management routes
		protected.GET("/object-store/buckets", h.ShowS3Buckets)
		protected.GET("/object-store/buckets/:bucket", h.ShowBucketDetails)
		protected.GET("/object-store/users", h.userHandlers.ShowObjectStoreUsers)
		protected.GET("/object-store/policies", h.policyHandlers.ShowPolicies)
		protected.GET("/object-store/service-accounts", h.serviceAccountHandlers.ShowServiceAccounts)

		// File browser routes
		protected.GET("/files", h.fileBrowserHandlers.ShowFileBrowser)

		// Cluster management routes
		protected.GET("/cluster/masters", h.clusterHandlers.ShowClusterMasters)
		protected.GET("/cluster/filers", h.clusterHandlers.ShowClusterFilers)
		protected.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)

		// Storage management routes
		protected.GET("/storage/volumes", h.clusterHandlers.ShowClusterVolumes)
		protected.GET("/storage/volumes/:id/:server", h.clusterHandlers.ShowVolumeDetails)
		protected.GET("/storage/collections", h.clusterHandlers.ShowClusterCollections)
		protected.GET("/storage/collections/:name", h.clusterHandlers.ShowCollectionDetails)
		protected.GET("/storage/ec-shards", h.clusterHandlers.ShowClusterEcShards)
		protected.GET("/storage/ec-volumes/:id", h.clusterHandlers.ShowEcVolumeDetails)

		// Message Queue management routes
		protected.GET("/mq/brokers", h.mqHandlers.ShowBrokers)
		protected.GET("/mq/topics", h.mqHandlers.ShowTopics)
		protected.GET("/mq/topics/:namespace/:topic", h.mqHandlers.ShowTopicDetails)

		// Maintenance system routes
		protected.GET("/maintenance", h.maintenanceHandlers.ShowMaintenanceQueue)
		protected.GET("/maintenance/workers", h.maintenanceHandlers.ShowMaintenanceWorkers)
		protected.GET("/maintenance/config", h.maintenanceHandlers.ShowMaintenanceConfig)
		protected.POST("/maintenance/config", dash.RequireWriteAccess(), h.maintenanceHandlers.UpdateMaintenanceConfig)
		protected.GET("/maintenance/config/:taskType", h.maintenanceHandlers.ShowTaskConfig)
		protected.POST("/maintenance/config/:taskType", dash.RequireWriteAccess(), h.maintenanceHandlers.UpdateTaskConfig)
		protected.GET("/maintenance/tasks/:id", h.maintenanceHandlers.ShowTaskDetail)

		// API routes for AJAX calls
		api := r.Group("/api")
		api.Use(dash.RequireAuthAPI()) // Use API-specific auth middleware
		{
			api.GET("/cluster/topology", h.clusterHandlers.GetClusterTopology)
			api.GET("/cluster/masters", h.clusterHandlers.GetMasters)
			api.GET("/cluster/volumes", h.clusterHandlers.GetVolumeServers)
			api.GET("/admin", h.adminServer.ShowAdmin)      // JSON API for admin data
			api.GET("/config", h.adminServer.GetConfigInfo) // Configuration information

			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", dash.RequireWriteAccess(), h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", dash.RequireWriteAccess(), h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
				s3Api.PUT("/buckets/:bucket/quota", dash.RequireWriteAccess(), h.adminServer.UpdateBucketQuota)
				s3Api.PUT("/buckets/:bucket/owner", dash.RequireWriteAccess(), h.adminServer.UpdateBucketOwner)
			}

			// User management API routes
			usersApi := api.Group("/users")
			{
				usersApi.GET("", h.userHandlers.GetUsers)
				usersApi.POST("", dash.RequireWriteAccess(), h.userHandlers.CreateUser)
				usersApi.GET("/:username", h.userHandlers.GetUserDetails)
				usersApi.PUT("/:username", dash.RequireWriteAccess(), h.userHandlers.UpdateUser)
				usersApi.DELETE("/:username", dash.RequireWriteAccess(), h.userHandlers.DeleteUser)
				usersApi.POST("/:username/access-keys", dash.RequireWriteAccess(), h.userHandlers.CreateAccessKey)
				usersApi.DELETE("/:username/access-keys/:accessKeyId", dash.RequireWriteAccess(), h.userHandlers.DeleteAccessKey)
				usersApi.GET("/:username/policies", h.userHandlers.GetUserPolicies)
				usersApi.PUT("/:username/policies", dash.RequireWriteAccess(), h.userHandlers.UpdateUserPolicies)
			}

			// Service Account management API routes
			saApi := api.Group("/service-accounts")
			{
				saApi.GET("", h.serviceAccountHandlers.GetServiceAccounts)
				saApi.POST("", dash.RequireWriteAccess(), h.serviceAccountHandlers.CreateServiceAccount)
				saApi.GET("/:id", h.serviceAccountHandlers.GetServiceAccountDetails)
				saApi.PUT("/:id", dash.RequireWriteAccess(), h.serviceAccountHandlers.UpdateServiceAccount)
				saApi.DELETE("/:id", dash.RequireWriteAccess(), h.serviceAccountHandlers.DeleteServiceAccount)
			}

			// Object Store Policy management API routes
			objectStorePoliciesApi := api.Group("/object-store/policies")
			{
				objectStorePoliciesApi.GET("", h.policyHandlers.GetPolicies)
				objectStorePoliciesApi.POST("", dash.RequireWriteAccess(), h.policyHandlers.CreatePolicy)
				objectStorePoliciesApi.GET("/:name", h.policyHandlers.GetPolicy)
				objectStorePoliciesApi.PUT("/:name", dash.RequireWriteAccess(), h.policyHandlers.UpdatePolicy)
				objectStorePoliciesApi.DELETE("/:name", dash.RequireWriteAccess(), h.policyHandlers.DeletePolicy)
				objectStorePoliciesApi.POST("/validate", h.policyHandlers.ValidatePolicy)
			}

			// File management API routes
			filesApi := api.Group("/files")
			{
				filesApi.DELETE("/delete", dash.RequireWriteAccess(), h.fileBrowserHandlers.DeleteFile)
				filesApi.DELETE("/delete-multiple", dash.RequireWriteAccess(), h.fileBrowserHandlers.DeleteMultipleFiles)
				filesApi.POST("/create-folder", dash.RequireWriteAccess(), h.fileBrowserHandlers.CreateFolder)
				filesApi.POST("/upload", dash.RequireWriteAccess(), h.fileBrowserHandlers.UploadFile)
				filesApi.GET("/download", h.fileBrowserHandlers.DownloadFile)
				filesApi.GET("/view", h.fileBrowserHandlers.ViewFile)
				filesApi.GET("/properties", h.fileBrowserHandlers.GetFileProperties)
			}

			// Volume management API routes
			volumeApi := api.Group("/volumes")
			{
				volumeApi.POST("/:id/:server/vacuum", dash.RequireWriteAccess(), h.clusterHandlers.VacuumVolume)
			}

			// Maintenance API routes
			maintenanceApi := api.Group("/maintenance")
			{
				maintenanceApi.POST("/scan", dash.RequireWriteAccess(), h.adminServer.TriggerMaintenanceScan)
				maintenanceApi.GET("/tasks", h.adminServer.GetMaintenanceTasks)
				maintenanceApi.GET("/tasks/:id", h.adminServer.GetMaintenanceTask)
				maintenanceApi.GET("/tasks/:id/detail", h.adminServer.GetMaintenanceTaskDetailAPI)
				maintenanceApi.POST("/tasks/:id/cancel", dash.RequireWriteAccess(), h.adminServer.CancelMaintenanceTask)
				maintenanceApi.GET("/workers", h.adminServer.GetMaintenanceWorkersAPI)
				maintenanceApi.GET("/workers/:id", h.adminServer.GetMaintenanceWorker)
				maintenanceApi.GET("/workers/:id/logs", h.adminServer.GetWorkerLogs)
				maintenanceApi.GET("/stats", h.adminServer.GetMaintenanceStats)
				maintenanceApi.GET("/config", h.adminServer.GetMaintenanceConfigAPI)
				maintenanceApi.PUT("/config", dash.RequireWriteAccess(), h.adminServer.UpdateMaintenanceConfigAPI)
			}

			// Message Queue API routes
			mqApi := api.Group("/mq")
			{
				mqApi.GET("/topics/:namespace/:topic", h.mqHandlers.GetTopicDetailsAPI)
				mqApi.POST("/topics/create", dash.RequireWriteAccess(), h.mqHandlers.CreateTopicAPI)
				mqApi.POST("/topics/retention/update", dash.RequireWriteAccess(), h.mqHandlers.UpdateTopicRetentionAPI)
				mqApi.POST("/retention/purge", dash.RequireWriteAccess(), h.adminServer.TriggerTopicRetentionPurgeAPI)
			}
		}
	} else {
		// No authentication required - all routes are public
		r.GET("/", h.ShowDashboard)
		r.GET("/admin", h.ShowDashboard)

		// Object Store management routes
		r.GET("/object-store/buckets", h.ShowS3Buckets)
		r.GET("/object-store/buckets/:bucket", h.ShowBucketDetails)
		r.GET("/object-store/users", h.userHandlers.ShowObjectStoreUsers)
		r.GET("/object-store/policies", h.policyHandlers.ShowPolicies)
		r.GET("/object-store/service-accounts", h.serviceAccountHandlers.ShowServiceAccounts)

		// File browser routes
		r.GET("/files", h.fileBrowserHandlers.ShowFileBrowser)

		// Cluster management routes
		r.GET("/cluster/masters", h.clusterHandlers.ShowClusterMasters)
		r.GET("/cluster/filers", h.clusterHandlers.ShowClusterFilers)
		r.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)

		// Storage management routes
		r.GET("/storage/volumes", h.clusterHandlers.ShowClusterVolumes)
		r.GET("/storage/volumes/:id/:server", h.clusterHandlers.ShowVolumeDetails)
		r.GET("/storage/collections", h.clusterHandlers.ShowClusterCollections)
		r.GET("/storage/collections/:name", h.clusterHandlers.ShowCollectionDetails)
		r.GET("/storage/ec-shards", h.clusterHandlers.ShowClusterEcShards)
		r.GET("/storage/ec-volumes/:id", h.clusterHandlers.ShowEcVolumeDetails)

		// Message Queue management routes
		r.GET("/mq/brokers", h.mqHandlers.ShowBrokers)
		r.GET("/mq/topics", h.mqHandlers.ShowTopics)
		r.GET("/mq/topics/:namespace/:topic", h.mqHandlers.ShowTopicDetails)

		// Maintenance system routes
		r.GET("/maintenance", h.maintenanceHandlers.ShowMaintenanceQueue)
		r.GET("/maintenance/workers", h.maintenanceHandlers.ShowMaintenanceWorkers)
		r.GET("/maintenance/config", h.maintenanceHandlers.ShowMaintenanceConfig)
		r.POST("/maintenance/config", h.maintenanceHandlers.UpdateMaintenanceConfig)
		r.GET("/maintenance/config/:taskType", h.maintenanceHandlers.ShowTaskConfig)
		r.POST("/maintenance/config/:taskType", h.maintenanceHandlers.UpdateTaskConfig)
		r.GET("/maintenance/tasks/:id", h.maintenanceHandlers.ShowTaskDetail)

		// API routes for AJAX calls
		api := r.Group("/api")
		{
			api.GET("/cluster/topology", h.clusterHandlers.GetClusterTopology)
			api.GET("/cluster/masters", h.clusterHandlers.GetMasters)
			api.GET("/cluster/volumes", h.clusterHandlers.GetVolumeServers)
			api.GET("/admin", h.adminServer.ShowAdmin)      // JSON API for admin data
			api.GET("/config", h.adminServer.GetConfigInfo) // Configuration information

			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
				s3Api.PUT("/buckets/:bucket/quota", h.adminServer.UpdateBucketQuota)
				s3Api.PUT("/buckets/:bucket/owner", h.adminServer.UpdateBucketOwner)
			}

			// User management API routes
			usersApi := api.Group("/users")
			{
				usersApi.GET("", h.userHandlers.GetUsers)
				usersApi.POST("", h.userHandlers.CreateUser)
				usersApi.GET("/:username", h.userHandlers.GetUserDetails)
				usersApi.PUT("/:username", h.userHandlers.UpdateUser)
				usersApi.DELETE("/:username", h.userHandlers.DeleteUser)
				usersApi.POST("/:username/access-keys", h.userHandlers.CreateAccessKey)
				usersApi.DELETE("/:username/access-keys/:accessKeyId", h.userHandlers.DeleteAccessKey)
				usersApi.GET("/:username/policies", h.userHandlers.GetUserPolicies)
				usersApi.PUT("/:username/policies", h.userHandlers.UpdateUserPolicies)
			}

			// Service Account management API routes
			saApi := api.Group("/service-accounts")
			{
				saApi.GET("", h.serviceAccountHandlers.GetServiceAccounts)
				saApi.POST("", h.serviceAccountHandlers.CreateServiceAccount)
				saApi.GET("/:id", h.serviceAccountHandlers.GetServiceAccountDetails)
				saApi.PUT("/:id", h.serviceAccountHandlers.UpdateServiceAccount)
				saApi.DELETE("/:id", h.serviceAccountHandlers.DeleteServiceAccount)
			}

			// Object Store Policy management API routes
			objectStorePoliciesApi := api.Group("/object-store/policies")
			{
				objectStorePoliciesApi.GET("", h.policyHandlers.GetPolicies)
				objectStorePoliciesApi.POST("", h.policyHandlers.CreatePolicy)
				objectStorePoliciesApi.GET("/:name", h.policyHandlers.GetPolicy)
				objectStorePoliciesApi.PUT("/:name", h.policyHandlers.UpdatePolicy)
				objectStorePoliciesApi.DELETE("/:name", h.policyHandlers.DeletePolicy)
				objectStorePoliciesApi.POST("/validate", h.policyHandlers.ValidatePolicy)
			}

			// File management API routes
			filesApi := api.Group("/files")
			{
				filesApi.DELETE("/delete", h.fileBrowserHandlers.DeleteFile)
				filesApi.DELETE("/delete-multiple", h.fileBrowserHandlers.DeleteMultipleFiles)
				filesApi.POST("/create-folder", h.fileBrowserHandlers.CreateFolder)
				filesApi.POST("/upload", h.fileBrowserHandlers.UploadFile)
				filesApi.GET("/download", h.fileBrowserHandlers.DownloadFile)
				filesApi.GET("/view", h.fileBrowserHandlers.ViewFile)
				filesApi.GET("/properties", h.fileBrowserHandlers.GetFileProperties)
			}

			// Volume management API routes
			volumeApi := api.Group("/volumes")
			{
				volumeApi.POST("/:id/:server/vacuum", h.clusterHandlers.VacuumVolume)
			}

			// Maintenance API routes
			maintenanceApi := api.Group("/maintenance")
			{
				maintenanceApi.POST("/scan", h.adminServer.TriggerMaintenanceScan)
				maintenanceApi.GET("/tasks", h.adminServer.GetMaintenanceTasks)
				maintenanceApi.GET("/tasks/:id", h.adminServer.GetMaintenanceTask)
				maintenanceApi.GET("/tasks/:id/detail", h.adminServer.GetMaintenanceTaskDetailAPI)
				maintenanceApi.POST("/tasks/:id/cancel", h.adminServer.CancelMaintenanceTask)
				maintenanceApi.GET("/workers", h.adminServer.GetMaintenanceWorkersAPI)
				maintenanceApi.GET("/workers/:id", h.adminServer.GetMaintenanceWorker)
				maintenanceApi.GET("/workers/:id/logs", h.adminServer.GetWorkerLogs)
				maintenanceApi.GET("/stats", h.adminServer.GetMaintenanceStats)
				maintenanceApi.GET("/config", h.adminServer.GetMaintenanceConfigAPI)
				maintenanceApi.PUT("/config", h.adminServer.UpdateMaintenanceConfigAPI)
			}

			// Message Queue API routes
			mqApi := api.Group("/mq")
			{
				mqApi.GET("/topics/:namespace/:topic", h.mqHandlers.GetTopicDetailsAPI)
				mqApi.POST("/topics/create", h.mqHandlers.CreateTopicAPI)
				mqApi.POST("/topics/retention/update", h.mqHandlers.UpdateTopicRetentionAPI)
				mqApi.POST("/retention/purge", h.adminServer.TriggerTopicRetentionPurgeAPI)
			}
		}
	}
}

// HealthCheck returns the health status of the admin interface
func (h *AdminHandlers) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{"health": "ok"})
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

// ShowS3Buckets renders the Object Store buckets management page
func (h *AdminHandlers) ShowS3Buckets(c *gin.Context) {
	// Get Object Store buckets data from the server
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

// getS3BucketsData retrieves Object Store buckets data from the server
func (h *AdminHandlers) getS3BucketsData(c *gin.Context) dash.S3BucketsData {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	// Get Object Store buckets
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
			},
		}

		return dash.AdminData{
			Username:      username,
			TotalVolumes:  0,
			TotalFiles:    0,
			TotalSize:     0,
			MasterNodes:   masterNodes,
			VolumeServers: []dash.VolumeServer{},
			FilerNodes:    []dash.FilerNode{},
			DataCenters:   []dash.DataCenter{},
			LastUpdated:   time.Now(),
		}
	}

	return adminData
}

// Helper functions
