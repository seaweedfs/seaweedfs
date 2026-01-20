package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/layout"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"bytes"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminHandlers contains all the HTTP handlers for the admin interface
type AdminHandlers struct {
	adminServer         *dash.AdminServer
	authHandlers        *AuthHandlers
	clusterHandlers     *ClusterHandlers
	fileBrowserHandlers *FileBrowserHandlers
	userHandlers        *UserHandlers
	policyHandlers      *PolicyHandlers
	maintenanceHandlers *MaintenanceHandlers
	mqHandlers          *MessageQueueHandlers
	roleHandlers        *RoleHandlers
	groupHandlers       *GroupHandlers
}

// NewAdminHandlers creates a new instance of AdminHandlers
func NewAdminHandlers(adminServer *dash.AdminServer) *AdminHandlers {
	return &AdminHandlers{
		adminServer:         adminServer,
		authHandlers:        NewAuthHandlers(adminServer),
		clusterHandlers:     NewClusterHandlers(adminServer),
		fileBrowserHandlers: NewFileBrowserHandlers(adminServer),
		userHandlers:        NewUserHandlers(adminServer),
		policyHandlers:      NewPolicyHandlers(adminServer),
		maintenanceHandlers: NewMaintenanceHandlers(adminServer),
		mqHandlers:          NewMessageQueueHandlers(adminServer),
		roleHandlers:        NewRoleHandlers(adminServer),
		groupHandlers:       NewGroupHandlers(adminServer),
	}
}

// ShowMetrics renders the human-readable metrics page
func (h *AdminHandlers) ShowMetrics(c *gin.Context) {
	// Create a new context with the recorder
	// We can't replace c.Writer easily without side effects, so we just call the handler directly with a fake request/response
	// Actually, promhttp handler writes to http.ResponseWriter.
	// Let's us a temporary buffer.
	buf := new(bytes.Buffer)
	// We need a dummy response writer that writes to buf
	dummyWriter := &bufferResponseWriter{header: make(http.Header), body: buf}
	
	promhttp.HandlerFor(stats.Gather, promhttp.HandlerOpts{DisableCompression: true}).ServeHTTP(dummyWriter, c.Request)

	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	data := dash.MetricsData{
		Username:     username,
		Content:      buf.String(),
	}
	
	c.Header("Content-Type", "text/html")
	metricsComponent := app.Metrics(data)
	layoutComponent := layout.Layout(c, metricsComponent)
	layoutComponent.Render(c.Request.Context(), c.Writer)
}

type bufferResponseWriter struct {
	header http.Header
	code   int
	body   *bytes.Buffer
}

func (w *bufferResponseWriter) Header() http.Header { return w.header }
func (w *bufferResponseWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *bufferResponseWriter) WriteHeader(code int) { w.code = code }

// SetupRoutes configures all the routes for the admin interface
func (h *AdminHandlers) SetupRoutes(r *gin.Engine, authRequired bool, username, password string) {
	// Health check (no auth required)
	r.GET("/health", h.HealthCheck)

	// Favicon route (no auth required) - redirect to static version
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/static/favicon.ico")
	})

	// Metrics route (no auth required)
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(stats.Gather, promhttp.HandlerOpts{})))

	// Metrics UI route (no auth required for now, or use protected?)
	r.GET("/admin/metrics", h.ShowMetrics)


	// Authentication routes (always available for UI consistency)
	r.GET("/login", h.authHandlers.ShowLogin)
	r.POST("/login", h.authHandlers.HandleLogin(username, password))

	r.POST("/logout", h.authHandlers.HandleLogout)

	if authRequired {
		// Protected routes group
		protected := r.Group("/")
		protected.Use(dash.RequireAuth())

		// Main admin interface routes (Dashboard accessible to all authenticated)
		protected.GET("/", h.ShowDashboard)
		protected.GET("/admin", h.ShowDashboard)

		// Object Store management routes (Visible to all authenticated - ReadOnly seeing buckets is fine?)
        // Actually, let's allow all authenticated to see Buckets/Files (Standard user)
		protected.GET("/object-store/buckets", h.ShowS3Buckets)
		protected.GET("/object-store/buckets/:bucket", h.ShowBucketDetails)
        
        // File browser routes (Standard user)
		protected.GET("/files", h.fileBrowserHandlers.ShowFileBrowser)

        // ADMIN ONLY ROUTES
        adminOnly := protected.Group("/")
        adminOnly.Use(h.adminServer.RequirePermission("Admin"))
        {
            // Identity Management
            adminOnly.GET("/iam/users", h.userHandlers.ShowObjectStoreUsers)
            adminOnly.GET("/iam/policies", h.policyHandlers.ShowPolicies)
            adminOnly.GET("/iam/roles", h.roleHandlers.ShowRoles)
            adminOnly.GET("/iam/groups", h.groupHandlers.ShowGroups)

            // Cluster management routes
            adminOnly.GET("/cluster/masters", h.clusterHandlers.ShowClusterMasters)
            adminOnly.GET("/cluster/filers", h.clusterHandlers.ShowClusterFilers)
            adminOnly.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)
            adminOnly.GET("/cluster/volumes", h.clusterHandlers.ShowClusterVolumes)
            adminOnly.GET("/cluster/volumes/:id/:server", h.clusterHandlers.ShowVolumeDetails)
            adminOnly.GET("/cluster/collections", h.clusterHandlers.ShowClusterCollections)
            adminOnly.GET("/cluster/collections/:name", h.clusterHandlers.ShowCollectionDetails)
            adminOnly.GET("/cluster/ec-shards", h.clusterHandlers.ShowClusterEcShards)
            adminOnly.GET("/cluster/ec-volumes/:id", h.clusterHandlers.ShowEcVolumeDetails)

            // Message Queue management routes
            adminOnly.GET("/mq/brokers", h.mqHandlers.ShowBrokers)
            adminOnly.GET("/mq/topics", h.mqHandlers.ShowTopics)
            adminOnly.GET("/mq/topics/:namespace/:topic", h.mqHandlers.ShowTopicDetails)



            // Maintenance system routes
            adminOnly.GET("/maintenance", h.maintenanceHandlers.ShowMaintenanceQueue)
            adminOnly.GET("/maintenance/workers", h.maintenanceHandlers.ShowMaintenanceWorkers)
            adminOnly.GET("/maintenance/config", h.maintenanceHandlers.ShowMaintenanceConfig)
            
            // System Logs
            adminOnly.GET("/logs", h.ShowLogs)

            adminOnly.POST("/maintenance/config", h.maintenanceHandlers.UpdateMaintenanceConfig)
            adminOnly.GET("/maintenance/config/:taskType", h.maintenanceHandlers.ShowTaskConfig)
            adminOnly.POST("/maintenance/config/:taskType", h.maintenanceHandlers.UpdateTaskConfig)
            adminOnly.GET("/maintenance/tasks/:id", h.maintenanceHandlers.ShowTaskDetail)
        }

		// API routes for AJAX calls
		api := r.Group("/api")
		api.Use(dash.RequireAuthAPI()) // Use API-specific auth middleware
		{
			api.GET("/cluster/topology", h.clusterHandlers.GetClusterTopology)
			api.GET("/cluster/masters", h.clusterHandlers.GetMasters)
			api.GET("/cluster/volumes", h.clusterHandlers.GetVolumeServers)
			api.GET("/admin", h.adminServer.ShowAdmin)      // JSON API for admin data
			api.GET("/config", h.adminServer.GetConfigInfo) // Configuration information
			api.GET("/logs", h.GetLogsApi)                  // System Logs API

			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
				s3Api.PUT("/buckets/:bucket/quota", h.adminServer.UpdateBucketQuota)
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

			// Role management API routes
			rolesApi := api.Group("/object-store/roles")
			{
				rolesApi.GET("", h.roleHandlers.GetRoles)
				rolesApi.POST("", h.roleHandlers.CreateRole)
				rolesApi.PUT("/:name", h.roleHandlers.UpdateRole)
				rolesApi.DELETE("/:name", h.roleHandlers.DeleteRole)
			}

            // Group management API routes
            groupsApi := api.Group("/iam/groups")
            {
                groupsApi.GET("", h.groupHandlers.GetGroups)
                groupsApi.GET("/:name", h.groupHandlers.GetGroup)
                groupsApi.POST("", h.groupHandlers.CreateGroup)
                groupsApi.DELETE("/:name", h.groupHandlers.DeleteGroup)
                groupsApi.POST("/:name/members/:user", h.groupHandlers.AddUserToGroup)
                groupsApi.DELETE("/:name/members/:user", h.groupHandlers.RemoveUserFromGroup)
                groupsApi.POST("/:name/policies", h.groupHandlers.AttachPolicy)
                groupsApi.DELETE("/:name/policies", h.groupHandlers.DetachPolicy)
            }

            // IAM User management API routes
            iamUsersApi := api.Group("/iam/users")
            {
                iamUsersApi.GET("", h.userHandlers.GetUsers)
                iamUsersApi.POST("", h.userHandlers.CreateUser)
                iamUsersApi.GET("/:username", h.userHandlers.GetUserDetails)
                iamUsersApi.DELETE("/:username", h.userHandlers.DeleteUser)
                iamUsersApi.GET("/:username/keys", h.userHandlers.GetAccessKeys)
                iamUsersApi.POST("/:username/keys", h.userHandlers.CreateAccessKey)
                iamUsersApi.DELETE("/:username/keys/:accessKeyId", h.userHandlers.DeleteAccessKey)
                iamUsersApi.PUT("/:username/keys/:accessKeyId/status", h.userHandlers.UpdateAccessKeyStatus)
            }

			// File management API routes
			filesApi := api.Group("/files")
			{
				// Read-only routes
				filesApi.GET("/download", h.fileBrowserHandlers.DownloadFile)
				filesApi.GET("/view", h.fileBrowserHandlers.ViewFile)
				filesApi.GET("/properties", h.fileBrowserHandlers.GetFileProperties)

				// Write routes - require Write permission
				filesWriteApi := filesApi.Group("/")
				filesWriteApi.Use(h.adminServer.RequirePermission("Write"))
				{
					filesWriteApi.DELETE("/delete", h.fileBrowserHandlers.DeleteFile)
					filesWriteApi.DELETE("/delete-multiple", h.fileBrowserHandlers.DeleteMultipleFiles)
					filesWriteApi.POST("/create-folder", h.fileBrowserHandlers.CreateFolder)
					filesWriteApi.POST("/upload", h.fileBrowserHandlers.UploadFile)
				}
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
	} else {
		// No authentication required - all routes are public
		r.GET("/", h.ShowDashboard)
		r.GET("/admin", h.ShowDashboard)

		// Object Store management routes
		r.GET("/object-store/buckets", h.ShowS3Buckets)
		r.GET("/object-store/buckets/:bucket", h.ShowBucketDetails)
		r.GET("/iam/users", h.userHandlers.ShowObjectStoreUsers)
		r.GET("/iam/policies", h.policyHandlers.ShowPolicies)
		r.GET("/iam/roles", h.roleHandlers.ShowRoles)

		// File browser routes
		r.GET("/files", h.fileBrowserHandlers.ShowFileBrowser)

		// Cluster management routes
		r.GET("/cluster/masters", h.clusterHandlers.ShowClusterMasters)
		r.GET("/cluster/filers", h.clusterHandlers.ShowClusterFilers)
		r.GET("/cluster/volume-servers", h.clusterHandlers.ShowClusterVolumeServers)
		r.GET("/cluster/volumes", h.clusterHandlers.ShowClusterVolumes)
		r.GET("/cluster/volumes/:id/:server", h.clusterHandlers.ShowVolumeDetails)
		r.GET("/cluster/collections", h.clusterHandlers.ShowClusterCollections)
		r.GET("/cluster/collections/:name", h.clusterHandlers.ShowCollectionDetails)
		r.GET("/cluster/ec-shards", h.clusterHandlers.ShowClusterEcShards)
		r.GET("/cluster/ec-volumes/:id", h.clusterHandlers.ShowEcVolumeDetails)

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
			api.PUT("/config/loglevel", h.UpdateLogLevel)   // Update log level
			api.GET("/logs", h.GetLogsApi)                  // System Logs API
			// S3 API routes
			s3Api := api.Group("/s3")
			{
				s3Api.GET("/buckets", h.adminServer.ListBucketsAPI)
				s3Api.POST("/buckets", h.adminServer.CreateBucket)
				s3Api.DELETE("/buckets/:bucket", h.adminServer.DeleteBucket)
				s3Api.GET("/buckets/:bucket", h.adminServer.ShowBucketDetails)
				s3Api.PUT("/buckets/:bucket/quota", h.adminServer.UpdateBucketQuota)
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

// ShowLogs renders the audit logs page
func (h *AdminHandlers) ShowLogs(c *gin.Context) {
	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}

	data := dash.LogsData{
		Username:    username,
		LastUpdated: time.Now(),
	}

	c.Header("Content-Type", "text/html")
	logsComponent := app.Logs(data)
	layoutComponent := layout.Layout(c, logsComponent)
	layoutComponent.Render(c.Request.Context(), c.Writer)
}

// GetLogsApi returns system logs in JSON format
func (h *AdminHandlers) GetLogsApi(c *gin.Context) {
	limit := 1000
    // Optional: parse limit from query param
    
    offsetID := int64(0)
    // Parse offset from query param ?offset=123
    // ...

	logs := h.adminServer.GetLogs(limit, offsetID)
	c.JSON(http.StatusOK, logs)
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

// UpdateLogLevel updates the system logging verbosity
func (h *AdminHandlers) UpdateLogLevel(c *gin.Context) {
	var req struct {
		Level int `json:"level"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	h.adminServer.SetLogLevel(req.Level)
	c.JSON(http.StatusOK, gin.H{"status": "updated", "level": req.Level})
}

// Helper functions
