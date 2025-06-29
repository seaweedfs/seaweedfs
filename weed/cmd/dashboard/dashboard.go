package main

import (
	"crypto/tls"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/cmd/dashboard/dash"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

//go:embed static
var staticFs embed.FS

//go:embed templates
var templatesFs embed.FS

func main() {
	port := flag.Int("port", 9999, "The port of the SeaweedFS dashboard")
	masters := flag.String("masters", "localhost:9333", "Comma-separated list of master servers")
	filer := flag.String("filer", "localhost:8888", "Filer server address")
	tlsCertPath := flag.String("tlsCert", "", "Path to the server certificate")
	tlsKeyPath := flag.String("tlsKey", "", "Path to the server private key")
	sessionSecret := flag.String("sessionSecret", "SeaweedFS-Dashboard-Secret-2024", "Session encryption secret")
	metricsServer := flag.String("metricsServer", "", "The host:port of the metrics server")
	adminUser := flag.String("adminUser", "admin", "Default admin username")
	adminPassword := flag.String("adminPassword", "", "Default admin password (if empty, authentication is disabled)")
	flag.Parse()

	// Parse master addresses
	masterAddresses := util.StringSplit(*masters, ",")
	var masterServerAddresses []pb.ServerAddress
	for _, addr := range masterAddresses {
		masterServerAddresses = append(masterServerAddresses, pb.ServerAddress(addr))
	}

	// Parse filer address
	filerAddress := pb.ServerAddress(*filer)

	// Set up TLS if certificates provided
	var tlsConfig *tls.Config
	if *tlsCertPath != "" && *tlsKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
		if err != nil {
			log.Fatalf("Failed to load TLS certificates: %v", err)
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	// Create dashboard server
	dashboardServer := dash.NewDashboardServer(
		masterServerAddresses,
		filerAddress,
		security.LoadClientTLS(util.GetViper(), "grpc.client"),
		*adminUser,
		*adminPassword,
	)

	// Initialize Gin router
	if gin.Mode() == gin.DebugMode {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Set up static file serving
	fsys := fs.FS(staticFs)
	contentStatic, _ := fs.Sub(fsys, "static")
	r.StaticFS("/static", http.FS(contentStatic))
	r.StaticFileFS("/favicon.ico", "static/favicon.ico", http.FS(fsys))

	// Set up session store
	store := cookie.NewStore([]byte(*sessionSecret))
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   86400 * 7, // 7 days
		HttpOnly: true,
		Secure:   tlsConfig != nil,
	})
	r.Use(sessions.Sessions("seaweedfs-dashboard", store))

	// Public routes (no authentication required)
	r.GET("/", dashboardServer.ShowLoginForm)
	r.GET("/login", dashboardServer.ShowLoginForm)
	r.POST("/login", dashboardServer.ProcessLogin)
	r.GET("/logout", dashboardServer.Logout)
	r.GET("/health", dashboardServer.HealthCheck)

	// Protected routes
	auth := r.Group("/")
	if *adminPassword != "" {
		auth.Use(dashboardServer.AuthMiddleware())
	}
	{
		// Main dashboard
		auth.GET("dashboard", dashboardServer.ShowDashboardPage)
		auth.GET("overview", dashboardServer.ShowOverviewPage)

		// Cluster management
		cluster := auth.Group("cluster")
		{
			cluster.GET("/", dashboardServer.ShowClusterPage)
			cluster.GET("topology", dashboardServer.GetClusterTopology)
			cluster.GET("status", dashboardServer.GetClusterStatus)
			cluster.POST("grow", dashboardServer.GrowVolumes)
			cluster.POST("vacuum", dashboardServer.VacuumVolumes)
			cluster.POST("rebalance", dashboardServer.RebalanceCluster)
		}

		// Volume management
		volumes := auth.Group("volumes")
		{
			volumes.GET("/", dashboardServer.ShowVolumesPage)
			volumes.GET("list", dashboardServer.GetVolumesList)
			volumes.GET("details/:id", dashboardServer.GetVolumeDetails)
			volumes.POST("create", dashboardServer.CreateVolume)
			volumes.DELETE("delete/:id", dashboardServer.DeleteVolume)
			volumes.POST("replicate/:id", dashboardServer.ReplicateVolume)
		}

		// Filer management
		filer := auth.Group("filer")
		{
			filer.GET("/", dashboardServer.ShowFilerPage)
			filer.GET("browser", dashboardServer.ShowFileBrowser)
			filer.GET("browser/api/*path", dashboardServer.FileBrowserAPI)
			filer.POST("upload", dashboardServer.UploadFile)
			filer.DELETE("delete", dashboardServer.DeleteFile)
			filer.POST("mkdir", dashboardServer.CreateDirectory)
		}

		// Monitoring & metrics
		metrics := auth.Group("metrics")
		{
			metrics.GET("/", dashboardServer.ShowMetricsPage)
			metrics.GET("data", dashboardServer.GetMetricsData)
			metrics.GET("realtime", dashboardServer.GetRealtimeMetrics)
			metrics.GET("performance", dashboardServer.GetPerformanceMetrics)
			metrics.GET("storage", dashboardServer.GetStorageMetrics)
		}

		// Configuration management
		config := auth.Group("config")
		{
			config.GET("/", dashboardServer.ShowConfigPage)
			config.GET("current", dashboardServer.GetCurrentConfig)
			config.POST("update", dashboardServer.UpdateConfig)
			config.GET("backup", dashboardServer.BackupConfig)
			config.POST("restore", dashboardServer.RestoreConfig)
		}

		// Logs and diagnostics
		logs := auth.Group("logs")
		{
			logs.GET("/", dashboardServer.ShowLogsPage)
			logs.GET("master", dashboardServer.GetMasterLogs)
			logs.GET("volume", dashboardServer.GetVolumeLogs)
			logs.GET("filer", dashboardServer.GetFilerLogs)
			logs.GET("download/:type", dashboardServer.DownloadLogs)
		}

		// System maintenance
		maintenance := auth.Group("maintenance")
		{
			maintenance.GET("/", dashboardServer.ShowMaintenancePage)
			maintenance.POST("gc", dashboardServer.RunGarbageCollection)
			maintenance.POST("compact", dashboardServer.CompactVolumes)
			maintenance.POST("fix", dashboardServer.FixVolumes)
			maintenance.GET("status", dashboardServer.GetMaintenanceStatus)
		}

		// API endpoints for external access
		api := auth.Group("api/v1")
		{
			api.GET("cluster/info", dashboardServer.APIClusterInfo)
			api.GET("volumes", dashboardServer.APIVolumesList)
			api.GET("metrics", dashboardServer.APIMetrics)
			api.POST("volumes/grow", dashboardServer.APIGrowVolumes)
		}
	}

	// Start metrics reporting if configured
	if *metricsServer != "" {
		go stats.LoopPushingMetric("seaweedfs-dashboard", "main", *metricsServer, 15)
	}

	// Start server
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	if tlsConfig != nil {
		srv := &http.Server{
			Addr:      addr,
			Handler:   r,
			TLSConfig: tlsConfig,
		}
		log.Printf("SeaweedFS Dashboard running on https://%s", addr)
		if err := srv.ListenAndServeTLS("", ""); err != nil {
			log.Fatalf("Failed to start HTTPS server: %v", err)
		}
	} else {
		log.Printf("SeaweedFS Dashboard running on http://%s", addr)
		if err := r.Run(addr); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}
}
