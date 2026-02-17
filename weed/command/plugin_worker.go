package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc"
)

var cmdPluginWorker = &Command{
	UsageLine: "plugin_worker -admin=<grpc_address> [-id=<worker_id>] [-plugins=<plugin_types>] [-workingDir=<path>]",
	Short:     "start a plugin-based worker using the new plugin system",
	Long: `Start a worker using the new plugin system. This worker connects to the admin server
via gRPC and registers capabilities for handling maintenance tasks.

Supported plugins: erasure_coding, vacuum, balance

Examples:
  weed plugin_worker -admin=localhost:23646
  weed plugin_worker -admin=admin.example.com:23646 -id=worker-1
  weed plugin_worker -admin=localhost:23646 -plugins=erasure_coding,vacuum
  weed plugin_worker -admin=localhost:23646 -id=ec-worker-1 -workingDir=/tmp/worker
  weed plugin_worker -admin=localhost:23646 -debug
`,
}

var (
	pluginWorkerAdminServer = cmdPluginWorker.Flag.String("admin", "localhost:23646",
		"admin server gRPC address (usually admin HTTP port + 10000)")
	pluginWorkerID = cmdPluginWorker.Flag.String("id", "",
		"plugin worker ID (auto-generated if not specified)")
	pluginWorkerPlugins = cmdPluginWorker.Flag.String("plugins", "erasure_coding,vacuum,balance",
		"comma-separated list of plugin types to enable")
	pluginWorkerWorkingDir = cmdPluginWorker.Flag.String("workingDir", "",
		"working directory for the plugin worker")
	pluginWorkerMaxConcurrent = cmdPluginWorker.Flag.Int("maxConcurrent", 2,
		"maximum number of concurrent jobs")
	pluginWorkerDebug = cmdPluginWorker.Flag.Bool("debug", false,
		"enable debug logging")
	pluginWorkerDebugPort = cmdPluginWorker.Flag.Int("debug.port", 6061,
		"http port for debugging")
	pluginWorkerTimeout = cmdPluginWorker.Flag.Duration("timeout", 30*time.Second,
		"gRPC connection timeout")
)

func init() {
	cmdPluginWorker.Run = runPluginWorker
}

// GenericPluginWorker is a multi-plugin worker that connects to admin server
type GenericPluginWorker struct {
	ID                  string
	AdminServer         pb.ServerAddress
	Plugins             []string
	MaxConcurrentJobs   int
	WorkingDir          string
	PluginServiceClient plugin_pb.PluginServiceClient
	Conn                *grpc.ClientConn
	Context             context.Context
	Cancel              context.CancelFunc
	mu                  sync.RWMutex
	isRunning           bool
}

// NewGenericPluginWorker creates a new generic plugin worker
func NewGenericPluginWorker(adminServer pb.ServerAddress, plugins []string, workingDir string, maxConcurrent int, id string) *GenericPluginWorker {
	workerID := id
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%s-%d", hostname(), time.Now().UnixNano())
	}
	return &GenericPluginWorker{
		ID:                workerID,
		AdminServer:       adminServer,
		Plugins:           plugins,
		MaxConcurrentJobs: maxConcurrent,
		WorkingDir:        workingDir,
	}
}

// Start connects to admin server and registers plugins
func (w *GenericPluginWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isRunning {
		return fmt.Errorf("worker is already running")
	}

	w.Context, w.Cancel = context.WithCancel(ctx)

	// Create gRPC connection
	dialCtx, cancel := context.WithTimeout(w.Context, *pluginWorkerTimeout)
	defer cancel()

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	conn, err := grpc.DialContext(dialCtx, w.AdminServer.ToGrpcAddress(), grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to admin grpc server at %s: %v", w.AdminServer.ToGrpcAddress(), err)
	}

	w.Conn = conn
	w.PluginServiceClient = plugin_pb.NewPluginServiceClient(conn)

	glog.Infof("Successfully connected to admin grpc server at %s", w.AdminServer)

	// Register plugin with admin server
	capabilities := []string{}
	for _, plugin := range w.Plugins {
		plugin = strings.TrimSpace(plugin)
		if plugin != "" {
			capabilities = append(capabilities, plugin)
		}
	}

	connectReq := &plugin_pb.PluginConnectRequest{
		PluginId:          w.ID,
		PluginName:        fmt.Sprintf("GenericWorker-%s", strings.Join(w.Plugins, ",")),
		Version:           version.VERSION,
		Capabilities:      capabilities,
		MaxConcurrentJobs: int32(w.MaxConcurrentJobs),
		SupportsStreaming: false,
		Port:              0,
		Metadata: map[string]string{
			"working_dir": w.WorkingDir,
		},
	}

	connectCtx, cancel := context.WithTimeout(w.Context, 10*time.Second)
	defer cancel()

	connectResp, err := w.PluginServiceClient.Connect(connectCtx, connectReq)
	if err != nil {
		return fmt.Errorf("failed to register with admin server: %v", err)
	}

	if !connectResp.Success {
		return fmt.Errorf("plugin registration failed: %s", connectResp.Message)
	}

	glog.Infof("Plugin registered successfully. Assigned types: %v", connectResp.AssignedTypes)

	w.isRunning = true

	// Start background health reporting
	go w.healthReportLoop()

	glog.Infof("Plugin worker started successfully with ID: %s", w.ID)
	return nil
}

// Stop gracefully stops the worker
func (w *GenericPluginWorker) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isRunning {
		return nil
	}

	w.isRunning = false
	if w.Cancel != nil {
		w.Cancel()
	}

	if w.Conn != nil {
		return w.Conn.Close()
	}

	return nil
}

// healthReportLoop sends periodic health reports to admin server
func (w *GenericPluginWorker) healthReportLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.Context.Done():
			return
		case <-ticker.C:
			w.sendHealthReport()
		}
	}
}

// sendHealthReport sends a health report to the admin server
func (w *GenericPluginWorker) sendHealthReport() {
	healthReport := &plugin_pb.HealthReport{
		PluginId:    w.ID,
		TimestampMs: time.Now().UnixMilli(),
		Status:      plugin_pb.HealthStatus_HEALTH_STATUS_HEALTHY,
		ActiveJobs:  0,
		CpuPercent:  0,
		MemoryBytes: 0,
		JobProgress: []*plugin_pb.JobProgress{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := w.PluginServiceClient.ReportHealth(ctx, healthReport)
	if err != nil {
		glog.Warningf("Failed to send health report: %v", err)
	}
}

// IsRunning checks if the worker is currently running
func (w *GenericPluginWorker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isRunning
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return hostname
}

func runPluginWorker(cmd *Command, args []string) bool {
	if *pluginWorkerDebug {
		grace.StartDebugServer(*pluginWorkerDebugPort)
	}

	util.LoadConfiguration("security", false)

	glog.Infof("Starting plugin worker (v%s)", version.VERSION)
	glog.Infof("Admin server: %s", *pluginWorkerAdminServer)
	if *pluginWorkerID != "" {
		glog.Infof("Worker ID: %s", *pluginWorkerID)
	}
	glog.Infof("Plugins: %s", *pluginWorkerPlugins)

	// Parse plugins
	plugins := strings.Split(*pluginWorkerPlugins, ",")
	validPlugins := []string{}
	for _, p := range plugins {
		p = strings.TrimSpace(p)
		if p != "" {
			validPlugins = append(validPlugins, p)
		}
	}

	if len(validPlugins) == 0 {
		glog.Fatalf("No valid plugins specified. Valid options: erasure_coding, vacuum, balance")
		return false
	}

	// Set up working directory
	workingDir := *pluginWorkerWorkingDir
	if workingDir == "" {
		var err error
		workingDir, err = os.Getwd()
		if err != nil {
			glog.Fatalf("Failed to get current working directory: %v", err)
			return false
		}
	}

	// Create and validate working directory
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		glog.Fatalf("Failed to create working directory: %v", err)
		return false
	}

	glog.Infof("Working directory: %s", workingDir)

	// Create plugin-specific subdirectories
	for _, plugin := range validPlugins {
		pluginDir := filepath.Join(workingDir, plugin)
		if err := os.MkdirAll(pluginDir, 0755); err != nil {
			glog.Fatalf("Failed to create plugin directory %s: %v", pluginDir, err)
			return false
		}
	}

	// Create and start the worker
	adminServerAddress := pb.ServerAddress(*pluginWorkerAdminServer)
	worker := NewGenericPluginWorker(adminServerAddress, validPlugins, workingDir, *pluginWorkerMaxConcurrent, *pluginWorkerID)

	ctx := context.Background()
	if err := worker.Start(ctx); err != nil {
		glog.Fatalf("Failed to start plugin worker: %v", err)
		return false
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	glog.Infof("Plugin worker is running. Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-sigChan
	glog.Infof("Shutdown signal received, stopping plugin worker...")

	if err := worker.Stop(); err != nil {
		glog.Errorf("Error stopping plugin worker: %v", err)
	}

	glog.Infof("Plugin worker stopped")
	return true
}
