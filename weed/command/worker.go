package command

import (
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	statsCollect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	// TODO: Implement additional task packages (add to default capabilities when ready):
	// _ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/remote" - for uploading volumes to remote/cloud storage
	// _ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/replication" - for fixing replication issues and maintaining data consistency
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-capabilities=<task_types>] [-maxConcurrent=<num>] [-workingDir=<path>] [-metricsPort=<port>] [-debug]",
	Short:     "start a maintenance worker to process cluster maintenance tasks",
	Long: `Start a maintenance worker that connects to an admin server to process
maintenance tasks like vacuum, erasure coding, remote upload, and replication fixes.

The worker ID and address are automatically generated.
The worker connects to the admin server via gRPC (admin HTTP port + 10000).

Examples:
  weed worker -admin=localhost:23646
  weed worker -admin=admin.example.com:23646
  weed worker -admin=localhost:23646 -capabilities=vacuum,replication
  weed worker -admin=localhost:23646 -maxConcurrent=4
  weed worker -admin=localhost:23646 -workingDir=/tmp/worker
  weed worker -admin=localhost:23646 -metricsPort=9327
  weed worker -admin=localhost:23646 -debug -debug.port=6060
`,
}

var (
	workerAdminServer         = cmdWorker.Flag.String("admin", "localhost:23646", "admin server address")
	workerCapabilities        = cmdWorker.Flag.String("capabilities", "vacuum,ec,balance", "comma-separated list of task types this worker can handle")
	workerMaxConcurrent       = cmdWorker.Flag.Int("maxConcurrent", 2, "maximum number of concurrent tasks")
	workerHeartbeatInterval   = cmdWorker.Flag.Duration("heartbeat", 30*time.Second, "heartbeat interval")
	workerTaskRequestInterval = cmdWorker.Flag.Duration("taskInterval", 5*time.Second, "task request interval")
	workerWorkingDir          = cmdWorker.Flag.String("workingDir", "", "working directory for the worker")
	workerMetricsPort         = cmdWorker.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	workerMetricsIp           = cmdWorker.Flag.String("metricsIp", "0.0.0.0", "Prometheus metrics listen IP")
	workerDebug               = cmdWorker.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	workerDebugPort           = cmdWorker.Flag.Int("debug.port", 6060, "http port for debugging")
)

func init() {
	cmdWorker.Run = runWorker

	// Set default capabilities from registered task types
	// This happens after package imports have triggered auto-registration
	tasks.SetDefaultCapabilitiesFromRegistry()
}

func runWorker(cmd *Command, args []string) bool {
	if *workerDebug {
		grace.StartDebugServer(*workerDebugPort)
	}

	util.LoadConfiguration("security", false)

	glog.Infof("Starting maintenance worker")
	glog.Infof("Admin server: %s", *workerAdminServer)
	glog.Infof("Capabilities: %s", *workerCapabilities)

	// Parse capabilities
	capabilities := parseCapabilities(*workerCapabilities)
	if len(capabilities) == 0 {
		glog.Fatalf("No valid capabilities specified")
		return false
	}

	// Set working directory and create task-specific subdirectories
	var baseWorkingDir string
	if *workerWorkingDir != "" {
		glog.Infof("Setting working directory to: %s", *workerWorkingDir)
		if err := os.Chdir(*workerWorkingDir); err != nil {
			glog.Fatalf("Failed to change working directory: %v", err)
			return false
		}
		wd, err := os.Getwd()
		if err != nil {
			glog.Fatalf("Failed to get working directory: %v", err)
			return false
		}
		baseWorkingDir = wd
		glog.Infof("Current working directory: %s", baseWorkingDir)
	} else {
		// Use default working directory when not specified
		wd, err := os.Getwd()
		if err != nil {
			glog.Fatalf("Failed to get current working directory: %v", err)
			return false
		}
		baseWorkingDir = wd
		glog.Infof("Using current working directory: %s", baseWorkingDir)
	}

	// Create task-specific subdirectories
	for _, capability := range capabilities {
		taskDir := filepath.Join(baseWorkingDir, string(capability))
		if err := os.MkdirAll(taskDir, 0755); err != nil {
			glog.Fatalf("Failed to create task directory %s: %v", taskDir, err)
			return false
		}
		glog.Infof("Created task directory: %s", taskDir)
	}

	// Create gRPC dial option using TLS configuration
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	// Create worker configuration
	config := &types.WorkerConfig{
		AdminServer:         *workerAdminServer,
		Capabilities:        capabilities,
		MaxConcurrent:       *workerMaxConcurrent,
		HeartbeatInterval:   *workerHeartbeatInterval,
		TaskRequestInterval: *workerTaskRequestInterval,
		BaseWorkingDir:      baseWorkingDir,
		GrpcDialOption:      grpcDialOption,
	}

	// Create worker instance
	workerInstance, err := worker.NewWorker(config)
	if err != nil {
		glog.Fatalf("Failed to create worker: %v", err)
		return false
	}
	adminClient, err := worker.CreateAdminClient(*workerAdminServer, workerInstance.ID(), grpcDialOption)
	if err != nil {
		glog.Fatalf("Failed to create admin client: %v", err)
		return false
	}

	// Set admin client
	workerInstance.SetAdminClient(adminClient)

	// Set working directory
	if *workerWorkingDir != "" {
		glog.Infof("Setting working directory to: %s", *workerWorkingDir)
		if err := os.Chdir(*workerWorkingDir); err != nil {
			glog.Fatalf("Failed to change working directory: %v", err)
			return false
		}
		wd, err := os.Getwd()
		if err != nil {
			glog.Fatalf("Failed to get working directory: %v", err)
			return false
		}
		glog.Infof("Current working directory: %s", wd)
	}

	// Start metrics HTTP server if port is specified
	if *workerMetricsPort > 0 {
		go startWorkerMetricsServer(*workerMetricsIp, *workerMetricsPort, workerInstance)
	}

	// Start the worker
	err = workerInstance.Start()
	if err != nil {
		glog.Errorf("Failed to start worker: %v", err)
		return false
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	glog.Infof("Maintenance worker %s started successfully", workerInstance.ID())
	glog.Infof("Press Ctrl+C to stop the worker")

	// Wait for shutdown signal
	<-sigChan
	glog.Infof("Shutdown signal received, stopping worker...")

	// Gracefully stop the worker
	err = workerInstance.Stop()
	if err != nil {
		glog.Errorf("Error stopping worker: %v", err)
	}
	glog.Infof("Worker stopped")

	return true
}

// parseCapabilities converts comma-separated capability string to task types
func parseCapabilities(capabilityStr string) []types.TaskType {
	if capabilityStr == "" {
		return nil
	}

	capabilityMap := map[string]types.TaskType{}

	// Populate capabilityMap with registered task types
	typesRegistry := tasks.GetGlobalTypesRegistry()
	for taskType := range typesRegistry.GetAllDetectors() {
		// Use the task type string directly as the key
		capabilityMap[strings.ToLower(string(taskType))] = taskType
	}

	// Add common aliases for convenience
	if taskType, exists := capabilityMap["erasure_coding"]; exists {
		capabilityMap["ec"] = taskType
	}
	if taskType, exists := capabilityMap["remote_upload"]; exists {
		capabilityMap["remote"] = taskType
	}
	if taskType, exists := capabilityMap["fix_replication"]; exists {
		capabilityMap["replication"] = taskType
	}

	var capabilities []types.TaskType
	parts := strings.Split(capabilityStr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if taskType, exists := capabilityMap[part]; exists {
			capabilities = append(capabilities, taskType)
		} else {
			glog.Warningf("Unknown capability: %s", part)
		}
	}

	return capabilities
}

// Legacy compatibility types for backward compatibility
// These will be deprecated in future versions

// WorkerStatus represents the current status of a worker (deprecated)
type WorkerStatus struct {
	WorkerID       string           `json:"worker_id"`
	Address        string           `json:"address"`
	Status         string           `json:"status"`
	Capabilities   []types.TaskType `json:"capabilities"`
	MaxConcurrent  int              `json:"max_concurrent"`
	CurrentLoad    int              `json:"current_load"`
	LastHeartbeat  time.Time        `json:"last_heartbeat"`
	CurrentTasks   []types.Task     `json:"current_tasks"`
	Uptime         time.Duration    `json:"uptime"`
	TasksCompleted int              `json:"tasks_completed"`
	TasksFailed    int              `json:"tasks_failed"`
}

// startWorkerMetricsServer starts the HTTP metrics server for the worker
func startWorkerMetricsServer(ip string, port int, _ *worker.Worker) {
	mux := http.NewServeMux()

	// Register Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.HandlerFor(statsCollect.Gather, promhttp.HandlerOpts{}))

	// Register health check endpoint for Kubernetes probes
	mux.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		// Worker is considered healthy if it's running
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{"status":"ok"}`))
	})

	addr := statsCollect.JoinHostPort(ip, port)
	glog.Infof("Worker metrics HTTP server starting on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		glog.Errorf("Worker metrics HTTP server error: %v", err)
	}
}
