package command

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-capabilities=<task_types>] [-maxConcurrent=<num>]",
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
`,
}

var (
	workerAdminServer         = cmdWorker.Flag.String("admin", "localhost:23646", "admin server address")
	workerCapabilities        = cmdWorker.Flag.String("capabilities", "vacuum,ec,remote,replication,balance", "comma-separated list of task types this worker can handle")
	workerMaxConcurrent       = cmdWorker.Flag.Int("maxConcurrent", 2, "maximum number of concurrent tasks")
	workerHeartbeatInterval   = cmdWorker.Flag.Duration("heartbeat", 30*time.Second, "heartbeat interval")
	workerTaskRequestInterval = cmdWorker.Flag.Duration("taskInterval", 5*time.Second, "task request interval")
)

func init() {
	cmdWorker.Run = runWorker

	// Set default capabilities from registered task types
	// This happens after package imports have triggered auto-registration
	tasks.SetDefaultCapabilitiesFromRegistry()
}

func runWorker(cmd *Command, args []string) bool {
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

	// Create worker configuration
	config := &types.WorkerConfig{
		AdminServer:         *workerAdminServer,
		Capabilities:        capabilities,
		MaxConcurrent:       *workerMaxConcurrent,
		HeartbeatInterval:   *workerHeartbeatInterval,
		TaskRequestInterval: *workerTaskRequestInterval,
	}

	// Create worker instance
	workerInstance, err := worker.NewWorker(config)
	if err != nil {
		glog.Fatalf("Failed to create worker: %v", err)
		return false
	}

	// Create admin client with LoadClientTLS
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")
	adminClient, err := worker.CreateAdminClient(*workerAdminServer, workerInstance.ID(), grpcDialOption)
	if err != nil {
		glog.Fatalf("Failed to create admin client: %v", err)
		return false
	}

	// Set admin client
	workerInstance.SetAdminClient(adminClient)

	// Start the worker
	err = workerInstance.Start()
	if err != nil {
		glog.Fatalf("Failed to start worker: %v", err)
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
