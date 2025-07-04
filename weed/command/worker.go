package command

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-id=<worker_id>] [-addr=<listen_address>] [-capabilities=<task_types>]",
	Short:     "start a maintenance worker to process cluster maintenance tasks",
	Long: `Start a maintenance worker that connects to an admin server to process
maintenance tasks like vacuum, erasure coding, remote upload, and replication fixes.

Examples:
  weed worker -admin=localhost:9333
  weed worker -admin=admin.example.com:9333 -id=worker-1 -addr=:8082
  weed worker -admin=localhost:9333 -capabilities=vacuum,replication
`,
}

var (
	workerAdminServer         = cmdWorker.Flag.String("admin", "localhost:9333", "admin server address")
	workerID                  = cmdWorker.Flag.String("id", "", "worker identifier (auto-generated if empty)")
	workerListenAddr          = cmdWorker.Flag.String("addr", ":8082", "worker listen address")
	workerCapabilities        = cmdWorker.Flag.String("capabilities", "vacuum,ec,remote,replication,balance", "comma-separated list of task types this worker can handle")
	workerMaxConcurrent       = cmdWorker.Flag.Int("maxConcurrent", 2, "maximum number of concurrent tasks")
	workerHeartbeatInterval   = cmdWorker.Flag.Duration("heartbeat", 30*time.Second, "heartbeat interval")
	workerTaskRequestInterval = cmdWorker.Flag.Duration("taskInterval", 5*time.Second, "task request interval")
)

func init() {
	cmdWorker.Run = runWorker
}

func runWorker(cmd *Command, args []string) bool {
	util.LoadConfiguration("security", false)

	// Generate worker ID if not provided
	workerIDValue := *workerID
	if workerIDValue == "" {
		hostname, _ := os.Hostname()
		workerIDValue = fmt.Sprintf("worker-%s-%d", hostname, time.Now().Unix())
	}

	glog.Infof("Starting maintenance worker %s", workerIDValue)
	glog.Infof("Admin server: %s", *workerAdminServer)
	glog.Infof("Listen address: %s", *workerListenAddr)
	glog.Infof("Capabilities: %s", *workerCapabilities)

	// Parse capabilities
	capabilities := parseCapabilities(*workerCapabilities)
	if len(capabilities) == 0 {
		glog.Fatalf("No valid capabilities specified")
		return false
	}

	// Create and configure worker service
	worker := dash.NewMaintenanceWorkerService(workerIDValue, *workerListenAddr, *workerAdminServer)
	worker.SetCapabilities(capabilities)
	worker.SetMaxConcurrent(*workerMaxConcurrent)
	worker.SetHeartbeatInterval(*workerHeartbeatInterval)
	worker.SetTaskRequestInterval(*workerTaskRequestInterval)

	// Create admin client connection
	adminClient, err := createAdminClient(*workerAdminServer)
	if err != nil {
		glog.Fatalf("Failed to create admin client: %v", err)
		return false
	}
	worker.SetAdminClient(adminClient)

	// Start the worker
	err = worker.Start()
	if err != nil {
		glog.Fatalf("Failed to start worker: %v", err)
		return false
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	glog.Infof("Maintenance worker %s started successfully", workerIDValue)
	glog.Infof("Press Ctrl+C to stop the worker")

	// Wait for shutdown signal
	<-sigChan
	glog.Infof("Shutdown signal received, stopping worker...")

	// Gracefully stop the worker
	worker.Stop()
	glog.Infof("Worker stopped")

	return true
}

// parseCapabilities converts comma-separated capability string to task types
func parseCapabilities(capabilityStr string) []dash.MaintenanceTaskType {
	if capabilityStr == "" {
		return nil
	}

	capabilityMap := map[string]dash.MaintenanceTaskType{
		"vacuum":              dash.TaskTypeVacuum,
		"ec":                  dash.TaskTypeErasureCoding,
		"remote":              dash.TaskTypeRemoteUpload,
		"replication":         dash.TaskTypeFixReplication,
		"balance":             dash.TaskTypeBalance,
		"cluster_replication": dash.TaskTypeClusterReplication,
	}

	var capabilities []dash.MaintenanceTaskType
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

// createAdminClient creates a connection to the admin server
func createAdminClient(adminAddress string) (*dash.AdminServer, error) {
	// For now, we'll create a minimal admin client that can perform operations
	// In a real implementation, this would be a proper gRPC client to the admin server
	client := &dash.AdminServer{}

	// Set up connection details
	// This is a simplified implementation - in practice you'd want proper gRPC client

	return client, nil
}

// WorkerStatus represents the current status of a worker
type WorkerStatus struct {
	WorkerID       string                     `json:"worker_id"`
	Address        string                     `json:"address"`
	Status         string                     `json:"status"`
	Capabilities   []dash.MaintenanceTaskType `json:"capabilities"`
	MaxConcurrent  int                        `json:"max_concurrent"`
	CurrentLoad    int                        `json:"current_load"`
	LastHeartbeat  time.Time                  `json:"last_heartbeat"`
	CurrentTasks   []dash.MaintenanceTask     `json:"current_tasks"`
	Uptime         time.Duration              `json:"uptime"`
	TasksCompleted int                        `json:"tasks_completed"`
	TasksFailed    int                        `json:"tasks_failed"`
}
