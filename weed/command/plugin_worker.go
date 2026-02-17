package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc"
)

var cmdPluginWorker = &Command{
	UsageLine: "plugin.worker -admin=<admin_server> [-id=<worker_id>] [-jobType=vacuum] [-workingDir=<path>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=2]",
	Short:     "start a plugin.proto worker process",
	Long: `Start an external plugin worker using weed/pb/plugin.proto over gRPC.

This command currently provides the vacuum job type contract and stream runtime,
including descriptor delivery, heartbeat/load reporting, detection, and execution.

Behavior:
  - Use -jobType to choose which plugin job handler to serve (currently: vacuum)
  - Use -workingDir to persist plugin.worker.id for stable worker identity across restarts

Examples:
  weed plugin.worker -admin=localhost:23646
  weed plugin.worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
  weed plugin.worker -admin=localhost:23646 -workingDir=/var/lib/seaweedfs-plugin
`,
}

var (
	pluginWorkerAdminServer = cmdPluginWorker.Flag.String("admin", "localhost:23646", "admin server address")
	pluginWorkerID          = cmdPluginWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
	pluginWorkerWorkingDir  = cmdPluginWorker.Flag.String("workingDir", "", "working directory for persistent worker state")
	pluginWorkerJobType     = cmdPluginWorker.Flag.String("jobType", "vacuum", "plugin job type to serve")
	pluginWorkerHeartbeat   = cmdPluginWorker.Flag.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	pluginWorkerReconnect   = cmdPluginWorker.Flag.Duration("reconnect", 5*time.Second, "reconnect delay")
	pluginWorkerMaxDetect   = cmdPluginWorker.Flag.Int("maxDetect", 1, "max concurrent detection requests")
	pluginWorkerMaxExecute  = cmdPluginWorker.Flag.Int("maxExecute", 2, "max concurrent execute requests")
	pluginWorkerAddress     = cmdPluginWorker.Flag.String("address", "", "worker address advertised to admin")
)

func init() {
	cmdPluginWorker.Run = runPluginWorker
}

func runPluginWorker(cmd *Command, args []string) bool {
	util.LoadConfiguration("security", false)

	dialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")
	workerID, err := resolvePluginWorkerID(*pluginWorkerID, *pluginWorkerWorkingDir)
	if err != nil {
		glog.Errorf("Failed to resolve plugin worker ID: %v", err)
		return false
	}

	handler, err := buildPluginWorkerHandler(*pluginWorkerJobType, dialOption)
	if err != nil {
		glog.Errorf("Failed to build plugin worker handler: %v", err)
		return false
	}
	worker, err := pluginworker.NewWorker(pluginworker.WorkerOptions{
		AdminServer:             *pluginWorkerAdminServer,
		WorkerID:                workerID,
		WorkerVersion:           version.Version(),
		WorkerAddress:           *pluginWorkerAddress,
		HeartbeatInterval:       *pluginWorkerHeartbeat,
		ReconnectDelay:          *pluginWorkerReconnect,
		MaxDetectionConcurrency: *pluginWorkerMaxDetect,
		MaxExecutionConcurrency: *pluginWorkerMaxExecute,
		GrpcDialOption:          dialOption,
		Handler:                 handler,
	})
	if err != nil {
		glog.Errorf("Failed to create plugin worker: %v", err)
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		sig := <-sigCh
		fmt.Printf("\nReceived signal %v, stopping plugin worker...\n", sig)
		cancel()
	}()

	fmt.Printf("Starting plugin worker (admin=%s)\n", *pluginWorkerAdminServer)
	if err := worker.Run(ctx); err != nil {
		glog.Errorf("Plugin worker stopped with error: %v", err)
		return false
	}
	fmt.Println("Plugin worker stopped")
	return true
}

func resolvePluginWorkerID(explicitID string, workingDir string) (string, error) {
	id := strings.TrimSpace(explicitID)
	if id != "" {
		return id, nil
	}

	workingDir = strings.TrimSpace(workingDir)
	if workingDir == "" {
		return "", nil
	}
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		return "", err
	}

	workerIDPath := filepath.Join(workingDir, "plugin.worker.id")
	if data, err := os.ReadFile(workerIDPath); err == nil {
		if persisted := strings.TrimSpace(string(data)); persisted != "" {
			return persisted, nil
		}
	}

	generated := fmt.Sprintf("plugin-%d", time.Now().UnixNano())
	if err := os.WriteFile(workerIDPath, []byte(generated+"\n"), 0644); err != nil {
		return "", err
	}
	return generated, nil
}

func buildPluginWorkerHandler(jobType string, dialOption grpc.DialOption) (pluginworker.JobHandler, error) {
	switch strings.ToLower(strings.TrimSpace(jobType)) {
	case "", "vacuum":
		return pluginworker.NewVacuumHandler(dialOption), nil
	default:
		return nil, fmt.Errorf("unsupported plugin job type %q", jobType)
	}
}
