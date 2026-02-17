package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var cmdPluginWorker = &Command{
	UsageLine: "plugin.worker -admin=<admin_server> [-id=<worker_id>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=2]",
	Short:     "start a plugin.proto worker process",
	Long: `Start an external plugin worker using weed/pb/plugin.proto over gRPC.

This command currently provides the vacuum job type contract and stream runtime,
including descriptor delivery, heartbeat/load reporting, detection, and execution.

Examples:
  weed plugin.worker -admin=localhost:23646
  weed plugin.worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
`,
}

var (
	pluginWorkerAdminServer = cmdPluginWorker.Flag.String("admin", "localhost:23646", "admin server address")
	pluginWorkerID          = cmdPluginWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
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
	handler := pluginworker.NewVacuumHandler(dialOption)
	worker, err := pluginworker.NewWorker(pluginworker.WorkerOptions{
		AdminServer:             *pluginWorkerAdminServer,
		WorkerID:                *pluginWorkerID,
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
