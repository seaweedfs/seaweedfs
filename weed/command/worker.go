package command

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-id=<worker_id>] [-jobType=all] [-workingDir=<path>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=4] [-metricsPort=<port>] [-metricsIp=<ip>] [-debug]",
	Short:     "start a plugin.proto worker process",
	Long: `Start an external plugin worker using weed/pb/plugin.proto over gRPC.

This command provides plugin job type handlers for cluster maintenance,
including descriptor delivery, heartbeat/load reporting, detection, and execution.

Behavior:
  - Use -jobType to choose handlers by category or explicit name (comma-separated)
  - Categories: "all" (every registered handler), "default" (lightweight jobs),
    "heavy" (resource-intensive jobs like erasure coding)
  - Explicit job type names and aliases are still supported (e.g. "vacuum", "ec")
  - Categories and explicit names can be mixed (e.g. "default,iceberg")
  - Use -workingDir to persist worker.id for stable worker identity across restarts
  - Use -metricsPort/-metricsIp to expose /health, /ready, and /metrics

Examples:
  weed worker -admin=localhost:23646
  weed worker -admin=localhost:23646 -jobType=all
  weed worker -admin=localhost:23646 -jobType=default
  weed worker -admin=localhost:23646 -jobType=heavy
  weed worker -admin=localhost:23646 -jobType=default,iceberg
  weed worker -admin=localhost:23646 -jobType=vacuum,volume_balance
  weed worker -admin=localhost:23646 -jobType=erasure_coding
  weed worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
  weed worker -admin=localhost:23646 -workingDir=/var/lib/seaweedfs-plugin
  weed worker -admin=localhost:23646 -metricsPort=9327 -metricsIp=0.0.0.0
`,
}

var (
	workerAdminServer = cmdWorker.Flag.String("admin", "localhost:23646", "admin server address")
	workerID          = cmdWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
	workerWorkingDir  = cmdWorker.Flag.String("workingDir", "", "working directory for persistent worker state")
	workerJobType     = cmdWorker.Flag.String("jobType", defaultPluginWorkerJobTypes, "job types or categories to serve: all, default, heavy, or explicit names/aliases such as ec, balance, iceberg (comma-separated)")
	workerHeartbeat   = cmdWorker.Flag.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	workerReconnect   = cmdWorker.Flag.Duration("reconnect", 5*time.Second, "reconnect delay")
	workerMaxDetect   = cmdWorker.Flag.Int("maxDetect", 1, "max concurrent detection requests")
	workerMaxExecute  = cmdWorker.Flag.Int("maxExecute", 4, "max concurrent execute requests")
	workerAddress     = cmdWorker.Flag.String("address", "", "worker address advertised to admin")
	workerMetricsPort = cmdWorker.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	workerMetricsIp   = cmdWorker.Flag.String("metricsIp", "0.0.0.0", "Prometheus metrics listen IP")
	workerDebug       = cmdWorker.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	workerDebugPort   = cmdWorker.Flag.Int("debug.port", 6060, "http port for debugging")
)

func init() {
	cmdWorker.Run = runWorker
}

func runWorker(cmd *Command, args []string) bool {
	if *workerDebug {
		grace.StartDebugServer(*workerDebugPort)
	}

	return runPluginWorkerWithOptions(pluginWorkerRunOptions{
		AdminServer: *workerAdminServer,
		WorkerID:    *workerID,
		WorkingDir:  *workerWorkingDir,
		JobTypes:    *workerJobType,
		Heartbeat:   *workerHeartbeat,
		Reconnect:   *workerReconnect,
		MaxDetect:   *workerMaxDetect,
		MaxExecute:  *workerMaxExecute,
		Address:     *workerAddress,
		MetricsPort: *workerMetricsPort,
		MetricsIP:   *workerMetricsIp,
	})
}
