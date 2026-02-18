package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-id=<worker_id>] [-jobType=vacuum,volume_balance,erasure_coding] [-workingDir=<path>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=2]",
	Short:     "start a plugin.proto worker process",
	Long: `Start an external plugin worker using weed/pb/plugin.proto over gRPC.

This command now uses the same implementation as "weed plugin.worker".

Compatibility flags:
  -capabilities maps to -jobType (legacy maintenance worker flag)
  -maxConcurrent maps to -maxExecute (legacy maintenance worker flag)

Examples:
  weed worker -admin=localhost:23646
  weed worker -admin=localhost:23646 -jobType=volume_balance
  weed worker -admin=localhost:23646 -jobType=vacuum,volume_balance
  weed worker -admin=localhost:23646 -jobType=erasure_coding
  weed worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
  weed worker -admin=localhost:23646 -workingDir=/var/lib/seaweedfs-plugin
`,
}

var (
	workerAdminServer = cmdWorker.Flag.String("admin", "localhost:23646", "admin server address")
	workerID          = cmdWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
	workerWorkingDir  = cmdWorker.Flag.String("workingDir", "", "working directory for persistent worker state")
	workerJobType     = cmdWorker.Flag.String("jobType", defaultPluginWorkerJobTypes, "job types to serve (comma-separated list)")
	workerHeartbeat   = cmdWorker.Flag.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	workerReconnect   = cmdWorker.Flag.Duration("reconnect", 5*time.Second, "reconnect delay")
	workerMaxDetect   = cmdWorker.Flag.Int("maxDetect", 1, "max concurrent detection requests")
	workerMaxExecute  = cmdWorker.Flag.Int("maxExecute", 4, "max concurrent execute requests")
	workerAddress     = cmdWorker.Flag.String("address", "", "worker address advertised to admin")

	workerCapabilitiesLegacy = cmdWorker.Flag.String("capabilities", "", "deprecated alias for -jobType (e.g. vacuum,ec,balance)")
	workerMaxConcurrentAlias = cmdWorker.Flag.Int("maxConcurrent", 0, "deprecated alias for -maxExecute")
	workerTaskIntervalLegacy = cmdWorker.Flag.Duration("taskInterval", 0, "deprecated and ignored")
	workerMetricsPortLegacy  = cmdWorker.Flag.Int("metricsPort", 0, "deprecated and ignored")
	workerMetricsIpLegacy    = cmdWorker.Flag.String("metricsIp", "0.0.0.0", "deprecated and ignored")
	workerDebug              = cmdWorker.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	workerDebugPort          = cmdWorker.Flag.Int("debug.port", 6060, "http port for debugging")
)

func init() {
	cmdWorker.Run = runWorker
}

func runWorker(cmd *Command, args []string) bool {
	if *workerDebug {
		grace.StartDebugServer(*workerDebugPort)
	}

	jobTypes := strings.TrimSpace(*workerJobType)
	if legacy := strings.TrimSpace(*workerCapabilitiesLegacy); legacy != "" {
		mappedJobTypes, err := canonicalizeLegacyWorkerCapabilities(legacy)
		if err != nil {
			glog.Errorf("Invalid legacy worker capabilities: %v", err)
			return false
		}
		if strings.TrimSpace(*workerJobType) != "" && strings.TrimSpace(*workerJobType) != defaultPluginWorkerJobTypes {
			glog.Warningf("Both -jobType and deprecated -capabilities provided, using -capabilities=%s", legacy)
		} else {
			glog.Warningf("Flag -capabilities is deprecated, use -jobType instead")
		}
		jobTypes = mappedJobTypes
	}

	maxExecute := *workerMaxExecute
	if *workerMaxConcurrentAlias > 0 {
		glog.Warningf("Flag -maxConcurrent is deprecated, use -maxExecute instead")
		maxExecute = *workerMaxConcurrentAlias
	}

	if *workerTaskIntervalLegacy > 0 {
		glog.Warningf("Flag -taskInterval is ignored by plugin worker runtime")
	}
	if *workerMetricsPortLegacy > 0 {
		glog.Warningf("Flag -metricsPort is ignored by plugin worker runtime")
	}
	if strings.TrimSpace(*workerMetricsIpLegacy) != "" && strings.TrimSpace(*workerMetricsIpLegacy) != "0.0.0.0" {
		glog.Warningf("Flag -metricsIp is ignored by plugin worker runtime")
	}

	return runPluginWorkerWithOptions(pluginWorkerRunOptions{
		AdminServer: *workerAdminServer,
		WorkerID:    *workerID,
		WorkingDir:  *workerWorkingDir,
		JobTypes:    jobTypes,
		Heartbeat:   *workerHeartbeat,
		Reconnect:   *workerReconnect,
		MaxDetect:   *workerMaxDetect,
		MaxExecute:  maxExecute,
		Address:     *workerAddress,
	})
}

func canonicalizeLegacyWorkerCapabilities(legacyValue string) (string, error) {
	parts := strings.Split(legacyValue, ",")
	canonical := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		switch strings.ToLower(name) {
		case "replication", "fix_replication":
			return "", fmt.Errorf("legacy capability %q is no longer supported in plugin worker", name)
		case "remote", "remote_upload":
			return "", fmt.Errorf("legacy capability %q is no longer supported in plugin worker", name)
		}

		jobType, err := canonicalPluginWorkerJobType(name)
		if err != nil {
			return "", err
		}
		if _, ok := seen[jobType]; ok {
			continue
		}
		seen[jobType] = struct{}{}
		canonical = append(canonical, jobType)
	}

	if len(canonical) == 0 {
		return "", fmt.Errorf("no valid job types found in %q", legacyValue)
	}

	return strings.Join(canonical, ","), nil
}
