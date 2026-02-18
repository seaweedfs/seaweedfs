package command

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	UsageLine: "plugin.worker -admin=<admin_server> [-id=<worker_id>] [-jobType=vacuum,volume_balance,erasure_coding,dummy_stress] [-workingDir=<path>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=2]",
	Short:     "start a plugin.proto worker process",
	Long: `Start an external plugin worker using weed/pb/plugin.proto over gRPC.

This command currently provides vacuum, volume_balance, erasure_coding, and dummy_stress job type
contracts with the plugin stream runtime, including descriptor delivery,
heartbeat/load reporting, detection, and execution.

Behavior:
  - Use -jobType to choose one or more plugin job handlers (comma-separated list)
  - Use -workingDir to persist plugin.worker.id for stable worker identity across restarts

Examples:
  weed plugin.worker -admin=localhost:23646
  weed plugin.worker -admin=localhost:23646 -jobType=volume_balance
  weed plugin.worker -admin=localhost:23646 -jobType=vacuum,volume_balance
  weed plugin.worker -admin=localhost:23646 -jobType=erasure_coding
  weed plugin.worker -admin=localhost:23646 -jobType=dummy_stress
  weed plugin.worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
  weed plugin.worker -admin=localhost:23646 -workingDir=/var/lib/seaweedfs-plugin
`,
}

var (
	pluginWorkerAdminServer = cmdPluginWorker.Flag.String("admin", "localhost:23646", "admin server address")
	pluginWorkerID          = cmdPluginWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
	pluginWorkerWorkingDir  = cmdPluginWorker.Flag.String("workingDir", "", "working directory for persistent worker state")
	pluginWorkerJobType     = cmdPluginWorker.Flag.String("jobType", "vacuum,volume_balance,erasure_coding", "job types to serve (comma-separated list)")
	pluginWorkerHeartbeat   = cmdPluginWorker.Flag.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	pluginWorkerReconnect   = cmdPluginWorker.Flag.Duration("reconnect", 5*time.Second, "reconnect delay")
	pluginWorkerMaxDetect   = cmdPluginWorker.Flag.Int("maxDetect", 1, "max concurrent detection requests")
	pluginWorkerMaxExecute  = cmdPluginWorker.Flag.Int("maxExecute", 4, "max concurrent execute requests")
	pluginWorkerAddress     = cmdPluginWorker.Flag.String("address", "", "worker address advertised to admin")
)

func init() {
	cmdPluginWorker.Run = runPluginWorker
}

func runPluginWorker(cmd *Command, args []string) bool {
	util.LoadConfiguration("security", false)

	resolvedAdminServer := resolvePluginWorkerAdminServer(*pluginWorkerAdminServer)
	if resolvedAdminServer != *pluginWorkerAdminServer {
		fmt.Printf("Resolved admin worker gRPC endpoint: %s -> %s\n", *pluginWorkerAdminServer, resolvedAdminServer)
	}

	dialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")
	workerID, err := resolvePluginWorkerID(*pluginWorkerID, *pluginWorkerWorkingDir)
	if err != nil {
		glog.Errorf("Failed to resolve plugin worker ID: %v", err)
		return false
	}

	handlers, err := buildPluginWorkerHandlers(*pluginWorkerJobType, dialOption)
	if err != nil {
		glog.Errorf("Failed to build plugin worker handlers: %v", err)
		return false
	}
	worker, err := pluginworker.NewWorker(pluginworker.WorkerOptions{
		AdminServer:             resolvedAdminServer,
		WorkerID:                workerID,
		WorkerVersion:           version.Version(),
		WorkerAddress:           *pluginWorkerAddress,
		HeartbeatInterval:       *pluginWorkerHeartbeat,
		ReconnectDelay:          *pluginWorkerReconnect,
		MaxDetectionConcurrency: *pluginWorkerMaxDetect,
		MaxExecutionConcurrency: *pluginWorkerMaxExecute,
		GrpcDialOption:          dialOption,
		Handlers:                handlers,
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

	fmt.Printf("Starting plugin worker (admin=%s)\n", resolvedAdminServer)
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
	canonicalJobType, err := canonicalPluginWorkerJobType(jobType)
	if err != nil {
		return nil, err
	}

	switch canonicalJobType {
	case "vacuum":
		return pluginworker.NewVacuumHandler(dialOption), nil
	case "volume_balance":
		return pluginworker.NewVolumeBalanceHandler(dialOption), nil
	case "erasure_coding":
		return pluginworker.NewErasureCodingHandler(dialOption), nil
	case "dummy_stress":
		return pluginworker.NewDummyStressHandler(dialOption), nil
	default:
		return nil, fmt.Errorf("unsupported plugin job type %q", canonicalJobType)
	}
}

func buildPluginWorkerHandlers(jobTypes string, dialOption grpc.DialOption) ([]pluginworker.JobHandler, error) {
	parsedJobTypes, err := parsePluginWorkerJobTypes(jobTypes)
	if err != nil {
		return nil, err
	}

	handlers := make([]pluginworker.JobHandler, 0, len(parsedJobTypes))
	for _, jobType := range parsedJobTypes {
		handler, buildErr := buildPluginWorkerHandler(jobType, dialOption)
		if buildErr != nil {
			return nil, buildErr
		}
		handlers = append(handlers, handler)
	}
	return handlers, nil
}

func parsePluginWorkerJobTypes(jobTypes string) ([]string, error) {
	jobTypes = strings.TrimSpace(jobTypes)
	if jobTypes == "" {
		return []string{"vacuum"}, nil
	}

	parts := strings.Split(jobTypes, ",")
	parsed := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		canonical, err := canonicalPluginWorkerJobType(part)
		if err != nil {
			return nil, err
		}
		if _, found := seen[canonical]; found {
			continue
		}
		seen[canonical] = struct{}{}
		parsed = append(parsed, canonical)
	}

	if len(parsed) == 0 {
		return []string{"vacuum"}, nil
	}
	return parsed, nil
}

func canonicalPluginWorkerJobType(jobType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(jobType)) {
	case "", "vacuum":
		return "vacuum", nil
	case "volume_balance", "balance", "volume.balance", "volume-balance":
		return "volume_balance", nil
	case "erasure_coding", "erasure-coding", "erasure.coding", "ec":
		return "erasure_coding", nil
	case "dummy_stress", "dummy-stress", "dummy.stress", "dummy", "stress", "stress_test":
		return "dummy_stress", nil
	default:
		return "", fmt.Errorf("unsupported plugin job type %q", jobType)
	}
}

func resolvePluginWorkerAdminServer(adminServer string) string {
	adminServer = strings.TrimSpace(adminServer)
	host, httpPort, hasExplicitGrpcPort, err := parsePluginWorkerAdminAddress(adminServer)
	if err != nil || hasExplicitGrpcPort {
		return adminServer
	}

	workerGrpcPort, err := fetchPluginWorkerGrpcPort(host, httpPort)
	if err != nil || workerGrpcPort <= 0 {
		return adminServer
	}

	// Keep canonical host:http form when admin gRPC follows the default +10000 rule.
	if workerGrpcPort == httpPort+10000 {
		return adminServer
	}

	return fmt.Sprintf("%s:%d.%d", host, httpPort, workerGrpcPort)
}

func parsePluginWorkerAdminAddress(adminServer string) (host string, httpPort int, hasExplicitGrpcPort bool, err error) {
	adminServer = strings.TrimSpace(adminServer)
	colonIndex := strings.LastIndex(adminServer, ":")
	if colonIndex <= 0 || colonIndex >= len(adminServer)-1 {
		return "", 0, false, fmt.Errorf("invalid admin address %q", adminServer)
	}

	host = adminServer[:colonIndex]
	portPart := adminServer[colonIndex+1:]
	if dotIndex := strings.LastIndex(portPart, "."); dotIndex > 0 && dotIndex < len(portPart)-1 {
		if _, parseErr := strconv.Atoi(portPart[dotIndex+1:]); parseErr == nil {
			hasExplicitGrpcPort = true
			portPart = portPart[:dotIndex]
		}
	}

	httpPort, err = strconv.Atoi(portPart)
	if err != nil || httpPort <= 0 {
		return "", 0, false, fmt.Errorf("invalid admin http port in %q", adminServer)
	}
	return host, httpPort, hasExplicitGrpcPort, nil
}

func fetchPluginWorkerGrpcPort(host string, httpPort int) (int, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	address := util.JoinHostPort(host, httpPort)
	var lastErr error

	for _, scheme := range []string{"http", "https"} {
		statusURL := fmt.Sprintf("%s://%s/api/plugin/status", scheme, address)
		resp, err := client.Get(statusURL)
		if err != nil {
			lastErr = err
			continue
		}

		var payload struct {
			WorkerGrpcPort int `json:"worker_grpc_port"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status code %d from %s", resp.StatusCode, statusURL)
			continue
		}
		if decodeErr != nil {
			lastErr = fmt.Errorf("decode plugin status from %s: %w", statusURL, decodeErr)
			continue
		}
		if payload.WorkerGrpcPort <= 0 {
			lastErr = fmt.Errorf("plugin status from %s returned empty worker_grpc_port", statusURL)
			continue
		}

		return payload.WorkerGrpcPort, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("plugin status endpoint unavailable")
	}
	return 0, lastErr
}
