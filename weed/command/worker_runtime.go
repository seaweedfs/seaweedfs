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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/security"
	statsCollect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc"
)

const defaultPluginWorkerJobTypes = "vacuum,volume_balance,erasure_coding"

type pluginWorkerRunOptions struct {
	AdminServer string
	WorkerID    string
	WorkingDir  string
	JobTypes    string
	Heartbeat   time.Duration
	Reconnect   time.Duration
	MaxDetect   int
	MaxExecute  int
	Address     string
	MetricsPort int
	MetricsIP   string
}

func runPluginWorkerWithOptions(options pluginWorkerRunOptions) bool {
	util.LoadConfiguration("security", false)

	options.AdminServer = strings.TrimSpace(options.AdminServer)
	if options.AdminServer == "" {
		options.AdminServer = "localhost:23646"
	}

	options.JobTypes = strings.TrimSpace(options.JobTypes)
	if options.JobTypes == "" {
		options.JobTypes = defaultPluginWorkerJobTypes
	}

	if options.Heartbeat <= 0 {
		options.Heartbeat = 15 * time.Second
	}
	if options.Reconnect <= 0 {
		options.Reconnect = 5 * time.Second
	}
	if options.MaxDetect <= 0 {
		options.MaxDetect = 1
	}
	if options.MaxExecute <= 0 {
		options.MaxExecute = 4
	}
	options.MetricsIP = strings.TrimSpace(options.MetricsIP)
	if options.MetricsIP == "" {
		options.MetricsIP = "0.0.0.0"
	}

	resolvedAdminServer := resolvePluginWorkerAdminServer(options.AdminServer)
	if resolvedAdminServer != options.AdminServer {
		fmt.Printf("Resolved admin worker gRPC endpoint: %s -> %s\n", options.AdminServer, resolvedAdminServer)
	}

	dialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")
	workerID, err := resolvePluginWorkerID(options.WorkerID, options.WorkingDir)
	if err != nil {
		glog.Errorf("Failed to resolve plugin worker ID: %v", err)
		return false
	}

	handlers, err := buildPluginWorkerHandlers(options.JobTypes, dialOption, options.MaxExecute, options.WorkingDir)
	if err != nil {
		glog.Errorf("Failed to build plugin worker handlers: %v", err)
		return false
	}
	worker, err := pluginworker.NewWorker(pluginworker.WorkerOptions{
		AdminServer:             resolvedAdminServer,
		WorkerID:                workerID,
		WorkerVersion:           version.Version(),
		WorkerAddress:           options.Address,
		HeartbeatInterval:       options.Heartbeat,
		ReconnectDelay:          options.Reconnect,
		MaxDetectionConcurrency: options.MaxDetect,
		MaxExecutionConcurrency: options.MaxExecute,
		GrpcDialOption:          dialOption,
		Handlers:                handlers,
	})
	if err != nil {
		glog.Errorf("Failed to create plugin worker: %v", err)
		return false
	}

	if options.MetricsPort > 0 {
		go startPluginWorkerMetricsServer(options.MetricsIP, options.MetricsPort, worker)
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

	workerIDPath := filepath.Join(workingDir, "worker.id")
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

// buildPluginWorkerHandler constructs the JobHandler for the given job type.
// maxExecute is forwarded to handlers that use it to report their own
// MaxExecutionConcurrency in Capability for consistency and future-proofing.
// The scheduler's effective per-worker MaxExecutionConcurrency is derived from
// the worker-level configuration (e.g. WorkerOptions.MaxExecutionConcurrency),
// not directly from the handler's Capability.
func buildPluginWorkerHandler(jobType string, dialOption grpc.DialOption, maxExecute int, workingDir string) (pluginworker.JobHandler, error) {
	canonicalJobType, err := canonicalPluginWorkerJobType(jobType)
	if err != nil {
		return nil, err
	}

	switch canonicalJobType {
	case "vacuum":
		return pluginworker.NewVacuumHandler(dialOption, int32(maxExecute)), nil
	case "volume_balance":
		return pluginworker.NewVolumeBalanceHandler(dialOption), nil
	case "erasure_coding":
		return pluginworker.NewErasureCodingHandler(dialOption, workingDir), nil
	default:
		return nil, fmt.Errorf("unsupported plugin job type %q", canonicalJobType)
	}
}

// buildPluginWorkerHandlers constructs a deduplicated slice of JobHandlers for
// the comma-separated jobTypes string, forwarding maxExecute to each handler.
func buildPluginWorkerHandlers(jobTypes string, dialOption grpc.DialOption, maxExecute int, workingDir string) ([]pluginworker.JobHandler, error) {
	parsedJobTypes, err := parsePluginWorkerJobTypes(jobTypes)
	if err != nil {
		return nil, err
	}

	handlers := make([]pluginworker.JobHandler, 0, len(parsedJobTypes))
	for _, jobType := range parsedJobTypes {
		handler, buildErr := buildPluginWorkerHandler(jobType, dialOption, maxExecute, workingDir)
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

func pluginWorkerHealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func pluginWorkerReadyHandler(pluginRuntime *pluginworker.Worker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if pluginRuntime == nil || !pluginRuntime.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func startPluginWorkerMetricsServer(ip string, port int, pluginRuntime *pluginworker.Worker) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", pluginWorkerHealthHandler)
	mux.HandleFunc("/ready", pluginWorkerReadyHandler(pluginRuntime))
	mux.Handle("/metrics", promhttp.HandlerFor(statsCollect.Gather, promhttp.HandlerOpts{}))

	glog.V(0).Infof("Starting plugin worker metrics server at %s", statsCollect.JoinHostPort(ip, port))
	if err := http.ListenAndServe(statsCollect.JoinHostPort(ip, port), mux); err != nil {
		glog.Errorf("Plugin worker metrics server failed to start: %v", err)
	}
}
