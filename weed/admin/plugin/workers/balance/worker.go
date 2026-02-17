package balance

import (
"context"
"flag"
"fmt"
"log"
"net"
"time"

"google.golang.org/grpc"

"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// WorkerConfig holds worker-specific configuration
type WorkerConfig struct {
WorkerID                    string
AdminHost                   string
AdminPort                   int
PluginPort                  int
MinVolumeSize               uint64
MaxVolumeSize               uint64
DataNodeCount               int
ReplicationFactor           int
PreferBalancedDistribution  bool
RebalanceInterval           time.Duration
MaxConcurrentJobs           int
HealthCheckInterval         time.Duration
DiskUsageThreshold          int
AcceptableImbalancePercent  int
}

// Worker represents the balance plugin worker
type Worker struct {
config       *WorkerConfig
pluginClient plugin_pb.PluginServiceClient
conn         *grpc.ClientConn
detector     *Detector
executor     *Executor
activeJobs   map[string]*plugin_pb.ExecuteJobRequest
done         chan bool
isRunning    bool
}

// NewWorker creates a new balance worker
func NewWorker(config *WorkerConfig) *Worker {
return &Worker{
config:     config,
activeJobs: make(map[string]*plugin_pb.ExecuteJobRequest),
done:       make(chan bool),
}
}

// Start initializes and starts the worker
func (w *Worker) Start(ctx context.Context) error {
log.Printf("Starting balance worker: %s", w.config.WorkerID)

// Connect to admin server
if err := w.connectToAdmin(ctx); err != nil {
return fmt.Errorf("failed to connect to admin: %v", err)
}

// Initialize detector
w.detector = NewDetector(DetectionOptions{
AcceptableImbalance: float32(w.config.AcceptableImbalancePercent),
DiskUsageThreshold:  float32(w.config.DiskUsageThreshold),
MinVolumeSize:       w.config.MinVolumeSize,
MaxVolumeSize:       w.config.MaxVolumeSize,
PreferBalancedDist:  w.config.PreferBalancedDistribution,
})

// Initialize executor
w.executor = NewExecutor(&ExecutorConfig{
MinVolumeSize:  w.config.MinVolumeSize,
MaxVolumeSize:  w.config.MaxVolumeSize,
TimeoutPerStep: 2 * time.Minute,
MaxRetries:     3,
})

// Register with admin
if err := w.registerPlugin(ctx); err != nil {
return fmt.Errorf("failed to register: %v", err)
}

w.isRunning = true

// Start background goroutines
go w.heartbeatLoop(ctx)

log.Printf("Balance worker started successfully")
return nil
}

// connectToAdmin establishes connection to admin server
func (w *Worker) connectToAdmin(ctx context.Context) error {
address := fmt.Sprintf("%s:%d", w.config.AdminHost, w.config.AdminPort)

dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

conn, err := grpc.DialContext(dialCtx, address, grpc.WithInsecure())
if err != nil {
return fmt.Errorf("failed to dial: %v", err)
}

w.conn = conn
w.pluginClient = plugin_pb.NewPluginServiceClient(conn)

return nil
}

// registerPlugin registers the plugin with the admin server
func (w *Worker) registerPlugin(ctx context.Context) error {
schema := GetConfigurationSchema()

req := &plugin_pb.PluginConnectRequest{
PluginId:          w.config.WorkerID,
PluginName:        "balance-plugin",
Version:           "1.0.0",
Capabilities:      []string{"detect", "execute", "report_health"},
MaxConcurrentJobs: int32(w.config.MaxConcurrentJobs),
SupportsStreaming: true,
Port:              int32(w.config.PluginPort),
}

// Add capabilities detail
req.CapabilitiesDetail = &plugin_pb.PluginCapabilities{
Detection: []*plugin_pb.DetectionCapability{
{
Type:               "rebalance_candidates",
Description:        "Detect nodes that need rebalancing",
MinIntervalSeconds: int32(w.config.RebalanceInterval.Seconds()),
RequiresFullScan:   true,
},
},
Maintenance: []*plugin_pb.MaintenanceCapability{
{
Type:                    "rebalance_data",
Description:             "Rebalance data across nodes",
RequiredDetectionTypes:  []string{"rebalance_candidates"},
EstimatedDurationSeconds: 3600,
},
},
}

// Add schema to metadata
if schema != nil {
if req.Metadata == nil {
req.Metadata = make(map[string]string)
}
for k, v := range schema.Properties {
req.Metadata[k] = v
}
}

ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

resp, err := w.pluginClient.Connect(ctx, req)
if err != nil {
return fmt.Errorf("connect RPC failed: %v", err)
}

if !resp.Success {
return fmt.Errorf("connect failed: %s", resp.Message)
}

log.Printf("Plugin registered with master: %s", resp.MasterId)
return nil
}

// heartbeatLoop sends periodic health reports
func (w *Worker) heartbeatLoop(ctx context.Context) {
ticker := time.NewTicker(w.config.HealthCheckInterval)
defer ticker.Stop()

for {
select {
case <-ctx.Done():
return
case <-w.done:
return
case <-ticker.C:
w.sendHealthReport(ctx)
}
}
}

// sendHealthReport sends a health report to the admin
func (w *Worker) sendHealthReport(ctx context.Context) {
report := &plugin_pb.HealthReport{
PluginId:    w.config.WorkerID,
TimestampMs: time.Now().UnixMilli(),
Status:      plugin_pb.HealthStatus_HEALTH_STATUS_HEALTHY,
ActiveJobs:  int32(len(w.activeJobs)),
}

ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

_, err := w.pluginClient.ReportHealth(ctx, report)
if err != nil {
log.Printf("Failed to send health report: %v", err)
}
}

// ExecuteDetection performs detection for rebalance opportunities
func (w *Worker) ExecuteDetection(ctx context.Context, nodeMetrics map[string]*NodeMetric) ([]*RebalanceCandidate, error) {
return w.detector.DetectJobs(nodeMetrics)
}

// ExecuteJob executes a rebalance job
func (w *Worker) ExecuteJob(ctx context.Context, jobID string, payload *plugin_pb.JobPayload, source, dest string) error {
req := &plugin_pb.ExecuteJobRequest{
JobId:      jobID,
JobType:    "rebalance_data",
Payload:    payload,
RetryCount: 0,
}

w.activeJobs[jobID] = req

defer delete(w.activeJobs, jobID)

// Execute the job
result, err := w.executor.ExecuteJob(req, source, dest)
if err != nil {
log.Printf("Job execution failed: %v", err)
return err
}

if result.Success {
log.Printf("Job %s completed successfully", jobID)
return w.submitResult(ctx, jobID, result)
}

log.Printf("Job %s failed: %s", jobID, result.ErrorMessage)
return fmt.Errorf("%s", result.ErrorMessage)
}

// submitResult submits job results to admin
func (w *Worker) submitResult(ctx context.Context, jobID string, result *BalanceExecutionResult) error {
jobResult := &plugin_pb.JobResult{
Success:  result.Success,
Metadata: result.Metadata,
}

req := &plugin_pb.JobResultRequest{
JobId:            jobID,
JobType:          "rebalance_data",
Status:           plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
Message:          "Rebalancing completed successfully",
Result:           jobResult,
RetryCountUsed:   0,
}

ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

_, err := w.pluginClient.SubmitResult(ctx, req)
return err
}

// Stop gracefully stops the worker
func (w *Worker) Stop(ctx context.Context) error {
log.Printf("Stopping balance worker")
w.isRunning = false
close(w.done)

if w.conn != nil {
return w.conn.Close()
}

return nil
}

// GetStatus returns the current worker status
func (w *Worker) GetStatus() map[string]interface{} {
return map[string]interface{}{
"worker_id":       w.config.WorkerID,
"is_running":      w.isRunning,
"active_jobs":     len(w.activeJobs),
"admin_connected": w.conn != nil,
}
}

// ParseFlags parses command line flags for balance worker
func ParseFlags() *WorkerConfig {
config := &WorkerConfig{
WorkerID:                   "balance-worker-1",
AdminHost:                  "localhost",
AdminPort:                  50051,
PluginPort:                 50054,
MinVolumeSize:              500,
MaxVolumeSize:              10000,
DataNodeCount:              10,
ReplicationFactor:          2,
PreferBalancedDistribution: true,
RebalanceInterval:          2 * time.Hour,
MaxConcurrentJobs:          2,
HealthCheckInterval:        30 * time.Second,
DiskUsageThreshold:         85,
AcceptableImbalancePercent: 10,
}

flag.StringVar(&config.WorkerID, "worker-id", config.WorkerID, "Worker ID")
flag.StringVar(&config.AdminHost, "admin-host", config.AdminHost, "Admin server host")
flag.IntVar(&config.AdminPort, "admin-port", config.AdminPort, "Admin server port")
flag.IntVar(&config.PluginPort, "plugin-port", config.PluginPort, "Plugin server port")
flag.Uint64Var(&config.MinVolumeSize, "min-volume-size", config.MinVolumeSize, "Minimum volume size in MB")
flag.Uint64Var(&config.MaxVolumeSize, "max-volume-size", config.MaxVolumeSize, "Maximum volume size in MB")
flag.IntVar(&config.DataNodeCount, "data-node-count", config.DataNodeCount, "Data node count")
flag.IntVar(&config.ReplicationFactor, "replication-factor", config.ReplicationFactor, "Replication factor")
flag.BoolVar(&config.PreferBalancedDistribution, "prefer-balanced", config.PreferBalancedDistribution, "Prefer balanced distribution")
flag.DurationVar(&config.RebalanceInterval, "rebalance-interval", config.RebalanceInterval, "Rebalance interval")
flag.IntVar(&config.MaxConcurrentJobs, "max-concurrent-jobs", config.MaxConcurrentJobs, "Max concurrent jobs")
flag.DurationVar(&config.HealthCheckInterval, "health-check-interval", config.HealthCheckInterval, "Health check interval")
flag.IntVar(&config.DiskUsageThreshold, "disk-usage-threshold", config.DiskUsageThreshold, "Disk usage threshold percent")
flag.IntVar(&config.AcceptableImbalancePercent, "acceptable-imbalance", config.AcceptableImbalancePercent, "Acceptable imbalance percent")

flag.Parse()

return config
}

// ListenAndServe starts the gRPC server for the worker
func (w *Worker) ListenAndServe(port int) error {
listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
if err != nil {
return fmt.Errorf("failed to listen on port %d: %v", port, err)
}

server := grpc.NewServer()

log.Printf("Worker listening on port %d", port)
return server.Serve(listener)
}
