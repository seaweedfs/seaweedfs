package vacuum

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
	WorkerID             string
	AdminHost            string
	AdminPort            int
	PluginPort           int
	MinVolumeSize        uint64
	MaxVolumeSize        uint64
	TargetUtilization    int
	BatchSize            int
	VacuumInterval       time.Duration
	MaxConcurrentJobs    int
	HealthCheckInterval  time.Duration
	DeadSpaceThreshold   int
}

// Worker represents the vacuum plugin worker
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

// NewWorker creates a new vacuum worker
func NewWorker(config *WorkerConfig) *Worker {
	return &Worker{
		config:     config,
		activeJobs: make(map[string]*plugin_pb.ExecuteJobRequest),
		done:       make(chan bool),
	}
}

// Start initializes and starts the worker
func (w *Worker) Start(ctx context.Context) error {
	log.Printf("Starting vacuum worker: %s", w.config.WorkerID)

	// Connect to admin server
	if err := w.connectToAdmin(ctx); err != nil {
		return fmt.Errorf("failed to connect to admin: %v", err)
	}

	// Initialize detector
	w.detector = NewDetector(DetectionOptions{
		MinVolumeSize:      w.config.MinVolumeSize,
		MaxVolumeSize:      w.config.MaxVolumeSize,
		DeadSpaceThreshold: float32(w.config.DeadSpaceThreshold),
	})

	// Initialize executor
	w.executor = NewExecutor(&ExecutorConfig{
		MinVolumeSize:  w.config.MinVolumeSize,
		MaxVolumeSize:  w.config.MaxVolumeSize,
		TimeoutPerStep: 3 * time.Minute,
		MaxRetries:     3,
	})

	// Register with admin
	if err := w.registerPlugin(ctx); err != nil {
		return fmt.Errorf("failed to register: %v", err)
	}

	w.isRunning = true

	// Start background goroutines
	go w.heartbeatLoop(ctx)

	log.Printf("Vacuum worker started successfully")
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
		PluginName:        "vacuum-plugin",
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
				Type:               "vacuum_candidates",
				Description:        "Detect volumes eligible for vacuum",
				MinIntervalSeconds: int32(w.config.VacuumInterval.Seconds()),
				RequiresFullScan:   false,
			},
		},
		Maintenance: []*plugin_pb.MaintenanceCapability{
			{
				Type:                    "vacuum_volume",
				Description:             "Vacuum a volume to free dead space",
				RequiredDetectionTypes:  []string{"vacuum_candidates"},
				EstimatedDurationSeconds: 1800,
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

// ExecuteDetection performs detection for vacuum candidates
func (w *Worker) ExecuteDetection(ctx context.Context, volumeMetrics map[uint32]*VolumeMetric) ([]*VacuumCandidate, error) {
	return w.detector.DetectJobs(volumeMetrics)
}

// ExecuteJob executes a vacuum job
func (w *Worker) ExecuteJob(ctx context.Context, jobID string, payload *plugin_pb.JobPayload) error {
	req := &plugin_pb.ExecuteJobRequest{
		JobId:      jobID,
		JobType:    "vacuum_volume",
		Payload:    payload,
		RetryCount: 0,
	}

	w.activeJobs[jobID] = req

	defer delete(w.activeJobs, jobID)

	// Execute the job
	result, err := w.executor.ExecuteJob(req)
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
func (w *Worker) submitResult(ctx context.Context, jobID string, result *VacuumExecutionResult) error {
	jobResult := &plugin_pb.JobResult{
		Success:  result.Success,
		Metadata: result.Metadata,
	}

	req := &plugin_pb.JobResultRequest{
		JobId:            jobID,
		JobType:          "vacuum_volume",
		Status:           plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
		Message:          "Vacuum completed successfully",
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
	log.Printf("Stopping vacuum worker")
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

// ParseFlags parses command line flags for vacuum worker
func ParseFlags() *WorkerConfig {
	config := &WorkerConfig{
		WorkerID:             "vacuum-worker-1",
		AdminHost:            "localhost",
		AdminPort:            50051,
		PluginPort:           50053,
		MinVolumeSize:        500,
		MaxVolumeSize:        5000,
		TargetUtilization:    80,
		BatchSize:            10,
		VacuumInterval:       4 * time.Hour,
		MaxConcurrentJobs:    3,
		HealthCheckInterval:  30 * time.Second,
		DeadSpaceThreshold:   30,
	}

	flag.StringVar(&config.WorkerID, "worker-id", config.WorkerID, "Worker ID")
	flag.StringVar(&config.AdminHost, "admin-host", config.AdminHost, "Admin server host")
	flag.IntVar(&config.AdminPort, "admin-port", config.AdminPort, "Admin server port")
	flag.IntVar(&config.PluginPort, "plugin-port", config.PluginPort, "Plugin server port")
	flag.Uint64Var(&config.MinVolumeSize, "min-volume-size", config.MinVolumeSize, "Minimum volume size in MB")
	flag.Uint64Var(&config.MaxVolumeSize, "max-volume-size", config.MaxVolumeSize, "Maximum volume size in MB")
	flag.IntVar(&config.TargetUtilization, "target-utilization", config.TargetUtilization, "Target utilization percentage")
	flag.IntVar(&config.BatchSize, "batch-size", config.BatchSize, "Batch size")
	flag.DurationVar(&config.VacuumInterval, "vacuum-interval", config.VacuumInterval, "Vacuum interval")
	flag.IntVar(&config.MaxConcurrentJobs, "max-concurrent-jobs", config.MaxConcurrentJobs, "Max concurrent jobs")
	flag.DurationVar(&config.HealthCheckInterval, "health-check-interval", config.HealthCheckInterval, "Health check interval")
	flag.IntVar(&config.DeadSpaceThreshold, "dead-space-threshold", config.DeadSpaceThreshold, "Dead space threshold percentage")

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
