package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// GRPCServer implements the plugin service gRPC handlers
type GRPCServer struct {
	mu        sync.RWMutex
	registry  *Registry
	queue     *JobQueue
	dispatcher *Dispatcher
	configMgr *ConfigManager
	streamMu  sync.RWMutex
	activeStreams map[string][]chan interface{}
	plugin_pb.UnimplementedPluginServiceServer
	plugin_pb.UnimplementedAdminQueryServiceServer
	plugin_pb.UnimplementedAdminCommandServiceServer
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(registry *Registry, queue *JobQueue, dispatcher *Dispatcher, configMgr *ConfigManager) *GRPCServer {
	return &GRPCServer{
		registry:      registry,
		queue:         queue,
		dispatcher:    dispatcher,
		configMgr:     configMgr,
		activeStreams: make(map[string][]chan interface{}),
	}
}

// Connect registers a plugin with the master
func (gs *GRPCServer) Connect(ctx context.Context, req *plugin_pb.PluginConnectRequest) (*plugin_pb.PluginConnectResponse, error) {
	if req.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	// Create ConnectedPlugin instance
	plugin := &ConnectedPlugin{
		ID:                  req.PluginId,
		Name:                req.PluginName,
		Version:             req.Version,
		Status:              "CONNECTED",
		Capabilities:        req.Capabilities,
		MaxConcurrentJobs:   int(req.MaxConcurrentJobs),
		ConnectedAt:         time.Now(),
		LastHeartbeat:       time.Now(),
		Metadata:            req.Metadata,
		HealthCheckInterval: 30 * time.Second,
		JobTimeout:          5 * time.Minute,
	}

	// Register plugin
	if err := gs.registry.RegisterPlugin(plugin); err != nil {
		return nil, fmt.Errorf("failed to register plugin: %w", err)
	}

	// Load or create configuration
	config, err := gs.configMgr.LoadConfig(req.PluginId)
	if err != nil {
		// Create default config
		config = &PluginConfig{
			PluginID:            req.PluginId,
			Properties:          make(map[string]string),
			JobTypes:            make(map[string]*JobTypeConfig),
			MaxRetries:          3,
			HealthCheckInterval: 30 * time.Second,
			JobTimeout:          5 * time.Minute,
			Environment:         make(map[string]string),
		}
		gs.configMgr.SaveConfig(config, false)
	}

	// Build response
	pbConfig := &plugin_pb.PluginConfig{
		PluginId:            config.PluginID,
		Properties:          config.Properties,
		MaxRetries:          int32(config.MaxRetries),
		Environment:         config.Environment,
	}

	response := &plugin_pb.PluginConnectResponse{
		Success:     true,
		Message:     "Plugin registered successfully",
		MasterId:    "master-1",
		Config:      pbConfig,
		AssignedTypes: req.Capabilities,
	}

	return response, nil
}

// ExecuteJob processes a detection or maintenance job
func (gs *GRPCServer) ExecuteJob(ctx context.Context, req *plugin_pb.ExecuteJobRequest) (*plugin_pb.ExecuteJobResponse, error) {
	if req.JobId == "" || req.JobType == "" {
		return nil, fmt.Errorf("job_id and job_type are required")
	}

	response := &plugin_pb.ExecuteJobResponse{
		JobId:  req.JobId,
		Status: plugin_pb.ExecutionStatus_EXECUTION_STATUS_ACCEPTED,
		Message: "Job accepted for execution",
	}

	return response, nil
}

// ReportHealth processes health reports from plugins
func (gs *GRPCServer) ReportHealth(ctx context.Context, report *plugin_pb.HealthReport) (*plugin_pb.HealthReportResponse, error) {
	if report.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	// Update heartbeat
	if err := gs.registry.UpdateHeartbeat(report.PluginId); err != nil {
		return nil, fmt.Errorf("plugin not found: %w", err)
	}

	// Update plugin stats
	if plugin, err := gs.registry.GetPlugin(report.PluginId); err == nil {
		plugin.mu.Lock()
		plugin.ActiveJobs = int(report.ActiveJobs)
		plugin.CPUUsagePercent = float64(report.CpuPercent)
		plugin.MemoryUsageBytes = report.MemoryBytes
		plugin.mu.Unlock()
	}

	return &plugin_pb.HealthReportResponse{
		Acknowledged: true,
		Feedback:     "Health report received",
	}, nil
}

// GetConfig retrieves the latest configuration
func (gs *GRPCServer) GetConfig(ctx context.Context, req *plugin_pb.GetConfigRequest) (*plugin_pb.GetConfigResponse, error) {
	if req.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	config, exists := gs.configMgr.GetConfig(req.PluginId)
	if !exists {
		return nil, fmt.Errorf("config not found for plugin: %s", req.PluginId)
	}

	pbConfig := &plugin_pb.PluginConfig{
		PluginId:            config.PluginID,
		Properties:          config.Properties,
		MaxRetries:          int32(config.MaxRetries),
		Environment:         config.Environment,
	}

	response := &plugin_pb.GetConfigResponse{
		Config:  pbConfig,
		Version: gs.configMgr.GetVersion(req.PluginId),
	}

	return response, nil
}

// SubmitResult sends job execution results back to master
func (gs *GRPCServer) SubmitResult(ctx context.Context, req *plugin_pb.JobResultRequest) (*plugin_pb.JobResultResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	actions := []string{}
	
	// Process results based on job status
	switch req.Status {
	case plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED:
		actions = append(actions, "ARCHIVED")
	case plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED:
		actions = append(actions, "RETRY", "NOTIFY_ADMIN")
	}

	response := &plugin_pb.JobResultResponse{
		Acknowledged:  true,
		ActionsToTake: actions,
	}

	return response, nil
}

// GetPluginStats returns statistics for all connected plugins
func (gs *GRPCServer) GetPluginStats(ctx context.Context, req *plugin_pb.GetPluginStatsRequest) (*plugin_pb.GetPluginStatsResponse, error) {
	response := &plugin_pb.GetPluginStatsResponse{
		Stats: []*plugin_pb.PluginStats{},
	}

	var plugins []*ConnectedPlugin
	if req.PluginId != "" {
		if plugin, err := gs.registry.GetPlugin(req.PluginId); err == nil {
			plugins = append(plugins, plugin)
		}
	} else {
		plugins = gs.registry.ListPlugins(false)
	}

	for _, plugin := range plugins {
		stat := &plugin_pb.PluginStats{
			PluginId:            plugin.ID,
			Status:              plugin.Status,
			ActiveJobs:          int32(plugin.ActiveJobs),
			CompletedJobs:       int32(plugin.CompletedJobs),
			FailedJobs:          int32(plugin.FailedJobs),
			TotalDetections:     plugin.TotalDetections,
			AvgExecutionTimeMs:  float32(plugin.AvgExecutionTimeMs),
			CpuUsagePercent:     float32(plugin.CPUUsagePercent),
			MemoryUsageBytes:    plugin.MemoryUsageBytes,
			UptimeSeconds:       int32(time.Since(plugin.ConnectedAt).Seconds()),
		}
		response.Stats = append(response.Stats, stat)
	}

	return response, nil
}

// ListPlugins returns information about all registered plugins
func (gs *GRPCServer) ListPlugins(ctx context.Context, req *plugin_pb.ListPluginsRequest) (*plugin_pb.ListPluginsResponse, error) {
	response := &plugin_pb.ListPluginsResponse{
		Plugins: []*plugin_pb.PluginInfo{},
	}

	plugins := gs.registry.ListPlugins(!req.IncludeDisabled)

	for _, plugin := range plugins {
		// Filter by capability if specified
		if len(req.FilterByCapability) > 0 {
			hasCapability := false
			for _, filterCap := range req.FilterByCapability {
				for _, cap := range plugin.Capabilities {
					if cap == filterCap {
						hasCapability = true
						break
					}
				}
				if hasCapability {
					break
				}
			}
			if !hasCapability {
				continue
			}
		}

		info := &plugin_pb.PluginInfo{
			PluginId:         plugin.ID,
			Name:             plugin.Name,
			Version:          plugin.Version,
			Status:           plugin.Status,
			Capabilities:     plugin.Capabilities,
			MaxConcurrentJobs: int32(plugin.MaxConcurrentJobs),
			ActiveJobs:       int32(plugin.ActiveJobs),
			Metadata:         plugin.Metadata,
		}
		response.Plugins = append(response.Plugins, info)
	}

	return response, nil
}

// ListJobs returns current and historical job information
func (gs *GRPCServer) ListJobs(ctx context.Context, req *plugin_pb.ListJobsRequest) (*plugin_pb.ListJobsResponse, error) {
	response := &plugin_pb.ListJobsResponse{
		Jobs: []*plugin_pb.JobInfo{},
	}

	var records []*ExecutionRecord
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	if req.PluginId != "" {
		records = gs.queue.GetHistoryForPlugin(req.PluginId, limit)
	} else {
		records = gs.queue.GetHistory(limit)
	}

	for _, record := range records {
		info := &plugin_pb.JobInfo{
			JobId:      record.JobID,
			JobType:    record.JobType,
			PluginId:   record.PluginID,
			RetryCount: int32(record.RetryCount),
			LastError:  record.LastError,
		}
		response.Jobs = append(response.Jobs, info)
	}

	response.TotalCount = int32(gs.queue.HistorySize())
	return response, nil
}

// GetJobStatus returns detailed status of a specific job
func (gs *GRPCServer) GetJobStatus(ctx context.Context, req *plugin_pb.GetJobStatusRequest) (*plugin_pb.GetJobStatusResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	// Search in queue history
	records := gs.queue.GetHistory(10000)
	for _, record := range records {
		if record.JobID == req.JobId {
			response := &plugin_pb.GetJobStatusResponse{
				DetailedStatus: record.State.String(),
			}
			return response, nil
		}
	}

	return nil, fmt.Errorf("job not found: %s", req.JobId)
}

// GetPluginLogs returns logs from a specific plugin (stub implementation)
func (gs *GRPCServer) GetPluginLogs(ctx context.Context, req *plugin_pb.GetPluginLogsRequest) (*plugin_pb.GetPluginLogsResponse, error) {
	response := &plugin_pb.GetPluginLogsResponse{
		Entries: []*plugin_pb.LogEntry{},
	}
	return response, nil
}

// SaveConfig persists plugin configuration
func (gs *GRPCServer) SaveConfig(ctx context.Context, req *plugin_pb.SaveConfigRequest) (*plugin_pb.SaveConfigResponse, error) {
	if req.Config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Convert from protobuf config to internal config
	config := &PluginConfig{
		PluginID:    req.Config.PluginId,
		Properties:  req.Config.Properties,
		MaxRetries:  int(req.Config.MaxRetries),
		Environment: req.Config.Environment,
		JobTypes:    make(map[string]*JobTypeConfig),
	}

	if err := gs.configMgr.SaveConfig(config, req.BackupExisting); err != nil {
		return nil, fmt.Errorf("failed to save config: %w", err)
	}

	response := &plugin_pb.SaveConfigResponse{
		Success:       true,
		Message:       "Configuration saved successfully",
		ConfigVersion: gs.configMgr.GetVersion(req.Config.PluginId),
	}

	return response, nil
}

// ReloadConfig reloads configuration without restarting
func (gs *GRPCServer) ReloadConfig(ctx context.Context, req *plugin_pb.ReloadConfigRequest) (*plugin_pb.ReloadConfigResponse, error) {
	if req.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	if _, err := gs.configMgr.LoadConfig(req.PluginId); err != nil {
		return nil, fmt.Errorf("failed to reload config: %w", err)
	}

	response := &plugin_pb.ReloadConfigResponse{
		Success: true,
		Message: "Configuration reloaded successfully",
	}

	return response, nil
}

// EnablePlugin enables a specific plugin
func (gs *GRPCServer) EnablePlugin(ctx context.Context, req *plugin_pb.EnablePluginRequest) (*plugin_pb.EnablePluginResponse, error) {
	if err := gs.registry.UpdatePluginStatus(req.PluginId, "ENABLED"); err != nil {
		return nil, fmt.Errorf("failed to enable plugin: %w", err)
	}

	response := &plugin_pb.EnablePluginResponse{
		Success: true,
		Message: "Plugin enabled successfully",
	}

	return response, nil
}

// DisablePlugin disables a specific plugin
func (gs *GRPCServer) DisablePlugin(ctx context.Context, req *plugin_pb.DisablePluginRequest) (*plugin_pb.DisablePluginResponse, error) {
	if err := gs.registry.UpdatePluginStatus(req.PluginId, "DISABLED"); err != nil {
		return nil, fmt.Errorf("failed to disable plugin: %w", err)
	}

	response := &plugin_pb.DisablePluginResponse{
		Success: true,
		Message: "Plugin disabled successfully",
	}

	return response, nil
}

// TriggerDetection manually triggers a detection for specific types
func (gs *GRPCServer) TriggerDetection(ctx context.Context, req *plugin_pb.TriggerDetectionRequest) (*plugin_pb.TriggerDetectionResponse, error) {
	response := &plugin_pb.TriggerDetectionResponse{
		Success:         true,
		TriggeredJobIds: []string{},
	}

	for _, detectionType := range req.DetectionTypes {
		jobID := fmt.Sprintf("trig-%s-%d", detectionType, time.Now().UnixNano())
		job := &Job{
			ID:        jobID,
			Type:      detectionType,
			State:     JobStatePending,
			CreatedAt: time.Now(),
		}

		if err := gs.queue.Enqueue(job); err != nil {
			continue
		}
		response.TriggeredJobIds = append(response.TriggeredJobIds, jobID)
	}

	return response, nil
}

// CancelJob cancels a running job
func (gs *GRPCServer) CancelJob(ctx context.Context, req *plugin_pb.CancelJobRequest) (*plugin_pb.CancelJobResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	if gs.queue.RemoveJob(req.JobId) {
		return &plugin_pb.CancelJobResponse{
			Success: true,
			Message: "Job cancelled successfully",
		}, nil
	}

	return &plugin_pb.CancelJobResponse{
		Success: false,
		Message: "Job not found or already completed",
	}, nil
}

// PurgeHistory clears job history
func (gs *GRPCServer) PurgeHistory(ctx context.Context, req *plugin_pb.PurgeHistoryRequest) (*plugin_pb.PurgeHistoryResponse, error) {
	beforeTime := time.Unix(0, req.BeforeTimestampMs*1000000)
	deleted := gs.queue.PurgeOldHistory(beforeTime)

	response := &plugin_pb.PurgeHistoryResponse{
		Success:        true,
		RecordsDeleted: int32(deleted),
	}

	return response, nil
}
