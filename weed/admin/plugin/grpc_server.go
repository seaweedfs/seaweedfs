package plugin

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
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
func (gs *GRPCServer) Connect(ctx context.Context, req *PluginConnectRequest) (*PluginConnectResponse, error) {
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
	response := &PluginConnectResponse{
		Success:     true,
		Message:     "Plugin registered successfully",
		MasterId:    "master-1",
		Config:      config,
		AssignedTypes: req.Capabilities,
	}

	return response, nil
}

// ExecuteJob processes a detection or maintenance job
func (gs *GRPCServer) ExecuteJob(ctx context.Context, req *ExecuteJobRequest) (*ExecuteJobResponse, error) {
	if req.JobId == "" || req.JobType == "" {
		return nil, fmt.Errorf("job_id and job_type are required")
	}

	response := &ExecuteJobResponse{
		JobId:  req.JobId,
		Status: ExecutionStatus_ACCEPTED,
		Message: "Job accepted for execution",
	}

	return response, nil
}

// ReportHealth processes health reports from plugins
func (gs *GRPCServer) ReportHealth(ctx context.Context, report *HealthReport) (*HealthReportResponse, error) {
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

	return &HealthReportResponse{
		Acknowledged: true,
		Feedback:     "Health report received",
	}, nil
}

// GetConfig retrieves the latest configuration
func (gs *GRPCServer) GetConfig(ctx context.Context, req *GetConfigRequest) (*GetConfigResponse, error) {
	if req.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	config, exists := gs.configMgr.GetConfig(req.PluginId)
	if !exists {
		return nil, fmt.Errorf("config not found for plugin: %s", req.PluginId)
	}

	response := &GetConfigResponse{
		Config:  config,
		Version: gs.configMgr.GetVersion(req.PluginId),
	}

	return response, nil
}

// SubmitResult sends job execution results back to master
func (gs *GRPCServer) SubmitResult(ctx context.Context, req *JobResultRequest) (*JobResultResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	actions := []string{}
	
	// Process results based on job status
	switch req.Status {
	case ExecutionStatus_COMPLETED:
		actions = append(actions, "ARCHIVED")
	case ExecutionStatus_FAILED:
		actions = append(actions, "RETRY", "NOTIFY_ADMIN")
	}

	response := &JobResultResponse{
		Acknowledged:  true,
		ActionsToTake: actions,
	}

	return response, nil
}

// GetPluginStats returns statistics for all connected plugins
func (gs *GRPCServer) GetPluginStats(ctx context.Context, req *GetPluginStatsRequest) (*GetPluginStatsResponse, error) {
	response := &GetPluginStatsResponse{
		Stats: []*PluginStats{},
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
		stat := &PluginStats{
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
func (gs *GRPCServer) ListPlugins(ctx context.Context, req *ListPluginsRequest) (*ListPluginsResponse, error) {
	response := &ListPluginsResponse{
		Plugins: []*PluginInfo{},
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

		info := &PluginInfo{
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
func (gs *GRPCServer) ListJobs(ctx context.Context, req *ListJobsRequest) (*ListJobsResponse, error) {
	response := &ListJobsResponse{
		Jobs: []*JobInfo{},
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
		// Filter by state if specified
		if req.FilterState != JobState_PENDING && req.FilterState != record.State {
			continue
		}

		var startedAt, completedAt *google.protobuf.Timestamp
		if record.StartedAt != nil {
			startedAt = &google.protobuf.Timestamp{
				Seconds: record.StartedAt.Unix(),
				Nanos:   int32(record.StartedAt.Nanosecond()),
			}
		}
		if record.CompletedAt != nil {
			completedAt = &google.protobuf.Timestamp{
				Seconds: record.CompletedAt.Unix(),
				Nanos:   int32(record.CompletedAt.Nanosecond()),
			}
		}

		info := &JobInfo{
			JobId:      record.JobID,
			JobType:    record.JobType,
			PluginId:   record.PluginID,
			State:      record.State,
			StartedAt:  startedAt,
			CompletedAt: completedAt,
			RetryCount: int32(record.RetryCount),
			LastError:  record.LastError,
		}
		response.Jobs = append(response.Jobs, info)
	}

	response.TotalCount = int32(gs.queue.HistorySize())
	return response, nil
}

// GetJobStatus returns detailed status of a specific job
func (gs *GRPCServer) GetJobStatus(ctx context.Context, req *GetJobStatusRequest) (*GetJobStatusResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	// Search in queue history
	records := gs.queue.GetHistory(10000)
	for _, record := range records {
		if record.JobID == req.JobId {
			response := &GetJobStatusResponse{
				Result:         record.Result,
				DetailedStatus: record.State.String(),
			}
			return response, nil
		}
	}

	return nil, fmt.Errorf("job not found: %s", req.JobId)
}

// GetPluginLogs returns logs from a specific plugin (stub implementation)
func (gs *GRPCServer) GetPluginLogs(ctx context.Context, req *GetPluginLogsRequest) (*GetPluginLogsResponse, error) {
	response := &GetPluginLogsResponse{
		Entries: []*LogEntry{},
	}
	return response, nil
}

// SaveConfig persists plugin configuration
func (gs *GRPCServer) SaveConfig(ctx context.Context, req *SaveConfigRequest) (*SaveConfigResponse, error) {
	if req.Config == nil {
		return nil, fmt.Errorf("config is required")
	}

	if err := gs.configMgr.SaveConfig(req.Config, req.BackupExisting); err != nil {
		return nil, fmt.Errorf("failed to save config: %w", err)
	}

	response := &SaveConfigResponse{
		Success:       true,
		Message:       "Configuration saved successfully",
		ConfigVersion: gs.configMgr.GetVersion(req.Config.PluginId),
	}

	return response, nil
}

// ReloadConfig reloads configuration without restarting
func (gs *GRPCServer) ReloadConfig(ctx context.Context, req *ReloadConfigRequest) (*ReloadConfigResponse, error) {
	if req.PluginId == "" {
		return nil, fmt.Errorf("plugin_id is required")
	}

	if _, err := gs.configMgr.LoadConfig(req.PluginId); err != nil {
		return nil, fmt.Errorf("failed to reload config: %w", err)
	}

	response := &ReloadConfigResponse{
		Success: true,
		Message: "Configuration reloaded successfully",
	}

	return response, nil
}

// EnablePlugin enables a specific plugin
func (gs *GRPCServer) EnablePlugin(ctx context.Context, req *EnablePluginRequest) (*EnablePluginResponse, error) {
	if err := gs.registry.UpdatePluginStatus(req.PluginId, "ENABLED"); err != nil {
		return nil, fmt.Errorf("failed to enable plugin: %w", err)
	}

	response := &EnablePluginResponse{
		Success: true,
		Message: "Plugin enabled successfully",
	}

	return response, nil
}

// DisablePlugin disables a specific plugin
func (gs *GRPCServer) DisablePlugin(ctx context.Context, req *DisablePluginRequest) (*DisablePluginResponse, error) {
	if err := gs.registry.UpdatePluginStatus(req.PluginId, "DISABLED"); err != nil {
		return nil, fmt.Errorf("failed to disable plugin: %w", err)
	}

	response := &DisablePluginResponse{
		Success: true,
		Message: "Plugin disabled successfully",
	}

	return response, nil
}

// TriggerDetection manually triggers a detection for specific types
func (gs *GRPCServer) TriggerDetection(ctx context.Context, req *TriggerDetectionRequest) (*TriggerDetectionResponse, error) {
	response := &TriggerDetectionResponse{
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
func (gs *GRPCServer) CancelJob(ctx context.Context, req *CancelJobRequest) (*CancelJobResponse, error) {
	if req.JobId == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	if gs.queue.RemoveJob(req.JobId) {
		return &CancelJobResponse{
			Success: true,
			Message: "Job cancelled successfully",
		}, nil
	}

	return &CancelJobResponse{
		Success: false,
		Message: "Job not found or already completed",
	}, nil
}

// PurgeHistory clears job history
func (gs *GRPCServer) PurgeHistory(ctx context.Context, req *PurgeHistoryRequest) (*PurgeHistoryResponse, error) {
	beforeTime := time.Unix(0, req.BeforeTimestampMs*1000000)
	deleted := gs.queue.PurgeOldHistory(beforeTime)

	response := &PurgeHistoryResponse{
		Success:        true,
		RecordsDeleted: int32(deleted),
	}

	return response, nil
}
