package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Manager is the main component orchestrating the plugin system
type Manager struct {
	mu                sync.RWMutex
	registry          *Registry
	queue             *JobQueue
	dispatcher        *Dispatcher
	configMgr         *ConfigManager
	grpcServer        *GRPCServer
	isRunning         bool
	stopChan          chan bool
	scheduleTicker    *time.Ticker
	healthCheckTicker *time.Ticker
	detectionTicker   *time.Ticker
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	config            *ManagerConfig
}

// ManagerConfig holds configuration for the plugin manager
type ManagerConfig struct {
	ConfigDir              string
	ScheduleInterval       time.Duration
	HealthCheckInterval    time.Duration
	DetectionInterval      time.Duration
	MaxQueueSize           int
	MaxHistorySize         int
	DeduplicationTTL       time.Duration
	HealthCheckTimeout     time.Duration
	FailureDetectionWindow time.Duration
	FailureThreshold       int
}

// DefaultManagerConfig returns default configuration
func DefaultManagerConfig(configDir string) *ManagerConfig {
	return &ManagerConfig{
		ConfigDir:              configDir,
		ScheduleInterval:       5 * time.Second,
		HealthCheckInterval:    30 * time.Second,
		DetectionInterval:      10 * time.Second,
		MaxQueueSize:           10000,
		MaxHistorySize:         5000,
		DeduplicationTTL:       1 * time.Minute,
		HealthCheckTimeout:     90 * time.Second,
		FailureDetectionWindow: 5 * time.Minute,
		FailureThreshold:       3,
	}
}

// NewManager creates a new plugin manager instance
func NewManager(config *ManagerConfig) (*Manager, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Create configuration manager
	configMgr, err := NewConfigManager(config.ConfigDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create config manager: %w", err)
	}

	// Create registry
	registry := NewRegistry(
		config.HealthCheckTimeout,
		config.FailureDetectionWindow,
		config.FailureThreshold,
	)

	// Create job queue
	queue := NewJobQueue(config.MaxHistorySize, config.DeduplicationTTL)

	// Create dispatcher
	dispatcher := NewDispatcher(registry, queue)

	// Create gRPC server
	grpcServer := NewGRPCServer(registry, queue, dispatcher, configMgr)

	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		registry:   registry,
		queue:      queue,
		dispatcher: dispatcher,
		configMgr:  configMgr,
		grpcServer: grpcServer,
		stopChan:   make(chan bool),
		ctx:        ctx,
		cancel:     cancel,
		config:     config,
	}

	return manager, nil
}

// Start initializes and starts the plugin manager
func (m *Manager) Start() error {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("manager already running")
	}
	m.isRunning = true
	m.mu.Unlock()

	// Load existing configurations
	if err := m.configMgr.LoadAllConfigs(); err != nil {
		m.isRunning = false
		return fmt.Errorf("failed to load configurations: %w", err)
	}

	// Start background tasks
	m.wg.Add(3)
	go m.schedulerLoop()
	go m.healthCheckLoop()
	go m.detectionLoop()

	return nil
}

// Stop gracefully stops the plugin manager
func (m *Manager) Stop() error {
	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("manager not running")
	}
	m.isRunning = false
	m.mu.Unlock()

	// Signal all goroutines to stop
	m.cancel()
	close(m.stopChan)

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for manager to stop")
	}
}

// schedulerLoop periodically schedules detection jobs
func (m *Manager) schedulerLoop() {
	defer m.wg.Done()

	m.scheduleTicker = time.NewTicker(m.config.ScheduleInterval)
	defer m.scheduleTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-m.scheduleTicker.C:
			m.performScheduling()
		}
	}
}

// performScheduling executes the scheduling logic
func (m *Manager) performScheduling() {
	scheduledJobs := m.dispatcher.ScheduleDetections()
	if len(scheduledJobs) > 0 {
		// Jobs have been queued for processing
	}
}

// healthCheckLoop periodically checks plugin health
func (m *Manager) healthCheckLoop() {
	defer m.wg.Done()

	m.healthCheckTicker = time.NewTicker(m.config.HealthCheckInterval)
	defer m.healthCheckTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-m.healthCheckTicker.C:
			m.performHealthCheck()
		}
	}
}

// performHealthCheck checks health of all plugins
func (m *Manager) performHealthCheck() {
	plugins := m.registry.ListPlugins(true)

	for _, plugin := range plugins {
		isHealthy, _ := m.registry.HealthCheck(plugin.ID)
		if !isHealthy {
			// Check if exceeded threshold
			if m.registry.HasExceededFailureThreshold(plugin.ID) {
				m.registry.UnregisterPlugin(plugin.ID)
			}
		}
	}
}

// detectionLoop periodically triggers detection execution
func (m *Manager) detectionLoop() {
	defer m.wg.Done()

	m.detectionTicker = time.NewTicker(m.config.DetectionInterval)
	defer m.detectionTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-m.detectionTicker.C:
			m.processDetectionJobs()
		}
	}
}

// processDetectionJobs dequeues and dispatches pending jobs
func (m *Manager) processDetectionJobs() {
	for {
		job := m.queue.Dequeue()
		if job == nil {
			break
		}

		// Dispatch job to available plugin
		pluginID, err := m.dispatcher.DispatchJob(job)
		if err != nil {
			// Requeue job if dispatch failed
			m.queue.Enqueue(job)
			break
		}

		job.PluginID = pluginID
	}
}

// IsRunning returns whether the manager is currently running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isRunning
}

// RegisterDetectionType registers a new detection type
func (m *Manager) RegisterDetectionType(detectionType string, interval time.Duration, maxConcurrent int) error {
	return m.dispatcher.RegisterDetectionType(detectionType, interval, maxConcurrent)
}

// UnregisterDetectionType unregisters a detection type
func (m *Manager) UnregisterDetectionType(detectionType string) error {
	return m.dispatcher.UnregisterDetectionType(detectionType)
}

// GetStats returns overall statistics
func (m *Manager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"registry":   m.registry.GetStats(),
		"queue":      m.queue.GetExecutionStats(),
		"dispatcher": m.dispatcher.GetDispatcherStats(),
		"running":    m.IsRunning(),
	}
}

// GetPluginStats returns statistics for a specific plugin
func (m *Manager) GetPluginStats(pluginID string) (map[string]interface{}, error) {
	plugin, err := m.registry.GetPlugin(pluginID)
	if err != nil {
		return nil, err
	}

	plugin.mu.RLock()
	defer plugin.mu.RUnlock()

	return map[string]interface{}{
		"id":                      plugin.ID,
		"name":                    plugin.Name,
		"version":                 plugin.Version,
		"status":                  plugin.Status,
		"capabilities":            plugin.Capabilities,
		"active_jobs":             plugin.ActiveJobs,
		"completed_jobs":          plugin.CompletedJobs,
		"failed_jobs":             plugin.FailedJobs,
		"total_detections":        plugin.TotalDetections,
		"avg_execution_time_ms":   plugin.AvgExecutionTimeMs,
		"cpu_usage_percent":       plugin.CPUUsagePercent,
		"memory_usage_bytes":      plugin.MemoryUsageBytes,
		"connected_at":            plugin.ConnectedAt,
		"last_heartbeat":          plugin.LastHeartbeat,
		"uptime_seconds":          int(time.Since(plugin.ConnectedAt).Seconds()),
	}, nil
}

// ListPlugins returns all registered plugins
func (m *Manager) ListPlugins(includeUnhealthy bool) []*ConnectedPlugin {
	return m.registry.ListPlugins(includeUnhealthy)
}

// ListJobs returns job history
func (m *Manager) ListJobs(limit int) []*ExecutionRecord {
	return m.queue.GetHistory(limit)
}

// ListJobsForPlugin returns jobs for a specific plugin
func (m *Manager) ListJobsForPlugin(pluginID string, limit int) []*ExecutionRecord {
	return m.queue.GetHistoryForPlugin(pluginID, limit)
}

// ListJobsForType returns jobs for a specific type
func (m *Manager) ListJobsForType(jobType string, limit int) []*ExecutionRecord {
	return m.queue.GetHistoryForJobType(jobType, limit)
}

// TriggerDetection manually triggers detection for specific types
func (m *Manager) TriggerDetection(detectionTypes []string) ([]string, error) {
	var jobIDs []string

	for _, detectionType := range detectionTypes {
		jobID := fmt.Sprintf("manual-%s-%d", detectionType, time.Now().UnixNano())
		job := &Job{
			ID:        jobID,
			Type:      detectionType,
			State:     JobStatePending,
			CreatedAt: time.Now(),
		}

		if err := m.queue.Enqueue(job); err != nil {
			continue
		}
		jobIDs = append(jobIDs, jobID)
	}

	return jobIDs, nil
}

// GetJobStatus returns the status of a specific job
func (m *Manager) GetJobStatus(jobID string) (*ExecutionRecord, error) {
	records := m.queue.GetHistory(10000)
	for _, record := range records {
		if record.JobID == jobID {
			return record, nil
		}
	}
	return nil, fmt.Errorf("job not found: %s", jobID)
}

// CancelJob cancels a pending or scheduled job
func (m *Manager) CancelJob(jobID string) error {
	if !m.queue.RemoveJob(jobID) {
		return fmt.Errorf("job not found or already completed: %s", jobID)
	}
	return nil
}

// PurgeHistory removes old job history
func (m *Manager) PurgeHistory(beforeTime time.Time) int {
	return m.queue.PurgeOldHistory(beforeTime)
}

// SaveConfig saves plugin configuration
func (m *Manager) SaveConfig(config *PluginConfig, backup bool) error {
	return m.configMgr.SaveConfig(config, backup)
}

// LoadConfig loads plugin configuration
func (m *Manager) LoadConfig(pluginID string) (*PluginConfig, error) {
	config, err := m.configMgr.LoadConfig(pluginID)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	return config, nil
}

// ListConfigs returns all loaded configurations
func (m *Manager) ListConfigs() map[string]*PluginConfig {
	return m.configMgr.ListConfigs()
}

// DeleteConfig deletes a configuration
func (m *Manager) DeleteConfig(pluginID string) error {
	return m.configMgr.DeleteConfig(pluginID)
}

// GetRegistry returns the plugin registry
func (m *Manager) GetRegistry() *Registry {
	return m.registry
}

// GetQueue returns the job queue
func (m *Manager) GetQueue() *JobQueue {
	return m.queue
}

// GetDispatcher returns the dispatcher
func (m *Manager) GetDispatcher() *Dispatcher {
	return m.dispatcher
}

// GetGRPCServer returns the gRPC server
func (m *Manager) GetGRPCServer() *GRPCServer {
	return m.grpcServer
}

// GetDetectionHistory returns detection history for a job type
func (m *Manager) GetDetectionHistory(jobType string) []DetectionRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := m.configMgr.ListConfigs()
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			cfg.mu.RLock()
			defer cfg.mu.RUnlock()
			history := make([]DetectionRecord, len(jobCfg.DetectionHistory))
			copy(history, jobCfg.DetectionHistory)
			return history
		}
	}
	return []DetectionRecord{}
}

// GetExecutionHistory returns execution history for a job type
func (m *Manager) GetExecutionHistory(jobType string) []ExecutionRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := m.configMgr.ListConfigs()
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			cfg.mu.RLock()
			defer cfg.mu.RUnlock()
			history := make([]ExecutionRecord, len(jobCfg.ExecutionHistory))
			copy(history, jobCfg.ExecutionHistory)
			return history
		}
	}
	return []ExecutionRecord{}
}

// RecordDetection adds a detection record to history
func (m *Manager) RecordDetection(jobType string, record *DetectionRecord) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := m.configMgr.ListConfigs()
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			cfg.mu.Lock()
			maxSize := 50
			jobCfg.DetectionHistory = append([]DetectionRecord{*record}, jobCfg.DetectionHistory...)
			if len(jobCfg.DetectionHistory) > maxSize {
				jobCfg.DetectionHistory = jobCfg.DetectionHistory[:maxSize]
			}
			cfg.mu.Unlock()
			break
		}
	}
}

// RecordExecution adds an execution record to history
func (m *Manager) RecordExecution(jobType string, record *ExecutionRecord) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	configs := m.configMgr.ListConfigs()
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			cfg.mu.Lock()
			maxSize := 100
			jobCfg.ExecutionHistory = append([]ExecutionRecord{*record}, jobCfg.ExecutionHistory...)
			if len(jobCfg.ExecutionHistory) > maxSize {
				jobCfg.ExecutionHistory = jobCfg.ExecutionHistory[:maxSize]
			}
			cfg.mu.Unlock()
			break
		}
	}
}
