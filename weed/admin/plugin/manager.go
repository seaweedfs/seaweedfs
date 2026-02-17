package plugin

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
)

// Manager is the main plugin system manager
type Manager struct {
	registry   *Registry
	dispatcher *Dispatcher
	configMgr  *ConfigManager
	grpcServer *GrpcServer

	// gRPC server
	grpcListener net.Listener
	grpc         *grpc.Server

	// Configuration
	listenAddr          string
	dataDir             string
	healthCheckInterval time.Duration

	// State
	mu       sync.RWMutex
	running  bool
	stopChan chan struct{}
}

// NewManager creates a new plugin manager
func NewManager(listenAddr, dataDir string) (*Manager, error) {
	// Create configuration manager
	configMgr, err := NewConfigManager(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create config manager: %w", err)
	}

	// Create registry
	registry := NewRegistry()

	// Create dispatcher
	dispatcher := NewDispatcher(registry, configMgr)

	// Create gRPC server implementation
	grpcServer := NewGrpcServer(registry, dispatcher, configMgr)

	return &Manager{
		registry:            registry,
		dispatcher:          dispatcher,
		configMgr:           configMgr,
		grpcServer:          grpcServer,
		listenAddr:          listenAddr,
		dataDir:             dataDir,
		healthCheckInterval: 30 * time.Second,
		stopChan:            make(chan struct{}),
	}, nil
}

// Start starts the plugin manager and gRPC server
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("plugin manager already running")
	}

	// Start registry health checks
	m.registry.Start()

	// Start dispatcher
	m.dispatcher.Start()

	// Create gRPC server
	m.grpc = grpc.NewServer()

	// Register services
	plugin_pb.RegisterPluginServiceServer(m.grpc, m.grpcServer)
	plugin_pb.RegisterAdminQueryServiceServer(m.grpc, m.grpcServer)
	plugin_pb.RegisterAdminCommandServiceServer(m.grpc, m.grpcServer)

	// Start listening
	listener, err := net.Listen("tcp", m.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", m.listenAddr, err)
	}
	m.grpcListener = listener

	// Start gRPC server in background
	go func() {
		if err := m.grpc.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	m.running = true

	fmt.Printf("Plugin manager started on %s\n", m.listenAddr)

	return nil
}

// Stop stops the plugin manager and gRPC server
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("plugin manager not running")
	}

	close(m.stopChan)

	// Stop dispatcher
	m.dispatcher.Stop()

	// Stop registry
	m.registry.Stop()

	// Stop gRPC server
	if m.grpc != nil {
		m.grpc.GracefulStop()
	}

	m.running = false

	return nil
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// RegisterJobType registers a job type for detection and execution
func (m *Manager) RegisterJobType(jobType string, detectionInterval time.Duration) error {
	m.mu.RLock()
	running := m.running
	m.mu.RUnlock()

	if !running {
		return fmt.Errorf("plugin manager not running")
	}

	// Register job type
	if err := m.dispatcher.RegisterJobType(jobType, detectionInterval); err != nil {
		return err
	}

	_, err := m.configMgr.LoadConfig(jobType)
	if err != nil {
		return err
	}

	return nil
}

// QueueJob queues a new job for execution
func (m *Manager) QueueJob(jobType string, description string, priority int64, metadata map[string]string) (*Job, error) {
	m.mu.RLock()
	running := m.running
	m.mu.RUnlock()

	if !running {
		return nil, fmt.Errorf("plugin manager not running")
	}

	jobReq := &plugin_pb.JobRequest{
		JobType:       jobType,
		Description:   description,
		Priority:      priority,
		Metadata:      metadata,
		RequestSource: "admin",
	}

	return m.dispatcher.QueueJob(jobType, jobReq, "")
}

// DispatchJob dispatches the next available job
func (m *Manager) DispatchJob(jobType string) (*Job, *ConnectedPlugin, error) {
	m.mu.RLock()
	running := m.running
	m.mu.RUnlock()

	if !running {
		return nil, nil, fmt.Errorf("plugin manager not running")
	}

	return m.dispatcher.DispatchJob(jobType)
}

// GetJob retrieves a job by ID
func (m *Manager) GetJob(jobID string) (*Job, error) {
	return m.dispatcher.GetJob(jobID)
}

// ListJobs returns jobs of a specific type and status
func (m *Manager) ListJobs(jobType string, status JobState) []*Job {
	return m.dispatcher.ListJobs(jobType, status)
}

// ListAllJobs returns all jobs
func (m *Manager) ListAllJobs() []*Job {
	return m.dispatcher.ListAllJobs()
}

// ListPlugins returns all connected plugins
func (m *Manager) ListPlugins() []*ConnectedPlugin {
	return m.registry.ListPlugins()
}

// GetPlugin retrieves a plugin by ID
func (m *Manager) GetPlugin(pluginID string) (*ConnectedPlugin, error) {
	return m.registry.GetPlugin(pluginID)
}

// ListPluginsByCapability returns plugins that support a job type
func (m *Manager) ListPluginsByCapability(jobType string) []*ConnectedPlugin {
	return m.registry.ListPluginsByCapability(jobType)
}

// UpdateJobTypeConfig updates configuration for a job type
func (m *Manager) UpdateJobTypeConfig(jobType string, enabled bool, config []*plugin_pb.ConfigFieldValue) (*JobTypeConfig, error) {
	return m.configMgr.UpdateConfig(jobType, enabled, config, nil, "admin")
}

// GetJobTypeConfig retrieves configuration for a job type
func (m *Manager) GetJobTypeConfig(jobType string) (*JobTypeConfig, error) {
	return m.configMgr.GetConfig(jobType)
}

// CancelJob cancels a job
func (m *Manager) CancelJob(jobID string) error {
	return m.dispatcher.CancelJob(jobID)
}

// RetryJob retries a failed job
func (m *Manager) RetryJob(jobID string) error {
	return m.dispatcher.RetryJob(jobID)
}

// GetStats returns system statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	running := m.running
	m.mu.RUnlock()

	stats := map[string]interface{}{
		"running": running,
	}

	if running {
		stats["registry"] = m.registry.GetStats()
		stats["dispatcher"] = m.dispatcher.GetStats()
	}

	return stats
}

// WaitForPluginConnection waits for a plugin to connect with timeout
func (m *Manager) WaitForPluginConnection(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		plugins := m.registry.ListPlugins()
		if len(plugins) > 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("no plugin connected within %v", timeout)
}

// CreateTestJob creates a job for testing
func (m *Manager) CreateTestJob(jobType string, description string) (*Job, error) {
	return m.QueueJob(jobType, description, 100, map[string]string{
		"test": "true",
	})
}

// GetHealthStatus returns the health status of the plugin system
func (m *Manager) GetHealthStatus() map[string]interface{} {
	m.mu.RLock()
	running := m.running
	m.mu.RUnlock()

	status := map[string]interface{}{
		"running": running,
	}

	if !running {
		return status
	}

	plugins := m.registry.ListPlugins()
	var healthyCount int
	for _, p := range plugins {
		if p.IsHealthy() {
			healthyCount++
		}
	}

	jobs := m.dispatcher.ListAllJobs()
	var runningJobs, pendingJobs, completedJobs, failedJobs int

	for _, job := range jobs {
		switch job.GetState() {
		case JobStateRunning:
			runningJobs++
		case JobStatePending:
			pendingJobs++
		case JobStateCompleted:
			completedJobs++
		case JobStateFailed:
			failedJobs++
		}
	}

	status["plugins"] = map[string]interface{}{
		"total":   len(plugins),
		"healthy": healthyCount,
	}

	status["jobs"] = map[string]interface{}{
		"running":   runningJobs,
		"pending":   pendingJobs,
		"completed": completedJobs,
		"failed":    failedJobs,
		"total":     len(jobs),
	}

	return status
}

// LoadAllConfigs loads all configurations from disk
func (m *Manager) LoadAllConfigs() error {
	return m.configMgr.LoadAllConfigs()
}

// GetDataDir returns the data directory
func (m *Manager) GetDataDir() string {
	return m.configMgr.GetDataDir()
}

// QueryPluginService queries a plugin service for capabilities
func (m *Manager) QueryPluginService(ctx context.Context, pluginID string, addr string) (*plugin_pb.PluginRegister, error) {
	// This would be implemented to connect to a plugin and query its capabilities
	// For now, return a placeholder error
	return nil, fmt.Errorf("not yet implemented")
}

// ExecuteDetection triggers detection for a job type
func (m *Manager) ExecuteDetection(ctx context.Context, jobType string) ([]*DetectedJobInfo, error) {
	_, err := m.registry.GetDetectorForJobType(jobType)
	if err != nil {
		return nil, fmt.Errorf("no detector available for job type %s: %w", jobType, err)
	}

	// TODO: Call detector plugin's DetectJobs method
	// This would require connecting to the plugin and calling its service

	return nil, fmt.Errorf("detection not yet implemented")
}

// DetectedJobInfo holds information about a detected job
type DetectedJobInfo struct {
	Key       string
	JobType   string
	Priority  int64
	Metadata  map[string]string
	Timestamp time.Time
}

// GetListenAddr returns the listen address
func (m *Manager) GetListenAddr() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.listenAddr
}

// GetContext returns a context for the manager
func (m *Manager) GetContext() context.Context {
	return context.Background()
}

// Close closes the manager and releases resources
func (m *Manager) Close() error {
	if m.IsRunning() {
		return m.Stop()
	}
	return nil
}
