package plugin

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// Registry manages all connected plugins and their capabilities
type Registry struct {
	plugins map[string]*ConnectedPlugin

	// Index by job type and capability (detector/executor)
	detectorsByJobType map[string][]*ConnectedPlugin
	executorsByJobType map[string][]*ConnectedPlugin

	mu sync.RWMutex

	// Configuration
	healthCheckInterval time.Duration
	healthCheckTimeout  time.Duration
	healthCheckTicker   *time.Ticker
	stopChan            chan struct{}
}

// NewRegistry creates a new plugin registry
func NewRegistry() *Registry {
	return &Registry{
		plugins:             make(map[string]*ConnectedPlugin),
		detectorsByJobType:  make(map[string][]*ConnectedPlugin),
		executorsByJobType:  make(map[string][]*ConnectedPlugin),
		healthCheckInterval: 30 * time.Second,
		healthCheckTimeout:  90 * time.Second,
		stopChan:            make(chan struct{}),
	}
}

// RegisterPlugin adds a new plugin to the registry
func (r *Registry) RegisterPlugin(pluginID string, register *plugin_pb.PluginRegister) (*ConnectedPlugin, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.plugins[pluginID]; exists {
		return nil, fmt.Errorf("plugin %s already registered", pluginID)
	}

	plugin := &ConnectedPlugin{
		ID:              pluginID,
		Name:            register.Name,
		Version:         register.Version,
		ProtocolVersion: register.ProtocolVersion,
		ConnectedAt:     time.Now(),
		LastHeartbeat:   time.Now(),
		State:           PluginStateConnected,
		Healthy:         true,
		Capabilities:    make(map[string]*plugin_pb.JobTypeCapability),
		StreamConnected: true,
	}

	// Index capabilities
	for _, cap := range register.Capabilities {
		plugin.Capabilities[cap.JobType] = cap

		if cap.CanDetect {
			r.detectorsByJobType[cap.JobType] = append(r.detectorsByJobType[cap.JobType], plugin)
		}
		if cap.CanExecute {
			r.executorsByJobType[cap.JobType] = append(r.executorsByJobType[cap.JobType], plugin)
		}
	}

	r.plugins[pluginID] = plugin
	return plugin, nil
}

// UnregisterPlugin removes a plugin from the registry
func (r *Registry) UnregisterPlugin(pluginID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	plugin, exists := r.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	// Remove from capability indexes
	for jobType := range plugin.Capabilities {
		r.removePluginFromDetectors(jobType, plugin)
		r.removePluginFromExecutors(jobType, plugin)
	}

	delete(r.plugins, pluginID)
	plugin.SetState(PluginStateDisconnected)
	return nil
}

// removePluginFromDetectors removes plugin from detector index for a job type
func (r *Registry) removePluginFromDetectors(jobType string, plugin *ConnectedPlugin) {
	detectors := r.detectorsByJobType[jobType]
	for i, p := range detectors {
		if p.ID == plugin.ID {
			r.detectorsByJobType[jobType] = append(detectors[:i], detectors[i+1:]...)
			break
		}
	}
	if len(r.detectorsByJobType[jobType]) == 0 {
		delete(r.detectorsByJobType, jobType)
	}
}

// removePluginFromExecutors removes plugin from executor index for a job type
func (r *Registry) removePluginFromExecutors(jobType string, plugin *ConnectedPlugin) {
	executors := r.executorsByJobType[jobType]
	for i, p := range executors {
		if p.ID == plugin.ID {
			r.executorsByJobType[jobType] = append(executors[:i], executors[i+1:]...)
			break
		}
	}
	if len(r.executorsByJobType[jobType]) == 0 {
		delete(r.executorsByJobType, jobType)
	}
}

// GetPlugin retrieves a plugin by ID
func (r *Registry) GetPlugin(pluginID string) (*ConnectedPlugin, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plugin, exists := r.plugins[pluginID]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", pluginID)
	}
	return plugin, nil
}

// GetDetectorForJobType returns an available detector plugin for a job type
func (r *Registry) GetDetectorForJobType(jobType string) (*ConnectedPlugin, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	detectors, exists := r.detectorsByJobType[jobType]
	if !exists || len(detectors) == 0 {
		return nil, fmt.Errorf("no detector available for job type %s", jobType)
	}

	// Return the first healthy detector
	for _, detector := range detectors {
		if detector.IsHealthy() {
			return detector, nil
		}
	}

	return nil, fmt.Errorf("no healthy detector available for job type %s", jobType)
}

// GetExecutorForJobType returns an available executor plugin for a job type
func (r *Registry) GetExecutorForJobType(jobType string) (*ConnectedPlugin, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	executors, exists := r.executorsByJobType[jobType]
	if !exists || len(executors) == 0 {
		return nil, fmt.Errorf("no executor available for job type %s", jobType)
	}

	// Return the first healthy executor with lowest workload
	var bestExecutor *ConnectedPlugin
	var minLoad int32 = int32(^uint32(0) >> 1)

	for _, executor := range executors {
		if executor.IsHealthy() {
			load := executor.PendingJobs + executor.RunningJobs
			if load < minLoad {
				minLoad = load
				bestExecutor = executor
			}
		}
	}

	if bestExecutor == nil {
		return nil, fmt.Errorf("no healthy executor available for job type %s", jobType)
	}

	return bestExecutor, nil
}

// ListPlugins returns all connected plugins
func (r *Registry) ListPlugins() []*ConnectedPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plugins := make([]*ConnectedPlugin, 0, len(r.plugins))
	for _, plugin := range r.plugins {
		plugins = append(plugins, plugin)
	}
	return plugins
}

// ListPluginsByCapability returns plugins that support a specific job type
func (r *Registry) ListPluginsByCapability(jobType string) []*ConnectedPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	detectors := r.detectorsByJobType[jobType]
	executors := r.executorsByJobType[jobType]

	// Combine and deduplicate
	pluginMap := make(map[string]*ConnectedPlugin)
	for _, p := range detectors {
		pluginMap[p.ID] = p
	}
	for _, p := range executors {
		pluginMap[p.ID] = p
	}

	plugins := make([]*ConnectedPlugin, 0, len(pluginMap))
	for _, p := range pluginMap {
		plugins = append(plugins, p)
	}
	return plugins
}

// UpdateHeartbeat updates plugin heartbeat and health status
func (r *Registry) UpdateHeartbeat(pluginID string, heartbeat *plugin_pb.PluginHeartbeat) error {
	r.mu.Lock()
	plugin, exists := r.plugins[pluginID]
	r.mu.Unlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	plugin.UpdateHeartbeat(
		heartbeat.CpuUsagePercent,
		heartbeat.MemoryUsageMb,
		heartbeat.PendingJobs,
		int32(len(plugin.Capabilities)), // rough estimate
	)

	return nil
}

// Start starts the health check loop
func (r *Registry) Start() {
	r.healthCheckTicker = time.NewTicker(r.healthCheckInterval)
	go r.healthCheckLoop()
}

// Stop stops the health check loop
func (r *Registry) Stop() {
	close(r.stopChan)
	if r.healthCheckTicker != nil {
		r.healthCheckTicker.Stop()
	}
}

// healthCheckLoop periodically checks plugin health
func (r *Registry) healthCheckLoop() {
	for {
		select {
		case <-r.healthCheckTicker.C:
			r.checkHealth()
		case <-r.stopChan:
			return
		}
	}
}

// checkHealth checks the health of all plugins
func (r *Registry) checkHealth() {
	r.mu.RLock()
	plugins := make([]*ConnectedPlugin, 0, len(r.plugins))
	for _, p := range r.plugins {
		plugins = append(plugins, p)
	}
	r.mu.RUnlock()

	now := time.Now()
	for _, plugin := range plugins {
		plugin.mu.RLock()
		lastHeartbeat := plugin.LastHeartbeat
		plugin.mu.RUnlock()

		timeSinceHeartbeat := now.Sub(lastHeartbeat)

		if timeSinceHeartbeat > r.healthCheckTimeout {
			plugin.SetState(PluginStateUnhealthy)
			plugin.mu.Lock()
			plugin.Healthy = false
			plugin.mu.Unlock()
		} else if timeSinceHeartbeat > r.healthCheckTimeout/2 {
			plugin.SetState(PluginStateUnhealthy)
		} else {
			plugin.SetState(PluginStateHealthy)
			plugin.mu.Lock()
			plugin.Healthy = true
			plugin.mu.Unlock()
		}
	}
}

// GetStats returns registry statistics
func (r *Registry) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := map[string]interface{}{
		"total_plugins": len(r.plugins),
		"job_types":     len(r.detectorsByJobType),
	}

	var healthyCount int
	for _, p := range r.plugins {
		if p.IsHealthy() {
			healthyCount++
		}
	}
	stats["healthy_plugins"] = healthyCount

	jobTypeStats := make(map[string]map[string]interface{})
	for jobType := range r.detectorsByJobType {
		jobTypeStats[jobType] = map[string]interface{}{
			"detectors": len(r.detectorsByJobType[jobType]),
			"executors": len(r.executorsByJobType[jobType]),
		}
	}
	stats["job_type_stats"] = jobTypeStats

	return stats
}
