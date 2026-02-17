package plugin

import (
	"fmt"
	"sync"
	"time"
)

// Registry manages plugin registration and lifecycle
type Registry struct {
	mu                     sync.RWMutex
	plugins                map[string]*ConnectedPlugin
	capabilityIndex        map[string][]string // Maps capability to plugin IDs
	healthCheckTimeout     time.Duration
	failureDetectionWindow time.Duration
	failureThreshold       int
	pluginFailureCount     map[string]int
}

// NewRegistry creates a new plugin registry
func NewRegistry(healthCheckTimeout, failureDetectionWindow time.Duration, failureThreshold int) *Registry {
	return &Registry{
		plugins:                make(map[string]*ConnectedPlugin),
		capabilityIndex:        make(map[string][]string),
		healthCheckTimeout:     healthCheckTimeout,
		failureDetectionWindow: failureDetectionWindow,
		failureThreshold:       failureThreshold,
		pluginFailureCount:     make(map[string]int),
	}
}

// RegisterPlugin adds a plugin to the registry
func (r *Registry) RegisterPlugin(plugin *ConnectedPlugin) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.plugins[plugin.ID]; exists {
		return fmt.Errorf("plugin %s already registered", plugin.ID)
	}

	r.plugins[plugin.ID] = plugin
	r.pluginFailureCount[plugin.ID] = 0

	// Build capability index
	for _, cap := range plugin.Capabilities {
		r.capabilityIndex[cap] = append(r.capabilityIndex[cap], plugin.ID)
	}

	return nil
}

// UnregisterPlugin removes a plugin from the registry
func (r *Registry) UnregisterPlugin(pluginID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	plugin, exists := r.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	// Remove from capability index
	for _, cap := range plugin.Capabilities {
		for i, id := range r.capabilityIndex[cap] {
			if id == pluginID {
				r.capabilityIndex[cap] = append(r.capabilityIndex[cap][:i], r.capabilityIndex[cap][i+1:]...)
				break
			}
		}
	}

	delete(r.plugins, pluginID)
	delete(r.pluginFailureCount, pluginID)

	return nil
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

// GetPluginsByCapability returns all plugins with a specific capability
func (r *Registry) GetPluginsByCapability(capability string) []*ConnectedPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pluginIDs, exists := r.capabilityIndex[capability]
	if !exists {
		return []*ConnectedPlugin{}
	}

	var result []*ConnectedPlugin
	for _, id := range pluginIDs {
		if plugin, ok := r.plugins[id]; ok {
			result = append(result, plugin)
		}
	}

	return result
}

// ListPlugins returns all registered plugins
func (r *Registry) ListPlugins(includeUnhealthy bool) []*ConnectedPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*ConnectedPlugin

	for _, plugin := range r.plugins {
		if !includeUnhealthy && time.Since(plugin.LastHeartbeat) > r.healthCheckTimeout {
			continue
		}
		result = append(result, plugin)
	}

	return result
}

// HealthCheck verifies plugin health based on heartbeat status
func (r *Registry) HealthCheck(pluginID string) (bool, error) {
	r.mu.RLock()
	plugin, exists := r.plugins[pluginID]
	r.mu.RUnlock()

	if !exists {
		return false, fmt.Errorf("plugin %s not found", pluginID)
	}

	isHealthy := plugin.IsHealthy(r.healthCheckTimeout)

	if !isHealthy {
		r.mu.Lock()
		r.pluginFailureCount[pluginID]++
		r.mu.Unlock()
	} else {
		r.mu.Lock()
		r.pluginFailureCount[pluginID] = 0
		r.mu.Unlock()
	}

	return isHealthy, nil
}

// GetFailureCount returns the current failure count for a plugin
func (r *Registry) GetFailureCount(pluginID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pluginFailureCount[pluginID]
}

// HasExceededFailureThreshold checks if a plugin has exceeded the failure threshold
func (r *Registry) HasExceededFailureThreshold(pluginID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pluginFailureCount[pluginID] > r.failureThreshold
}

// ResetFailureCount resets the failure counter for a plugin
func (r *Registry) ResetFailureCount(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pluginFailureCount[pluginID] = 0
}

// Count returns the total number of registered plugins
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.plugins)
}

// CountHealthy returns the number of healthy plugins
func (r *Registry) CountHealthy() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, plugin := range r.plugins {
		if plugin.IsHealthy(r.healthCheckTimeout) {
			count++
		}
	}
	return count
}

// GetCapabilities returns all registered capabilities
func (r *Registry) GetCapabilities() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var capabilities []string
	for cap := range r.capabilityIndex {
		capabilities = append(capabilities, cap)
	}
	return capabilities
}

// UpdateHeartbeat updates the heartbeat timestamp for a plugin
func (r *Registry) UpdateHeartbeat(pluginID string) error {
	r.mu.RLock()
	plugin, exists := r.plugins[pluginID]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	plugin.UpdateHeartbeat()
	r.ResetFailureCount(pluginID)
	return nil
}

// GetUnhealthyPlugins returns plugins that have failed health checks
func (r *Registry) GetUnhealthyPlugins() []*ConnectedPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var unhealthy []*ConnectedPlugin
	for _, plugin := range r.plugins {
		if !plugin.IsHealthy(r.healthCheckTimeout) {
			unhealthy = append(unhealthy, plugin)
		}
	}
	return unhealthy
}

// RemoveUnhealthyPlugins removes plugins that have exceeded the failure threshold
func (r *Registry) RemoveUnhealthyPlugins() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var removed []string
	for pluginID, failureCount := range r.pluginFailureCount {
		if failureCount > r.failureThreshold {
			if plugin, exists := r.plugins[pluginID]; exists {
				// Remove from capability index
				for _, cap := range plugin.Capabilities {
					for i, id := range r.capabilityIndex[cap] {
						if id == pluginID {
							r.capabilityIndex[cap] = append(r.capabilityIndex[cap][:i], r.capabilityIndex[cap][i+1:]...)
							break
						}
					}
				}
				delete(r.plugins, pluginID)
				delete(r.pluginFailureCount, pluginID)
				removed = append(removed, pluginID)
			}
		}
	}
	return removed
}

// UpdatePluginStatus updates the status field of a plugin
func (r *Registry) UpdatePluginStatus(pluginID, status string) error {
	r.mu.RLock()
	plugin, exists := r.plugins[pluginID]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	plugin.mu.Lock()
	plugin.Status = status
	plugin.mu.Unlock()

	return nil
}

// GetStats returns statistics for all plugins
func (r *Registry) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	totalPlugins := len(r.plugins)
	healthyPlugins := 0
	totalActiveJobs := 0
	totalCompletedJobs := 0
	totalFailedJobs := 0

	for _, plugin := range r.plugins {
		if plugin.IsHealthy(r.healthCheckTimeout) {
			healthyPlugins++
		}
		plugin.mu.RLock()
		totalActiveJobs += plugin.ActiveJobs
		totalCompletedJobs += plugin.CompletedJobs
		totalFailedJobs += plugin.FailedJobs
		plugin.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_plugins":     totalPlugins,
		"healthy_plugins":   healthyPlugins,
		"unhealthy_plugins": totalPlugins - healthyPlugins,
		"total_active_jobs": totalActiveJobs,
		"total_completed":   totalCompletedJobs,
		"total_failed":      totalFailedJobs,
		"capabilities":      len(r.capabilityIndex),
	}
}
