package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ConfigManager handles JSON-based configuration persistence
type ConfigManager struct {
	mu              sync.RWMutex
	configDir       string
	defaultConfigFile string
	pluginConfigs   map[string]*PluginConfig
	configVersions  map[string]int64
	lastModified    map[string]time.Time
	backupDir       string
	maxBackups      int
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(configDir string) (*ConfigManager, error) {
	// Ensure config directory exists
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	backupDir := filepath.Join(configDir, "backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	return &ConfigManager{
		configDir:         configDir,
		defaultConfigFile: filepath.Join(configDir, "plugins.json"),
		pluginConfigs:     make(map[string]*PluginConfig),
		configVersions:    make(map[string]int64),
		lastModified:      make(map[string]time.Time),
		backupDir:         backupDir,
		maxBackups:        10,
	}, nil
}

// SaveConfig persists a plugin configuration to disk
func (cm *ConfigManager) SaveConfig(config *PluginConfig, backup bool) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if backup {
		if err := cm.backupExistingConfig(config.PluginID); err != nil {
			return fmt.Errorf("failed to backup config: %w", err)
		}
	}

	configFile := filepath.Join(cm.configDir, fmt.Sprintf("%s.json", config.PluginID))
	configData := map[string]interface{}{
		"plugin_id":              config.PluginID,
		"properties":             config.Properties,
		"job_types":              config.JobTypes,
		"max_retries":            config.MaxRetries,
		"health_check_interval":  config.HealthCheckInterval.String(),
		"job_timeout":            config.JobTimeout.String(),
		"environment":            config.Environment,
	}

	data, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := ioutil.WriteFile(configFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	// Update in-memory state
	cm.pluginConfigs[config.PluginID] = config
	cm.configVersions[config.PluginID]++
	cm.lastModified[config.PluginID] = time.Now()

	return nil
}

// LoadConfig loads a plugin configuration from disk
func (cm *ConfigManager) LoadConfig(pluginID string) (*PluginConfig, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	configFile := filepath.Join(cm.configDir, fmt.Sprintf("%s.json", pluginID))

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var configData map[string]interface{}
	if err := json.Unmarshal(data, &configData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	config := &PluginConfig{
		PluginID:    pluginID,
		Properties:  make(map[string]string),
		JobTypes:    make(map[string]*JobTypeConfig),
		Environment: make(map[string]string),
	}

	// Parse basic fields
	if props, ok := configData["properties"].(map[string]interface{}); ok {
		for k, v := range props {
			if str, ok := v.(string); ok {
				config.Properties[k] = str
			}
		}
	}

	if maxRetries, ok := configData["max_retries"].(float64); ok {
		config.MaxRetries = int(maxRetries)
	}

	if hcInterval, ok := configData["health_check_interval"].(string); ok {
		if duration, err := time.ParseDuration(hcInterval); err == nil {
			config.HealthCheckInterval = duration
		}
	}

	if timeout, ok := configData["job_timeout"].(string); ok {
		if duration, err := time.ParseDuration(timeout); err == nil {
			config.JobTimeout = duration
		}
	}

	if env, ok := configData["environment"].(map[string]interface{}); ok {
		for k, v := range env {
			if str, ok := v.(string); ok {
				config.Environment[k] = str
			}
		}
	}

	// Parse job types
	if jobTypes, ok := configData["job_types"].(map[string]interface{}); ok {
		for jobType, typeConfig := range jobTypes {
			if typeCfg, ok := typeConfig.(map[string]interface{}); ok {
				jtc := &JobTypeConfig{
					Type:       jobType,
					Parameters: make(map[string]string),
				}

				if enabled, ok := typeCfg["enabled"].(bool); ok {
					jtc.Enabled = enabled
				}

				if priority, ok := typeCfg["priority"].(float64); ok {
					jtc.Priority = int(priority)
				}

				if interval, ok := typeCfg["interval"].(string); ok {
					if duration, err := time.ParseDuration(interval); err == nil {
						jtc.Interval = duration
					}
				}

				if maxConcurrent, ok := typeCfg["max_concurrent"].(float64); ok {
					jtc.MaxConcurrent = int(maxConcurrent)
				}

				if params, ok := typeCfg["parameters"].(map[string]interface{}); ok {
					for pk, pv := range params {
						if str, ok := pv.(string); ok {
							jtc.Parameters[pk] = str
						}
					}
				}

				config.JobTypes[jobType] = jtc
			}
		}
	}

	cm.pluginConfigs[pluginID] = config
	cm.configVersions[pluginID]++
	cm.lastModified[pluginID] = time.Now()

	return config, nil
}

// GetConfig retrieves a configuration from memory
func (cm *ConfigManager) GetConfig(pluginID string) (*PluginConfig, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	config, exists := cm.pluginConfigs[pluginID]
	return config, exists
}

// ListConfigs returns all loaded configurations
func (cm *ConfigManager) ListConfigs() map[string]*PluginConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]*PluginConfig)
	for pluginID, config := range cm.pluginConfigs {
		result[pluginID] = config
	}
	return result
}

// DeleteConfig removes a configuration
func (cm *ConfigManager) DeleteConfig(pluginID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	configFile := filepath.Join(cm.configDir, fmt.Sprintf("%s.json", pluginID))
	if err := os.Remove(configFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete config file: %w", err)
	}

	delete(cm.pluginConfigs, pluginID)
	delete(cm.configVersions, pluginID)
	delete(cm.lastModified, pluginID)

	return nil
}

// GetVersion returns the version number of a configuration
func (cm *ConfigManager) GetVersion(pluginID string) int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.configVersions[pluginID]
}

// GetLastModified returns the last modification time of a configuration
func (cm *ConfigManager) GetLastModified(pluginID string) time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.lastModified[pluginID]
}

// backupExistingConfig creates a backup of an existing configuration
func (cm *ConfigManager) backupExistingConfig(pluginID string) error {
	configFile := filepath.Join(cm.configDir, fmt.Sprintf("%s.json", pluginID))

	// Check if file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil // No existing config to back up
	}

	// Create backup filename with timestamp
	backupFilename := fmt.Sprintf("%s_%d.json.bak", pluginID, time.Now().Unix())
	backupFile := filepath.Join(cm.backupDir, backupFilename)

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("failed to read config for backup: %w", err)
	}

	if err := ioutil.WriteFile(backupFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write backup file: %w", err)
	}

	// Cleanup old backups
	cm.cleanupOldBackups(pluginID)

	return nil
}

// cleanupOldBackups removes old backup files, keeping only maxBackups
func (cm *ConfigManager) cleanupOldBackups(pluginID string) {
	pattern := filepath.Join(cm.backupDir, fmt.Sprintf("%s_*.json.bak", pluginID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	if len(files) > cm.maxBackups {
		// Sort by modification time and remove oldest
		for i := 0; i < len(files)-cm.maxBackups; i++ {
			os.Remove(files[i])
		}
	}
}

// LoadAllConfigs loads all configurations from the config directory
func (cm *ConfigManager) LoadAllConfigs() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(cm.configDir, "*.json"))
	if err != nil {
		return fmt.Errorf("failed to list config files: %w", err)
	}

	for _, file := range files {
		filename := filepath.Base(file)
		pluginID := filename[:len(filename)-5] // Remove .json extension

		if pluginID == "plugins" {
			continue // Skip main config file
		}

		data, err := ioutil.ReadFile(file)
		if err != nil {
			continue
		}

		var configData map[string]interface{}
		if err := json.Unmarshal(data, &configData); err != nil {
			continue
		}

		// Basic parsing (simplified)
		config := &PluginConfig{
			PluginID:    pluginID,
			Properties:  make(map[string]string),
			JobTypes:    make(map[string]*JobTypeConfig),
			Environment: make(map[string]string),
		}

		cm.pluginConfigs[pluginID] = config
		cm.configVersions[pluginID] = 1
		cm.lastModified[pluginID] = time.Now()
	}

	return nil
}

// ExportConfigs exports all configurations to a JSON file
func (cm *ConfigManager) ExportConfigs() (string, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	exportData := make(map[string]interface{})
	for pluginID, config := range cm.pluginConfigs {
		exportData[pluginID] = config
	}

	data, err := json.MarshalIndent(exportData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal configs: %w", err)
	}

	return string(data), nil
}

// ImportConfigs imports configurations from a JSON string
func (cm *ConfigManager) ImportConfigs(jsonData string) error {
	var importData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &importData); err != nil {
		return fmt.Errorf("failed to unmarshal import data: %w", err)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for pluginID, configData := range importData {
		if cfgMap, ok := configData.(map[string]interface{}); ok {
			config := &PluginConfig{
				PluginID:    pluginID,
				Properties:  make(map[string]string),
				JobTypes:    make(map[string]*JobTypeConfig),
				Environment: make(map[string]string),
			}
			cm.pluginConfigs[pluginID] = config
			cm.configVersions[pluginID]++
			cm.lastModified[pluginID] = time.Now()
		}
	}

	return nil
}
