package plugin

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// ConfigManager handles configuration persistence and retrieval
type ConfigManager struct {
	dataDir string
	configs map[string]*JobTypeConfig

	mu sync.RWMutex
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(dataDir string) (*ConfigManager, error) {
	cm := &ConfigManager{
		dataDir: filepath.Join(dataDir, "plugins"),
		configs: make(map[string]*JobTypeConfig),
	}

	// Create plugins directory if it doesn't exist
	if err := os.MkdirAll(cm.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create plugin config directory: %w", err)
	}

	return cm, nil
}

// LoadConfig loads a configuration from disk
func (cm *ConfigManager) LoadConfig(jobType string) (*JobTypeConfig, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check cache first
	if config, exists := cm.configs[jobType]; exists {
		return config, nil
	}

	configPath := filepath.Join(cm.dataDir, jobType+".json")

	// If file doesn't exist, create default config
	if _, err := os.Stat(configPath); err != nil {
		if os.IsNotExist(err) {
			config := &JobTypeConfig{
				JobType:      jobType,
				Enabled:      false,
				AdminConfig:  make([]*plugin_pb.ConfigFieldValue, 0),
				WorkerConfig: make([]*plugin_pb.ConfigFieldValue, 0),
				CreatedAt:    time.Now(),
				UpdatedAt:    time.Now(),
				CreatedBy:    "system",
			}
			cm.configs[jobType] = config
			return config, nil
		}
		return nil, fmt.Errorf("failed to access config file: %w", err)
	}

	// Load from file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var configData map[string]interface{}
	if err := json.Unmarshal(data, &configData); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	config := &JobTypeConfig{
		JobType:      jobType,
		AdminConfig:  make([]*plugin_pb.ConfigFieldValue, 0),
		WorkerConfig: make([]*plugin_pb.ConfigFieldValue, 0),
	}

	// Parse fields
	if enabled, ok := configData["enabled"].(bool); ok {
		config.Enabled = enabled
	}

	if createdBy, ok := configData["created_by"].(string); ok {
		config.CreatedBy = createdBy
	}

	if createdAt, ok := configData["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			config.CreatedAt = t
		}
	}

	if updatedAt, ok := configData["updated_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
			config.UpdatedAt = t
		}
	}

	cm.configs[jobType] = config
	return config, nil
}

// SaveConfig saves a configuration to disk
func (cm *ConfigManager) SaveConfig(config *JobTypeConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	config.UpdatedAt = time.Now()
	cm.configs[config.JobType] = config

	configPath := filepath.Join(cm.dataDir, config.JobType+".json")

	// Convert to JSON-serializable format
	configData := map[string]interface{}{
		"job_type":      config.JobType,
		"enabled":       config.Enabled,
		"created_by":    config.CreatedBy,
		"created_at":    config.CreatedAt.Format(time.RFC3339),
		"updated_at":    config.UpdatedAt.Format(time.RFC3339),
		"admin_config":  cm.configValuesToJSON(config.AdminConfig),
		"worker_config": cm.configValuesToJSON(config.WorkerConfig),
	}

	data, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// GetConfig retrieves a configuration
func (cm *ConfigManager) GetConfig(jobType string) (*JobTypeConfig, error) {
	cm.mu.RLock()
	if config, exists := cm.configs[jobType]; exists {
		cm.mu.RUnlock()
		return config, nil
	}
	cm.mu.RUnlock()

	// Load from disk if not in cache
	return cm.LoadConfig(jobType)
}

// UpdateConfig updates a configuration
func (cm *ConfigManager) UpdateConfig(jobType string, enabled bool, adminConfig, workerConfig []*plugin_pb.ConfigFieldValue, updatedBy string) (*JobTypeConfig, error) {
	config, err := cm.GetConfig(jobType)
	if err != nil {
		return nil, err
	}

	config.SetConfig(enabled, adminConfig, workerConfig)
	config.CreatedBy = updatedBy

	if err := cm.SaveConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

// ListConfigs lists all configurations
func (cm *ConfigManager) ListConfigs() []*JobTypeConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	configs := make([]*JobTypeConfig, 0, len(cm.configs))
	for _, config := range cm.configs {
		configs = append(configs, config)
	}
	return configs
}

// DeleteConfig deletes a configuration
func (cm *ConfigManager) DeleteConfig(jobType string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	configPath := filepath.Join(cm.dataDir, jobType+".json")

	if err := os.Remove(configPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete config file: %w", err)
	}

	delete(cm.configs, jobType)
	return nil
}

// configValuesToJSON converts ConfigFieldValue slice to JSON
func (cm *ConfigManager) configValuesToJSON(values []*plugin_pb.ConfigFieldValue) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(values))

	for _, val := range values {
		item := map[string]interface{}{
			"field_name": val.FieldName,
		}

		// Include non-empty values
		if val.StringValue != "" {
			item["string_value"] = val.StringValue
		}
		if val.IntValue != 0 {
			item["int_value"] = val.IntValue
		}
		if val.FloatValue != 0 {
			item["float_value"] = val.FloatValue
		}
		if val.BoolValue {
			item["bool_value"] = val.BoolValue
		}
		if val.DurationValue != nil {
			item["duration_value"] = val.DurationValue.String()
		}
		if len(val.MultiselectValues) > 0 {
			item["multiselect_values"] = val.MultiselectValues
		}
		if val.JsonValue != "" {
			item["json_value"] = val.JsonValue
		}

		result = append(result, item)
	}

	return result
}

// LoadAllConfigs loads all configurations from disk
func (cm *ConfigManager) LoadAllConfigs() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	entries, err := os.ReadDir(cm.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read config directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only load .json files
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		jobType := entry.Name()[:len(entry.Name())-5] // Remove .json

		configPath := filepath.Join(cm.dataDir, entry.Name())

		data, err := os.ReadFile(configPath)
		if err != nil {
			continue
		}

		var configData map[string]interface{}
		if err := json.Unmarshal(data, &configData); err != nil {
			continue
		}

		config := &JobTypeConfig{
			JobType:      jobType,
			AdminConfig:  make([]*plugin_pb.ConfigFieldValue, 0),
			WorkerConfig: make([]*plugin_pb.ConfigFieldValue, 0),
		}

		// Parse fields
		if enabled, ok := configData["enabled"].(bool); ok {
			config.Enabled = enabled
		}

		if createdBy, ok := configData["created_by"].(string); ok {
			config.CreatedBy = createdBy
		}

		if createdAt, ok := configData["created_at"].(string); ok {
			if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
				config.CreatedAt = t
			}
		}

		if updatedAt, ok := configData["updated_at"].(string); ok {
			if t, err := time.Parse(time.RFC3339, updatedAt); err == nil {
				config.UpdatedAt = t
			}
		}

		cm.configs[jobType] = config
	}

	return nil
}

// GetDataDir returns the data directory path
func (cm *ConfigManager) GetDataDir() string {
	return cm.dataDir
}
