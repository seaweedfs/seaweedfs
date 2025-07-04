package dash

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const (
	// Configuration file names
	MaintenanceConfigFile = "maintenance.json"
	AdminConfigFile       = "admin.json"
	ConfigDirPermissions  = 0755
	ConfigFilePermissions = 0644
)

// ConfigPersistence handles saving and loading configuration files
type ConfigPersistence struct {
	dataDir string
}

// NewConfigPersistence creates a new configuration persistence manager
func NewConfigPersistence(dataDir string) *ConfigPersistence {
	return &ConfigPersistence{
		dataDir: dataDir,
	}
}

// SaveMaintenanceConfig saves maintenance configuration to JSON file
func (cp *ConfigPersistence) SaveMaintenanceConfig(config *MaintenanceConfig) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save configuration")
	}

	configPath := filepath.Join(cp.dataDir, MaintenanceConfigFile)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(cp.dataDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	// Marshal configuration to JSON
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal maintenance config: %v", err)
	}

	// Write to file
	if err := os.WriteFile(configPath, configData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write maintenance config file: %v", err)
	}

	glog.V(1).Infof("Saved maintenance configuration to %s", configPath)
	return nil
}

// LoadMaintenanceConfig loads maintenance configuration from JSON file
func (cp *ConfigPersistence) LoadMaintenanceConfig() (*MaintenanceConfig, error) {
	if cp.dataDir == "" {
		glog.V(1).Infof("No data directory specified, using default maintenance configuration")
		return DefaultMaintenanceConfig(), nil
	}

	configPath := filepath.Join(cp.dataDir, MaintenanceConfigFile)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		glog.V(1).Infof("Maintenance config file does not exist, using defaults: %s", configPath)
		return DefaultMaintenanceConfig(), nil
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read maintenance config file: %v", err)
	}

	// Unmarshal JSON
	var config MaintenanceConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal maintenance config: %v", err)
	}

	glog.V(1).Infof("Loaded maintenance configuration from %s", configPath)
	return &config, nil
}

// SaveAdminConfig saves general admin configuration to JSON file
func (cp *ConfigPersistence) SaveAdminConfig(config map[string]interface{}) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save configuration")
	}

	configPath := filepath.Join(cp.dataDir, AdminConfigFile)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(cp.dataDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	// Marshal configuration to JSON
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal admin config: %v", err)
	}

	// Write to file
	if err := os.WriteFile(configPath, configData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write admin config file: %v", err)
	}

	glog.V(1).Infof("Saved admin configuration to %s", configPath)
	return nil
}

// LoadAdminConfig loads general admin configuration from JSON file
func (cp *ConfigPersistence) LoadAdminConfig() (map[string]interface{}, error) {
	if cp.dataDir == "" {
		glog.V(1).Infof("No data directory specified, using default admin configuration")
		return make(map[string]interface{}), nil
	}

	configPath := filepath.Join(cp.dataDir, AdminConfigFile)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		glog.V(1).Infof("Admin config file does not exist, using defaults: %s", configPath)
		return make(map[string]interface{}), nil
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read admin config file: %v", err)
	}

	// Unmarshal JSON
	var config map[string]interface{}
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal admin config: %v", err)
	}

	glog.V(1).Infof("Loaded admin configuration from %s", configPath)
	return config, nil
}

// GetConfigPath returns the path to a configuration file
func (cp *ConfigPersistence) GetConfigPath(filename string) string {
	if cp.dataDir == "" {
		return ""
	}
	return filepath.Join(cp.dataDir, filename)
}

// ListConfigFiles returns all configuration files in the data directory
func (cp *ConfigPersistence) ListConfigFiles() ([]string, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified")
	}

	files, err := os.ReadDir(cp.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read config directory: %v", err)
	}

	var configFiles []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			configFiles = append(configFiles, file.Name())
		}
	}

	return configFiles, nil
}

// BackupConfig creates a backup of a configuration file
func (cp *ConfigPersistence) BackupConfig(filename string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified")
	}

	configPath := filepath.Join(cp.dataDir, filename)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("config file does not exist: %s", filename)
	}

	// Create backup filename with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	backupName := fmt.Sprintf("%s.backup_%s", filename, timestamp)
	backupPath := filepath.Join(cp.dataDir, backupName)

	// Copy file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}

	if err := os.WriteFile(backupPath, configData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to create backup: %v", err)
	}

	glog.V(1).Infof("Created backup of %s as %s", filename, backupName)
	return nil
}

// RestoreConfig restores a configuration file from a backup
func (cp *ConfigPersistence) RestoreConfig(filename, backupName string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified")
	}

	backupPath := filepath.Join(cp.dataDir, backupName)
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist: %s", backupName)
	}

	// Read backup file
	backupData, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %v", err)
	}

	// Write to config file
	configPath := filepath.Join(cp.dataDir, filename)
	if err := os.WriteFile(configPath, backupData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to restore config: %v", err)
	}

	glog.V(1).Infof("Restored %s from backup %s", filename, backupName)
	return nil
}

// GetDataDir returns the data directory path
func (cp *ConfigPersistence) GetDataDir() string {
	return cp.dataDir
}

// IsConfigured returns true if a data directory is configured
func (cp *ConfigPersistence) IsConfigured() bool {
	return cp.dataDir != ""
}

// GetConfigInfo returns information about the configuration storage
func (cp *ConfigPersistence) GetConfigInfo() map[string]interface{} {
	info := map[string]interface{}{
		"data_dir_configured": cp.IsConfigured(),
		"data_dir":            cp.dataDir,
	}

	if cp.IsConfigured() {
		// Check if data directory exists
		if _, err := os.Stat(cp.dataDir); err == nil {
			info["data_dir_exists"] = true

			// List config files
			configFiles, err := cp.ListConfigFiles()
			if err == nil {
				info["config_files"] = configFiles
			}
		} else {
			info["data_dir_exists"] = false
		}
	}

	return info
}
