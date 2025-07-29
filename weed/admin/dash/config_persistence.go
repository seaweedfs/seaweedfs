package dash

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// Configuration subdirectory
	ConfigSubdir = "conf"

	// Configuration file names (protobuf binary)
	MaintenanceConfigFile     = "maintenance.pb"
	VacuumTaskConfigFile      = "task_vacuum.pb"
	ECTaskConfigFile          = "task_erasure_coding.pb"
	BalanceTaskConfigFile     = "task_balance.pb"
	ReplicationTaskConfigFile = "task_replication.pb"

	// JSON reference files
	MaintenanceConfigJSONFile     = "maintenance.json"
	VacuumTaskConfigJSONFile      = "task_vacuum.json"
	ECTaskConfigJSONFile          = "task_erasure_coding.json"
	BalanceTaskConfigJSONFile     = "task_balance.json"
	ReplicationTaskConfigJSONFile = "task_replication.json"

	ConfigDirPermissions  = 0755
	ConfigFilePermissions = 0644
)

// Task configuration types
type (
	VacuumTaskConfig        = worker_pb.VacuumTaskConfig
	ErasureCodingTaskConfig = worker_pb.ErasureCodingTaskConfig
	BalanceTaskConfig       = worker_pb.BalanceTaskConfig
	ReplicationTaskConfig   = worker_pb.ReplicationTaskConfig
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

// SaveMaintenanceConfig saves maintenance configuration to protobuf file and JSON reference
func (cp *ConfigPersistence) SaveMaintenanceConfig(config *MaintenanceConfig) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save configuration")
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	if err := os.MkdirAll(confDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Save as protobuf (primary format)
	pbConfigPath := filepath.Join(confDir, MaintenanceConfigFile)
	pbData, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal maintenance config to protobuf: %w", err)
	}

	if err := os.WriteFile(pbConfigPath, pbData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write protobuf config file: %w", err)
	}

	// Save JSON reference copy for debugging
	jsonConfigPath := filepath.Join(confDir, MaintenanceConfigJSONFile)
	jsonData, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal maintenance config to JSON: %w", err)
	}

	if err := os.WriteFile(jsonConfigPath, jsonData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write JSON reference file: %w", err)
	}

	return nil
}

// LoadMaintenanceConfig loads maintenance configuration from protobuf file
func (cp *ConfigPersistence) LoadMaintenanceConfig() (*MaintenanceConfig, error) {
	if cp.dataDir == "" {
		return DefaultMaintenanceConfig(), nil
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, MaintenanceConfigFile)

	// Try to load from protobuf file
	if configData, err := os.ReadFile(configPath); err == nil {
		var config MaintenanceConfig
		if err := proto.Unmarshal(configData, &config); err == nil {
			// Always populate policy from separate task configuration files
			config.Policy = buildPolicyFromTaskConfigs()
			return &config, nil
		}
	}

	// File doesn't exist or failed to load, use defaults
	return DefaultMaintenanceConfig(), nil
}

// GetConfigPath returns the path to a configuration file
func (cp *ConfigPersistence) GetConfigPath(filename string) string {
	if cp.dataDir == "" {
		return ""
	}

	// All configs go in conf subdirectory
	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	return filepath.Join(confDir, filename)
}

// ListConfigFiles returns all configuration files in the conf subdirectory
func (cp *ConfigPersistence) ListConfigFiles() ([]string, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified")
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	files, err := os.ReadDir(confDir)
	if err != nil {
		// If conf directory doesn't exist, return empty list
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read config directory: %w", err)
	}

	var configFiles []string
	for _, file := range files {
		if !file.IsDir() {
			ext := filepath.Ext(file.Name())
			if ext == ".json" || ext == ".pb" {
				configFiles = append(configFiles, file.Name())
			}
		}
	}

	return configFiles, nil
}

// BackupConfig creates a backup of a configuration file
func (cp *ConfigPersistence) BackupConfig(filename string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified")
	}

	configPath := cp.GetConfigPath(filename)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("config file does not exist: %s", filename)
	}

	// Create backup filename with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	backupName := fmt.Sprintf("%s.backup_%s", filename, timestamp)

	// Determine backup directory (conf subdirectory)
	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	backupPath := filepath.Join(confDir, backupName)

	// Copy file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := os.WriteFile(backupPath, configData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	glog.V(1).Infof("Created backup of %s as %s", filename, backupName)
	return nil
}

// RestoreConfig restores a configuration file from a backup
func (cp *ConfigPersistence) RestoreConfig(filename, backupName string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified")
	}

	// Determine backup path (conf subdirectory)
	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	backupPath := filepath.Join(confDir, backupName)

	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist: %s", backupName)
	}

	// Read backup file
	backupData, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	// Write to config file
	configPath := cp.GetConfigPath(filename)
	if err := os.WriteFile(configPath, backupData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to restore config: %w", err)
	}

	glog.V(1).Infof("Restored %s from backup %s", filename, backupName)
	return nil
}

// SaveVacuumTaskConfig saves vacuum task configuration to protobuf file
func (cp *ConfigPersistence) SaveVacuumTaskConfig(config *VacuumTaskConfig) error {
	return cp.saveTaskConfig(VacuumTaskConfigFile, config)
}

// SaveVacuumTaskPolicy saves complete vacuum task policy to protobuf file
func (cp *ConfigPersistence) SaveVacuumTaskPolicy(policy *worker_pb.TaskPolicy) error {
	return cp.saveTaskConfig(VacuumTaskConfigFile, policy)
}

// LoadVacuumTaskConfig loads vacuum task configuration from protobuf file
func (cp *ConfigPersistence) LoadVacuumTaskConfig() (*VacuumTaskConfig, error) {
	// Load as TaskPolicy and extract vacuum config
	if taskPolicy, err := cp.LoadVacuumTaskPolicy(); err == nil && taskPolicy != nil {
		if vacuumConfig := taskPolicy.GetVacuumConfig(); vacuumConfig != nil {
			return vacuumConfig, nil
		}
	}

	// Return default config if no valid config found
	return &VacuumTaskConfig{
		GarbageThreshold:   0.3,
		MinVolumeAgeHours:  24,
		MinIntervalSeconds: 7 * 24 * 60 * 60, // 7 days
	}, nil
}

// LoadVacuumTaskPolicy loads complete vacuum task policy from protobuf file
func (cp *ConfigPersistence) LoadVacuumTaskPolicy() (*worker_pb.TaskPolicy, error) {
	if cp.dataDir == "" {
		// Return default policy if no data directory
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         2,
			RepeatIntervalSeconds: 24 * 3600, // 24 hours in seconds
			CheckIntervalSeconds:  6 * 3600,  // 6 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
				VacuumConfig: &worker_pb.VacuumTaskConfig{
					GarbageThreshold:   0.3,
					MinVolumeAgeHours:  24,
					MinIntervalSeconds: 7 * 24 * 60 * 60, // 7 days
				},
			},
		}, nil
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, VacuumTaskConfigFile)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Return default policy if file doesn't exist
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         2,
			RepeatIntervalSeconds: 24 * 3600, // 24 hours in seconds
			CheckIntervalSeconds:  6 * 3600,  // 6 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
				VacuumConfig: &worker_pb.VacuumTaskConfig{
					GarbageThreshold:   0.3,
					MinVolumeAgeHours:  24,
					MinIntervalSeconds: 7 * 24 * 60 * 60, // 7 days
				},
			},
		}, nil
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read vacuum task config file: %w", err)
	}

	// Try to unmarshal as TaskPolicy
	var policy worker_pb.TaskPolicy
	if err := proto.Unmarshal(configData, &policy); err == nil {
		// Validate that it's actually a TaskPolicy with vacuum config
		if policy.GetVacuumConfig() != nil {
			glog.V(1).Infof("Loaded vacuum task policy from %s", configPath)
			return &policy, nil
		}
	}

	return nil, fmt.Errorf("failed to unmarshal vacuum task configuration")
}

// SaveErasureCodingTaskConfig saves EC task configuration to protobuf file
func (cp *ConfigPersistence) SaveErasureCodingTaskConfig(config *ErasureCodingTaskConfig) error {
	return cp.saveTaskConfig(ECTaskConfigFile, config)
}

// SaveErasureCodingTaskPolicy saves complete EC task policy to protobuf file
func (cp *ConfigPersistence) SaveErasureCodingTaskPolicy(policy *worker_pb.TaskPolicy) error {
	return cp.saveTaskConfig(ECTaskConfigFile, policy)
}

// LoadErasureCodingTaskConfig loads EC task configuration from protobuf file
func (cp *ConfigPersistence) LoadErasureCodingTaskConfig() (*ErasureCodingTaskConfig, error) {
	// Load as TaskPolicy and extract EC config
	if taskPolicy, err := cp.LoadErasureCodingTaskPolicy(); err == nil && taskPolicy != nil {
		if ecConfig := taskPolicy.GetErasureCodingConfig(); ecConfig != nil {
			return ecConfig, nil
		}
	}

	// Return default config if no valid config found
	return &ErasureCodingTaskConfig{
		FullnessRatio:    0.9,
		QuietForSeconds:  3600,
		MinVolumeSizeMb:  1024,
		CollectionFilter: "",
	}, nil
}

// LoadErasureCodingTaskPolicy loads complete EC task policy from protobuf file
func (cp *ConfigPersistence) LoadErasureCodingTaskPolicy() (*worker_pb.TaskPolicy, error) {
	if cp.dataDir == "" {
		// Return default policy if no data directory
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 168 * 3600, // 1 week in seconds
			CheckIntervalSeconds:  24 * 3600,  // 24 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_ErasureCodingConfig{
				ErasureCodingConfig: &worker_pb.ErasureCodingTaskConfig{
					FullnessRatio:    0.9,
					QuietForSeconds:  3600,
					MinVolumeSizeMb:  1024,
					CollectionFilter: "",
				},
			},
		}, nil
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, ECTaskConfigFile)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Return default policy if file doesn't exist
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 168 * 3600, // 1 week in seconds
			CheckIntervalSeconds:  24 * 3600,  // 24 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_ErasureCodingConfig{
				ErasureCodingConfig: &worker_pb.ErasureCodingTaskConfig{
					FullnessRatio:    0.9,
					QuietForSeconds:  3600,
					MinVolumeSizeMb:  1024,
					CollectionFilter: "",
				},
			},
		}, nil
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read EC task config file: %w", err)
	}

	// Try to unmarshal as TaskPolicy
	var policy worker_pb.TaskPolicy
	if err := proto.Unmarshal(configData, &policy); err == nil {
		// Validate that it's actually a TaskPolicy with EC config
		if policy.GetErasureCodingConfig() != nil {
			glog.V(1).Infof("Loaded EC task policy from %s", configPath)
			return &policy, nil
		}
	}

	return nil, fmt.Errorf("failed to unmarshal EC task configuration")
}

// SaveBalanceTaskConfig saves balance task configuration to protobuf file
func (cp *ConfigPersistence) SaveBalanceTaskConfig(config *BalanceTaskConfig) error {
	return cp.saveTaskConfig(BalanceTaskConfigFile, config)
}

// SaveBalanceTaskPolicy saves complete balance task policy to protobuf file
func (cp *ConfigPersistence) SaveBalanceTaskPolicy(policy *worker_pb.TaskPolicy) error {
	return cp.saveTaskConfig(BalanceTaskConfigFile, policy)
}

// LoadBalanceTaskConfig loads balance task configuration from protobuf file
func (cp *ConfigPersistence) LoadBalanceTaskConfig() (*BalanceTaskConfig, error) {
	// Load as TaskPolicy and extract balance config
	if taskPolicy, err := cp.LoadBalanceTaskPolicy(); err == nil && taskPolicy != nil {
		if balanceConfig := taskPolicy.GetBalanceConfig(); balanceConfig != nil {
			return balanceConfig, nil
		}
	}

	// Return default config if no valid config found
	return &BalanceTaskConfig{
		ImbalanceThreshold: 0.1,
		MinServerCount:     2,
	}, nil
}

// LoadBalanceTaskPolicy loads complete balance task policy from protobuf file
func (cp *ConfigPersistence) LoadBalanceTaskPolicy() (*worker_pb.TaskPolicy, error) {
	if cp.dataDir == "" {
		// Return default policy if no data directory
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 6 * 3600,  // 6 hours in seconds
			CheckIntervalSeconds:  12 * 3600, // 12 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
				BalanceConfig: &worker_pb.BalanceTaskConfig{
					ImbalanceThreshold: 0.1,
					MinServerCount:     2,
				},
			},
		}, nil
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, BalanceTaskConfigFile)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Return default policy if file doesn't exist
		return &worker_pb.TaskPolicy{
			Enabled:               true,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 6 * 3600,  // 6 hours in seconds
			CheckIntervalSeconds:  12 * 3600, // 12 hours in seconds
			TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
				BalanceConfig: &worker_pb.BalanceTaskConfig{
					ImbalanceThreshold: 0.1,
					MinServerCount:     2,
				},
			},
		}, nil
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read balance task config file: %w", err)
	}

	// Try to unmarshal as TaskPolicy
	var policy worker_pb.TaskPolicy
	if err := proto.Unmarshal(configData, &policy); err == nil {
		// Validate that it's actually a TaskPolicy with balance config
		if policy.GetBalanceConfig() != nil {
			glog.V(1).Infof("Loaded balance task policy from %s", configPath)
			return &policy, nil
		}
	}

	return nil, fmt.Errorf("failed to unmarshal balance task configuration")
}

// SaveReplicationTaskConfig saves replication task configuration to protobuf file
func (cp *ConfigPersistence) SaveReplicationTaskConfig(config *ReplicationTaskConfig) error {
	return cp.saveTaskConfig(ReplicationTaskConfigFile, config)
}

// LoadReplicationTaskConfig loads replication task configuration from protobuf file
func (cp *ConfigPersistence) LoadReplicationTaskConfig() (*ReplicationTaskConfig, error) {
	var config ReplicationTaskConfig
	err := cp.loadTaskConfig(ReplicationTaskConfigFile, &config)
	if err != nil {
		// Return default config if file doesn't exist
		if os.IsNotExist(err) {
			return &ReplicationTaskConfig{
				TargetReplicaCount: 1,
			}, nil
		}
		return nil, err
	}
	return &config, nil
}

// saveTaskConfig is a generic helper for saving task configurations with both protobuf and JSON reference
func (cp *ConfigPersistence) saveTaskConfig(filename string, config proto.Message) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save task configuration")
	}

	// Create conf subdirectory path
	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, filename)

	// Generate JSON reference filename
	jsonFilename := filename[:len(filename)-3] + ".json" // Replace .pb with .json
	jsonPath := filepath.Join(confDir, jsonFilename)

	// Create conf directory if it doesn't exist
	if err := os.MkdirAll(confDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal configuration to protobuf binary format
	configData, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal task config: %w", err)
	}

	// Write protobuf file
	if err := os.WriteFile(configPath, configData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write task config file: %w", err)
	}

	// Marshal configuration to JSON for reference
	marshaler := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}
	jsonData, err := marshaler.Marshal(config)
	if err != nil {
		glog.Warningf("Failed to marshal task config to JSON reference: %v", err)
	} else {
		// Write JSON reference file
		if err := os.WriteFile(jsonPath, jsonData, ConfigFilePermissions); err != nil {
			glog.Warningf("Failed to write task config JSON reference: %v", err)
		}
	}

	glog.V(1).Infof("Saved task configuration to %s (with JSON reference)", configPath)
	return nil
}

// loadTaskConfig is a generic helper for loading task configurations from conf subdirectory
func (cp *ConfigPersistence) loadTaskConfig(filename string, config proto.Message) error {
	if cp.dataDir == "" {
		return os.ErrNotExist // Will trigger default config return
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, filename)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return err // Will trigger default config return
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read task config file: %w", err)
	}

	// Unmarshal protobuf binary data
	if err := proto.Unmarshal(configData, config); err != nil {
		return fmt.Errorf("failed to unmarshal task config: %w", err)
	}

	glog.V(1).Infof("Loaded task configuration from %s", configPath)
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
		"config_subdir":       ConfigSubdir,
	}

	if cp.IsConfigured() {
		// Check if data directory exists
		if _, err := os.Stat(cp.dataDir); err == nil {
			info["data_dir_exists"] = true

			// Check if conf subdirectory exists
			confDir := filepath.Join(cp.dataDir, ConfigSubdir)
			if _, err := os.Stat(confDir); err == nil {
				info["conf_dir_exists"] = true

				// List config files
				configFiles, err := cp.ListConfigFiles()
				if err == nil {
					info["config_files"] = configFiles
				}
			} else {
				info["conf_dir_exists"] = false
			}
		} else {
			info["data_dir_exists"] = false
		}
	}

	return info
}

// buildPolicyFromTaskConfigs loads task configurations from separate files and builds a MaintenancePolicy
func buildPolicyFromTaskConfigs() *worker_pb.MaintenancePolicy {
	policy := &worker_pb.MaintenancePolicy{
		GlobalMaxConcurrent:          4,
		DefaultRepeatIntervalSeconds: 6 * 3600,  // 6 hours in seconds
		DefaultCheckIntervalSeconds:  12 * 3600, // 12 hours in seconds
		TaskPolicies:                 make(map[string]*worker_pb.TaskPolicy),
	}

	// Load vacuum task configuration
	if vacuumConfig := vacuum.LoadConfigFromPersistence(nil); vacuumConfig != nil {
		policy.TaskPolicies["vacuum"] = &worker_pb.TaskPolicy{
			Enabled:               vacuumConfig.Enabled,
			MaxConcurrent:         int32(vacuumConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(vacuumConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(vacuumConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
				VacuumConfig: &worker_pb.VacuumTaskConfig{
					GarbageThreshold:   float64(vacuumConfig.GarbageThreshold),
					MinVolumeAgeHours:  int32(vacuumConfig.MinVolumeAgeSeconds / 3600), // Convert seconds to hours
					MinIntervalSeconds: int32(vacuumConfig.MinIntervalSeconds),
				},
			},
		}
	}

	// Load erasure coding task configuration
	if ecConfig := erasure_coding.LoadConfigFromPersistence(nil); ecConfig != nil {
		policy.TaskPolicies["erasure_coding"] = &worker_pb.TaskPolicy{
			Enabled:               ecConfig.Enabled,
			MaxConcurrent:         int32(ecConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(ecConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(ecConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_ErasureCodingConfig{
				ErasureCodingConfig: &worker_pb.ErasureCodingTaskConfig{
					FullnessRatio:    float64(ecConfig.FullnessRatio),
					QuietForSeconds:  int32(ecConfig.QuietForSeconds),
					MinVolumeSizeMb:  int32(ecConfig.MinSizeMB),
					CollectionFilter: ecConfig.CollectionFilter,
				},
			},
		}
	}

	// Load balance task configuration
	if balanceConfig := balance.LoadConfigFromPersistence(nil); balanceConfig != nil {
		policy.TaskPolicies["balance"] = &worker_pb.TaskPolicy{
			Enabled:               balanceConfig.Enabled,
			MaxConcurrent:         int32(balanceConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(balanceConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(balanceConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
				BalanceConfig: &worker_pb.BalanceTaskConfig{
					ImbalanceThreshold: float64(balanceConfig.ImbalanceThreshold),
					MinServerCount:     int32(balanceConfig.MinServerCount),
				},
			},
		}
	}

	glog.V(1).Infof("Built maintenance policy from separate task configs - %d task policies loaded", len(policy.TaskPolicies))
	return policy
}
