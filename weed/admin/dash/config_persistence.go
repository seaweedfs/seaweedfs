package dash

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// Configuration subdirectory
	ConfigSubdir = "conf"

	// Configuration file names (protobuf binary)
	MaintenanceConfigFile = "maintenance.pb"

	// JSON reference files
	MaintenanceConfigJSONFile = "maintenance.json"

	// Task persistence subdirectories and settings
	TasksSubdir       = "tasks"
	TaskDetailsSubdir = "task_details"
	TaskLogsSubdir    = "task_logs"
	MaxCompletedTasks = 10 // Only keep last 10 completed tasks

	ConfigDirPermissions  = 0755
	ConfigFilePermissions = 0644
)

// isValidTaskID validates that a task ID is safe for use in file paths
// This prevents path traversal attacks by ensuring the task ID doesn't contain
// path separators or parent directory references
func isValidTaskID(taskID string) bool {
	if taskID == "" {
		return false
	}

	// Reject task IDs with leading or trailing whitespace
	if strings.TrimSpace(taskID) != taskID {
		return false
	}

	// Check for path traversal patterns
	if strings.Contains(taskID, "/") ||
		strings.Contains(taskID, "\\") ||
		strings.Contains(taskID, "..") ||
		strings.Contains(taskID, ":") {
		return false
	}

	// Additional safety: ensure it's not just dots or empty after trim
	if taskID == "." || taskID == ".." {
		return false
	}

	return true
}

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

// SaveTaskPolicyGeneric saves a task policy for any task type dynamically
func (cp *ConfigPersistence) SaveTaskPolicyGeneric(taskType string, policy *worker_pb.TaskPolicy) error {
	filename := fmt.Sprintf("task_%s.pb", taskType)
	return cp.saveTaskConfig(filename, policy)
}

// LoadTaskPolicyGeneric loads a task policy for any task type dynamically
func (cp *ConfigPersistence) LoadTaskPolicyGeneric(taskType string) (*worker_pb.TaskPolicy, error) {
	filename := fmt.Sprintf("task_%s.pb", taskType)

	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory configured")
	}

	confDir := filepath.Join(cp.dataDir, ConfigSubdir)
	configPath := filepath.Join(confDir, filename)

	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("no configuration found for task type: %s", taskType)
	}

	// Read file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read task config file: %w", err)
	}

	// Unmarshal as TaskPolicy
	var policy worker_pb.TaskPolicy
	if err := proto.Unmarshal(configData, &policy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task configuration: %w", err)
	}

	glog.V(1).Infof("Loaded task policy for %s from %s", taskType, configPath)
	return &policy, nil
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

// SaveTaskDetail saves detailed task information to disk
func (cp *ConfigPersistence) SaveTaskDetail(taskID string, detail *maintenance.TaskDetailData) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save task detail")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskDetailDir := filepath.Join(cp.dataDir, TaskDetailsSubdir)
	if err := os.MkdirAll(taskDetailDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create task details directory: %w", err)
	}

	// Save task detail as JSON for easy reading and debugging
	taskDetailPath := filepath.Join(taskDetailDir, fmt.Sprintf("%s.json", taskID))
	jsonData, err := json.MarshalIndent(detail, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal task detail to JSON: %w", err)
	}

	if err := os.WriteFile(taskDetailPath, jsonData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write task detail file: %w", err)
	}

	glog.V(2).Infof("Saved task detail for task %s to %s", taskID, taskDetailPath)
	return nil
}

// LoadTaskDetail loads detailed task information from disk
func (cp *ConfigPersistence) LoadTaskDetail(taskID string) (*maintenance.TaskDetailData, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified, cannot load task detail")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return nil, fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskDetailPath := filepath.Join(cp.dataDir, TaskDetailsSubdir, fmt.Sprintf("%s.json", taskID))
	if _, err := os.Stat(taskDetailPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("task detail file not found: %s", taskID)
	}

	jsonData, err := os.ReadFile(taskDetailPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read task detail file: %w", err)
	}

	var detail maintenance.TaskDetailData
	if err := json.Unmarshal(jsonData, &detail); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task detail JSON: %w", err)
	}

	glog.V(2).Infof("Loaded task detail for task %s from %s", taskID, taskDetailPath)
	return &detail, nil
}

// SaveTaskExecutionLogs saves execution logs for a task
func (cp *ConfigPersistence) SaveTaskExecutionLogs(taskID string, logs []*maintenance.TaskExecutionLog) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save task logs")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskLogsDir := filepath.Join(cp.dataDir, TaskLogsSubdir)
	if err := os.MkdirAll(taskLogsDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create task logs directory: %w", err)
	}

	// Save logs as JSON for easy reading
	taskLogsPath := filepath.Join(taskLogsDir, fmt.Sprintf("%s.json", taskID))
	logsData := struct {
		TaskID string                          `json:"task_id"`
		Logs   []*maintenance.TaskExecutionLog `json:"logs"`
	}{
		TaskID: taskID,
		Logs:   logs,
	}
	jsonData, err := json.MarshalIndent(logsData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal task logs to JSON: %w", err)
	}

	if err := os.WriteFile(taskLogsPath, jsonData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write task logs file: %w", err)
	}

	glog.V(2).Infof("Saved %d execution logs for task %s to %s", len(logs), taskID, taskLogsPath)
	return nil
}

// LoadTaskExecutionLogs loads execution logs for a task
func (cp *ConfigPersistence) LoadTaskExecutionLogs(taskID string) ([]*maintenance.TaskExecutionLog, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified, cannot load task logs")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return nil, fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskLogsPath := filepath.Join(cp.dataDir, TaskLogsSubdir, fmt.Sprintf("%s.json", taskID))
	if _, err := os.Stat(taskLogsPath); os.IsNotExist(err) {
		// Return empty slice if logs don't exist yet
		return []*maintenance.TaskExecutionLog{}, nil
	}

	jsonData, err := os.ReadFile(taskLogsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read task logs file: %w", err)
	}

	var logsData struct {
		TaskID string                          `json:"task_id"`
		Logs   []*maintenance.TaskExecutionLog `json:"logs"`
	}
	if err := json.Unmarshal(jsonData, &logsData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task logs JSON: %w", err)
	}

	glog.V(2).Infof("Loaded %d execution logs for task %s from %s", len(logsData.Logs), taskID, taskLogsPath)
	return logsData.Logs, nil
}

// DeleteTaskDetail removes task detail and logs from disk
func (cp *ConfigPersistence) DeleteTaskDetail(taskID string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot delete task detail")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	// Delete task detail file
	taskDetailPath := filepath.Join(cp.dataDir, TaskDetailsSubdir, fmt.Sprintf("%s.json", taskID))
	if err := os.Remove(taskDetailPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete task detail file: %w", err)
	}

	// Delete task logs file
	taskLogsPath := filepath.Join(cp.dataDir, TaskLogsSubdir, fmt.Sprintf("%s.json", taskID))
	if err := os.Remove(taskLogsPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete task logs file: %w", err)
	}

	glog.V(2).Infof("Deleted task detail and logs for task %s", taskID)
	return nil
}

// ListTaskDetails returns a list of all task IDs that have stored details
func (cp *ConfigPersistence) ListTaskDetails() ([]string, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified, cannot list task details")
	}

	taskDetailDir := filepath.Join(cp.dataDir, TaskDetailsSubdir)
	if _, err := os.Stat(taskDetailDir); os.IsNotExist(err) {
		return []string{}, nil
	}

	entries, err := os.ReadDir(taskDetailDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read task details directory: %w", err)
	}

	var taskIDs []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			taskID := entry.Name()[:len(entry.Name())-5] // Remove .json extension
			taskIDs = append(taskIDs, taskID)
		}
	}

	return taskIDs, nil
}

// CleanupCompletedTasks removes old completed tasks beyond the retention limit
func (cp *ConfigPersistence) CleanupCompletedTasks() error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot cleanup completed tasks")
	}

	tasksDir := filepath.Join(cp.dataDir, TasksSubdir)
	if _, err := os.Stat(tasksDir); os.IsNotExist(err) {
		return nil // No tasks directory, nothing to cleanup
	}

	// Load all tasks and find completed/failed ones
	allTasks, err := cp.LoadAllTaskStates()
	if err != nil {
		return fmt.Errorf("failed to load tasks for cleanup: %w", err)
	}

	// Filter completed and failed tasks, sort by completion time
	var completedTasks []*maintenance.MaintenanceTask
	for _, task := range allTasks {
		if (task.Status == maintenance.TaskStatusCompleted || task.Status == maintenance.TaskStatusFailed) && task.CompletedAt != nil {
			completedTasks = append(completedTasks, task)
		}
	}

	// Sort by completion time (most recent first)
	sort.Slice(completedTasks, func(i, j int) bool {
		return completedTasks[i].CompletedAt.After(*completedTasks[j].CompletedAt)
	})

	// Keep only the most recent MaxCompletedTasks, delete the rest
	if len(completedTasks) > MaxCompletedTasks {
		tasksToDelete := completedTasks[MaxCompletedTasks:]
		for _, task := range tasksToDelete {
			if err := cp.DeleteTaskState(task.ID); err != nil {
				glog.Warningf("Failed to delete old completed task %s: %v", task.ID, err)
			} else {
				glog.V(2).Infof("Cleaned up old completed task %s (completed: %v)", task.ID, task.CompletedAt)
			}
		}
		glog.V(1).Infof("Cleaned up %d old completed tasks (keeping %d most recent)", len(tasksToDelete), MaxCompletedTasks)
	}

	return nil
}

// SaveTaskState saves a task state to protobuf file
func (cp *ConfigPersistence) SaveTaskState(task *maintenance.MaintenanceTask) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot save task state")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(task.ID) {
		return fmt.Errorf("invalid task ID: %q contains illegal path characters", task.ID)
	}

	tasksDir := filepath.Join(cp.dataDir, TasksSubdir)
	if err := os.MkdirAll(tasksDir, ConfigDirPermissions); err != nil {
		return fmt.Errorf("failed to create tasks directory: %w", err)
	}

	taskFilePath := filepath.Join(tasksDir, fmt.Sprintf("%s.pb", task.ID))

	// Convert task to protobuf
	pbTask := cp.maintenanceTaskToProtobuf(task)
	taskStateFile := &worker_pb.TaskStateFile{
		Task:         pbTask,
		LastUpdated:  time.Now().Unix(),
		AdminVersion: "unknown", // TODO: add version info
	}

	pbData, err := proto.Marshal(taskStateFile)
	if err != nil {
		return fmt.Errorf("failed to marshal task state protobuf: %w", err)
	}

	if err := os.WriteFile(taskFilePath, pbData, ConfigFilePermissions); err != nil {
		return fmt.Errorf("failed to write task state file: %w", err)
	}

	glog.V(2).Infof("Saved task state for task %s to %s", task.ID, taskFilePath)
	return nil
}

// LoadTaskState loads a task state from protobuf file
func (cp *ConfigPersistence) LoadTaskState(taskID string) (*maintenance.MaintenanceTask, error) {
	if cp.dataDir == "" {
		return nil, fmt.Errorf("no data directory specified, cannot load task state")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return nil, fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskFilePath := filepath.Join(cp.dataDir, TasksSubdir, fmt.Sprintf("%s.pb", taskID))
	if _, err := os.Stat(taskFilePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("task state file not found: %s", taskID)
	}

	pbData, err := os.ReadFile(taskFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read task state file: %w", err)
	}

	var taskStateFile worker_pb.TaskStateFile
	if err := proto.Unmarshal(pbData, &taskStateFile); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task state protobuf: %w", err)
	}

	// Convert protobuf to maintenance task
	task := cp.protobufToMaintenanceTask(taskStateFile.Task)

	glog.V(2).Infof("Loaded task state for task %s from %s", taskID, taskFilePath)
	return task, nil
}

// LoadAllTaskStates loads all task states from disk
func (cp *ConfigPersistence) LoadAllTaskStates() ([]*maintenance.MaintenanceTask, error) {
	if cp.dataDir == "" {
		return []*maintenance.MaintenanceTask{}, nil
	}

	tasksDir := filepath.Join(cp.dataDir, TasksSubdir)
	if _, err := os.Stat(tasksDir); os.IsNotExist(err) {
		return []*maintenance.MaintenanceTask{}, nil
	}

	entries, err := os.ReadDir(tasksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read tasks directory: %w", err)
	}

	var tasks []*maintenance.MaintenanceTask
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".pb" {
			taskID := entry.Name()[:len(entry.Name())-3] // Remove .pb extension
			task, err := cp.LoadTaskState(taskID)
			if err != nil {
				glog.Warningf("Failed to load task state for %s: %v", taskID, err)
				continue
			}
			tasks = append(tasks, task)
		}
	}

	glog.V(1).Infof("Loaded %d task states from disk", len(tasks))
	return tasks, nil
}

// DeleteTaskState removes a task state file from disk
func (cp *ConfigPersistence) DeleteTaskState(taskID string) error {
	if cp.dataDir == "" {
		return fmt.Errorf("no data directory specified, cannot delete task state")
	}

	// Validate task ID to prevent path traversal
	if !isValidTaskID(taskID) {
		return fmt.Errorf("invalid task ID: %q contains illegal path characters", taskID)
	}

	taskFilePath := filepath.Join(cp.dataDir, TasksSubdir, fmt.Sprintf("%s.pb", taskID))
	if err := os.Remove(taskFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete task state file: %w", err)
	}

	glog.V(2).Infof("Deleted task state for task %s", taskID)
	return nil
}

// maintenanceTaskToProtobuf converts a MaintenanceTask to protobuf format
func (cp *ConfigPersistence) maintenanceTaskToProtobuf(task *maintenance.MaintenanceTask) *worker_pb.MaintenanceTaskData {
	pbTask := &worker_pb.MaintenanceTaskData{
		Id:              task.ID,
		Type:            string(task.Type),
		Priority:        cp.priorityToString(task.Priority),
		Status:          string(task.Status),
		VolumeId:        task.VolumeID,
		Server:          task.Server,
		Collection:      task.Collection,
		Reason:          task.Reason,
		CreatedAt:       task.CreatedAt.Unix(),
		ScheduledAt:     task.ScheduledAt.Unix(),
		WorkerId:        task.WorkerID,
		Error:           task.Error,
		Progress:        task.Progress,
		RetryCount:      int32(task.RetryCount),
		MaxRetries:      int32(task.MaxRetries),
		CreatedBy:       task.CreatedBy,
		CreationContext: task.CreationContext,
		DetailedReason:  task.DetailedReason,
		Tags:            task.Tags,
	}

	// Handle optional timestamps
	if task.StartedAt != nil {
		pbTask.StartedAt = task.StartedAt.Unix()
	}
	if task.CompletedAt != nil {
		pbTask.CompletedAt = task.CompletedAt.Unix()
	}

	// Convert assignment history
	if task.AssignmentHistory != nil {
		for _, record := range task.AssignmentHistory {
			pbRecord := &worker_pb.TaskAssignmentRecord{
				WorkerId:      record.WorkerID,
				WorkerAddress: record.WorkerAddress,
				AssignedAt:    record.AssignedAt.Unix(),
				Reason:        record.Reason,
			}
			if record.UnassignedAt != nil {
				pbRecord.UnassignedAt = record.UnassignedAt.Unix()
			}
			pbTask.AssignmentHistory = append(pbTask.AssignmentHistory, pbRecord)
		}
	}

	// Convert typed parameters if available
	if task.TypedParams != nil {
		pbTask.TypedParams = task.TypedParams
	}

	return pbTask
}

// protobufToMaintenanceTask converts protobuf format to MaintenanceTask
func (cp *ConfigPersistence) protobufToMaintenanceTask(pbTask *worker_pb.MaintenanceTaskData) *maintenance.MaintenanceTask {
	task := &maintenance.MaintenanceTask{
		ID:              pbTask.Id,
		Type:            maintenance.MaintenanceTaskType(pbTask.Type),
		Priority:        cp.stringToPriority(pbTask.Priority),
		Status:          maintenance.MaintenanceTaskStatus(pbTask.Status),
		VolumeID:        pbTask.VolumeId,
		Server:          pbTask.Server,
		Collection:      pbTask.Collection,
		Reason:          pbTask.Reason,
		CreatedAt:       time.Unix(pbTask.CreatedAt, 0),
		ScheduledAt:     time.Unix(pbTask.ScheduledAt, 0),
		WorkerID:        pbTask.WorkerId,
		Error:           pbTask.Error,
		Progress:        pbTask.Progress,
		RetryCount:      int(pbTask.RetryCount),
		MaxRetries:      int(pbTask.MaxRetries),
		CreatedBy:       pbTask.CreatedBy,
		CreationContext: pbTask.CreationContext,
		DetailedReason:  pbTask.DetailedReason,
		Tags:            pbTask.Tags,
	}

	// Handle optional timestamps
	if pbTask.StartedAt > 0 {
		startTime := time.Unix(pbTask.StartedAt, 0)
		task.StartedAt = &startTime
	}
	if pbTask.CompletedAt > 0 {
		completedTime := time.Unix(pbTask.CompletedAt, 0)
		task.CompletedAt = &completedTime
	}

	// Convert assignment history
	if pbTask.AssignmentHistory != nil {
		task.AssignmentHistory = make([]*maintenance.TaskAssignmentRecord, 0, len(pbTask.AssignmentHistory))
		for _, pbRecord := range pbTask.AssignmentHistory {
			record := &maintenance.TaskAssignmentRecord{
				WorkerID:      pbRecord.WorkerId,
				WorkerAddress: pbRecord.WorkerAddress,
				AssignedAt:    time.Unix(pbRecord.AssignedAt, 0),
				Reason:        pbRecord.Reason,
			}
			if pbRecord.UnassignedAt > 0 {
				unassignedTime := time.Unix(pbRecord.UnassignedAt, 0)
				record.UnassignedAt = &unassignedTime
			}
			task.AssignmentHistory = append(task.AssignmentHistory, record)
		}
	}

	// Convert typed parameters if available
	if pbTask.TypedParams != nil {
		task.TypedParams = pbTask.TypedParams
	}

	return task
}

// priorityToString converts MaintenanceTaskPriority to string for protobuf storage
func (cp *ConfigPersistence) priorityToString(priority maintenance.MaintenanceTaskPriority) string {
	switch priority {
	case maintenance.PriorityLow:
		return "low"
	case maintenance.PriorityNormal:
		return "normal"
	case maintenance.PriorityHigh:
		return "high"
	case maintenance.PriorityCritical:
		return "critical"
	default:
		return "normal"
	}
}

// stringToPriority converts string from protobuf to MaintenanceTaskPriority
func (cp *ConfigPersistence) stringToPriority(priorityStr string) maintenance.MaintenanceTaskPriority {
	switch priorityStr {
	case "low":
		return maintenance.PriorityLow
	case "normal":
		return maintenance.PriorityNormal
	case "high":
		return maintenance.PriorityHigh
	case "critical":
		return maintenance.PriorityCritical
	default:
		return maintenance.PriorityNormal
	}
}
