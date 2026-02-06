package tasks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TaskLogger provides file-based logging for individual tasks
type TaskLogger interface {
	// Log methods
	Info(message string, args ...interface{})
	Warning(message string, args ...interface{})
	Error(message string, args ...interface{})
	Debug(message string, args ...interface{})

	// Progress and status logging
	LogProgress(progress float64, message string)
	LogStatus(status string, message string)

	// Structured logging
	LogWithFields(level string, message string, fields map[string]interface{})

	// Lifecycle
	Sync() error
	Close() error
	GetLogDir() string
}

// LoggerProvider interface for tasks that support logging
type LoggerProvider interface {
	InitializeTaskLogger(taskID string, workerID string, params types.TaskParams) error
	GetTaskLogger() TaskLogger
}

// TaskLoggerConfig holds configuration for task logging
type TaskLoggerConfig struct {
	BaseLogDir    string
	MaxTasks      int  // Maximum number of task logs to keep
	MaxLogSizeMB  int  // Maximum log file size in MB
	EnableConsole bool // Also log to console
}

// FileTaskLogger implements TaskLogger using file-based logging
type FileTaskLogger struct {
	taskID   string
	taskType types.TaskType
	workerID string
	logDir   string
	logFile  *os.File
	mutex    sync.Mutex
	config   TaskLoggerConfig
	metadata *TaskLogMetadata
	closed   bool
}

// TaskLogMetadata contains metadata about the task execution
type TaskLogMetadata struct {
	TaskID      string                 `json:"task_id"`
	TaskType    string                 `json:"task_type"`
	WorkerID    string                 `json:"worker_id"`
	StartTime   time.Time              `json:"start_time"`
	EndTime     *time.Time             `json:"end_time,omitempty"`
	Duration    *time.Duration         `json:"duration,omitempty"`
	Status      string                 `json:"status"`
	Progress    float64                `json:"progress"`
	VolumeID    uint32                 `json:"volume_id,omitempty"`
	Server      string                 `json:"server,omitempty"`
	Collection  string                 `json:"collection,omitempty"`
	CustomData  map[string]interface{} `json:"custom_data,omitempty"`
	LogFilePath string                 `json:"log_file_path"`
	CreatedAt   time.Time              `json:"created_at"`
}

// TaskLogEntry represents a single log entry
type TaskLogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Progress  *float64               `json:"progress,omitempty"`
	Status    *string                `json:"status,omitempty"`
}

// DefaultTaskLoggerConfig returns default configuration
func DefaultTaskLoggerConfig() TaskLoggerConfig {
	return TaskLoggerConfig{
		BaseLogDir:    "/data/task_logs", // Use persistent data directory
		MaxTasks:      100,               // Keep last 100 task logs
		MaxLogSizeMB:  10,
		EnableConsole: true,
	}
}

// NewTaskLogger creates a new file-based task logger
func NewTaskLogger(taskID string, taskType types.TaskType, workerID string, params types.TaskParams, config TaskLoggerConfig) (TaskLogger, error) {
	// Create unique directory name with timestamp
	timestamp := time.Now().Format("20060102_150405")
	dirName := fmt.Sprintf("%s_%s_%s_%s", taskID, taskType, workerID, timestamp)
	logDir := filepath.Join(config.BaseLogDir, dirName)

	// Create log directory
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	// Create log file
	logFilePath := filepath.Join(logDir, "task.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file %s: %w", logFilePath, err)
	}

	// Create metadata
	metadata := &TaskLogMetadata{
		TaskID:      taskID,
		TaskType:    string(taskType),
		WorkerID:    workerID,
		StartTime:   time.Now(),
		Status:      "started",
		Progress:    0.0,
		VolumeID:    params.VolumeID,
		Server:      getServerFromSources(params.TypedParams.Sources),
		Collection:  params.Collection,
		CustomData:  make(map[string]interface{}),
		LogFilePath: logFilePath,
		CreatedAt:   time.Now(),
	}

	logger := &FileTaskLogger{
		taskID:   taskID,
		taskType: taskType,
		workerID: workerID,
		logDir:   logDir,
		logFile:  logFile,
		config:   config,
		metadata: metadata,
		closed:   false,
	}

	// Write initial log entry
	logger.Info("Task logger initialized for %s (type: %s, worker: %s)", taskID, taskType, workerID)
	logger.LogWithFields("INFO", "Task parameters", map[string]interface{}{
		"volume_id":  params.VolumeID,
		"server":     getServerFromSources(params.TypedParams.Sources),
		"collection": params.Collection,
	})

	// Save initial metadata
	if err := logger.saveMetadata(); err != nil {
		glog.Warningf("Failed to save initial task metadata: %v", err)
	}

	// Clean up old task logs
	go logger.cleanupOldLogs()

	return logger, nil
}

// Info logs an info message
func (l *FileTaskLogger) Info(message string, args ...interface{}) {
	l.log("INFO", message, args...)
}

// Warning logs a warning message
func (l *FileTaskLogger) Warning(message string, args ...interface{}) {
	l.log("WARNING", message, args...)
}

// Error logs an error message
func (l *FileTaskLogger) Error(message string, args ...interface{}) {
	l.log("ERROR", message, args...)
}

// Debug logs a debug message
func (l *FileTaskLogger) Debug(message string, args ...interface{}) {
	l.log("DEBUG", message, args...)
}

// LogProgress logs task progress
func (l *FileTaskLogger) LogProgress(progress float64, message string) {
	l.mutex.Lock()
	l.metadata.Progress = progress
	l.mutex.Unlock()

	entry := TaskLogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   message,
		Progress:  &progress,
	}

	l.writeLogEntry(entry)
	l.saveMetadata() // Update metadata with new progress
}

// LogStatus logs task status change
func (l *FileTaskLogger) LogStatus(status string, message string) {
	l.mutex.Lock()
	l.metadata.Status = status
	l.mutex.Unlock()

	entry := TaskLogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   message,
		Status:    &status,
	}

	l.writeLogEntry(entry)
	l.saveMetadata() // Update metadata with new status
}

// LogWithFields logs a message with structured fields
func (l *FileTaskLogger) LogWithFields(level string, message string, fields map[string]interface{}) {
	entry := TaskLogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Fields:    fields,
	}

	l.writeLogEntry(entry)
}

// Sync flushes buffered data to disk
func (l *FileTaskLogger) Sync() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.logFile != nil {
		return l.logFile.Sync()
	}
	return nil
}

// Close closes the logger and finalizes metadata
func (l *FileTaskLogger) Close() error {
	l.Info("Task logger closed for %s", l.taskID)
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return nil
	}

	// Finalize metadata
	endTime := time.Now()
	duration := endTime.Sub(l.metadata.StartTime)
	l.metadata.EndTime = &endTime
	l.metadata.Duration = &duration

	if l.metadata.Status == "started" {
		l.metadata.Status = "completed"
	}

	// Save final metadata
	l.saveMetadata()

	// Close log file
	if l.logFile != nil {
		if err := l.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
	}

	l.closed = true

	return nil
}

// GetLogDir returns the log directory path
func (l *FileTaskLogger) GetLogDir() string {
	return l.logDir
}

// log is the internal logging method
func (l *FileTaskLogger) log(level string, message string, args ...interface{}) {
	formattedMessage := fmt.Sprintf(message, args...)

	entry := TaskLogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   formattedMessage,
	}

	l.writeLogEntry(entry)
}

// writeLogEntry writes a log entry to the file
func (l *FileTaskLogger) writeLogEntry(entry TaskLogEntry) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed || l.logFile == nil {
		return
	}

	// Format as JSON line
	jsonData, err := json.Marshal(entry)
	if err != nil {
		glog.Errorf("Failed to marshal log entry: %v", err)
		return
	}

	// Write to file
	if _, err := l.logFile.WriteString(string(jsonData) + "\n"); err != nil {
		glog.Errorf("Failed to write log entry: %v", err)
		return
	}

	// Flush to disk
	if err := l.logFile.Sync(); err != nil {
		glog.Errorf("Failed to sync log file: %v", err)
	}

	// Also log to console and stderr if enabled
	if l.config.EnableConsole {
		// Log to glog with proper call depth to show actual source location
		// We need depth 3 to skip: writeLogEntry -> log -> Info/Warning/Error calls to reach the original caller
		formattedMsg := fmt.Sprintf("[TASK-%s] %s: %s", l.taskID, entry.Level, entry.Message)
		switch entry.Level {
		case "ERROR":
			glog.ErrorDepth(3, formattedMsg)
		case "WARNING":
			glog.WarningDepth(3, formattedMsg)
		default: // INFO, DEBUG, etc.
			glog.InfoDepth(3, formattedMsg)
		}
		// Also log to stderr for immediate visibility
		fmt.Fprintf(os.Stderr, "[TASK-%s] %s: %s\n", l.taskID, entry.Level, entry.Message)
	}
}

// saveMetadata saves task metadata to file
func (l *FileTaskLogger) saveMetadata() error {
	metadataPath := filepath.Join(l.logDir, "metadata.json")

	data, err := json.MarshalIndent(l.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return os.WriteFile(metadataPath, data, 0644)
}

// cleanupOldLogs removes old task log directories to maintain the limit
func (l *FileTaskLogger) cleanupOldLogs() {
	baseDir := l.config.BaseLogDir

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		glog.Warningf("Failed to read log directory %s: %v", baseDir, err)
		return
	}

	// Filter for directories only
	var dirs []os.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry)
		}
	}

	// If we're under the limit, nothing to clean
	if len(dirs) <= l.config.MaxTasks {
		return
	}

	// Sort by modification time (oldest first)
	sort.Slice(dirs, func(i, j int) bool {
		infoI, errI := dirs[i].Info()
		infoJ, errJ := dirs[j].Info()
		if errI != nil || errJ != nil {
			return false
		}
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	// Remove oldest directories
	numToRemove := len(dirs) - l.config.MaxTasks
	for i := 0; i < numToRemove; i++ {
		dirPath := filepath.Join(baseDir, dirs[i].Name())
		if err := os.RemoveAll(dirPath); err != nil {
			glog.Warningf("Failed to remove old log directory %s: %v", dirPath, err)
		} else {
			glog.V(1).Infof("Cleaned up old task log directory: %s", dirPath)
		}
	}

	glog.V(1).Infof("Task log cleanup completed: removed %d old directories", numToRemove)
}

// GetTaskLogMetadata reads metadata from a task log directory
func GetTaskLogMetadata(logDir string) (*TaskLogMetadata, error) {
	metadataPath := filepath.Join(logDir, "metadata.json")

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata TaskLogMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// ReadTaskLogs reads all log entries from a task log file
func ReadTaskLogs(logDir string) ([]TaskLogEntry, error) {
	logPath := filepath.Join(logDir, "task.log")

	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	var entries []TaskLogEntry
	decoder := json.NewDecoder(file)

	for {
		var entry TaskLogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			// If we fail to decode an entry, it might be a partial write at the end of the file
			// Return what we have so far instead of failing the entire request
			glog.V(1).Infof("Failed to decode log entry in %s: %v (returning %d partial logs)", logPath, err, len(entries))
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
