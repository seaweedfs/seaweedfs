package tasks

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// TaskLogHandler handles task log requests from admin server
type TaskLogHandler struct {
	baseLogDir string
}

// NewTaskLogHandler creates a new task log handler
func NewTaskLogHandler(baseLogDir string) *TaskLogHandler {
	if baseLogDir == "" {
		baseLogDir = "/tmp/seaweedfs/task_logs"
	}
	// Best-effort ensure the base directory exists so reads don't fail due to missing dir
	if err := os.MkdirAll(baseLogDir, 0755); err != nil {
		glog.Warningf("Failed to create base task log directory %s: %v", baseLogDir, err)
	}
	return &TaskLogHandler{
		baseLogDir: baseLogDir,
	}
}

// HandleLogRequest processes a task log request and returns the response
func (h *TaskLogHandler) HandleLogRequest(request *worker_pb.TaskLogRequest) *worker_pb.TaskLogResponse {
	response := &worker_pb.TaskLogResponse{
		TaskId:   request.TaskId,
		WorkerId: request.WorkerId,
		Success:  false,
	}

	// Find the task log directory
	logDir, err := h.findTaskLogDirectory(request.TaskId)
	if err != nil {
		response.ErrorMessage = fmt.Sprintf("Task log directory not found: %v", err)
		glog.Warningf("Task log request failed for %s: %v", request.TaskId, err)

		// Add diagnostic information to help debug the issue
		response.LogEntries = []*worker_pb.TaskLogEntry{
			{
				Timestamp: time.Now().Unix(),
				Level:     "WARNING",
				Message:   fmt.Sprintf("Task logs not available: %v", err),
				Fields:    map[string]string{"source": "task_log_handler"},
			},
			{
				Timestamp: time.Now().Unix(),
				Level:     "INFO",
				Message:   fmt.Sprintf("This usually means the task was never executed on this worker or logs were cleaned up. Base log directory: %s", h.baseLogDir),
				Fields:    map[string]string{"source": "task_log_handler"},
			},
		}
		// response.Success remains false to indicate logs were not found
		return response
	}

	// Read metadata if requested
	if request.IncludeMetadata {
		metadata, err := h.readTaskMetadata(logDir)
		if err != nil {
			response.ErrorMessage = fmt.Sprintf("Failed to read task metadata: %v", err)
			glog.Warningf("Failed to read metadata for task %s: %v", request.TaskId, err)
			return response
		}
		response.Metadata = metadata
	}

	// Read log entries
	logEntries, err := h.readTaskLogEntries(logDir, request)
	if err != nil {
		response.ErrorMessage = fmt.Sprintf("Failed to read task logs: %v", err)
		glog.Warningf("Failed to read logs for task %s: %v", request.TaskId, err)
		return response
	}

	response.LogEntries = logEntries
	response.Success = true

	glog.V(1).Infof("Successfully retrieved %d log entries for task %s", len(logEntries), request.TaskId)
	return response
}

// findTaskLogDirectory searches for the task log directory by task ID
func (h *TaskLogHandler) findTaskLogDirectory(taskID string) (string, error) {
	entries, err := os.ReadDir(h.baseLogDir)
	if err != nil {
		return "", fmt.Errorf("failed to read base log directory %s: %w", h.baseLogDir, err)
	}

	// Look for directories that start with the task ID
	var candidateDirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			candidateDirs = append(candidateDirs, entry.Name())
			if strings.HasPrefix(entry.Name(), taskID+"_") {
				return filepath.Join(h.baseLogDir, entry.Name()), nil
			}
		}
	}

	// Enhanced error message with diagnostic information
	return "", fmt.Errorf("task log directory not found for task ID: %s (searched %d directories in %s, directories found: %v)",
		taskID, len(candidateDirs), h.baseLogDir, candidateDirs)
}

// readTaskMetadata reads task metadata from the log directory
func (h *TaskLogHandler) readTaskMetadata(logDir string) (*worker_pb.TaskLogMetadata, error) {
	metadata, err := GetTaskLogMetadata(logDir)
	if err != nil {
		return nil, err
	}

	// Convert to protobuf metadata
	pbMetadata := &worker_pb.TaskLogMetadata{
		TaskId:      metadata.TaskID,
		TaskType:    metadata.TaskType,
		WorkerId:    metadata.WorkerID,
		StartTime:   metadata.StartTime.Unix(),
		Status:      metadata.Status,
		Progress:    float32(metadata.Progress),
		VolumeId:    metadata.VolumeID,
		Server:      metadata.Server,
		Collection:  metadata.Collection,
		LogFilePath: metadata.LogFilePath,
		CreatedAt:   metadata.CreatedAt.Unix(),
		CustomData:  make(map[string]string),
	}

	// Set end time and duration if available
	if metadata.EndTime != nil {
		pbMetadata.EndTime = metadata.EndTime.Unix()
	}
	if metadata.Duration != nil {
		pbMetadata.DurationMs = metadata.Duration.Milliseconds()
	}

	// Convert custom data
	for key, value := range metadata.CustomData {
		if strValue, ok := value.(string); ok {
			pbMetadata.CustomData[key] = strValue
		} else {
			pbMetadata.CustomData[key] = fmt.Sprintf("%v", value)
		}
	}

	return pbMetadata, nil
}

// readTaskLogEntries reads and filters log entries based on the request
func (h *TaskLogHandler) readTaskLogEntries(logDir string, request *worker_pb.TaskLogRequest) ([]*worker_pb.TaskLogEntry, error) {
	entries, err := ReadTaskLogs(logDir)
	if err != nil {
		return nil, err
	}

	// Apply filters
	var filteredEntries []TaskLogEntry

	for _, entry := range entries {
		// Filter by log level
		if request.LogLevel != "" && !strings.EqualFold(entry.Level, request.LogLevel) {
			continue
		}

		// Filter by time range
		if request.StartTime > 0 && entry.Timestamp.Unix() < request.StartTime {
			continue
		}
		if request.EndTime > 0 && entry.Timestamp.Unix() > request.EndTime {
			continue
		}

		filteredEntries = append(filteredEntries, entry)
	}

	// Limit entries if requested
	if request.MaxEntries > 0 && len(filteredEntries) > int(request.MaxEntries) {
		// Take the most recent entries
		start := len(filteredEntries) - int(request.MaxEntries)
		filteredEntries = filteredEntries[start:]
	}

	// Convert to protobuf entries
	var pbEntries []*worker_pb.TaskLogEntry
	for _, entry := range filteredEntries {
		pbEntry := &worker_pb.TaskLogEntry{
			Timestamp: entry.Timestamp.Unix(),
			Level:     entry.Level,
			Message:   entry.Message,
			Fields:    make(map[string]string),
		}

		// Set progress if available
		if entry.Progress != nil {
			pbEntry.Progress = float32(*entry.Progress)
		}

		// Set status if available
		if entry.Status != nil {
			pbEntry.Status = *entry.Status
		}

		// Convert fields
		for key, value := range entry.Fields {
			if strValue, ok := value.(string); ok {
				pbEntry.Fields[key] = strValue
			} else {
				pbEntry.Fields[key] = fmt.Sprintf("%v", value)
			}
		}

		pbEntries = append(pbEntries, pbEntry)
	}

	return pbEntries, nil
}

// ListAvailableTaskLogs returns a list of available task log directories
func (h *TaskLogHandler) ListAvailableTaskLogs() ([]string, error) {
	entries, err := os.ReadDir(h.baseLogDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base log directory: %w", err)
	}

	var taskDirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			taskDirs = append(taskDirs, entry.Name())
		}
	}

	return taskDirs, nil
}

// CleanupOldLogs removes old task logs beyond the specified limit
func (h *TaskLogHandler) CleanupOldLogs(maxTasks int) error {
	config := TaskLoggerConfig{
		BaseLogDir: h.baseLogDir,
		MaxTasks:   maxTasks,
	}

	// Create a temporary logger to trigger cleanup
	tempLogger := &FileTaskLogger{
		config: config,
	}

	tempLogger.cleanupOldLogs()
	return nil
}
