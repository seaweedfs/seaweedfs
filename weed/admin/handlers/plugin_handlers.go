package handlers

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// PluginHandlers handles HTTP requests for the plugin system
type PluginHandlers struct {
	adminServer *dash.AdminServer
}

// NewPluginHandlers creates a new plugin handlers instance
func NewPluginHandlers(adminServer *dash.AdminServer) *PluginHandlers {
	return &PluginHandlers{
		adminServer: adminServer,
	}
}

// ShowPlugins renders the main plugins page
func (h *PluginHandlers) ShowPlugins(c *gin.Context) {
	data := h.getPluginsData()
	html := app.PluginsPage(data)
	c.Header("Content-Type", "text/html")
	html.Render(c.Request.Context(), c.Writer)
}

// ShowPluginJobs renders the job monitoring page
func (h *PluginHandlers) ShowPluginJobs(c *gin.Context) {
	jobType := c.Param("jobType")
	data := h.getJobsData(jobType)
	html := app.PluginJobsPage(data)
	c.Header("Content-Type", "text/html")
	html.Render(c.Request.Context(), c.Writer)
}

// ShowPluginConfig renders the plugin configuration page
func (h *PluginHandlers) ShowPluginConfig(c *gin.Context) {
	jobType := c.Param("jobType")
	data := h.getPluginConfigData(jobType)
	html := app.PluginConfigPage(data)
	c.Header("Content-Type", "text/html")
	html.Render(c.Request.Context(), c.Writer)
}

// ListPluginsAPI returns JSON list of registered plugins
func (h *PluginHandlers) ListPluginsAPI(c *gin.Context) {
	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	plugins := h.adminServer.PluginManager.ListConnectedPlugins()

	// Convert to API response
	var pluginList []map[string]interface{}
	for _, p := range plugins {
		pluginList = append(pluginList, map[string]interface{}{
			"id":               p.ID,
			"name":             p.Name,
			"version":          p.Version,
			"protocol_version": p.ProtocolVersion,
			"connected_at":     p.ConnectedAt,
			"last_heartbeat":   p.LastHeartbeat,
			"state":            p.State.String(),
			"healthy":          p.IsHealthy(),
			"pending_jobs":     p.PendingJobs,
			"running_jobs":     p.RunningJobs,
			"cpu_usage":        p.CPUUsagePercent,
			"memory_usage_mb":  p.MemoryUsageMB,
		})
	}

	c.JSON(http.StatusOK, gin.H{"plugins": pluginList})
}

// ListJobsAPI returns JSON list of jobs for a job type
func (h *PluginHandlers) ListJobsAPI(c *gin.Context) {
	jobType := c.Param("jobType")
	stateStr := c.DefaultQuery("state", "")
	limit := 100

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	var state plugin.JobState
	if stateStr != "" {
		// Parse state string
		switch stateStr {
		case "pending":
			state = plugin.JobStatePending
		case "running":
			state = plugin.JobStateRunning
		case "completed":
			state = plugin.JobStateCompleted
		case "failed":
			state = plugin.JobStateFailed
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid state"})
			return
		}
	}

	jobs := h.adminServer.PluginManager.ListJobs(jobType, state)
	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}

	var jobList []map[string]interface{}
	for _, j := range jobs {
		jobList = append(jobList, map[string]interface{}{
			"id":          j.ID,
			"type":        j.Type,
			"description": j.Description,
			"state":       j.State.String(),
			"created_at":  j.CreatedAt,
			"updated_at":  j.UpdatedAt,
			"executor_id": j.ExecutorID,
			"progress":    j.ProgressPercent,
			"retries":     j.Retries,
		})
	}

	c.JSON(http.StatusOK, gin.H{"jobs": jobList, "job_type": jobType})
}

// GetConfigAPI returns the configuration for a job type
func (h *PluginHandlers) GetConfigAPI(c *gin.Context) {
	jobType := c.Param("jobType")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	config, err := h.adminServer.PluginManager.GetConfig(jobType)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("config not found: %v", err)})
		return
	}

	adminConfigMap := convertConfigFieldsToMap(config.AdminConfig)
	workerConfigMap := convertConfigFieldsToMap(config.WorkerConfig)

	c.JSON(http.StatusOK, gin.H{
		"job_type":      jobType,
		"enabled":       config.Enabled,
		"admin_config":  adminConfigMap,
		"worker_config": workerConfigMap,
		"created_at":    config.CreatedAt,
		"updated_at":    config.UpdatedAt,
		"created_by":    config.CreatedBy,
	})
}

// SaveConfigAPI saves the configuration for a job type
func (h *PluginHandlers) SaveConfigAPI(c *gin.Context) {
	jobType := c.Param("jobType")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	var req struct {
		Enabled      bool                   `json:"enabled"`
		AdminConfig  map[string]interface{} `json:"admin_config"`
		WorkerConfig map[string]interface{} `json:"worker_config"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	// Convert maps back to config fields
	adminConfigFields := convertMapToConfigFields(req.AdminConfig)
	workerConfigFields := convertMapToConfigFields(req.WorkerConfig)

	err := h.adminServer.PluginManager.SaveConfig(jobType, req.Enabled, adminConfigFields, workerConfigFields)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to save config: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "configuration saved"})
}

// GetDetectionHistoryAPI returns detection history for a job type
func (h *PluginHandlers) GetDetectionHistoryAPI(c *gin.Context) {
	jobType := c.Param("jobType")
	limitStr := c.DefaultQuery("limit", "10")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit > 100 {
		limit = 10
	}

	records := h.adminServer.PluginManager.GetDetectionHistory(jobType, limit)

	var historyList []map[string]interface{}
	for _, rec := range records {
		historyList = append(historyList, map[string]interface{}{
			"timestamp":     rec.Timestamp,
			"duration_ms":   rec.DurationMs,
			"jobs_detected": rec.JobsDetected,
			"status":        rec.Status,
			"error":         rec.Error,
			"worker_id":     rec.WorkerID,
			"detailed_logs": rec.DetailedLogs,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"job_type": jobType,
		"history":  historyList,
		"count":    len(historyList),
	})
}

// GetExecutionHistoryAPI returns execution history for a job type
func (h *PluginHandlers) GetExecutionHistoryAPI(c *gin.Context) {
	jobType := c.Param("jobType")
	limitStr := c.DefaultQuery("limit", "10")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit > 100 {
		limit = 10
	}

	records := h.adminServer.PluginManager.GetExecutionHistory(jobType, limit)

	var historyList []map[string]interface{}
	for _, rec := range records {
		historyList = append(historyList, map[string]interface{}{
			"job_id":          rec.JobID,
			"job_type":        rec.JobType,
			"started_at":      rec.StartedAt,
			"completed_at":    rec.CompletedAt,
			"duration_ms":     rec.DurationMs,
			"success":         rec.Success,
			"error":           rec.Error,
			"worker_id":       rec.WorkerID,
			"items_processed": rec.ItemsProcessed,
			"execution_logs":  rec.ExecutionLogs,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"job_type": jobType,
		"history":  historyList,
		"count":    len(historyList),
	})
}

// TriggerDetectionAPI manually triggers detection for a job type
func (h *PluginHandlers) TriggerDetectionAPI(c *gin.Context) {
	jobType := c.Param("jobType")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	err := h.adminServer.PluginManager.TriggerDetection(jobType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("detection failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "detection triggered"})
}

// CancelJobAPI cancels a running job
func (h *PluginHandlers) CancelJobAPI(c *gin.Context) {
	jobID := c.Param("jobID")

	if h.adminServer.PluginManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plugin manager not available"})
		return
	}

	err := h.adminServer.PluginManager.CancelJob(jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("cancel failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "job cancelled"})
}

// getPluginsData prepares data for the main plugins page
func (h *PluginHandlers) getPluginsData() map[string]interface{} {
	data := map[string]interface{}{
		"plugins":       []map[string]interface{}{},
		"job_types":     []string{},
		"total_plugins": 0,
		"total_jobs":    0,
	}

	if h.adminServer.PluginManager == nil {
		return data
	}

	// Get connected plugins
	plugins := h.adminServer.PluginManager.ListConnectedPlugins()
	var pluginList []map[string]interface{}
	for _, p := range plugins {
		pluginList = append(pluginList, map[string]interface{}{
			"id":           p.ID,
			"name":         p.Name,
			"version":      p.Version,
			"connected_at": p.ConnectedAt.Format(time.RFC3339),
			"state":        p.State.String(),
			"healthy":      p.IsHealthy(),
			"pending_jobs": p.PendingJobs,
			"running_jobs": p.RunningJobs,
			"cpu_usage":    fmt.Sprintf("%.1f%%", p.CPUUsagePercent),
			"memory_usage": fmt.Sprintf("%.1f MB", p.MemoryUsageMB),
		})
	}

	// Get registered job types
	jobTypes := h.adminServer.PluginManager.ListJobTypes()
	sort.Strings(jobTypes) // Sort alphabetically

	// Count jobs
	totalJobs := 0
	for _, jobType := range jobTypes {
		jobs := h.adminServer.PluginManager.ListJobs(jobType, 0)
		totalJobs += len(jobs)
	}

	data["plugins"] = pluginList
	data["job_types"] = jobTypes
	data["total_plugins"] = len(plugins)
	data["total_jobs"] = totalJobs

	return data
}

// getJobsData prepares data for the job monitoring page
func (h *PluginHandlers) getJobsData(jobType string) map[string]interface{} {
	data := map[string]interface{}{
		"job_type":        jobType,
		"jobs":            []map[string]interface{}{},
		"pending_count":   0,
		"running_count":   0,
		"completed_count": 0,
		"failed_count":    0,
	}

	if h.adminServer.PluginManager == nil {
		return data
	}

	// Get all jobs for this type
	allJobs := h.adminServer.PluginManager.ListJobs(jobType, 0)

	var jobsList []map[string]interface{}
	pendingCount, runningCount, completedCount, failedCount := 0, 0, 0, 0

	for _, job := range allJobs {
		jobsList = append(jobsList, map[string]interface{}{
			"id":          job.ID,
			"description": job.Description,
			"state":       job.State.String(),
			"created_at":  job.CreatedAt.Format(time.RFC3339),
			"updated_at":  job.UpdatedAt.Format(time.RFC3339),
			"progress":    job.ProgressPercent,
			"executor_id": job.ExecutorID,
			"retries":     job.Retries,
		})

		switch job.State {
		case plugin.JobStatePending:
			pendingCount++
		case plugin.JobStateRunning:
			runningCount++
		case plugin.JobStateCompleted:
			completedCount++
		case plugin.JobStateFailed:
			failedCount++
		}
	}

	data["jobs"] = jobsList
	data["pending_count"] = pendingCount
	data["running_count"] = runningCount
	data["completed_count"] = completedCount
	data["failed_count"] = failedCount

	return data
}

// getPluginConfigData prepares data for the plugin configuration page
func (h *PluginHandlers) getPluginConfigData(jobType string) map[string]interface{} {
	data := map[string]interface{}{
		"job_type":          jobType,
		"enabled":           false,
		"admin_config":      map[string]interface{}{},
		"worker_config":     map[string]interface{}{},
		"schema":            map[string]interface{}{},
		"detection_history": []map[string]interface{}{},
		"execution_history": []map[string]interface{}{},
	}

	if h.adminServer.PluginManager == nil {
		return data
	}

	// Get config
	config, err := h.adminServer.PluginManager.GetConfig(jobType)
	if err == nil && config != nil {
		data["enabled"] = config.Enabled
		data["admin_config"] = convertConfigFieldsToMap(config.AdminConfig)
		data["worker_config"] = convertConfigFieldsToMap(config.WorkerConfig)
	}

	// Get schema
	// detector, err := h.adminServer.PluginManager.GetDetectorForJobType(jobType)
	// if err == nil && detector != nil {
	// 	data["schema"] = detector.ConfigSchema  // Will be extended later
	// }

	// Get detection history
	detectionRecords := h.adminServer.PluginManager.GetDetectionHistory(jobType, 10)
	var detectionList []map[string]interface{}
	for _, rec := range detectionRecords {
		detectionList = append(detectionList, map[string]interface{}{
			"timestamp":     rec.Timestamp.Format(time.RFC3339),
			"duration_ms":   rec.DurationMs,
			"jobs_detected": rec.JobsDetected,
			"status":        rec.Status,
			"error":         rec.Error,
			"detailed_logs": rec.DetailedLogs,
		})
	}
	data["detection_history"] = detectionList

	// Get execution history
	executionRecords := h.adminServer.PluginManager.GetExecutionHistory(jobType, 10)
	var executionList []map[string]interface{}
	for _, rec := range executionRecords {
		executionList = append(executionList, map[string]interface{}{
			"job_id":          rec.JobID,
			"started_at":      rec.StartedAt.Format(time.RFC3339),
			"completed_at":    rec.CompletedAt.Format(time.RFC3339),
			"duration_ms":     rec.DurationMs,
			"success":         rec.Success,
			"error":           rec.Error,
			"items_processed": rec.ItemsProcessed,
		})
	}
	data["execution_history"] = executionList

	return data
}

// Helper functions to convert between data formats

func convertConfigFieldsToMap(fields []*plugin_pb.ConfigFieldValue) map[string]interface{} {
	result := make(map[string]interface{})
	for _, field := range fields {
		if field == nil {
			continue
		}
		if field.StringValue != "" {
			result[field.FieldName] = field.StringValue
		} else if field.IntValue != 0 {
			result[field.FieldName] = field.IntValue
		} else if field.FloatValue != 0 {
			result[field.FieldName] = field.FloatValue
		} else if field.BoolValue {
			result[field.FieldName] = field.BoolValue
		} else if field.JsonValue != "" {
			result[field.FieldName] = field.JsonValue
		}
	}
	return result
}

func convertMapToConfigFields(m map[string]interface{}) []*plugin_pb.ConfigFieldValue {
	var fields []*plugin_pb.ConfigFieldValue
	for k, v := range m {
		field := &plugin_pb.ConfigFieldValue{FieldName: k}
		switch val := v.(type) {
		case string:
			field.StringValue = val
		case float64:
			// Try to detect if it's an int or bool
			if val == float64(int64(val)) {
				field.IntValue = int64(val)
			} else {
				field.FloatValue = float32(val)
			}
		case bool:
			field.BoolValue = val
		case int:
			field.IntValue = int64(val)
		}
		fields = append(fields, field)
	}
	return fields
}

// convertConfigSchemaToMap converts schema to a map (placeholder for future use)
// func convertConfigSchemaToMap(schema interface{}) map[string]interface{} {
// 	// TODO: implement schema conversion when schema structure is finalized
// 	return make(map[string]interface{})
// }

