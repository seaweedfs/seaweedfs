package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type PluginHandlers struct {
	adminServer interface{}
	pluginMgr   *adminplugin.Manager
}

func NewPluginHandlers(adminServer interface{}, pluginMgr *adminplugin.Manager) *PluginHandlers {
	return &PluginHandlers{
		adminServer: adminServer,
		pluginMgr:   pluginMgr,
	}
}

// ListPluginsAPI returns list of connected plugins
func (h *PluginHandlers) ListPluginsAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	plugins := h.pluginMgr.ListPlugins(true)
	
	type PluginInfo struct {
		ID                  string    `json:"id"`
		Name                string    `json:"name"`
		Version             string    `json:"version"`
		Status              string    `json:"status"`
		Capabilities        []string  `json:"capabilities"`
		ActiveJobs          int       `json:"active_jobs"`
		CompletedJobs       int       `json:"completed_jobs"`
		FailedJobs          int       `json:"failed_jobs"`
		TotalDetections     int64     `json:"total_detections"`
		AvgExecutionTimeMs  float64   `json:"avg_execution_time_ms"`
		CPUUsagePercent     float64   `json:"cpu_usage_percent"`
		MemoryUsageBytes    int64     `json:"memory_usage_bytes"`
		ConnectedAt         time.Time `json:"connected_at"`
		LastHeartbeat       time.Time `json:"last_heartbeat"`
		HealthCheckInterval int64     `json:"health_check_interval_ms"`
		JobTimeout          int64     `json:"job_timeout_ms"`
	}

	var result []PluginInfo
	for _, p := range plugins {
		p.mu.RLock()
		result = append(result, PluginInfo{
			ID:                  p.ID,
			Name:                p.Name,
			Version:             p.Version,
			Status:              p.Status,
			Capabilities:        p.Capabilities,
			ActiveJobs:          p.ActiveJobs,
			CompletedJobs:       p.CompletedJobs,
			FailedJobs:          p.FailedJobs,
			TotalDetections:     p.TotalDetections,
			AvgExecutionTimeMs:  p.AvgExecutionTimeMs,
			CPUUsagePercent:     p.CPUUsagePercent,
			MemoryUsageBytes:    p.MemoryUsageBytes,
			ConnectedAt:         p.ConnectedAt,
			LastHeartbeat:       p.LastHeartbeat,
			HealthCheckInterval: int64(p.HealthCheckInterval.Milliseconds()),
			JobTimeout:          int64(p.JobTimeout.Milliseconds()),
		})
		p.mu.RUnlock()
	}

	util.ResponseOKJson(w, result)
}

// ListJobsAPI returns jobs for a specific type
func (h *PluginHandlers) ListJobsAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/jobs/")
	jobType = strings.TrimSuffix(jobType, "/")

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	jobs := h.pluginMgr.ListJobsForType(jobType, limit)

	type JobInfo struct {
		JobID       string      `json:"job_id"`
		JobType     string      `json:"job_type"`
		PluginID    string      `json:"plugin_id"`
		State       string      `json:"state"`
		CreatedAt   time.Time   `json:"created_at"`
		StartedAt   *time.Time  `json:"started_at,omitempty"`
		CompletedAt *time.Time  `json:"completed_at,omitempty"`
		RetryCount  int         `json:"retry_count"`
		LastError   string      `json:"last_error,omitempty"`
	}

	var result []JobInfo
	for _, job := range jobs {
		result = append(result, JobInfo{
			JobID:       job.JobID,
			JobType:     job.JobType,
			PluginID:    job.PluginID,
			State:       job.State.String(),
			CreatedAt:   job.CreatedAt,
			StartedAt:   job.StartedAt,
			CompletedAt: job.CompletedAt,
			RetryCount:  job.RetryCount,
			LastError:   job.LastError,
		})
	}

	util.ResponseOKJson(w, result)
}

// GetConfigAPI returns configuration for a job type
func (h *PluginHandlers) GetConfigAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/config/")
	jobType = strings.TrimSuffix(jobType, "/")

	configs := h.pluginMgr.ListConfigs()
	
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			type JobTypeConfigResponse struct {
				Type              string            `json:"type"`
				Enabled           bool              `json:"enabled"`
				Priority          int               `json:"priority"`
				Interval          int64             `json:"interval_ms"`
				MaxConcurrent     int               `json:"max_concurrent"`
				Parameters        map[string]string `json:"parameters"`
				RequiredDetections []string         `json:"required_detections"`
			}
			
			util.ResponseOKJson(w, JobTypeConfigResponse{
				Type:              jobCfg.Type,
				Enabled:           jobCfg.Enabled,
				Priority:          jobCfg.Priority,
				Interval:          int64(jobCfg.Interval.Milliseconds()),
				MaxConcurrent:     jobCfg.MaxConcurrent,
				Parameters:        jobCfg.Parameters,
				RequiredDetections: jobCfg.RequiredDetections,
			})
			return
		}
	}

	util.ResponseError(w, http.StatusNotFound, fmt.Sprintf("Config not found for job type: %s", jobType))
}

// SaveConfigAPI saves configuration for a job type
func (h *PluginHandlers) SaveConfigAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	if r.Method != http.MethodPost {
		util.ResponseError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/config/")
	jobType = strings.TrimSuffix(jobType, "/apply")
	jobType = strings.TrimSuffix(jobType, "/")

	var req struct {
		Parameters map[string]string `json:"parameters"`
		Enabled    *bool             `json:"enabled,omitempty"`
		Priority   *int              `json:"priority,omitempty"`
		Interval   *int64            `json:"interval_ms,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.ResponseError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	configs := h.pluginMgr.ListConfigs()
	for _, cfg := range configs {
		if jobCfg, ok := cfg.GetJobTypeConfig(jobType); ok {
			if req.Parameters != nil {
				jobCfg.Parameters = req.Parameters
			}
			if req.Enabled != nil {
				jobCfg.Enabled = *req.Enabled
			}
			if req.Priority != nil {
				jobCfg.Priority = *req.Priority
			}
			if req.Interval != nil {
				jobCfg.Interval = time.Duration(*req.Interval) * time.Millisecond
			}

			if err := h.pluginMgr.SaveConfig(cfg, false); err != nil {
				util.ResponseError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to save config: %v", err))
				return
			}

			util.ResponseOKJson(w, map[string]string{"status": "saved"})
			return
		}
	}

	util.ResponseError(w, http.StatusNotFound, fmt.Sprintf("Config not found for job type: %s", jobType))
}

// GetDetectionHistoryAPI returns detection history for a job type
func (h *PluginHandlers) GetDetectionHistoryAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/detection/history/")
	jobType = strings.TrimSuffix(jobType, "/")

	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	records := h.pluginMgr.GetDetectionHistory(jobType)
	if len(records) > limit {
		records = records[:limit]
	}

	type DetectionHistoryItem struct {
		DetectionType     string    `json:"detection_type"`
		Timestamp         time.Time `json:"timestamp"`
		Severity          string    `json:"severity"`
		Description       string    `json:"description"`
		AffectedResource  string    `json:"affected_resource"`
	}

	var result []DetectionHistoryItem
	for _, record := range records {
		result = append(result, DetectionHistoryItem{
			DetectionType:    record.DetectionType,
			Timestamp:        record.Timestamp,
			Severity:         record.Severity,
			Description:      record.Description,
			AffectedResource: record.AffectedResource,
		})
	}

	util.ResponseOKJson(w, result)
}

// GetExecutionHistoryAPI returns execution history for a job type
func (h *PluginHandlers) GetExecutionHistoryAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/execution/history/")
	jobType = strings.TrimSuffix(jobType, "/")

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	records := h.pluginMgr.GetExecutionHistory(jobType)
	if len(records) > limit {
		records = records[:limit]
	}

	type ExecutionHistoryItem struct {
		JobID       string     `json:"job_id"`
		JobType     string     `json:"job_type"`
		PluginID    string     `json:"plugin_id"`
		State       string     `json:"state"`
		CreatedAt   time.Time  `json:"created_at"`
		StartedAt   *time.Time `json:"started_at,omitempty"`
		CompletedAt *time.Time `json:"completed_at,omitempty"`
		RetryCount  int        `json:"retry_count"`
		LastError   string     `json:"last_error,omitempty"`
	}

	var result []ExecutionHistoryItem
	for _, record := range records {
		result = append(result, ExecutionHistoryItem{
			JobID:       record.JobID,
			JobType:     record.JobType,
			PluginID:    record.PluginID,
			State:       record.State.String(),
			CreatedAt:   record.CreatedAt,
			StartedAt:   record.StartedAt,
			CompletedAt: record.CompletedAt,
			RetryCount:  record.RetryCount,
			LastError:   record.LastError,
		})
	}

	util.ResponseOKJson(w, result)
}

// TriggerDetectionAPI manually triggers detection
func (h *PluginHandlers) TriggerDetectionAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	if r.Method != http.MethodPost {
		util.ResponseError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	jobType := strings.TrimPrefix(r.URL.Path, "/api/plugin/jobs/")
	jobType = strings.TrimSuffix(jobType, "/trigger-detection")
	jobType = strings.TrimSuffix(jobType, "/")

	jobIDs, err := h.pluginMgr.TriggerDetection([]string{jobType})
	if err != nil {
		util.ResponseError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to trigger detection: %v", err))
		return
	}

	util.ResponseOKJson(w, map[string]interface{}{
		"status": "triggered",
		"job_ids": jobIDs,
	})
}

// CancelJobAPI cancels a job
func (h *PluginHandlers) CancelJobAPI(w http.ResponseWriter, r *http.Request) {
	if h.pluginMgr == nil {
		util.ResponseError(w, http.StatusServiceUnavailable, "Plugin manager not initialized")
		return
	}

	if r.Method != http.MethodPost {
		util.ResponseError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/api/plugin/jobs/")
	jobID = strings.TrimSuffix(jobID, "/cancel")
	jobID = strings.TrimSuffix(jobID, "/")

	if err := h.pluginMgr.CancelJob(jobID); err != nil {
		util.ResponseError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to cancel job: %v", err))
		return
	}

	util.ResponseOKJson(w, map[string]string{"status": "cancelled"})
}
