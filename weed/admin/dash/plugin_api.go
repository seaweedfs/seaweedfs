package dash

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPluginDetectionTimeout = 45 * time.Second
	defaultPluginExecutionTimeout = 90 * time.Second
	maxPluginDetectionTimeout     = 5 * time.Minute
	maxPluginExecutionTimeout     = 10 * time.Minute
	defaultPluginRunTimeout       = 5 * time.Minute
	maxPluginRunTimeout           = 30 * time.Minute
)

// GetPluginStatusAPI returns plugin status.
func (s *AdminServer) GetPluginStatusAPI(c *gin.Context) {
	plugin := s.GetPlugin()
	if plugin == nil {
		c.JSON(http.StatusOK, gin.H{
			"enabled":          false,
			"worker_grpc_port": s.GetWorkerGrpcPort(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":          true,
		"configured":       plugin.IsConfigured(),
		"base_dir":         plugin.BaseDir(),
		"worker_count":     len(plugin.ListWorkers()),
		"worker_grpc_port": s.GetWorkerGrpcPort(),
	})
}

// GetPluginWorkersAPI returns currently connected plugin workers.
func (s *AdminServer) GetPluginWorkersAPI(c *gin.Context) {
	workers := s.GetPluginWorkers()
	if workers == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	c.JSON(http.StatusOK, workers)
}

// GetPluginJobTypesAPI returns known plugin job types from workers and persisted data.
func (s *AdminServer) GetPluginJobTypesAPI(c *gin.Context) {
	jobTypes, err := s.ListPluginJobTypes()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if jobTypes == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	c.JSON(http.StatusOK, jobTypes)
}

// GetPluginJobsAPI returns tracked jobs for monitoring.
func (s *AdminServer) GetPluginJobsAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Query("job_type"))
	state := strings.TrimSpace(c.Query("state"))
	limit := parsePositiveInt(c.Query("limit"), 200)
	jobs := s.ListPluginJobs(jobType, state, limit)
	if jobs == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// GetPluginJobAPI returns one tracked job.
func (s *AdminServer) GetPluginJobAPI(c *gin.Context) {
	jobID := strings.TrimSpace(c.Param("jobId"))
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobId is required"})
		return
	}

	job, found := s.GetPluginJob(jobID)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	c.JSON(http.StatusOK, job)
}

// GetPluginJobDetailAPI returns detailed information for one tracked plugin job.
func (s *AdminServer) GetPluginJobDetailAPI(c *gin.Context) {
	jobID := strings.TrimSpace(c.Param("jobId"))
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobId is required"})
		return
	}

	activityLimit := parsePositiveInt(c.Query("activity_limit"), 500)
	relatedLimit := parsePositiveInt(c.Query("related_limit"), 20)

	detail, found, err := s.GetPluginJobDetail(jobID, activityLimit, relatedLimit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !found || detail == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job detail not found"})
		return
	}

	c.JSON(http.StatusOK, detail)
}

// GetPluginActivitiesAPI returns recent plugin activities.
func (s *AdminServer) GetPluginActivitiesAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Query("job_type"))
	limit := parsePositiveInt(c.Query("limit"), 500)
	activities := s.ListPluginActivities(jobType, limit)
	if activities == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}
	c.JSON(http.StatusOK, activities)
}

// GetPluginSchedulerStatesAPI returns per-job-type scheduler status for monitoring.
func (s *AdminServer) GetPluginSchedulerStatesAPI(c *gin.Context) {
	jobTypeFilter := strings.TrimSpace(c.Query("job_type"))

	states, err := s.ListPluginSchedulerStates()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if jobTypeFilter != "" {
		filtered := make([]interface{}, 0, len(states))
		for _, state := range states {
			if state.JobType == jobTypeFilter {
				filtered = append(filtered, state)
			}
		}
		c.JSON(http.StatusOK, filtered)
		return
	}

	if states == nil {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	c.JSON(http.StatusOK, states)
}

// RequestPluginJobTypeSchemaAPI asks a worker for one job type schema.
func (s *AdminServer) RequestPluginJobTypeSchemaAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	forceRefresh := c.DefaultQuery("force_refresh", "false") == "true"

	ctx, cancel := context.WithTimeout(c.Request.Context(), defaultPluginDetectionTimeout)
	defer cancel()
	descriptor, err := s.RequestPluginJobTypeDescriptor(ctx, jobType, forceRefresh)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	renderProtoJSON(c, http.StatusOK, descriptor)
}

// GetPluginJobTypeDescriptorAPI returns persisted descriptor for a job type.
func (s *AdminServer) GetPluginJobTypeDescriptorAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	descriptor, err := s.LoadPluginJobTypeDescriptor(jobType)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if descriptor == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "descriptor not found"})
		return
	}

	renderProtoJSON(c, http.StatusOK, descriptor)
}

// GetPluginJobTypeConfigAPI loads persisted config for a job type.
func (s *AdminServer) GetPluginJobTypeConfigAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	config, err := s.LoadPluginJobTypeConfig(jobType)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if config == nil {
		config = &plugin_pb.PersistedJobTypeConfig{
			JobType:            jobType,
			AdminConfigValues:  map[string]*plugin_pb.ConfigValue{},
			WorkerConfigValues: map[string]*plugin_pb.ConfigValue{},
			AdminRuntime:       &plugin_pb.AdminRuntimeConfig{},
		}
	}

	renderProtoJSON(c, http.StatusOK, config)
}

// UpdatePluginJobTypeConfigAPI stores persisted config for a job type.
func (s *AdminServer) UpdatePluginJobTypeConfigAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	config := &plugin_pb.PersistedJobTypeConfig{}
	if err := parseProtoJSONBody(c, config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	config.JobType = jobType
	if config.UpdatedAt == nil {
		config.UpdatedAt = timestamppb.Now()
	}
	if config.AdminRuntime == nil {
		config.AdminRuntime = &plugin_pb.AdminRuntimeConfig{}
	}
	if config.AdminConfigValues == nil {
		config.AdminConfigValues = map[string]*plugin_pb.ConfigValue{}
	}
	if config.WorkerConfigValues == nil {
		config.WorkerConfigValues = map[string]*plugin_pb.ConfigValue{}
	}

	username := c.GetString("username")
	if username == "" {
		username = "admin"
	}
	config.UpdatedBy = username

	if err := s.SavePluginJobTypeConfig(config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	renderProtoJSON(c, http.StatusOK, config)
}

// GetPluginRunHistoryAPI returns bounded run history for a job type.
func (s *AdminServer) GetPluginRunHistoryAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	history, err := s.GetPluginRunHistory(jobType)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if history == nil {
		c.JSON(http.StatusOK, gin.H{
			"job_type":          jobType,
			"successful_runs":   []interface{}{},
			"error_runs":        []interface{}{},
			"last_updated_time": nil,
		})
		return
	}

	c.JSON(http.StatusOK, history)
}

// TriggerPluginDetectionAPI runs one detector for this job type and returns proposals.
func (s *AdminServer) TriggerPluginDetectionAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	var req struct {
		ClusterContext json.RawMessage `json:"cluster_context"`
		MaxResults     int32           `json:"max_results"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}

	if err := c.ShouldBindJSON(&req); err != nil && err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginDetectionTimeout, maxPluginDetectionTimeout)
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	report, err := s.RunPluginDetectionWithReport(ctx, jobType, clusterContext, req.MaxResults)
	proposals := make([]*plugin_pb.JobProposal, 0)
	requestID := ""
	detectorWorkerID := ""
	totalProposals := int32(0)
	if report != nil {
		proposals = report.Proposals
		requestID = report.RequestID
		detectorWorkerID = report.WorkerID
		if report.Complete != nil {
			totalProposals = report.Complete.TotalProposals
		}
	}

	proposalPayloads := make([]map[string]interface{}, 0, len(proposals))
	for _, proposal := range proposals {
		payload, marshalErr := protoMessageToMap(proposal)
		if marshalErr != nil {
			glog.Warningf("failed to marshal proposal for jobType=%s: %v", jobType, marshalErr)
			continue
		}
		proposalPayloads = append(proposalPayloads, payload)
	}

	sort.Slice(proposalPayloads, func(i, j int) bool {
		iPriority := int32(0)
		jPriority := int32(0)
		if v, ok := proposalPayloads[i]["priority"].(float64); ok {
			iPriority = int32(v)
		}
		if v, ok := proposalPayloads[j]["priority"].(float64); ok {
			jPriority = int32(v)
		}
		if iPriority != jPriority {
			return iPriority > jPriority
		}
		iID, _ := proposalPayloads[i]["proposal_id"].(string)
		jID, _ := proposalPayloads[j]["proposal_id"].(string)
		return iID < jID
	})

	activities := s.ListPluginActivities(jobType, 500)
	filteredActivities := make([]interface{}, 0, len(activities))
	if requestID != "" {
		for i := len(activities) - 1; i >= 0; i-- {
			activity := activities[i]
			if activity.RequestID != requestID {
				continue
			}
			filteredActivities = append(filteredActivities, activity)
		}
	}

	response := gin.H{
		"job_type":           jobType,
		"request_id":         requestID,
		"detector_worker_id": detectorWorkerID,
		"total_proposals":    totalProposals,
		"count":              len(proposalPayloads),
		"proposals":          proposalPayloads,
		"activities":         filteredActivities,
	}

	if err != nil {
		response["error"] = err.Error()
		c.JSON(http.StatusBadRequest, response)
		return
	}

	c.JSON(http.StatusOK, response)
}

// RunPluginJobTypeAPI runs full workflow for one job type: detect then dispatch detected jobs.
func (s *AdminServer) RunPluginJobTypeAPI(c *gin.Context) {
	jobType := strings.TrimSpace(c.Param("jobType"))
	if jobType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobType is required"})
		return
	}

	var req struct {
		ClusterContext json.RawMessage `json:"cluster_context"`
		MaxResults     int32           `json:"max_results"`
		TimeoutSeconds int             `json:"timeout_seconds"`
		Attempt        int32           `json:"attempt"`
	}

	if err := c.ShouldBindJSON(&req); err != nil && err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.Attempt < 1 {
		req.Attempt = 1
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginRunTimeout, maxPluginRunTimeout)
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	proposals, err := s.RunPluginDetection(ctx, jobType, clusterContext, req.MaxResults)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	detectedCount := len(proposals)

	filteredProposals, skippedActiveCount, err := s.FilterPluginProposalsWithActiveJobs(jobType, proposals)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	type executionResult struct {
		JobID      string                 `json:"job_id"`
		Success    bool                   `json:"success"`
		Error      string                 `json:"error,omitempty"`
		Completion map[string]interface{} `json:"completion,omitempty"`
	}

	results := make([]executionResult, 0, len(filteredProposals))
	successCount := 0
	errorCount := 0

	for index, proposal := range filteredProposals {
		job := buildJobSpecFromProposal(jobType, proposal, index)
		completed, execErr := s.ExecutePluginJob(ctx, job, clusterContext, req.Attempt)

		result := executionResult{
			JobID:   job.JobId,
			Success: execErr == nil,
		}

		if completed != nil {
			if payload, marshalErr := protoMessageToMap(completed); marshalErr == nil {
				result.Completion = payload
			}
		}

		if execErr != nil {
			result.Error = execErr.Error()
			errorCount++
		} else {
			successCount++
		}

		results = append(results, result)
	}

	c.JSON(http.StatusOK, gin.H{
		"job_type":               jobType,
		"detected_count":         detectedCount,
		"ready_to_execute_count": len(filteredProposals),
		"skipped_active_count":   skippedActiveCount,
		"executed_count":         len(results),
		"success_count":          successCount,
		"error_count":            errorCount,
		"execution_results":      results,
	})
}

// ExecutePluginJobAPI executes one job on a capable worker and waits for completion.
func (s *AdminServer) ExecutePluginJobAPI(c *gin.Context) {
	var req struct {
		Job            json.RawMessage `json:"job"`
		ClusterContext json.RawMessage `json:"cluster_context"`
		Attempt        int32           `json:"attempt"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if len(req.Job) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job is required"})
		return
	}

	job := &plugin_pb.JobSpec{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(req.Job, job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job payload: " + err.Error()})
		return
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Attempt < 1 {
		req.Attempt = 1
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginExecutionTimeout, maxPluginExecutionTimeout)
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	completed, err := s.ExecutePluginJob(ctx, job, clusterContext, req.Attempt)
	if err != nil {
		if completed != nil {
			payload, marshalErr := protoMessageToMap(completed)
			if marshalErr == nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "completion": payload})
				return
			}
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	renderProtoJSON(c, http.StatusOK, completed)
}

func (s *AdminServer) parseOrBuildClusterContext(raw json.RawMessage) (*plugin_pb.ClusterContext, error) {
	if len(raw) == 0 {
		return s.buildDefaultPluginClusterContext(), nil
	}

	contextMessage := &plugin_pb.ClusterContext{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, contextMessage); err != nil {
		return nil, fmt.Errorf("invalid cluster_context payload: %w", err)
	}

	fallback := s.buildDefaultPluginClusterContext()
	if len(contextMessage.MasterGrpcAddresses) == 0 {
		contextMessage.MasterGrpcAddresses = append(contextMessage.MasterGrpcAddresses, fallback.MasterGrpcAddresses...)
	}
	if len(contextMessage.FilerGrpcAddresses) == 0 {
		contextMessage.FilerGrpcAddresses = append(contextMessage.FilerGrpcAddresses, fallback.FilerGrpcAddresses...)
	}
	if len(contextMessage.VolumeGrpcAddresses) == 0 {
		contextMessage.VolumeGrpcAddresses = append(contextMessage.VolumeGrpcAddresses, fallback.VolumeGrpcAddresses...)
	}
	if contextMessage.Metadata == nil {
		contextMessage.Metadata = map[string]string{}
	}
	contextMessage.Metadata["source"] = "admin"

	return contextMessage, nil
}

func (s *AdminServer) buildDefaultPluginClusterContext() *plugin_pb.ClusterContext {
	clusterContext := &plugin_pb.ClusterContext{
		MasterGrpcAddresses: make([]string, 0),
		FilerGrpcAddresses:  make([]string, 0),
		VolumeGrpcAddresses: make([]string, 0),
		Metadata: map[string]string{
			"source": "admin",
		},
	}

	masterAddress := string(s.masterClient.GetMaster(context.Background()))
	if masterAddress != "" {
		clusterContext.MasterGrpcAddresses = append(clusterContext.MasterGrpcAddresses, masterAddress)
	}

	filerSeen := map[string]struct{}{}
	for _, filer := range s.GetAllFilers() {
		filer = strings.TrimSpace(filer)
		if filer == "" {
			continue
		}
		if _, exists := filerSeen[filer]; exists {
			continue
		}
		filerSeen[filer] = struct{}{}
		clusterContext.FilerGrpcAddresses = append(clusterContext.FilerGrpcAddresses, filer)
	}

	volumeSeen := map[string]struct{}{}
	if volumeServers, err := s.GetClusterVolumeServers(); err == nil {
		for _, server := range volumeServers.VolumeServers {
			address := strings.TrimSpace(server.GetDisplayAddress())
			if address == "" {
				address = strings.TrimSpace(server.Address)
			}
			if address == "" {
				continue
			}
			if _, exists := volumeSeen[address]; exists {
				continue
			}
			volumeSeen[address] = struct{}{}
			clusterContext.VolumeGrpcAddresses = append(clusterContext.VolumeGrpcAddresses, address)
		}
	} else {
		glog.V(1).Infof("failed to build default plugin volume context: %v", err)
	}

	sort.Strings(clusterContext.MasterGrpcAddresses)
	sort.Strings(clusterContext.FilerGrpcAddresses)
	sort.Strings(clusterContext.VolumeGrpcAddresses)

	return clusterContext
}

func parseProtoJSONBody(c *gin.Context, message proto.Message) error {
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	if len(data) == 0 {
		return fmt.Errorf("request body is empty")
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, message); err != nil {
		return fmt.Errorf("invalid protobuf json: %w", err)
	}
	return nil
}

func renderProtoJSON(c *gin.Context, statusCode int, message proto.Message) {
	payload, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode response: " + err.Error()})
		return
	}

	c.Data(statusCode, "application/json", payload)
}

func protoMessageToMap(message proto.Message) (map[string]interface{}, error) {
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeTimeout(timeoutSeconds int, defaultTimeout, maxTimeout time.Duration) time.Duration {
	if timeoutSeconds <= 0 {
		return defaultTimeout
	}
	timeout := time.Duration(timeoutSeconds) * time.Second
	if timeout > maxTimeout {
		return maxTimeout
	}
	return timeout
}

func buildJobSpecFromProposal(jobType string, proposal *plugin_pb.JobProposal, index int) *plugin_pb.JobSpec {
	now := timestamppb.Now()

	jobID := fmt.Sprintf("%s-%d-%d", jobType, now.AsTime().UnixNano(), index)

	jobSpec := &plugin_pb.JobSpec{
		JobId:       jobID,
		JobType:     jobType,
		Priority:    plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Parameters:  map[string]*plugin_pb.ConfigValue{},
		Labels:      map[string]string{},
		CreatedAt:   now,
		ScheduledAt: now,
	}

	if proposal == nil {
		return jobSpec
	}

	if proposal.JobType != "" {
		jobSpec.JobType = proposal.JobType
	}
	jobSpec.DedupeKey = proposal.DedupeKey
	jobSpec.Priority = proposal.Priority
	jobSpec.Summary = proposal.Summary
	jobSpec.Detail = proposal.Detail

	if proposal.Parameters != nil {
		jobSpec.Parameters = clonePluginConfigValueMap(proposal.Parameters)
	}
	if proposal.Labels != nil {
		jobSpec.Labels = make(map[string]string, len(proposal.Labels))
		for key, value := range proposal.Labels {
			jobSpec.Labels[key] = value
		}
	}
	if proposal.NotBefore != nil {
		jobSpec.ScheduledAt = proposal.NotBefore
	}

	return jobSpec
}

func parsePositiveInt(raw string, defaultValue int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

func clonePluginConfigValueMap(in map[string]*plugin_pb.ConfigValue) map[string]*plugin_pb.ConfigValue {
	if len(in) == 0 {
		return map[string]*plugin_pb.ConfigValue{}
	}
	out := make(map[string]*plugin_pb.ConfigValue, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		clone := *value
		out[key] = &clone
	}
	return out
}
