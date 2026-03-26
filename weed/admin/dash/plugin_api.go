package dash

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/admin/plugin"
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

func matchesPluginLane(jobType, laneFilter string) bool {
	laneFilter = strings.TrimSpace(laneFilter)
	if laneFilter == "" {
		return true
	}
	return plugin.JobTypeLane(jobType) == plugin.SchedulerLane(laneFilter)
}

func filterTrackedJobsByLane(jobs []plugin.TrackedJob, laneFilter string) []plugin.TrackedJob {
	if strings.TrimSpace(laneFilter) == "" {
		return jobs
	}

	filtered := make([]plugin.TrackedJob, 0, len(jobs))
	for _, job := range jobs {
		if matchesPluginLane(job.JobType, laneFilter) {
			filtered = append(filtered, job)
		}
	}
	return filtered
}

func filterActivitiesByLane(activities []plugin.JobActivity, laneFilter string) []plugin.JobActivity {
	if strings.TrimSpace(laneFilter) == "" {
		return activities
	}

	filtered := make([]plugin.JobActivity, 0, len(activities))
	for _, activity := range activities {
		if matchesPluginLane(activity.JobType, laneFilter) {
			filtered = append(filtered, activity)
		}
	}
	return filtered
}

// GetPluginStatusAPI returns plugin status.
func (s *AdminServer) GetPluginStatusAPI(w http.ResponseWriter, r *http.Request) {
	plugin := s.GetPlugin()
	if plugin == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled":          false,
			"worker_grpc_port": s.GetWorkerGrpcPort(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"enabled":          true,
		"configured":       plugin.IsConfigured(),
		"base_dir":         plugin.BaseDir(),
		"worker_count":     len(plugin.ListWorkers()),
		"worker_grpc_port": s.GetWorkerGrpcPort(),
	})
}

// GetPluginWorkersAPI returns currently connected plugin workers.
// Accepts an optional ?lane= query parameter to filter by scheduler lane.
func (s *AdminServer) GetPluginWorkersAPI(w http.ResponseWriter, r *http.Request) {
	workers := s.GetPluginWorkers()
	if workers == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}

	laneFilter := strings.TrimSpace(r.URL.Query().Get("lane"))
	if laneFilter != "" {
		lane := plugin.SchedulerLane(laneFilter)
		filtered := make([]*plugin.WorkerSession, 0, len(workers))
		for _, w := range workers {
			for jobType := range w.Capabilities {
				if plugin.JobTypeLane(jobType) == lane {
					filtered = append(filtered, w)
					break
				}
			}
		}
		writeJSON(w, http.StatusOK, filtered)
		return
	}

	writeJSON(w, http.StatusOK, workers)
}

// GetPluginJobTypesAPI returns known plugin job types from workers and persisted data.
// Accepts an optional ?lane= query parameter to filter by scheduler lane.
func (s *AdminServer) GetPluginJobTypesAPI(w http.ResponseWriter, r *http.Request) {
	jobTypes, err := s.ListPluginJobTypes()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if jobTypes == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}

	laneFilter := strings.TrimSpace(r.URL.Query().Get("lane"))
	if laneFilter != "" {
		lane := plugin.SchedulerLane(laneFilter)
		filtered := make([]plugin.JobTypeInfo, 0, len(jobTypes))
		for _, jt := range jobTypes {
			if plugin.JobTypeLane(jt.JobType) == lane {
				filtered = append(filtered, jt)
			}
		}
		writeJSON(w, http.StatusOK, filtered)
		return
	}

	writeJSON(w, http.StatusOK, jobTypes)
}

// GetPluginJobsAPI returns tracked jobs for monitoring.
func (s *AdminServer) GetPluginJobsAPI(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	jobType := strings.TrimSpace(query.Get("job_type"))
	state := strings.TrimSpace(query.Get("state"))
	laneFilter := strings.TrimSpace(query.Get("lane"))
	limit := parsePositiveInt(query.Get("limit"), 200)
	jobs := s.ListPluginJobs(jobType, state, limit)
	if jobs == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	writeJSON(w, http.StatusOK, filterTrackedJobsByLane(jobs, laneFilter))
}

// GetPluginJobAPI returns one tracked job.
func (s *AdminServer) GetPluginJobAPI(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(mux.Vars(r)["jobId"])
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "jobId is required")
		return
	}

	job, found := s.GetPluginJob(jobID)
	if !found {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// GetPluginJobDetailAPI returns detailed information for one tracked plugin job.
func (s *AdminServer) GetPluginJobDetailAPI(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(mux.Vars(r)["jobId"])
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "jobId is required")
		return
	}

	query := r.URL.Query()
	activityLimit := parsePositiveInt(query.Get("activity_limit"), 500)
	relatedLimit := parsePositiveInt(query.Get("related_limit"), 20)

	detail, found, err := s.GetPluginJobDetail(jobID, activityLimit, relatedLimit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found || detail == nil {
		writeJSONError(w, http.StatusNotFound, "job detail not found")
		return
	}

	writeJSON(w, http.StatusOK, detail)
}

// ExpirePluginJobAPI marks a job as failed so it no longer blocks scheduling.
func (s *AdminServer) ExpirePluginJobAPI(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(mux.Vars(r)["jobId"])
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "jobId is required")
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil && err != io.EOF {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	job, expired, err := s.ExpirePluginJob(jobID, req.Reason)
	if err != nil {
		if errors.Is(err, plugin.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]interface{}{
		"job_id":  jobID,
		"expired": expired,
	}
	if job != nil {
		response["job"] = job
	}
	if !expired {
		response["message"] = "job is not active"
	}

	writeJSON(w, http.StatusOK, response)
}

// GetPluginActivitiesAPI returns recent plugin activities.
func (s *AdminServer) GetPluginActivitiesAPI(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	jobType := strings.TrimSpace(query.Get("job_type"))
	laneFilter := strings.TrimSpace(query.Get("lane"))
	limit := parsePositiveInt(query.Get("limit"), 500)
	activities := s.ListPluginActivities(jobType, limit)
	if activities == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	writeJSON(w, http.StatusOK, filterActivitiesByLane(activities, laneFilter))
}

// GetPluginSchedulerStatesAPI returns per-job-type scheduler status for monitoring.
// Accepts optional ?job_type= and ?lane= query parameters.
func (s *AdminServer) GetPluginSchedulerStatesAPI(w http.ResponseWriter, r *http.Request) {
	jobTypeFilter := strings.TrimSpace(r.URL.Query().Get("job_type"))
	laneFilter := strings.TrimSpace(r.URL.Query().Get("lane"))

	states, err := s.ListPluginSchedulerStates()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if jobTypeFilter != "" || laneFilter != "" {
		filtered := make([]interface{}, 0, len(states))
		for _, state := range states {
			if jobTypeFilter != "" && state.JobType != jobTypeFilter {
				continue
			}
			if laneFilter != "" && state.Lane != laneFilter {
				continue
			}
			filtered = append(filtered, state)
		}
		writeJSON(w, http.StatusOK, filtered)
		return
	}

	if states == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}

	writeJSON(w, http.StatusOK, states)
}

// GetPluginSchedulerStatusAPI returns scheduler status including in-process jobs and lock state.
// Accepts optional ?lane= query parameter to scope to a specific lane.
func (s *AdminServer) GetPluginSchedulerStatusAPI(w http.ResponseWriter, r *http.Request) {
	pluginSvc := s.GetPlugin()
	if pluginSvc == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled": false,
		})
		return
	}

	laneFilter := strings.TrimSpace(r.URL.Query().Get("lane"))

	if laneFilter != "" {
		lane := plugin.SchedulerLane(laneFilter)
		response := map[string]interface{}{
			"enabled":   true,
			"scheduler": pluginSvc.GetLaneSchedulerStatus(lane),
		}
		if s.pluginLock != nil {
			response["lock"] = s.pluginLock.Status()
		}
		writeJSON(w, http.StatusOK, response)
		return
	}

	response := map[string]interface{}{
		"enabled":   true,
		"scheduler": pluginSvc.GetSchedulerStatus(),
	}
	if s.pluginLock != nil {
		response["lock"] = s.pluginLock.Status()
	}

	writeJSON(w, http.StatusOK, response)
}

// GetPluginLanesAPI returns all scheduler lanes and their current status.
func (s *AdminServer) GetPluginLanesAPI(w http.ResponseWriter, r *http.Request) {
	pluginSvc := s.GetPlugin()
	if pluginSvc == nil {
		writeJSON(w, http.StatusOK, []interface{}{})
		return
	}

	lanes := plugin.AllLanes()
	result := make([]map[string]interface{}, 0, len(lanes))
	for _, lane := range lanes {
		laneStatus := pluginSvc.GetLaneSchedulerStatus(lane)
		result = append(result, map[string]interface{}{
			"lane":           string(lane),
			"idle_sleep_sec": int(plugin.LaneIdleSleep(lane) / time.Second),
			"job_types":      plugin.LaneJobTypes(lane),
			"status":         laneStatus,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

// RequestPluginJobTypeSchemaAPI asks a worker for one job type schema.
func (s *AdminServer) RequestPluginJobTypeSchemaAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	forceRefresh := strings.EqualFold(r.URL.Query().Get("force_refresh"), "true")

	ctx, cancel := context.WithTimeout(r.Context(), defaultPluginDetectionTimeout)
	defer cancel()
	descriptor, err := s.RequestPluginJobTypeDescriptor(ctx, jobType, forceRefresh)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	renderProtoJSON(w, http.StatusOK, descriptor)
}

// GetPluginJobTypeDescriptorAPI returns persisted descriptor for a job type.
func (s *AdminServer) GetPluginJobTypeDescriptorAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	descriptor, err := s.LoadPluginJobTypeDescriptor(jobType)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if descriptor == nil {
		writeJSONError(w, http.StatusNotFound, "descriptor not found")
		return
	}

	renderProtoJSON(w, http.StatusOK, descriptor)
}

// GetPluginJobTypeConfigAPI loads persisted config for a job type.
func (s *AdminServer) GetPluginJobTypeConfigAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	config, err := s.LoadPluginJobTypeConfig(jobType)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
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
	if descriptor, err := s.LoadPluginJobTypeDescriptor(jobType); err == nil && descriptor != nil {
		applyDescriptorDefaultsToPersistedConfig(config, descriptor)
	}

	renderProtoJSON(w, http.StatusOK, config)
}

// UpdatePluginJobTypeConfigAPI stores persisted config for a job type.
func (s *AdminServer) UpdatePluginJobTypeConfigAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	config := &plugin_pb.PersistedJobTypeConfig{}
	if err := parseProtoJSONBody(w, r, config); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
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

	username := UsernameFromContext(r.Context())
	if username == "" {
		username = "admin"
	}
	config.UpdatedBy = username

	if err := s.SavePluginJobTypeConfig(config); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	renderProtoJSON(w, http.StatusOK, config)
}

// GetPluginRunHistoryAPI returns bounded run history for a job type.
func (s *AdminServer) GetPluginRunHistoryAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	history, err := s.GetPluginRunHistory(jobType)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if history == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"job_type":          jobType,
			"successful_runs":   []interface{}{},
			"error_runs":        []interface{}{},
			"last_updated_time": nil,
		})
		return
	}

	writeJSON(w, http.StatusOK, history)
}

// TriggerPluginDetectionAPI runs one detector for this job type and returns proposals.
func (s *AdminServer) TriggerPluginDetectionAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}

	var req struct {
		ClusterContext json.RawMessage `json:"cluster_context"`
		MaxResults     int32           `json:"max_results"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil && err != io.EOF {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginDetectionTimeout, maxPluginDetectionTimeout)
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
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
		iPriorityStr, _ := proposalPayloads[i]["priority"].(string)
		jPriorityStr, _ := proposalPayloads[j]["priority"].(string)

		iPriority := plugin_pb.JobPriority_value[iPriorityStr]
		jPriority := plugin_pb.JobPriority_value[jPriorityStr]

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

	response := map[string]interface{}{
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
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

// RunPluginJobTypeAPI runs full workflow for one job type: detect then dispatch detected jobs.
func (s *AdminServer) RunPluginJobTypeAPI(w http.ResponseWriter, r *http.Request) {
	jobType := strings.TrimSpace(mux.Vars(r)["jobType"])
	if jobType == "" {
		writeJSONError(w, http.StatusBadRequest, "jobType is required")
		return
	}
	releaseLock, err := s.acquirePluginLock(fmt.Sprintf("plugin detect+execute %s", jobType))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if releaseLock != nil {
		defer releaseLock()
	}

	var req struct {
		ClusterContext json.RawMessage `json:"cluster_context"`
		MaxResults     int32           `json:"max_results"`
		TimeoutSeconds int             `json:"timeout_seconds"`
		Attempt        int32           `json:"attempt"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil && err != io.EOF {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Attempt < 1 {
		req.Attempt = 1
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginRunTimeout, maxPluginRunTimeout)
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	proposals, err := s.RunPluginDetection(ctx, jobType, clusterContext, req.MaxResults)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	detectedCount := len(proposals)

	filteredProposals, skippedActiveCount, err := s.FilterPluginProposalsWithActiveJobs(jobType, proposals)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	successCount, errorCount, canceledCount, dispatchErr := s.DispatchPluginProposals(ctx, jobType, filteredProposals, clusterContext)
	if dispatchErr != nil {
		writeJSONError(w, http.StatusInternalServerError, dispatchErr.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job_type":               jobType,
		"detected_count":         detectedCount,
		"ready_to_execute_count": len(filteredProposals),
		"skipped_active_count":   skippedActiveCount,
		"executed_count":         successCount + errorCount + canceledCount,
		"success_count":          successCount,
		"error_count":            errorCount,
		"canceled_count":         canceledCount,
	})
}

// ExecutePluginJobAPI executes one job on a capable worker and waits for completion.
func (s *AdminServer) ExecutePluginJobAPI(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Job            json.RawMessage `json:"job"`
		ClusterContext json.RawMessage `json:"cluster_context"`
		Attempt        int32           `json:"attempt"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}

	if err := decodeJSONBody(newJSONMaxReader(w, r), &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(req.Job) == 0 {
		writeJSONError(w, http.StatusBadRequest, "job is required")
		return
	}

	job := &plugin_pb.JobSpec{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(req.Job, job); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid job payload: "+err.Error())
		return
	}

	clusterContext, err := s.parseOrBuildClusterContext(req.ClusterContext)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Attempt < 1 {
		req.Attempt = 1
	}

	timeout := normalizeTimeout(req.TimeoutSeconds, defaultPluginExecutionTimeout, maxPluginExecutionTimeout)
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	completed, err := s.ExecutePluginJob(ctx, job, clusterContext, req.Attempt)
	if err != nil {
		if completed != nil {
			payload, marshalErr := protoMessageToMap(completed)
			if marshalErr == nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error(), "completion": payload})
				return
			}
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	renderProtoJSON(w, http.StatusOK, completed)
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

const parseProtoJSONBodyMaxBytes = 1 << 20 // 1 MB

func parseProtoJSONBody(w http.ResponseWriter, r *http.Request, message proto.Message) error {
	limitedBody := http.MaxBytesReader(w, r.Body, parseProtoJSONBodyMaxBytes)
	data, err := io.ReadAll(limitedBody)
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

func renderProtoJSON(w http.ResponseWriter, statusCode int, message proto.Message) {
	payload, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(message)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to encode response: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(payload)
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
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		// Fallback to simpler ID if rand fails
		suffix = []byte(fmt.Sprintf("%d", index))
	}
	jobID := fmt.Sprintf("%s-%d-%s", jobType, now.AsTime().UnixNano(), hex.EncodeToString(suffix))

	jobSpec := &plugin_pb.JobSpec{
		JobId:      jobID,
		JobType:    jobType,
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		CreatedAt:  now,
		Labels:     make(map[string]string),
		Parameters: make(map[string]*plugin_pb.ConfigValue),
		DedupeKey:  "",
	}

	if proposal != nil {
		jobSpec.Summary = proposal.Summary
		jobSpec.Detail = proposal.Detail
		if proposal.Priority != plugin_pb.JobPriority_JOB_PRIORITY_UNSPECIFIED {
			jobSpec.Priority = proposal.Priority
		}
		jobSpec.DedupeKey = proposal.DedupeKey
		jobSpec.Parameters = plugin.CloneConfigValueMap(proposal.Parameters)
		if proposal.Labels != nil {
			for k, v := range proposal.Labels {
				jobSpec.Labels[k] = v
			}
		}
	}

	return jobSpec
}

func applyDescriptorDefaultsToPersistedConfig(
	config *plugin_pb.PersistedJobTypeConfig,
	descriptor *plugin_pb.JobTypeDescriptor,
) {
	if config == nil || descriptor == nil {
		return
	}

	if config.AdminConfigValues == nil {
		config.AdminConfigValues = map[string]*plugin_pb.ConfigValue{}
	}
	if config.WorkerConfigValues == nil {
		config.WorkerConfigValues = map[string]*plugin_pb.ConfigValue{}
	}
	if config.AdminRuntime == nil {
		config.AdminRuntime = &plugin_pb.AdminRuntimeConfig{}
	}

	if descriptor.AdminConfigForm != nil {
		for key, value := range descriptor.AdminConfigForm.DefaultValues {
			if value == nil {
				continue
			}
			current := config.AdminConfigValues[key]
			if current == nil {
				config.AdminConfigValues[key] = proto.Clone(value).(*plugin_pb.ConfigValue)
				continue
			}
			if strings.EqualFold(descriptor.JobType, "admin_script") &&
				key == "script" &&
				isBlankStringConfigValue(current) {
				config.AdminConfigValues[key] = proto.Clone(value).(*plugin_pb.ConfigValue)
			}
		}
	}
	if descriptor.WorkerConfigForm != nil {
		for key, value := range descriptor.WorkerConfigForm.DefaultValues {
			if value == nil {
				continue
			}
			if config.WorkerConfigValues[key] != nil {
				continue
			}
			config.WorkerConfigValues[key] = proto.Clone(value).(*plugin_pb.ConfigValue)
		}
	}
	if descriptor.AdminRuntimeDefaults != nil {
		runtime := config.AdminRuntime
		defaults := descriptor.AdminRuntimeDefaults
		if runtime.DetectionIntervalSeconds <= 0 {
			runtime.DetectionIntervalSeconds = defaults.DetectionIntervalSeconds
		}
		if runtime.DetectionTimeoutSeconds <= 0 {
			runtime.DetectionTimeoutSeconds = defaults.DetectionTimeoutSeconds
		}
		if runtime.MaxJobsPerDetection <= 0 {
			runtime.MaxJobsPerDetection = defaults.MaxJobsPerDetection
		}
		if runtime.GlobalExecutionConcurrency <= 0 {
			runtime.GlobalExecutionConcurrency = defaults.GlobalExecutionConcurrency
		}
		if runtime.PerWorkerExecutionConcurrency <= 0 {
			runtime.PerWorkerExecutionConcurrency = defaults.PerWorkerExecutionConcurrency
		}
		if runtime.JobTypeMaxRuntimeSeconds <= 0 {
			runtime.JobTypeMaxRuntimeSeconds = defaults.JobTypeMaxRuntimeSeconds
		}
		if runtime.RetryBackoffSeconds <= 0 {
			runtime.RetryBackoffSeconds = defaults.RetryBackoffSeconds
		}
		if runtime.RetryLimit < 0 {
			runtime.RetryLimit = defaults.RetryLimit
		}
	}
}

func isBlankStringConfigValue(value *plugin_pb.ConfigValue) bool {
	if value == nil {
		return true
	}
	kind, ok := value.Kind.(*plugin_pb.ConfigValue_StringValue)
	if !ok {
		return false
	}
	return strings.TrimSpace(kind.StringValue) == ""
}

func parsePositiveInt(raw string, defaultValue int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

// cloneConfigValueMap is now exported by the plugin package as CloneConfigValueMap
