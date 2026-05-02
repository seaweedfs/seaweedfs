package vacuum

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	defaultVacuumTaskBatchSize     = int32(1000)
	DefaultMaxExecutionConcurrency = int32(2)
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  "vacuum",
		Category: pluginworker.CategoryDefault,
		Aliases:  []string{"vol.vacuum", "volume.vacuum"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewVacuumHandler(opts.GrpcDialOption, int32(opts.MaxExecute)), nil
		},
	})
}

// VacuumHandler is the plugin job handler for vacuum job type.
type VacuumHandler struct {
	grpcDialOption          grpc.DialOption
	maxExecutionConcurrency int32
}

// NewVacuumHandler creates a VacuumHandler with the given gRPC dial option and
// maximum execution concurrency. When maxExecutionConcurrency is zero or
// negative, DefaultMaxExecutionConcurrency is used as the fallback.
func NewVacuumHandler(grpcDialOption grpc.DialOption, maxExecutionConcurrency int32) *VacuumHandler {
	return &VacuumHandler{grpcDialOption: grpcDialOption, maxExecutionConcurrency: maxExecutionConcurrency}
}

// Capability returns the job type capability for the vacuum handler.
// MaxExecutionConcurrency reflects the value passed at construction time,
// falling back to DefaultMaxExecutionConcurrency when unset.
func (h *VacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	maxExec := h.maxExecutionConcurrency
	if maxExec <= 0 {
		maxExec = DefaultMaxExecutionConcurrency
	}
	return &plugin_pb.JobTypeCapability{
		JobType:                 "vacuum",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: maxExec,
		DisplayName:             "Volume Vacuum",
		Description:             "Reclaims disk space by removing deleted files from volumes",
		Weight:                  60,
	}
}

func (h *VacuumHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "vacuum",
		DisplayName:       "Volume Vacuum",
		Description:       "Detect and vacuum volumes with high garbage ratio",
		Icon:              "fas fa-broom",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "vacuum-admin",
			Title:       "Vacuum Admin Config",
			Description: "Admin-side controls for vacuum detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filter to restrict detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Filter collections for vacuum detection. Use ALL_COLLECTIONS (default) to treat all volumes as one pool, EACH_COLLECTION to run detection separately per collection, or a regex pattern to match specific collections.",
							Placeholder: "ALL_COLLECTIONS",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "volume_state",
							Label:       "Volume State Filter",
							Description: "Filter volumes by state: ALL (default), ACTIVE (writable volumes below size limit), or FULL (read-only volumes above size limit).",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_ENUM,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_SELECT,
							Options: []*plugin_pb.ConfigOption{
								{Value: string(pluginworker.VolumeStateAll), Label: "All Volumes"},
								{Value: string(pluginworker.VolumeStateActive), Label: "Active (writable)"},
								{Value: string(pluginworker.VolumeStateFull), Label: "Full (read-only)"},
							},
						},
						{
							Name:        "data_center_filter",
							Label:       "Data Center Filter",
							Description: "Only vacuum volumes in matching data centers (comma-separated, wildcards supported). Leave empty for all.",
							Placeholder: "all data centers",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "rack_filter",
							Label:       "Rack Filter",
							Description: "Only vacuum volumes on matching racks (comma-separated, wildcards supported). Leave empty for all.",
							Placeholder: "all racks",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "node_filter",
							Label:       "Node Filter",
							Description: "Only vacuum volumes on matching nodes (comma-separated, wildcards supported). Leave empty for all.",
							Placeholder: "all nodes",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"collection_filter": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""},
				},
				"volume_state": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: string(pluginworker.VolumeStateAll)},
				},
				"data_center_filter": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""},
				},
				"rack_filter": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""},
				},
				"node_filter": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""},
				},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "vacuum-worker",
			Title:       "Vacuum Worker Config",
			Description: "Worker-side vacuum thresholds.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Thresholds",
					Description: "Detection thresholds and timing constraints.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "garbage_threshold",
							Label:       "Garbage Threshold",
							Description: "Detect volumes with garbage ratio >= threshold.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"garbage_threshold": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      17 * 60,
			DetectionTimeoutSeconds:       120,
			MaxJobsPerDetection:           200,
			GlobalExecutionConcurrency:    16,
			PerWorkerExecutionConcurrency: 4,
			RetryLimit:                    1,
			RetryBackoffSeconds:           10,
			JobTypeMaxRuntimeSeconds:      1800,
			ExecutionTimeoutSeconds:       1800,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"garbage_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
			},
		},
	}
}

func (h *VacuumHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != "vacuum" {
		return fmt.Errorf("job type %q is not handled by vacuum worker", request.JobType)
	}

	workerConfig := deriveVacuumConfig(request.GetWorkerConfigValues())
	collectionFilter := strings.TrimSpace(pluginworker.ReadStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}
	metrics, activeTopology, err := h.collectVolumeMetrics(ctx, masters, collectionFilter)
	if err != nil {
		return err
	}

	volState := pluginworker.VolumeState(strings.ToUpper(strings.TrimSpace(pluginworker.ReadStringConfig(request.GetAdminConfigValues(), "volume_state", string(pluginworker.VolumeStateAll)))))
	metrics = pluginworker.FilterMetricsByVolumeState(metrics, volState)

	dataCenterFilter := strings.TrimSpace(pluginworker.ReadStringConfig(request.GetAdminConfigValues(), "data_center_filter", ""))
	rackFilter := strings.TrimSpace(pluginworker.ReadStringConfig(request.GetAdminConfigValues(), "rack_filter", ""))
	nodeFilter := strings.TrimSpace(pluginworker.ReadStringConfig(request.GetAdminConfigValues(), "node_filter", ""))

	if dataCenterFilter != "" || rackFilter != "" || nodeFilter != "" {
		metrics = pluginworker.FilterMetricsByLocation(metrics, dataCenterFilter, rackFilter, nodeFilter)
	}

	clusterInfo := &workertypes.ClusterInfo{ActiveTopology: activeTopology}
	results, err := Detection(metrics, clusterInfo, workerConfig)
	if err != nil {
		return err
	}
	if traceErr := emitVacuumDetectionDecisionTrace(sender, metrics, workerConfig, results); traceErr != nil {
		glog.Warningf("Plugin worker failed to emit vacuum detection trace: %v", traceErr)
	}

	maxResults := int(request.MaxResults)
	hasMore := false
	if maxResults > 0 && len(results) > maxResults {
		hasMore = true
		results = results[:maxResults]
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(results))
	for _, result := range results {
		proposal, proposalErr := buildVacuumProposal(result)
		if proposalErr != nil {
			glog.Warningf("Plugin worker skip invalid vacuum proposal: %v", proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "vacuum",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "vacuum",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func emitVacuumDetectionDecisionTrace(
	sender pluginworker.DetectionSender,
	metrics []*workertypes.VolumeHealthMetrics,
	workerConfig *Config,
	results []*workertypes.TaskDetectionResult,
) error {
	if sender == nil || workerConfig == nil {
		return nil
	}

	totalVolumes := len(metrics)

	skippedDueToGarbage := 0
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		if metric.GarbageRatio >= workerConfig.GarbageThreshold {
			continue
		}
		skippedDueToGarbage++
	}

	summaryMessage := ""
	summaryStage := "decision_summary"
	if len(results) == 0 {
		summaryMessage = fmt.Sprintf(
			"VACUUM: No tasks created for %d volumes. Threshold=%.2f%%. Skipped: %d (garbage<threshold)",
			totalVolumes,
			workerConfig.GarbageThreshold*100,
			skippedDueToGarbage,
		)
	} else {
		summaryMessage = fmt.Sprintf(
			"VACUUM: Created %d task(s) from %d volumes. Threshold=%.2f%%",
			len(results),
			totalVolumes,
			workerConfig.GarbageThreshold*100,
		)
	}

	if err := sender.SendActivity(pluginworker.BuildDetectorActivity(summaryStage, summaryMessage, map[string]*plugin_pb.ConfigValue{
		"total_volumes": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalVolumes)},
		},
		"selected_tasks": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(results))},
		},
		"garbage_threshold_percent": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: workerConfig.GarbageThreshold * 100},
		},
		"skipped_garbage": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(skippedDueToGarbage)},
		},
	})); err != nil {
		return err
	}

	limit := 3
	if len(metrics) < limit {
		limit = len(metrics)
	}
	for i := 0; i < limit; i++ {
		metric := metrics[i]
		if metric == nil {
			continue
		}
		message := fmt.Sprintf(
			"VACUUM: Volume %d: garbage=%.2f%% (need ≥%.2f%%)",
			metric.VolumeID,
			metric.GarbageRatio*100,
			workerConfig.GarbageThreshold*100,
		)
		if err := sender.SendActivity(pluginworker.BuildDetectorActivity("decision_volume", message, map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(metric.VolumeID)},
			},
			"garbage_percent": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: metric.GarbageRatio * 100},
			},
			"required_garbage_percent": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: workerConfig.GarbageThreshold * 100},
			},
		})); err != nil {
			return err
		}
	}

	return nil
}

func (h *VacuumHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != "vacuum" {
		return fmt.Errorf("job type %q is not handled by vacuum worker", request.Job.JobType)
	}

	params, err := decodeVacuumTaskParams(request.Job)
	if err != nil {
		return err
	}
	if len(params.Sources) == 0 || strings.TrimSpace(params.Sources[0].Node) == "" {
		return fmt.Errorf("vacuum task source node is required")
	}

	workerConfig := deriveVacuumConfig(request.GetWorkerConfigValues())
	if vacuumParams := params.GetVacuumParams(); vacuumParams != nil {
		if vacuumParams.GarbageThreshold <= 0 {
			vacuumParams.GarbageThreshold = workerConfig.GarbageThreshold
		}
	} else {
		params.TaskParams = &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: workerConfig.GarbageThreshold,
				BatchSize:        defaultVacuumTaskBatchSize,
				VerifyChecksum:   true,
			},
		}
	}

	task := NewVacuumTask(
		request.Job.JobId,
		params.Sources[0].Node,
		params.VolumeId,
		params.Collection,
		h.grpcDialOption,
	)
	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("vacuum progress %.0f%%", progress)
		if strings.TrimSpace(stage) != "" {
			message = stage
		}
		if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           stage,
			Message:         message,
			Activities: []*plugin_pb.ActivityEvent{
				pluginworker.BuildExecutorActivity(stage, message),
			},
		}); err != nil {
			execCancel()
		}
	})

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "vacuum job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			pluginworker.BuildExecutorActivity("assigned", "vacuum job accepted"),
		},
	}); err != nil {
		return err
	}

	if err := task.Execute(execCtx, params); err != nil {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_FAILED,
			ProgressPercent: 100,
			Stage:           "failed",
			Message:         err.Error(),
			Activities: []*plugin_pb.ActivityEvent{
				pluginworker.BuildExecutorActivity("failed", err.Error()),
			},
		})
		return err
	}

	resultSummary := fmt.Sprintf("vacuum completed for volume %d", params.VolumeId)
	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: request.Job.JobType,
		Success: true,
		Result: &plugin_pb.JobResult{
			Summary: resultSummary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(params.VolumeId)},
				},
				"server": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: params.Sources[0].Node},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			pluginworker.BuildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *VacuumHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	metrics, activeTopology, _, err := pluginworker.CollectVolumeMetricsFromMasters(ctx, masterAddresses, collectionFilter, h.grpcDialOption)
	return metrics, activeTopology, err
}

func deriveVacuumConfig(values map[string]*plugin_pb.ConfigValue) *Config {
	config := NewDefaultConfig()
	config.GarbageThreshold = pluginworker.ReadDoubleConfig(values, "garbage_threshold", config.GarbageThreshold)
	config.MinVolumeAgeSeconds = 0 // plugin worker does not filter by volume age
	return config
}

func buildVacuumProposal(result *workertypes.TaskDetectionResult) (*plugin_pb.JobProposal, error) {
	if result == nil {
		return nil, fmt.Errorf("task detection result is nil")
	}
	if result.TypedParams == nil {
		return nil, fmt.Errorf("missing typed params for volume %d", result.VolumeID)
	}

	paramsPayload, err := proto.Marshal(result.TypedParams)
	if err != nil {
		return nil, fmt.Errorf("marshal task params: %w", err)
	}

	proposalID := strings.TrimSpace(result.TaskID)
	if proposalID == "" {
		proposalID = fmt.Sprintf("vacuum-%d-%d", result.VolumeID, time.Now().UnixNano())
	}

	dedupeKey := fmt.Sprintf("vacuum:%d", result.VolumeID)
	if result.Collection != "" {
		dedupeKey = dedupeKey + ":" + result.Collection
	}

	summary := fmt.Sprintf("Vacuum volume %d", result.VolumeID)
	if strings.TrimSpace(result.Server) != "" {
		summary = summary + " on " + result.Server
	}

	// Estimate runtime: 5 min/GB (compact + commit + overhead)
	volumeSizeGB := int64(result.TypedParams.VolumeSize/1024/1024/1024) + 1
	estimatedRuntimeSeconds := volumeSizeGB * 5 * 60

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "vacuum",
		Priority:   pluginworker.MapTaskPriority(result.Priority),
		Summary:    summary,
		Detail:     strings.TrimSpace(result.Reason),
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {
				Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: paramsPayload},
			},
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(result.VolumeID)},
			},
			"server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Server},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Collection},
			},
			"estimated_runtime_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: estimatedRuntimeSeconds},
			},
		},
		Labels: map[string]string{
			"task_type":   "vacuum",
			"volume_id":   fmt.Sprintf("%d", result.VolumeID),
			"collection":  result.Collection,
			"source_node": result.Server,
		},
	}, nil
}

func decodeVacuumTaskParams(job *plugin_pb.JobSpec) (*worker_pb.TaskParams, error) {
	if job == nil {
		return nil, fmt.Errorf("job spec is nil")
	}

	if payload := pluginworker.ReadBytesConfig(job.Parameters, "task_params_pb"); len(payload) > 0 {
		params := &worker_pb.TaskParams{}
		if err := proto.Unmarshal(payload, params); err != nil {
			return nil, fmt.Errorf("unmarshal task_params_pb: %w", err)
		}
		if params.TaskId == "" {
			params.TaskId = job.JobId
		}
		return params, nil
	}

	volumeID := pluginworker.ReadInt64Config(job.Parameters, "volume_id", 0)
	server := pluginworker.ReadStringConfig(job.Parameters, "server", "")
	collection := pluginworker.ReadStringConfig(job.Parameters, "collection", "")
	if volumeID <= 0 {
		return nil, fmt.Errorf("missing volume_id in job parameters")
	}
	if strings.TrimSpace(server) == "" {
		return nil, fmt.Errorf("missing server in job parameters")
	}

	return &worker_pb.TaskParams{
		TaskId:     job.JobId,
		VolumeId:   uint32(volumeID),
		Collection: collection,
		Sources: []*worker_pb.TaskSource{
			{
				Node:     server,
				VolumeId: uint32(volumeID),
			},
		},
		TaskParams: &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: 0.3,
				BatchSize:        defaultVacuumTaskBatchSize,
				VerifyChecksum:   true,
			},
		},
	}, nil
}
