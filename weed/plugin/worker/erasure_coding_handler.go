package pluginworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	erasurecodingtask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type erasureCodingWorkerConfig struct {
	TaskConfig         *erasurecodingtask.Config
	WorkingDir         string
	CleanupSource      bool
	MinIntervalSeconds int
}

// ErasureCodingHandler is the plugin job handler for erasure coding.
type ErasureCodingHandler struct {
	grpcDialOption grpc.DialOption
}

func NewErasureCodingHandler(grpcDialOption grpc.DialOption) *ErasureCodingHandler {
	return &ErasureCodingHandler{grpcDialOption: grpcDialOption}
}

func (h *ErasureCodingHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "erasure_coding",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "Erasure Coding",
		Description:             "Converts full and quiet volumes into EC shards",
	}
}

func (h *ErasureCodingHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	defaultWorkingDir := defaultErasureCodingWorkingDir()
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "erasure_coding",
		DisplayName:       "Erasure Coding",
		Description:       "Detect and execute erasure coding for suitable volumes",
		Icon:              "fas fa-shield-alt",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "erasure-coding-admin",
			Title:       "Erasure Coding Admin Config",
			Description: "Admin-side controls for erasure coding detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before erasure coding detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only detect erasure coding opportunities in this collection when set.",
							Placeholder: "all collections",
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
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "erasure-coding-worker",
			Title:       "Erasure Coding Worker Config",
			Description: "Worker-side detection thresholds and execution defaults.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Detection Thresholds",
					Description: "Controls for when erasure coding jobs should be proposed.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "quiet_for_seconds",
							Label:       "Quiet Period (s)",
							Description: "Volume must remain unmodified for at least this duration.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        "fullness_ratio",
							Label:       "Fullness Ratio",
							Description: "Minimum volume fullness ratio to trigger erasure coding.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
						{
							Name:        "min_size_mb",
							Label:       "Minimum Volume Size (MB)",
							Description: "Only volumes larger than this size are considered.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "min_interval_seconds",
							Label:       "Minimum Detection Interval (s)",
							Description: "Skip detection if the last successful run is more recent than this interval.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
				{
					SectionId:   "execution",
					Title:       "Execution",
					Description: "Execution defaults for erasure coding jobs.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "working_dir",
							Label:       "Working Directory",
							Description: "Directory used for local volume and shard files during EC execution.",
							Placeholder: defaultWorkingDir,
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "cleanup_source",
							Label:       "Cleanup Source",
							Description: "Whether to cleanup source replicas after successful EC conversion.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"quiet_for_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 300},
				},
				"fullness_ratio": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.8},
				},
				"min_size_mb": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 30},
				},
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 60 * 60},
				},
				"working_dir": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultWorkingDir},
				},
				"cleanup_source": {
					Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      60 * 60,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           50,
			GlobalExecutionConcurrency:    1,
			PerWorkerExecutionConcurrency: 1,
			RetryLimit:                    1,
			RetryBackoffSeconds:           30,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"quiet_for_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 300},
			},
			"fullness_ratio": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.8},
			},
			"min_size_mb": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 30},
			},
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 60 * 60},
			},
			"working_dir": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultWorkingDir},
			},
			"cleanup_source": {
				Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true},
			},
		},
	}
}

func (h *ErasureCodingHandler) Detect(
	ctx context.Context,
	request *plugin_pb.RunDetectionRequest,
	sender DetectionSender,
) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != "erasure_coding" {
		return fmt.Errorf("job type %q is not handled by erasure_coding worker", request.JobType)
	}

	workerConfig := deriveErasureCodingWorkerConfig(request.GetWorkerConfigValues())
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "erasure_coding",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "erasure_coding",
			Success:        true,
			TotalProposals: 0,
		})
	}

	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	if collectionFilter != "" {
		workerConfig.TaskConfig.CollectionFilter = collectionFilter
	}

	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	metrics, activeTopology, err := h.collectVolumeMetrics(ctx, masters, collectionFilter)
	if err != nil {
		return err
	}

	clusterInfo := &workertypes.ClusterInfo{ActiveTopology: activeTopology}
	results, err := erasurecodingtask.Detection(metrics, clusterInfo, workerConfig.TaskConfig)
	if err != nil {
		return err
	}

	maxResults := int(request.MaxResults)
	hasMore := false
	if maxResults > 0 && len(results) > maxResults {
		hasMore = true
		results = results[:maxResults]
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(results))
	for _, result := range results {
		proposal, proposalErr := buildErasureCodingProposal(result, workerConfig)
		if proposalErr != nil {
			glog.Warningf("Plugin worker skip invalid erasure_coding proposal: %v", proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "erasure_coding",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "erasure_coding",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *ErasureCodingHandler) Execute(
	ctx context.Context,
	request *plugin_pb.ExecuteJobRequest,
	sender ExecutionSender,
) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != "erasure_coding" {
		return fmt.Errorf("job type %q is not handled by erasure_coding worker", request.Job.JobType)
	}

	params, err := decodeErasureCodingTaskParams(request.Job)
	if err != nil {
		return err
	}

	workerConfig := deriveErasureCodingWorkerConfig(request.GetWorkerConfigValues())
	applyErasureCodingExecutionDefaults(params, workerConfig, request.GetClusterContext())

	if len(params.Sources) == 0 || strings.TrimSpace(params.Sources[0].Node) == "" {
		return fmt.Errorf("erasure coding source node is required")
	}
	if len(params.Targets) == 0 {
		return fmt.Errorf("erasure coding targets are required")
	}

	task := erasurecodingtask.NewErasureCodingTask(
		request.Job.JobId,
		params.Sources[0].Node,
		params.VolumeId,
		params.Collection,
	)
	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("erasure coding progress %.0f%%", progress)
		if strings.TrimSpace(stage) != "" {
			message = stage
		}
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           stage,
			Message:         message,
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity(stage, message),
			},
		})
	})

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "erasure coding job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", "erasure coding job accepted"),
		},
	}); err != nil {
		return err
	}

	if err := task.Execute(ctx, params); err != nil {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_FAILED,
			ProgressPercent: 100,
			Stage:           "failed",
			Message:         err.Error(),
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity("failed", err.Error()),
			},
		})
		return err
	}

	sourceNode := params.Sources[0].Node
	resultSummary := fmt.Sprintf("erasure coding completed for volume %d across %d targets", params.VolumeId, len(params.Targets))

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
				"source_server": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: sourceNode},
				},
				"target_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(params.Targets))},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *ErasureCodingHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	// Reuse the same master topology fetch/build flow used by the vacuum handler.
	helper := &VacuumHandler{grpcDialOption: h.grpcDialOption}
	return helper.collectVolumeMetrics(ctx, masterAddresses, collectionFilter)
}

func deriveErasureCodingWorkerConfig(values map[string]*plugin_pb.ConfigValue) *erasureCodingWorkerConfig {
	taskConfig := erasurecodingtask.NewDefaultConfig()

	quietForSeconds := int(readInt64Config(values, "quiet_for_seconds", int64(taskConfig.QuietForSeconds)))
	if quietForSeconds < 0 {
		quietForSeconds = 0
	}
	taskConfig.QuietForSeconds = quietForSeconds

	fullnessRatio := readDoubleConfig(values, "fullness_ratio", taskConfig.FullnessRatio)
	if fullnessRatio < 0 {
		fullnessRatio = 0
	}
	if fullnessRatio > 1 {
		fullnessRatio = 1
	}
	taskConfig.FullnessRatio = fullnessRatio

	minSizeMB := int(readInt64Config(values, "min_size_mb", int64(taskConfig.MinSizeMB)))
	if minSizeMB < 1 {
		minSizeMB = 1
	}
	taskConfig.MinSizeMB = minSizeMB

	workingDir := strings.TrimSpace(readStringConfig(values, "working_dir", defaultErasureCodingWorkingDir()))
	if workingDir == "" {
		workingDir = defaultErasureCodingWorkingDir()
	}

	minIntervalSeconds := int(readInt64Config(values, "min_interval_seconds", 60*60))
	if minIntervalSeconds < 0 {
		minIntervalSeconds = 0
	}

	return &erasureCodingWorkerConfig{
		TaskConfig:         taskConfig,
		WorkingDir:         workingDir,
		CleanupSource:      readBoolConfig(values, "cleanup_source", true),
		MinIntervalSeconds: minIntervalSeconds,
	}
}

func buildErasureCodingProposal(
	result *workertypes.TaskDetectionResult,
	workerConfig *erasureCodingWorkerConfig,
) (*plugin_pb.JobProposal, error) {
	if result == nil {
		return nil, fmt.Errorf("task detection result is nil")
	}
	if result.TypedParams == nil {
		return nil, fmt.Errorf("missing typed params for volume %d", result.VolumeID)
	}
	if workerConfig == nil {
		workerConfig = deriveErasureCodingWorkerConfig(nil)
	}

	params := proto.Clone(result.TypedParams).(*worker_pb.TaskParams)
	applyErasureCodingExecutionDefaults(params, workerConfig, nil)

	paramsPayload, err := proto.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal task params: %w", err)
	}

	proposalID := strings.TrimSpace(result.TaskID)
	if proposalID == "" {
		proposalID = fmt.Sprintf("erasure-coding-%d-%d", result.VolumeID, time.Now().UnixNano())
	}

	dedupeKey := fmt.Sprintf("erasure_coding:%d", result.VolumeID)
	if result.Collection != "" {
		dedupeKey += ":" + result.Collection
	}

	sourceNode := ""
	if len(params.Sources) > 0 {
		sourceNode = strings.TrimSpace(params.Sources[0].Node)
	}

	summary := fmt.Sprintf("Erasure code volume %d", result.VolumeID)
	if sourceNode != "" {
		summary = fmt.Sprintf("Erasure code volume %d from %s", result.VolumeID, sourceNode)
	}

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "erasure_coding",
		Priority:   mapTaskPriority(result.Priority),
		Summary:    summary,
		Detail:     strings.TrimSpace(result.Reason),
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {
				Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: paramsPayload},
			},
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(result.VolumeID)},
			},
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: sourceNode},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Collection},
			},
			"target_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(params.Targets))},
			},
		},
		Labels: map[string]string{
			"task_type":    "erasure_coding",
			"volume_id":    fmt.Sprintf("%d", result.VolumeID),
			"collection":   result.Collection,
			"source_node":  sourceNode,
			"target_count": fmt.Sprintf("%d", len(params.Targets)),
		},
	}, nil
}

func decodeErasureCodingTaskParams(job *plugin_pb.JobSpec) (*worker_pb.TaskParams, error) {
	if job == nil {
		return nil, fmt.Errorf("job spec is nil")
	}

	if payload := readBytesConfig(job.Parameters, "task_params_pb"); len(payload) > 0 {
		params := &worker_pb.TaskParams{}
		if err := proto.Unmarshal(payload, params); err != nil {
			return nil, fmt.Errorf("unmarshal task_params_pb: %w", err)
		}
		if params.TaskId == "" {
			params.TaskId = job.JobId
		}
		return params, nil
	}

	volumeID := readInt64Config(job.Parameters, "volume_id", 0)
	sourceNode := strings.TrimSpace(readStringConfig(job.Parameters, "source_server", ""))
	if sourceNode == "" {
		sourceNode = strings.TrimSpace(readStringConfig(job.Parameters, "server", ""))
	}
	targetServers := readStringListConfig(job.Parameters, "target_servers")
	if len(targetServers) == 0 {
		targetServers = readStringListConfig(job.Parameters, "targets")
	}
	collection := readStringConfig(job.Parameters, "collection", "")

	dataShards := int32(readInt64Config(job.Parameters, "data_shards", int64(ecstorage.DataShardsCount)))
	if dataShards <= 0 {
		dataShards = ecstorage.DataShardsCount
	}
	parityShards := int32(readInt64Config(job.Parameters, "parity_shards", int64(ecstorage.ParityShardsCount)))
	if parityShards <= 0 {
		parityShards = ecstorage.ParityShardsCount
	}
	totalShards := int(dataShards + parityShards)

	workingDir := strings.TrimSpace(readStringConfig(job.Parameters, "working_dir", ""))
	cleanupSource := readBoolConfig(job.Parameters, "cleanup_source", true)

	if volumeID <= 0 {
		return nil, fmt.Errorf("missing volume_id in job parameters")
	}
	if sourceNode == "" {
		return nil, fmt.Errorf("missing source_server in job parameters")
	}
	if len(targetServers) == 0 {
		return nil, fmt.Errorf("missing target_servers in job parameters")
	}
	if len(targetServers) < totalShards {
		return nil, fmt.Errorf("insufficient target_servers: got %d, need at least %d", len(targetServers), totalShards)
	}

	shardAssignments := assignECShardIDs(totalShards, len(targetServers))
	targets := make([]*worker_pb.TaskTarget, 0, len(targetServers))
	for i := 0; i < len(targetServers); i++ {
		targetNode := strings.TrimSpace(targetServers[i])
		if targetNode == "" {
			continue
		}
		targets = append(targets, &worker_pb.TaskTarget{
			Node:     targetNode,
			VolumeId: uint32(volumeID),
			ShardIds: shardAssignments[i],
		})
	}
	if len(targets) < totalShards {
		return nil, fmt.Errorf("insufficient non-empty target_servers after normalization: got %d, need at least %d", len(targets), totalShards)
	}

	return &worker_pb.TaskParams{
		TaskId:     job.JobId,
		VolumeId:   uint32(volumeID),
		Collection: collection,
		Sources: []*worker_pb.TaskSource{
			{
				Node:     sourceNode,
				VolumeId: uint32(volumeID),
			},
		},
		Targets: targets,
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:    dataShards,
				ParityShards:  parityShards,
				WorkingDir:    workingDir,
				CleanupSource: cleanupSource,
			},
		},
	}, nil
}

func applyErasureCodingExecutionDefaults(
	params *worker_pb.TaskParams,
	workerConfig *erasureCodingWorkerConfig,
	clusterContext *plugin_pb.ClusterContext,
) {
	if params == nil {
		return
	}
	if workerConfig == nil {
		workerConfig = deriveErasureCodingWorkerConfig(nil)
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		ecParams = &worker_pb.ErasureCodingTaskParams{
			DataShards:    ecstorage.DataShardsCount,
			ParityShards:  ecstorage.ParityShardsCount,
			WorkingDir:    workerConfig.WorkingDir,
			CleanupSource: workerConfig.CleanupSource,
		}
		params.TaskParams = &worker_pb.TaskParams_ErasureCodingParams{ErasureCodingParams: ecParams}
	}

	if ecParams.DataShards <= 0 {
		ecParams.DataShards = ecstorage.DataShardsCount
	}
	if ecParams.ParityShards <= 0 {
		ecParams.ParityShards = ecstorage.ParityShardsCount
	}
	if strings.TrimSpace(ecParams.WorkingDir) == "" {
		ecParams.WorkingDir = workerConfig.WorkingDir
	}
	if !ecParams.CleanupSource && workerConfig.CleanupSource {
		ecParams.CleanupSource = true
	}
	if strings.TrimSpace(ecParams.MasterClient) == "" && clusterContext != nil && len(clusterContext.MasterGrpcAddresses) > 0 {
		ecParams.MasterClient = clusterContext.MasterGrpcAddresses[0]
	}

	totalShards := int(ecParams.DataShards + ecParams.ParityShards)
	if totalShards <= 0 {
		totalShards = ecstorage.TotalShardsCount
	}
	needsShardAssignment := false
	for _, target := range params.Targets {
		if target == nil || len(target.ShardIds) == 0 {
			needsShardAssignment = true
			break
		}
	}
	if needsShardAssignment && len(params.Targets) > 0 {
		assignments := assignECShardIDs(totalShards, len(params.Targets))
		for i := 0; i < len(params.Targets); i++ {
			if params.Targets[i] == nil {
				continue
			}
			if len(params.Targets[i].ShardIds) == 0 {
				params.Targets[i].ShardIds = assignments[i]
			}
		}
	}
}

func readStringListConfig(values map[string]*plugin_pb.ConfigValue, field string) []string {
	if values == nil {
		return nil
	}
	value := values[field]
	if value == nil {
		return nil
	}

	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringList:
		return normalizeStringList(kind.StringList.GetValues())
	case *plugin_pb.ConfigValue_ListValue:
		out := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			itemText := readStringFromConfigValue(item)
			if itemText != "" {
				out = append(out, itemText)
			}
		}
		return normalizeStringList(out)
	case *plugin_pb.ConfigValue_StringValue:
		return normalizeStringList(strings.Split(kind.StringValue, ","))
	}

	return nil
}

func readStringFromConfigValue(value *plugin_pb.ConfigValue) string {
	if value == nil {
		return ""
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringValue:
		return strings.TrimSpace(kind.StringValue)
	case *plugin_pb.ConfigValue_Int64Value:
		return fmt.Sprintf("%d", kind.Int64Value)
	case *plugin_pb.ConfigValue_DoubleValue:
		return fmt.Sprintf("%g", kind.DoubleValue)
	case *plugin_pb.ConfigValue_BoolValue:
		if kind.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

func normalizeStringList(values []string) []string {
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		item := strings.TrimSpace(value)
		if item == "" {
			continue
		}
		if _, found := seen[item]; found {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	return normalized
}

func assignECShardIDs(totalShards int, targetCount int) [][]uint32 {
	if targetCount <= 0 {
		return nil
	}
	if totalShards <= 0 {
		totalShards = ecstorage.TotalShardsCount
	}

	assignments := make([][]uint32, targetCount)
	for i := 0; i < totalShards; i++ {
		targetIndex := i % targetCount
		assignments[targetIndex] = append(assignments[targetIndex], uint32(i))
	}
	return assignments
}

func defaultErasureCodingWorkingDir() string {
	return filepath.Join(os.TempDir(), "seaweedfs-ec")
}
