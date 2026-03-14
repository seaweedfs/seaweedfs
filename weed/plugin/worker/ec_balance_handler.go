package pluginworker

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	ecbalancetask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_balance"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	ecBalanceMinImbalanceThreshold = 0.05
	ecBalanceMaxImbalanceThreshold = 0.5
	ecBalanceMinServerCount        = 2
)

func init() {
	RegisterHandler(HandlerFactory{
		JobType:  "ec_balance",
		Category: CategoryDefault,
		Aliases:  []string{"ec-balance", "ec.balance", "ec_shard_balance"},
		Build: func(opts HandlerBuildOptions) (JobHandler, error) {
			return NewECBalanceHandler(opts.GrpcDialOption), nil
		},
	})
}

type ecBalanceWorkerConfig struct {
	TaskConfig         *ecbalancetask.Config
	MinIntervalSeconds int
}

// ECBalanceHandler is the plugin job handler for EC shard balancing.
type ECBalanceHandler struct {
	grpcDialOption grpc.DialOption
}

func NewECBalanceHandler(grpcDialOption grpc.DialOption) *ECBalanceHandler {
	return &ECBalanceHandler{grpcDialOption: grpcDialOption}
}

func (h *ECBalanceHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "ec_balance",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 3,
		DisplayName:             "EC Shard Balance",
		Description:             "Balance EC shard distribution across racks and nodes",
		Weight:                  60,
	}
}

func (h *ECBalanceHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "ec_balance",
		DisplayName:       "EC Shard Balance",
		Description:       "Detect and execute EC shard rebalancing across the cluster",
		Icon:              "fas fa-balance-scale-left",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-balance-admin",
			Title:       "EC Shard Balance Admin Config",
			Description: "Admin-side controls for EC shard balance detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before EC shard balance detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only balance EC shards in matching collections (wildcard supported).",
							Placeholder: "all collections",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "data_center_filter",
							Label:       "Data Center Filter",
							Description: "Only balance within matching data centers (wildcard supported).",
							Placeholder: "all data centers",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "disk_type",
							Label:       "Disk Type",
							Description: "Only balance EC shards on this disk type (hdd, ssd, or empty for all).",
							Placeholder: "all disk types",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"collection_filter":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"data_center_filter": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"disk_type":          {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-balance-worker",
			Title:       "EC Shard Balance Worker Config",
			Description: "Worker-side detection thresholds and execution controls.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Detection Thresholds",
					Description: "Controls for when EC shard balance jobs should be proposed.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "imbalance_threshold",
							Label:       "Imbalance Threshold",
							Description: "Minimum shard count imbalance ratio to trigger balancing (0.0-1.0).",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: ecBalanceMinImbalanceThreshold}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: ecBalanceMaxImbalanceThreshold}},
						},
						{
							Name:        "min_server_count",
							Label:       "Minimum Server Count",
							Description: "Minimum servers required for EC shard balancing.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: ecBalanceMinServerCount}},
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
						{
							Name:        "preferred_tags",
							Label:       "Preferred Tags",
							Description: "Comma-separated disk tags to prioritize for shard placement, ordered by preference.",
							Placeholder: "fast,ssd",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"imbalance_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.2}},
				"min_server_count":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 3}},
				"min_interval_seconds": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 60 * 60}},
				"preferred_tags":      {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      60 * 30,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           500,
			GlobalExecutionConcurrency:    16,
			PerWorkerExecutionConcurrency: 4,
			RetryLimit:                    1,
			RetryBackoffSeconds:           30,
			JobTypeMaxRuntimeSeconds:      1800,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"imbalance_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.2}},
			"min_server_count":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 3}},
			"min_interval_seconds": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 60 * 60}},
			"preferred_tags":      {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
		},
	}
}

func (h *ECBalanceHandler) Detect(
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
	if request.JobType != "" && request.JobType != "ec_balance" {
		return fmt.Errorf("job type %q is not handled by ec_balance worker", request.JobType)
	}

	workerConfig := deriveECBalanceWorkerConfig(request.GetWorkerConfigValues())
	if ShouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		minInterval := time.Duration(workerConfig.MinIntervalSeconds) * time.Second
		_ = sender.SendActivity(BuildDetectorActivity(
			"skipped_by_interval",
			fmt.Sprintf("EC BALANCE: Detection skipped due to min interval (%s)", minInterval),
			map[string]*plugin_pb.ConfigValue{
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(workerConfig.MinIntervalSeconds)},
				},
			},
		))
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "ec_balance",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "ec_balance",
			Success:        true,
			TotalProposals: 0,
		})
	}

	// Apply admin-side scope filters
	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	if collectionFilter != "" {
		workerConfig.TaskConfig.CollectionFilter = collectionFilter
	}
	dcFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "data_center_filter", ""))
	if dcFilter != "" {
		workerConfig.TaskConfig.DataCenterFilter = dcFilter
	}
	diskType := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "disk_type", ""))
	if diskType != "" {
		workerConfig.TaskConfig.DiskType = diskType
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
	maxResults := int(request.MaxResults)
	if maxResults < 0 {
		maxResults = 0
	}

	results, hasMore, err := ecbalancetask.Detection(ctx, metrics, clusterInfo, workerConfig.TaskConfig, maxResults)
	if err != nil {
		return err
	}

	if traceErr := emitECBalanceDecisionTrace(sender, workerConfig.TaskConfig, results, maxResults, hasMore); traceErr != nil {
		glog.Warningf("Plugin worker failed to emit ec_balance detection trace: %v", traceErr)
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(results))
	for _, result := range results {
		proposal, proposalErr := buildECBalanceProposal(result)
		if proposalErr != nil {
			glog.Warningf("Plugin worker skip invalid ec_balance proposal: %v", proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "ec_balance",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "ec_balance",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *ECBalanceHandler) Execute(
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
	if request.Job.JobType != "" && request.Job.JobType != "ec_balance" {
		return fmt.Errorf("job type %q is not handled by ec_balance worker", request.Job.JobType)
	}

	params, err := decodeECBalanceTaskParams(request.Job)
	if err != nil {
		return err
	}

	if len(params.Sources) == 0 || strings.TrimSpace(params.Sources[0].Node) == "" {
		return fmt.Errorf("ec balance source node is required")
	}
	if len(params.Targets) == 0 || strings.TrimSpace(params.Targets[0].Node) == "" {
		return fmt.Errorf("ec balance target node is required")
	}

	task := ecbalancetask.NewECBalanceTask(
		request.Job.JobId,
		params.VolumeId,
		params.Collection,
		h.grpcDialOption,
	)

	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()

	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("ec balance progress %.0f%%", progress)
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
				BuildExecutorActivity(stage, message),
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
		Message:         "ec balance job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			BuildExecutorActivity("assigned", "ec balance job accepted"),
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
				BuildExecutorActivity("failed", err.Error()),
			},
		})
		return err
	}

	sourceNode := params.Sources[0].Node
	targetNode := params.Targets[0].Node
	resultSummary := fmt.Sprintf("EC shard balance completed: volume %d shards moved from %s to %s",
		params.VolumeId, sourceNode, targetNode)

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
				"target_server": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: targetNode},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			BuildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *ECBalanceHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	metrics, activeTopology, _, err := collectVolumeMetricsFromMasters(ctx, masterAddresses, collectionFilter, h.grpcDialOption)
	return metrics, activeTopology, err
}

func deriveECBalanceWorkerConfig(values map[string]*plugin_pb.ConfigValue) *ecBalanceWorkerConfig {
	taskConfig := ecbalancetask.NewDefaultConfig()

	imbalanceThreshold := readDoubleConfig(values, "imbalance_threshold", taskConfig.ImbalanceThreshold)
	if imbalanceThreshold < ecBalanceMinImbalanceThreshold {
		imbalanceThreshold = ecBalanceMinImbalanceThreshold
	}
	if imbalanceThreshold > ecBalanceMaxImbalanceThreshold {
		imbalanceThreshold = ecBalanceMaxImbalanceThreshold
	}
	taskConfig.ImbalanceThreshold = imbalanceThreshold

	minServerCountRaw := readInt64Config(values, "min_server_count", int64(taskConfig.MinServerCount))
	if minServerCountRaw > math.MaxInt32 {
		minServerCountRaw = math.MaxInt32
	}
	minServerCount := int(minServerCountRaw)
	if minServerCount < ecBalanceMinServerCount {
		minServerCount = ecBalanceMinServerCount
	}
	taskConfig.MinServerCount = minServerCount

	minIntervalRaw := readInt64Config(values, "min_interval_seconds", 60*60)
	if minIntervalRaw > math.MaxInt32 {
		minIntervalRaw = math.MaxInt32
	}
	minIntervalSeconds := int(minIntervalRaw)
	if minIntervalSeconds < 0 {
		minIntervalSeconds = 0
	}

	taskConfig.PreferredTags = util.NormalizeTagList(readStringListConfig(values, "preferred_tags"))

	return &ecBalanceWorkerConfig{
		TaskConfig:         taskConfig,
		MinIntervalSeconds: minIntervalSeconds,
	}
}

func buildECBalanceProposal(result *workertypes.TaskDetectionResult) (*plugin_pb.JobProposal, error) {
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
		proposalID = fmt.Sprintf("ec-balance-%d-%d", result.VolumeID, time.Now().UnixNano())
	}

	// Dedupe key includes volume ID and shard ID(s)
	dedupeKey := fmt.Sprintf("ec_balance:%d", result.VolumeID)
	if len(result.TypedParams.Sources) > 0 && len(result.TypedParams.Sources[0].ShardIds) > 0 {
		dedupeKey = fmt.Sprintf("ec_balance:%d:%d", result.VolumeID, result.TypedParams.Sources[0].ShardIds[0])
	}
	if result.Collection != "" {
		dedupeKey += ":" + result.Collection
	}

	sourceNode := ""
	targetNode := ""
	if len(result.TypedParams.Sources) > 0 {
		sourceNode = strings.TrimSpace(result.TypedParams.Sources[0].Node)
	}
	if len(result.TypedParams.Targets) > 0 {
		targetNode = strings.TrimSpace(result.TypedParams.Targets[0].Node)
	}

	summary := fmt.Sprintf("Balance EC shard of volume %d", result.VolumeID)
	if sourceNode != "" && targetNode != "" {
		summary = fmt.Sprintf("Move EC shard of volume %d: %s → %s", result.VolumeID, sourceNode, targetNode)
	}

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "ec_balance",
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
			"target_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: targetNode},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Collection},
			},
		},
		Labels: map[string]string{
			"task_type":     "ec_balance",
			"volume_id":     fmt.Sprintf("%d", result.VolumeID),
			"collection":    result.Collection,
			"source_node":   sourceNode,
			"target_node":   targetNode,
		},
	}, nil
}

func decodeECBalanceTaskParams(job *plugin_pb.JobSpec) (*worker_pb.TaskParams, error) {
	if job == nil {
		return nil, fmt.Errorf("job spec is nil")
	}

	// Try protobuf-encoded params first
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

	// Fallback: construct from individual parameters
	volumeID := readInt64Config(job.Parameters, "volume_id", 0)
	sourceNode := strings.TrimSpace(readStringConfig(job.Parameters, "source_server", ""))
	targetNode := strings.TrimSpace(readStringConfig(job.Parameters, "target_server", ""))
	collection := readStringConfig(job.Parameters, "collection", "")

	if volumeID <= 0 || volumeID > math.MaxUint32 {
		return nil, fmt.Errorf("invalid volume_id in job parameters: %d", volumeID)
	}
	if sourceNode == "" {
		return nil, fmt.Errorf("missing source_server in job parameters")
	}
	if targetNode == "" {
		return nil, fmt.Errorf("missing target_server in job parameters")
	}

	shardIDVal, hasShardID := job.Parameters["shard_id"]
	if !hasShardID || shardIDVal == nil {
		return nil, fmt.Errorf("missing shard_id in job parameters")
	}
	shardID := readInt64Config(job.Parameters, "shard_id", -1)
	if shardID < 0 || shardID > math.MaxUint32 {
		return nil, fmt.Errorf("invalid shard_id in job parameters: %d", shardID)
	}

	return &worker_pb.TaskParams{
		TaskId:     job.JobId,
		VolumeId:   uint32(volumeID),
		Collection: collection,
		Sources: []*worker_pb.TaskSource{{
			Node:     sourceNode,
			ShardIds: []uint32{uint32(shardID)},
		}},
		Targets: []*worker_pb.TaskTarget{{
			Node:     targetNode,
			ShardIds: []uint32{uint32(shardID)},
		}},
		TaskParams: &worker_pb.TaskParams_EcBalanceParams{
			EcBalanceParams: &worker_pb.EcBalanceTaskParams{
				TimeoutSeconds: 600,
			},
		},
	}, nil
}

func emitECBalanceDecisionTrace(
	sender DetectionSender,
	taskConfig *ecbalancetask.Config,
	results []*workertypes.TaskDetectionResult,
	maxResults int,
	hasMore bool,
) error {
	if sender == nil || taskConfig == nil {
		return nil
	}

	// Count moves by phase
	phaseCounts := make(map[string]int)
	for _, result := range results {
		if result.Reason != "" {
			// Extract phase from reason string
			for _, phase := range []string{"dedup", "cross_rack", "within_rack", "global"} {
				if strings.Contains(result.Reason, phase) {
					phaseCounts[phase]++
					break
				}
			}
		}
	}

	summarySuffix := ""
	if hasMore {
		summarySuffix = fmt.Sprintf(" (max_results=%d reached)", maxResults)
	}

	summaryMessage := fmt.Sprintf(
		"EC balance detection: %d moves proposed%s (dedup=%d, cross_rack=%d, within_rack=%d, global=%d)",
		len(results),
		summarySuffix,
		phaseCounts["dedup"],
		phaseCounts["cross_rack"],
		phaseCounts["within_rack"],
		phaseCounts["global"],
	)

	return sender.SendActivity(BuildDetectorActivity("decision_summary", summaryMessage, map[string]*plugin_pb.ConfigValue{
		"total_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(results))},
		},
		"has_more": {
			Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: hasMore},
		},
		"dedup_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(phaseCounts["dedup"])},
		},
		"cross_rack_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(phaseCounts["cross_rack"])},
		},
		"within_rack_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(phaseCounts["within_rack"])},
		},
		"global_moves": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(phaseCounts["global"])},
		},
		"imbalance_threshold": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: taskConfig.ImbalanceThreshold},
		},
		"min_server_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(taskConfig.MinServerCount)},
		},
	}))
}
