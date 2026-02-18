package pluginworker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	balancetask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBalanceTimeoutSeconds = int32(10 * 60)
)

type volumeBalanceWorkerConfig struct {
	TaskConfig         *balancetask.Config
	TimeoutSeconds     int32
	ForceMove          bool
	MinIntervalSeconds int
}

// VolumeBalanceHandler is the plugin job handler for volume balancing.
type VolumeBalanceHandler struct {
	grpcDialOption grpc.DialOption
}

func NewVolumeBalanceHandler(grpcDialOption grpc.DialOption) *VolumeBalanceHandler {
	return &VolumeBalanceHandler{grpcDialOption: grpcDialOption}
}

func (h *VolumeBalanceHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "volume_balance",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "Volume Balance",
		Description:             "Moves volumes between servers to reduce skew in volume distribution",
	}
}

func (h *VolumeBalanceHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "volume_balance",
		DisplayName:       "Volume Balance",
		Description:       "Detect and execute volume moves to balance server load",
		Icon:              "fas fa-balance-scale",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "volume-balance-admin",
			Title:       "Volume Balance Admin Config",
			Description: "Admin-side controls for volume balance detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before balance detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only detect balance opportunities in this collection when set.",
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
			FormId:      "volume-balance-worker",
			Title:       "Volume Balance Worker Config",
			Description: "Worker-side balance thresholds and execution behavior.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Detection Thresholds",
					Description: "Controls for when balance jobs should be proposed.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "imbalance_threshold",
							Label:       "Imbalance Threshold",
							Description: "Detect when skew exceeds this ratio.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
						{
							Name:        "min_server_count",
							Label:       "Minimum Server Count",
							Description: "Require at least this many servers for balancing.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
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
					Description: "Execution defaults used when proposal payload omits balance params.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "timeout_seconds",
							Label:       "Move Timeout (s)",
							Description: "Maximum duration for one move operation.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "force_move",
							Label:       "Force Move",
							Description: "Force move execution when supported by the volume server.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"imbalance_threshold": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.2},
				},
				"min_server_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2},
				},
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 30 * 60},
				},
				"timeout_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(defaultBalanceTimeoutSeconds)},
				},
				"force_move": {
					Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: false},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      30 * 60,
			DetectionTimeoutSeconds:       120,
			MaxJobsPerDetection:           100,
			GlobalExecutionConcurrency:    1,
			PerWorkerExecutionConcurrency: 1,
			RetryLimit:                    1,
			RetryBackoffSeconds:           15,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"imbalance_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.2},
			},
			"min_server_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2},
			},
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 30 * 60},
			},
			"timeout_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(defaultBalanceTimeoutSeconds)},
			},
			"force_move": {
				Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: false},
			},
		},
	}
}

func (h *VolumeBalanceHandler) Detect(
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
	if request.JobType != "" && request.JobType != "volume_balance" {
		return fmt.Errorf("job type %q is not handled by volume_balance worker", request.JobType)
	}

	workerConfig := deriveBalanceWorkerConfig(request.GetWorkerConfigValues())
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "volume_balance",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "volume_balance",
			Success:        true,
			TotalProposals: 0,
		})
	}

	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	metrics, activeTopology, err := h.collectVolumeMetrics(ctx, masters, collectionFilter)
	if err != nil {
		return err
	}

	clusterInfo := &workertypes.ClusterInfo{ActiveTopology: activeTopology}
	results, err := balancetask.Detection(metrics, clusterInfo, workerConfig.TaskConfig)
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
		proposal, proposalErr := buildVolumeBalanceProposal(result, workerConfig)
		if proposalErr != nil {
			glog.Warningf("Plugin worker skip invalid volume_balance proposal: %v", proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "volume_balance",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "volume_balance",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *VolumeBalanceHandler) Execute(
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
	if request.Job.JobType != "" && request.Job.JobType != "volume_balance" {
		return fmt.Errorf("job type %q is not handled by volume_balance worker", request.Job.JobType)
	}

	params, err := decodeVolumeBalanceTaskParams(request.Job)
	if err != nil {
		return err
	}
	if len(params.Sources) == 0 || strings.TrimSpace(params.Sources[0].Node) == "" {
		return fmt.Errorf("volume balance source node is required")
	}
	if len(params.Targets) == 0 || strings.TrimSpace(params.Targets[0].Node) == "" {
		return fmt.Errorf("volume balance target node is required")
	}

	workerConfig := deriveBalanceWorkerConfig(request.GetWorkerConfigValues())
	applyBalanceExecutionDefaults(params, workerConfig)

	task := balancetask.NewBalanceTask(
		request.Job.JobId,
		params.Sources[0].Node,
		params.VolumeId,
		params.Collection,
	)
	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("balance progress %.0f%%", progress)
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
		Message:         "volume balance job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", "volume balance job accepted"),
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
	targetNode := params.Targets[0].Node
	resultSummary := fmt.Sprintf("volume %d moved from %s to %s", params.VolumeId, sourceNode, targetNode)

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
			buildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *VolumeBalanceHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	// Reuse the same master topology fetch/build flow used by the vacuum handler.
	helper := &VacuumHandler{grpcDialOption: h.grpcDialOption}
	return helper.collectVolumeMetrics(ctx, masterAddresses, collectionFilter)
}

func deriveBalanceWorkerConfig(values map[string]*plugin_pb.ConfigValue) *volumeBalanceWorkerConfig {
	taskConfig := balancetask.NewDefaultConfig()

	imbalanceThreshold := readDoubleConfig(values, "imbalance_threshold", taskConfig.ImbalanceThreshold)
	if imbalanceThreshold < 0 {
		imbalanceThreshold = 0
	}
	if imbalanceThreshold > 1 {
		imbalanceThreshold = 1
	}
	taskConfig.ImbalanceThreshold = imbalanceThreshold

	minServerCount := int(readInt64Config(values, "min_server_count", int64(taskConfig.MinServerCount)))
	if minServerCount < 2 {
		minServerCount = 2
	}
	taskConfig.MinServerCount = minServerCount

	timeoutSeconds := int32(readInt64Config(values, "timeout_seconds", int64(defaultBalanceTimeoutSeconds)))
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultBalanceTimeoutSeconds
	}

	minIntervalSeconds := int(readInt64Config(values, "min_interval_seconds", 0))
	if minIntervalSeconds < 0 {
		minIntervalSeconds = 0
	}

	return &volumeBalanceWorkerConfig{
		TaskConfig:         taskConfig,
		TimeoutSeconds:     timeoutSeconds,
		ForceMove:          readBoolConfig(values, "force_move", false),
		MinIntervalSeconds: minIntervalSeconds,
	}
}

func buildVolumeBalanceProposal(
	result *workertypes.TaskDetectionResult,
	workerConfig *volumeBalanceWorkerConfig,
) (*plugin_pb.JobProposal, error) {
	if result == nil {
		return nil, fmt.Errorf("task detection result is nil")
	}
	if result.TypedParams == nil {
		return nil, fmt.Errorf("missing typed params for volume %d", result.VolumeID)
	}
	if workerConfig == nil {
		workerConfig = deriveBalanceWorkerConfig(nil)
	}

	params := proto.Clone(result.TypedParams).(*worker_pb.TaskParams)
	applyBalanceExecutionDefaults(params, workerConfig)

	paramsPayload, err := proto.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal task params: %w", err)
	}

	proposalID := strings.TrimSpace(result.TaskID)
	if proposalID == "" {
		proposalID = fmt.Sprintf("volume-balance-%d-%d", result.VolumeID, time.Now().UnixNano())
	}

	dedupeKey := fmt.Sprintf("volume_balance:%d", result.VolumeID)
	if result.Collection != "" {
		dedupeKey += ":" + result.Collection
	}

	sourceNode := ""
	if len(params.Sources) > 0 {
		sourceNode = strings.TrimSpace(params.Sources[0].Node)
	}
	targetNode := ""
	if len(params.Targets) > 0 {
		targetNode = strings.TrimSpace(params.Targets[0].Node)
	}

	summary := fmt.Sprintf("Balance volume %d", result.VolumeID)
	if sourceNode != "" && targetNode != "" {
		summary = fmt.Sprintf("Move volume %d from %s to %s", result.VolumeID, sourceNode, targetNode)
	}

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "volume_balance",
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
			"task_type":     "balance",
			"volume_id":     fmt.Sprintf("%d", result.VolumeID),
			"collection":    result.Collection,
			"source_node":   sourceNode,
			"target_node":   targetNode,
			"source_server": sourceNode,
			"target_server": targetNode,
		},
	}, nil
}

func decodeVolumeBalanceTaskParams(job *plugin_pb.JobSpec) (*worker_pb.TaskParams, error) {
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
	targetNode := strings.TrimSpace(readStringConfig(job.Parameters, "target_server", ""))
	if targetNode == "" {
		targetNode = strings.TrimSpace(readStringConfig(job.Parameters, "target", ""))
	}
	collection := readStringConfig(job.Parameters, "collection", "")
	timeoutSeconds := int32(readInt64Config(job.Parameters, "timeout_seconds", int64(defaultBalanceTimeoutSeconds)))
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultBalanceTimeoutSeconds
	}
	forceMove := readBoolConfig(job.Parameters, "force_move", false)

	if volumeID <= 0 {
		return nil, fmt.Errorf("missing volume_id in job parameters")
	}
	if sourceNode == "" {
		return nil, fmt.Errorf("missing source_server in job parameters")
	}
	if targetNode == "" {
		return nil, fmt.Errorf("missing target_server in job parameters")
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
		Targets: []*worker_pb.TaskTarget{
			{
				Node:     targetNode,
				VolumeId: uint32(volumeID),
			},
		},
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      forceMove,
				TimeoutSeconds: timeoutSeconds,
			},
		},
	}, nil
}

func applyBalanceExecutionDefaults(params *worker_pb.TaskParams, workerConfig *volumeBalanceWorkerConfig) {
	if params == nil {
		return
	}
	if workerConfig == nil {
		workerConfig = deriveBalanceWorkerConfig(nil)
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		params.TaskParams = &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      workerConfig.ForceMove,
				TimeoutSeconds: workerConfig.TimeoutSeconds,
			},
		}
		return
	}

	if balanceParams.TimeoutSeconds <= 0 {
		balanceParams.TimeoutSeconds = workerConfig.TimeoutSeconds
	}
	if !balanceParams.ForceMove && workerConfig.ForceMove {
		balanceParams.ForceMove = true
	}
}

func readBoolConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback bool) bool {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_BoolValue:
		return kind.BoolValue
	case *plugin_pb.ConfigValue_Int64Value:
		return kind.Int64Value != 0
	case *plugin_pb.ConfigValue_DoubleValue:
		return kind.DoubleValue != 0
	case *plugin_pb.ConfigValue_StringValue:
		text := strings.TrimSpace(strings.ToLower(kind.StringValue))
		switch text {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}
