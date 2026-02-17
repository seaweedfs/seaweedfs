package pluginworker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	vacuumtask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultVacuumTaskBatchSize = int32(1000)
)

// VacuumHandler is the plugin job handler for vacuum job type.
type VacuumHandler struct {
	grpcDialOption grpc.DialOption
}

func NewVacuumHandler(grpcDialOption grpc.DialOption) *VacuumHandler {
	return &VacuumHandler{grpcDialOption: grpcDialOption}
}

func (h *VacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "vacuum",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 2,
		DisplayName:             "Volume Vacuum",
		Description:             "Reclaims disk space by removing deleted files from volumes",
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
							Description: "Only scan this collection when set.",
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
						{
							Name:        "min_volume_age_seconds",
							Label:       "Min Volume Age (s)",
							Description: "Only detect volumes older than this age.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        "min_interval_seconds",
							Label:       "Min Interval (s)",
							Description: "Minimum interval between vacuum on the same volume.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"garbage_threshold": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
				},
				"min_volume_age_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 24 * 60 * 60},
				},
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7 * 24 * 60 * 60},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      2 * 60 * 60,
			DetectionTimeoutSeconds:       120,
			MaxJobsPerDetection:           200,
			GlobalExecutionConcurrency:    2,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           10,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"garbage_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
			},
			"min_volume_age_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 24 * 60 * 60},
			},
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7 * 24 * 60 * 60},
			},
		},
	}
}

func (h *VacuumHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
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
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "vacuum",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "vacuum",
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
	results, err := vacuumtask.Detection(metrics, clusterInfo, workerConfig)
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

func (h *VacuumHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
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

	task := vacuumtask.NewVacuumTask(
		request.Job.JobId,
		params.Sources[0].Node,
		params.VolumeId,
		params.Collection,
	)
	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("vacuum progress %.0f%%", progress)
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
		})
	})

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "vacuum job accepted",
	}); err != nil {
		return err
	}

	if err := task.Execute(ctx, params); err != nil {
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
	})
}

func (h *VacuumHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	if h.grpcDialOption == nil {
		return nil, nil, fmt.Errorf("grpc dial option is not configured")
	}
	if len(masterAddresses) == 0 {
		return nil, nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	for _, masterAddress := range masterAddresses {
		response, err := h.fetchVolumeList(ctx, masterAddress)
		if err != nil {
			glog.Warningf("Plugin worker failed master volume list at %s: %v", masterAddress, err)
			continue
		}

		metrics, activeTopology, buildErr := buildVolumeMetrics(response, collectionFilter)
		if buildErr != nil {
			glog.Warningf("Plugin worker failed to build metrics from master %s: %v", masterAddress, buildErr)
			continue
		}
		return metrics, activeTopology, nil
	}

	return nil, nil, fmt.Errorf("failed to load topology from all provided masters")
}

func (h *VacuumHandler) fetchVolumeList(ctx context.Context, address string) (*master_pb.VolumeListResponse, error) {
	var lastErr error
	for _, candidate := range masterAddressCandidates(address) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
		conn, err := pb.GrpcDial(dialCtx, candidate, false, h.grpcDialOption)
		cancelDial()
		if err != nil {
			lastErr = err
			continue
		}

		client := master_pb.NewSeaweedClient(conn)
		callCtx, cancelCall := context.WithTimeout(ctx, 10*time.Second)
		response, callErr := client.VolumeList(callCtx, &master_pb.VolumeListRequest{})
		cancelCall()
		_ = conn.Close()

		if callErr == nil {
			return response, nil
		}
		lastErr = callErr
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no valid master address candidate")
	}
	return nil, lastErr
}

func deriveVacuumConfig(values map[string]*plugin_pb.ConfigValue) *vacuumtask.Config {
	config := vacuumtask.NewDefaultConfig()
	config.GarbageThreshold = readDoubleConfig(values, "garbage_threshold", config.GarbageThreshold)
	config.MinVolumeAgeSeconds = int(readInt64Config(values, "min_volume_age_seconds", int64(config.MinVolumeAgeSeconds)))
	config.MinIntervalSeconds = int(readInt64Config(values, "min_interval_seconds", int64(config.MinIntervalSeconds)))
	return config
}

func buildVolumeMetrics(
	response *master_pb.VolumeListResponse,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	if response == nil || response.TopologyInfo == nil {
		return nil, nil, fmt.Errorf("volume list response has no topology info")
	}

	activeTopology := topology.NewActiveTopology(10)
	if err := activeTopology.UpdateTopology(response.TopologyInfo); err != nil {
		return nil, nil, err
	}

	filter := strings.TrimSpace(collectionFilter)
	volumeSizeLimitBytes := uint64(response.VolumeSizeLimitMb) * 1024 * 1024
	now := time.Now()
	metrics := make([]*workertypes.VolumeHealthMetrics, 0, 256)

	for _, dc := range response.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType, diskInfo := range node.DiskInfos {
					for _, volume := range diskInfo.VolumeInfos {
						if filter != "" && volume.Collection != filter {
							continue
						}

						metric := &workertypes.VolumeHealthMetrics{
							VolumeID:         volume.Id,
							Server:           node.Id,
							ServerAddress:    node.Address,
							DiskType:         diskType,
							DiskId:           volume.DiskId,
							DataCenter:       dc.Id,
							Rack:             rack.Id,
							Collection:       volume.Collection,
							Size:             volume.Size,
							DeletedBytes:     volume.DeletedByteCount,
							LastModified:     time.Unix(volume.ModifiedAtSecond, 0),
							ReplicaCount:     1,
							ExpectedReplicas: int(volume.ReplicaPlacement),
							IsReadOnly:       volume.ReadOnly,
						}
						if metric.Size > 0 {
							metric.GarbageRatio = float64(metric.DeletedBytes) / float64(metric.Size)
						}
						if volumeSizeLimitBytes > 0 {
							metric.FullnessRatio = float64(metric.Size) / float64(volumeSizeLimitBytes)
						}
						metric.Age = now.Sub(metric.LastModified)
						metrics = append(metrics, metric)
					}
				}
			}
		}
	}

	replicaCounts := make(map[uint32]int)
	for _, metric := range metrics {
		replicaCounts[metric.VolumeID]++
	}
	for _, metric := range metrics {
		metric.ReplicaCount = replicaCounts[metric.VolumeID]
	}

	return metrics, activeTopology, nil
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

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "vacuum",
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
			"server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Server},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: result.Collection},
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
	server := readStringConfig(job.Parameters, "server", "")
	collection := readStringConfig(job.Parameters, "collection", "")
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

func readStringConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback string) string {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringValue:
		return kind.StringValue
	case *plugin_pb.ConfigValue_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10)
	case *plugin_pb.ConfigValue_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'f', -1, 64)
	case *plugin_pb.ConfigValue_BoolValue:
		return strconv.FormatBool(kind.BoolValue)
	}
	return fallback
}

func readDoubleConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback float64) float64 {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_DoubleValue:
		return kind.DoubleValue
	case *plugin_pb.ConfigValue_Int64Value:
		return float64(kind.Int64Value)
	case *plugin_pb.ConfigValue_StringValue:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(kind.StringValue), 64)
		if err == nil {
			return parsed
		}
	case *plugin_pb.ConfigValue_BoolValue:
		if kind.BoolValue {
			return 1
		}
		return 0
	}
	return fallback
}

func readInt64Config(values map[string]*plugin_pb.ConfigValue, field string, fallback int64) int64 {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_Int64Value:
		return kind.Int64Value
	case *plugin_pb.ConfigValue_DoubleValue:
		return int64(kind.DoubleValue)
	case *plugin_pb.ConfigValue_StringValue:
		parsed, err := strconv.ParseInt(strings.TrimSpace(kind.StringValue), 10, 64)
		if err == nil {
			return parsed
		}
	case *plugin_pb.ConfigValue_BoolValue:
		if kind.BoolValue {
			return 1
		}
		return 0
	}
	return fallback
}

func readBytesConfig(values map[string]*plugin_pb.ConfigValue, field string) []byte {
	if values == nil {
		return nil
	}
	value := values[field]
	if value == nil {
		return nil
	}
	if kind, ok := value.Kind.(*plugin_pb.ConfigValue_BytesValue); ok {
		return kind.BytesValue
	}
	return nil
}

func mapTaskPriority(priority workertypes.TaskPriority) plugin_pb.JobPriority {
	switch strings.ToLower(string(priority)) {
	case "low":
		return plugin_pb.JobPriority_JOB_PRIORITY_LOW
	case "medium", "normal":
		return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
	case "high":
		return plugin_pb.JobPriority_JOB_PRIORITY_HIGH
	case "critical":
		return plugin_pb.JobPriority_JOB_PRIORITY_CRITICAL
	default:
		return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
	}
}

func masterAddressCandidates(address string) []string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return nil
	}
	candidateSet := map[string]struct{}{
		trimmed: {},
	}
	converted := pb.ServerToGrpcAddress(trimmed)
	candidateSet[converted] = struct{}{}

	candidates := make([]string, 0, len(candidateSet))
	for candidate := range candidateSet {
		candidates = append(candidates, candidate)
	}
	sort.Strings(candidates)
	return candidates
}

func shouldSkipDetectionByInterval(lastSuccessfulRun *timestamppb.Timestamp, minIntervalSeconds int) bool {
	if lastSuccessfulRun == nil || minIntervalSeconds <= 0 {
		return false
	}
	lastRun := lastSuccessfulRun.AsTime()
	if lastRun.IsZero() {
		return false
	}
	return time.Since(lastRun) < time.Duration(minIntervalSeconds)*time.Second
}
