package pluginworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"google.golang.org/grpc"
)

const (
	defaultEcVacuumDeletedRatioThreshold = 0.2
	defaultEcVacuumMinIntervalSeconds    = 7 * 24 * 60 * 60
)

type ecVacuumWorkerConfig struct {
	DeletedRatioThreshold float64
	MinIntervalSeconds    int
}

type EcVacuumHandler struct {
	grpcDialOption grpc.DialOption
	workingDir     string
	executor       *ecVacuumExecutor
}

func NewEcVacuumHandler(grpcDialOption grpc.DialOption, workingDir string) *EcVacuumHandler {
	return &EcVacuumHandler{
		grpcDialOption: grpcDialOption,
		workingDir:     strings.TrimSpace(workingDir),
		executor:       newEcVacuumExecutor(grpcDialOption, workingDir),
	}
}

func (h *EcVacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "ec_vacuum",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "EC Vacuum",
		Description:             "Vacuum erasure-coded volumes with excessive deletions",
		Weight:                  70,
	}
}

func (h *EcVacuumHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "ec_vacuum",
		DisplayName:       "EC Vacuum",
		Description:       "Detect and vacuum erasure-coded volumes with high delete ratios",
		Icon:              "fas fa-broom",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-vacuum-admin",
			Title:       "EC Vacuum Admin Config",
			Description: "Admin-side controls for EC vacuum detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filter to restrict EC vacuum detection.",
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
			FormId:      "ec-vacuum-worker",
			Title:       "EC Vacuum Worker Config",
			Description: "Worker-side EC vacuum thresholds.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Thresholds",
					Description: "Detection thresholds and timing constraints.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "deleted_ratio_threshold",
							Label:       "Deleted Ratio Threshold",
							Description: "Detect EC volumes with deleted ratio >= threshold.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
						{
							Name:        "min_interval_seconds",
							Label:       "Min Interval (s)",
							Description: "Minimum interval between EC vacuum runs.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"deleted_ratio_threshold": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: defaultEcVacuumDeletedRatioThreshold},
				},
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultEcVacuumMinIntervalSeconds},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      2 * 60 * 60,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           200,
			GlobalExecutionConcurrency:    16,
			PerWorkerExecutionConcurrency: 4,
			RetryLimit:                    1,
			RetryBackoffSeconds:           30,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"deleted_ratio_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: defaultEcVacuumDeletedRatioThreshold},
			},
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultEcVacuumMinIntervalSeconds},
			},
		},
	}
}

func (h *EcVacuumHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != "ec_vacuum" {
		return fmt.Errorf("job type %q is not handled by ec_vacuum worker", request.JobType)
	}

	workerConfig := deriveEcVacuumWorkerConfig(request.GetWorkerConfigValues())
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		minInterval := time.Duration(workerConfig.MinIntervalSeconds) * time.Second
		_ = sender.SendActivity(buildDetectorActivity(
			"skipped_by_interval",
			fmt.Sprintf("EC VACUUM: Detection skipped due to min interval (%s)", minInterval),
			map[string]*plugin_pb.ConfigValue{
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(workerConfig.MinIntervalSeconds)},
				},
			},
		))
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "ec_vacuum",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "ec_vacuum",
			Success:        true,
			TotalProposals: 0,
		})
	}

	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	candidates, activeTopology, err := collectEcVolumeCandidatesFromMasters(ctx, masters, collectionFilter, h.grpcDialOption)
	if err != nil {
		return err
	}

	maxResults := int(request.MaxResults)
	if maxResults < 0 {
		maxResults = 0
	}

	var proposals []*plugin_pb.JobProposal
	skippedBelowThreshold := 0
	skippedMissingShards := 0
	failedVolumes := 0
	hasMore := false

	for _, candidate := range candidates {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if maxResults > 0 && len(proposals) >= maxResults {
			hasMore = true
			break
		}
		if activeTopology != nil && activeTopology.HasAnyTask(candidate.VolumeID) {
			continue
		}

		ratio, deletedCount, totalEntries, err := computeEcDeletionRatio(ctx, candidate, h.workingDir, h.grpcDialOption)
		if err != nil {
			failedVolumes++
			if strings.Contains(err.Error(), "missing EC shard") {
				skippedMissingShards++
			}
			glog.Warningf("EC vacuum detection failed for volume %d: %v", candidate.VolumeID, err)
			continue
		}
		if ratio < workerConfig.DeletedRatioThreshold {
			skippedBelowThreshold++
			continue
		}

		proposal, proposalErr := buildEcVacuumProposal(candidate, ratio, deletedCount, totalEntries)
		if proposalErr != nil {
			glog.Warningf("EC vacuum proposal skipped for volume %d: %v", candidate.VolumeID, proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if traceErr := emitEcVacuumDetectionSummary(sender, len(candidates), len(proposals), skippedBelowThreshold, skippedMissingShards, failedVolumes, workerConfig, hasMore); traceErr != nil {
		glog.Warningf("Plugin worker failed to emit ec_vacuum detection trace: %v", traceErr)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "ec_vacuum",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "ec_vacuum",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *EcVacuumHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != "ec_vacuum" {
		return fmt.Errorf("job type %q is not handled by ec_vacuum worker", request.Job.JobType)
	}

	volumeId, collection, err := decodeEcVacuumJob(request.Job)
	if err != nil {
		return err
	}

	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}
	if len(masters) == 0 {
		return fmt.Errorf("master grpc addresses are required for ec_vacuum execution")
	}

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "ec_vacuum job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", "ec_vacuum job accepted"),
		},
	}); err != nil {
		return err
	}

	progress := func(percent float64, stage, message string) {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: percent,
			Stage:           stage,
			Message:         message,
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity(stage, message),
			},
		})
	}

	if err := h.executor.Run(ctx, volumeId, collection, masters, progress); err != nil {
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

	resultSummary := fmt.Sprintf("EC vacuum completed for volume %d", volumeId)
	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: request.Job.JobType,
		Success: true,
		Result: &plugin_pb.JobResult{
			Summary: resultSummary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeId)},
				},
				"collection": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
	})
}

type ecVolumeCandidate struct {
	VolumeID       uint32
	Collection     string
	ShardLocations map[uint32][]pb.ServerAddress
	Servers        []pb.ServerAddress
	CanonicalNode  string
}

func collectEcVolumeCandidatesFromMasters(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
	grpcDialOption grpc.DialOption,
) ([]*ecVolumeCandidate, *topology.ActiveTopology, error) {
	if grpcDialOption == nil {
		return nil, nil, fmt.Errorf("grpc dial option is not configured")
	}
	if len(masterAddresses) == 0 {
		return nil, nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	for _, masterAddress := range masterAddresses {
		response, err := fetchVolumeList(ctx, masterAddress, grpcDialOption)
		if err != nil {
			glog.Warningf("Plugin worker failed master volume list at %s: %v", masterAddress, err)
			continue
		}
		candidates, activeTopology, buildErr := buildEcVolumeCandidates(response, collectionFilter)
		if buildErr != nil {
			glog.Warningf("Plugin worker failed to build EC metrics from master %s: %v", masterAddress, buildErr)
			continue
		}
		return candidates, activeTopology, nil
	}
	return nil, nil, fmt.Errorf("failed to load topology from all provided masters")
}

func buildEcVolumeCandidates(
	response *master_pb.VolumeListResponse,
	collectionFilter string,
) ([]*ecVolumeCandidate, *topology.ActiveTopology, error) {
	if response == nil || response.TopologyInfo == nil {
		return nil, nil, fmt.Errorf("volume list response has no topology info")
	}

	activeTopology := topology.NewActiveTopology(10)
	if err := activeTopology.UpdateTopology(response.TopologyInfo); err != nil {
		return nil, nil, err
	}

	patterns := wildcard.CompileWildcardMatchers(collectionFilter)
	byVolume := make(map[uint32]*ecVolumeCandidate)

	for _, dc := range response.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				serverAddr := pb.NewServerAddressFromDataNode(node)
				for _, diskInfo := range node.DiskInfos {
					for _, shardInfo := range diskInfo.EcShardInfos {
						if shardInfo == nil {
							continue
						}
						if len(patterns) > 0 && !wildcard.MatchesAnyWildcard(patterns, shardInfo.Collection) {
							continue
						}
						candidate := byVolume[shardInfo.Id]
						if candidate == nil {
							candidate = &ecVolumeCandidate{
								VolumeID:       shardInfo.Id,
								Collection:     shardInfo.Collection,
								ShardLocations: make(map[uint32][]pb.ServerAddress),
							}
							byVolume[shardInfo.Id] = candidate
						}
						if candidate.Collection == "" {
							candidate.Collection = shardInfo.Collection
						}
						candidate.Servers = appendUniqueServer(candidate.Servers, serverAddr)
						shards := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo)
						for _, shardId := range shards.Ids() {
							candidate.ShardLocations[uint32(shardId)] = appendUniqueServer(candidate.ShardLocations[uint32(shardId)], serverAddr)
						}
					}
				}
			}
		}
	}

	volumeIds := make([]int, 0, len(byVolume))
	for volumeId := range byVolume {
		volumeIds = append(volumeIds, int(volumeId))
	}
	sort.Ints(volumeIds)

	candidates := make([]*ecVolumeCandidate, 0, len(volumeIds))
	for _, volumeId := range volumeIds {
		candidate := byVolume[uint32(volumeId)]
		if candidate == nil {
			continue
		}
		sort.Slice(candidate.Servers, func(i, j int) bool {
			return candidate.Servers[i] < candidate.Servers[j]
		})
		if len(candidate.Servers) > 0 {
			candidate.CanonicalNode = string(candidate.Servers[0])
		}
		candidates = append(candidates, candidate)
	}
	return candidates, activeTopology, nil
}

func appendUniqueServer(servers []pb.ServerAddress, server pb.ServerAddress) []pb.ServerAddress {
	for _, existing := range servers {
		if existing == server {
			return servers
		}
	}
	return append(servers, server)
}

func computeEcDeletionRatio(
	ctx context.Context,
	candidate *ecVolumeCandidate,
	baseWorkingDir string,
	grpcDialOption grpc.DialOption,
) (float64, int, int, error) {
	if candidate == nil {
		return 0, 0, 0, fmt.Errorf("candidate is nil")
	}
	if len(candidate.Servers) == 0 {
		return 0, 0, 0, fmt.Errorf("no EC servers found for volume %d", candidate.VolumeID)
	}

	baseDir := filepath.Join(defaultEcVacuumWorkingDir(baseWorkingDir), "detect")
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return 0, 0, 0, err
	}

	tempDir, err := os.MkdirTemp(baseDir, fmt.Sprintf("vol_%d_", candidate.VolumeID))
	if err != nil {
		return 0, 0, 0, err
	}
	defer cleanupEcVacuumWorkingDir(tempDir)

	baseFileName := erasure_coding.EcShardFileName(candidate.Collection, tempDir, int(candidate.VolumeID))
	vifPath := baseFileName + ".vif"
	_, _ = copyEcFileFromAnyServer(ctx, candidate.Servers, candidate.Collection, candidate.VolumeID, ".vif", vifPath, grpcDialOption, true)
	_, shardConfig, err := loadEcShardConfigFromVif(vifPath)
	if err != nil {
		return 0, 0, 0, err
	}
	if err := ensureAllEcShardsPresent(candidate.ShardLocations, shardConfig.totalShards()); err != nil {
		return 0, 0, 0, err
	}

	ecxPath := baseFileName + ".ecx"
	if _, err := copyEcFileFromAnyServer(ctx, candidate.Servers, candidate.Collection, candidate.VolumeID, ".ecx", ecxPath, grpcDialOption, false); err != nil {
		return 0, 0, 0, err
	}
	totalEntries, err := countEcxEntries(ecxPath)
	if err != nil {
		return 0, 0, 0, err
	}

	deletedNeedles, err := collectDedupedEcj(ctx, candidate.Servers, candidate.Collection, candidate.VolumeID, tempDir, grpcDialOption)
	if err != nil {
		return 0, 0, 0, err
	}
	deletedCount := len(deletedNeedles)
	if totalEntries == 0 {
		return 0, deletedCount, totalEntries, nil
	}
	return float64(deletedCount) / float64(totalEntries), deletedCount, totalEntries, nil
}

func buildEcVacuumProposal(candidate *ecVolumeCandidate, ratio float64, deletedCount int, totalEntries int) (*plugin_pb.JobProposal, error) {
	if candidate == nil {
		return nil, fmt.Errorf("candidate is nil")
	}
	if candidate.VolumeID == 0 {
		return nil, fmt.Errorf("missing volume id")
	}
	proposalId := fmt.Sprintf("ec-vacuum-%d-%d", candidate.VolumeID, time.Now().UnixNano())
	dedupeKey := fmt.Sprintf("ec_vacuum:%d", candidate.VolumeID)
	if candidate.Collection != "" {
		dedupeKey += ":" + candidate.Collection
	}
	summary := fmt.Sprintf("EC vacuum volume %d (deleted %.1f%%)", candidate.VolumeID, ratio*100)
	detail := fmt.Sprintf("deleted %d of %d entries (%.1f%%)", deletedCount, totalEntries, ratio*100)

	return &plugin_pb.JobProposal{
		ProposalId: proposalId,
		DedupeKey:  dedupeKey,
		JobType:    "ec_vacuum",
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Summary:    summary,
		Detail:     detail,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(candidate.VolumeID)},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: candidate.Collection},
			},
			"deleted_ratio": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: ratio},
			},
			"deleted_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(deletedCount)},
			},
			"total_entries": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalEntries)},
			},
		},
		Labels: map[string]string{
			"task_type":      "ec_vacuum",
			"volume_id":      fmt.Sprintf("%d", candidate.VolumeID),
			"collection":     candidate.Collection,
			"deleted_ratio":  fmt.Sprintf("%.3f", ratio),
			"deleted_count":  fmt.Sprintf("%d", deletedCount),
			"total_entries":  fmt.Sprintf("%d", totalEntries),
			"source_node":    candidate.CanonicalNode,
			"candidate_type": "ec",
		},
	}, nil
}

func emitEcVacuumDetectionSummary(
	sender DetectionSender,
	totalVolumes int,
	selected int,
	skippedBelowThreshold int,
	skippedMissingShards int,
	failedVolumes int,
	workerConfig *ecVacuumWorkerConfig,
	hasMore bool,
) error {
	if sender == nil || workerConfig == nil {
		return nil
	}
	summary := fmt.Sprintf(
		"EC VACUUM: Created %d task(s) from %d volumes (skipped: %d below threshold, %d missing shards, %d errors)%s",
		selected,
		totalVolumes,
		skippedBelowThreshold,
		skippedMissingShards,
		failedVolumes,
		summarySuffix(hasMore, selected),
	)
	return sender.SendActivity(buildDetectorActivity("decision_summary", summary, map[string]*plugin_pb.ConfigValue{
		"total_volumes": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalVolumes)},
		},
		"selected_tasks": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(selected)},
		},
		"deleted_ratio_threshold": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: workerConfig.DeletedRatioThreshold},
		},
		"skipped_below_threshold": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(skippedBelowThreshold)},
		},
		"skipped_missing_shards": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(skippedMissingShards)},
		},
		"failed_volumes": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(failedVolumes)},
		},
	}))
}

func summarySuffix(hasMore bool, selected int) string {
	if !hasMore {
		return ""
	}
	return fmt.Sprintf(" (max_results reached after %d selections)", selected)
}

func decodeEcVacuumJob(job *plugin_pb.JobSpec) (uint32, string, error) {
	if job == nil {
		return 0, "", fmt.Errorf("job spec is nil")
	}
	volumeId := readInt64Config(job.Parameters, "volume_id", 0)
	if volumeId <= 0 {
		return 0, "", fmt.Errorf("missing volume_id in job parameters")
	}
	collection := strings.TrimSpace(readStringConfig(job.Parameters, "collection", ""))
	if collection == "" {
		return 0, "", fmt.Errorf("missing collection in job parameters")
	}
	return uint32(volumeId), collection, nil
}

func deriveEcVacuumWorkerConfig(values map[string]*plugin_pb.ConfigValue) *ecVacuumWorkerConfig {
	threshold := readDoubleConfig(values, "deleted_ratio_threshold", defaultEcVacuumDeletedRatioThreshold)
	if threshold < 0 {
		threshold = 0
	}
	if threshold > 1 {
		threshold = 1
	}
	minInterval := int(readInt64Config(values, "min_interval_seconds", int64(defaultEcVacuumMinIntervalSeconds)))
	if minInterval < 0 {
		minInterval = 0
	}
	return &ecVacuumWorkerConfig{
		DeletedRatioThreshold: threshold,
		MinIntervalSeconds:    minInterval,
	}
}
