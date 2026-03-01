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
	dedupeKey := fmt.Sprintf("erasure_coding:ec_vacuum:%d", candidate.VolumeID)
	if candidate.Collection != "" {
		dedupeKey += ":" + candidate.Collection
	}
	summary := fmt.Sprintf("EC vacuum volume %d (deleted %.1f%%)", candidate.VolumeID, ratio*100)
	detail := fmt.Sprintf("deleted %d of %d entries (%.1f%%)", deletedCount, totalEntries, ratio*100)

	return &plugin_pb.JobProposal{
		ProposalId: proposalId,
		DedupeKey:  dedupeKey,
		JobType:    "erasure_coding",
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
			"task_type": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "ec_vacuum"},
			},
			"ec_vacuum": {
				Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true},
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
	return sender.SendActivity(buildDetectorActivity("ec_vacuum_summary", summary, map[string]*plugin_pb.ConfigValue{
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
	threshold := readDoubleConfig(values, "ec_deleted_ratio_threshold", defaultEcVacuumDeletedRatioThreshold)
	if threshold < 0 {
		threshold = 0
	}
	if threshold > 1 {
		threshold = 1
	}
	minInterval := int(readInt64Config(values, "ec_min_interval_seconds", int64(defaultEcVacuumMinIntervalSeconds)))
	if minInterval < 0 {
		minInterval = 0
	}
	return &ecVacuumWorkerConfig{
		DeletedRatioThreshold: threshold,
		MinIntervalSeconds:    minInterval,
	}
}

func isEcVacuumJob(job *plugin_pb.JobSpec) bool {
	if job == nil || job.Parameters == nil {
		return false
	}
	if value := job.Parameters["ec_vacuum"]; value != nil {
		if kind, ok := value.Kind.(*plugin_pb.ConfigValue_BoolValue); ok {
			return kind.BoolValue
		}
	}
	if value := job.Parameters["task_type"]; value != nil {
		if kind, ok := value.Kind.(*plugin_pb.ConfigValue_StringValue); ok {
			return strings.EqualFold(strings.TrimSpace(kind.StringValue), "ec_vacuum")
		}
	}
	return false
}
