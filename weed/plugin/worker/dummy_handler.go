package pluginworker

import (
	"context"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
)

const (
	dummyStressJobType        = "dummy_stress"
	dummyStressExecutionDelay = 2 * time.Second
)

type dummyVolumeSnapshot struct {
	VolumeID     uint32
	Collection   string
	Size         uint64
	DeletedBytes uint64
	ReplicaCount int
	ECShardCount int
	FoundRegular bool
}

// DummyStressHandler provides a synthetic detector/executor pair for stress testing.
type DummyStressHandler struct {
	grpcDialOption   grpc.DialOption
	fetchVolumeList  func(context.Context, []string) (*master_pb.VolumeListResponse, error)
	sleepWithContext func(context.Context, time.Duration) error

	selectionMu     sync.Mutex
	nextSelectStart int
}

func NewDummyStressHandler(grpcDialOption grpc.DialOption) *DummyStressHandler {
	return &DummyStressHandler{grpcDialOption: grpcDialOption}
}

func (h *DummyStressHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 dummyStressJobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 4,
		DisplayName:             "Dummy Stress",
		Description:             "Synthetic detector/executor for plugin scheduling stress tests",
	}
}

func (h *DummyStressHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           dummyStressJobType,
		DisplayName:       "Dummy Stress",
		Description:       "Scans volume IDs and creates synthetic execution jobs for stress testing",
		Icon:              "fas fa-flask",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "dummy-stress-admin",
			Title:       "Dummy Stress Admin Config",
			Description: "Optional filters for synthetic job generation.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filter to limit generated jobs by collection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only include these collections (comma-separated) when set.",
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
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      10,
			DetectionTimeoutSeconds:       30,
			MaxJobsPerDetection:           5000,
			GlobalExecutionConcurrency:    8,
			PerWorkerExecutionConcurrency: 8,
			RetryLimit:                    0,
			RetryBackoffSeconds:           1,
		},
	}
}

func (h *DummyStressHandler) Detect(
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
	if request.JobType != "" && request.JobType != dummyStressJobType {
		return fmt.Errorf("job type %q is not handled by %s worker", request.JobType, dummyStressJobType)
	}

	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}
	volumeList, err := h.loadVolumeList(ctx, masters)
	if err != nil {
		return err
	}

	collectionFilter := readStringConfig(request.GetAdminConfigValues(), "collection_filter", "")
	volumeIDs := collectDummyVolumeIDs(volumeList, collectionFilter)
	totalVolumeIDs := len(volumeIDs)

	maxResults := int(request.MaxResults)
	selectedVolumeIDs, hasMore := h.selectVolumeIDs(volumeIDs, maxResults)

	if err := sender.SendActivity(buildDetectorActivity(
		"decision_summary",
		fmt.Sprintf(
			"DUMMY STRESS: generated %d proposal(s) from %d volume IDs (max_results=%d)",
			len(selectedVolumeIDs),
			totalVolumeIDs,
			maxResults,
		),
		map[string]*plugin_pb.ConfigValue{
			"total_volume_ids": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalVolumeIDs)},
			},
			"selected_proposals": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(selectedVolumeIDs))},
			},
			"has_more": {
				Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: hasMore},
			},
			"next_selection_start_index": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(h.currentSelectionStart())},
			},
		},
	)); err != nil {
		return err
	}

	previewCount := 3
	if len(selectedVolumeIDs) < previewCount {
		previewCount = len(selectedVolumeIDs)
	}
	for i := 0; i < previewCount; i++ {
		volumeID := selectedVolumeIDs[i]
		if err := sender.SendActivity(buildDetectorActivity(
			"decision_volume",
			fmt.Sprintf("DUMMY STRESS: selected volume %d for synthetic execution", volumeID),
			map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
				},
			},
		)); err != nil {
			return err
		}
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(selectedVolumeIDs))
	for _, volumeID := range selectedVolumeIDs {
		proposalID := fmt.Sprintf("dummy-stress-%d", volumeID)
		proposals = append(proposals, &plugin_pb.JobProposal{
			ProposalId: proposalID,
			DedupeKey:  fmt.Sprintf("%s:%d", dummyStressJobType, volumeID),
			JobType:    dummyStressJobType,
			Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
			Summary:    fmt.Sprintf("Dummy stress task for volume %d", volumeID),
			Detail:     "Synthetic stress-test task generated from volume topology",
			Parameters: map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
				},
			},
			Labels: map[string]string{
				"task_type": dummyStressJobType,
				"volume_id": fmt.Sprintf("%d", volumeID),
			},
		})
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   dummyStressJobType,
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        dummyStressJobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *DummyStressHandler) selectVolumeIDs(volumeIDs []uint32, maxResults int) ([]uint32, bool) {
	if len(volumeIDs) == 0 {
		h.selectionMu.Lock()
		h.nextSelectStart = 0
		h.selectionMu.Unlock()
		return nil, false
	}
	if maxResults <= 0 || maxResults >= len(volumeIDs) {
		out := append(make([]uint32, 0, len(volumeIDs)), volumeIDs...)
		h.selectionMu.Lock()
		h.nextSelectStart = 0
		h.selectionMu.Unlock()
		return out, false
	}

	h.selectionMu.Lock()
	start := h.nextSelectStart % len(volumeIDs)
	selected := make([]uint32, 0, maxResults)
	for i := 0; i < maxResults; i++ {
		idx := (start + i) % len(volumeIDs)
		selected = append(selected, volumeIDs[idx])
	}
	h.nextSelectStart = (start + maxResults) % len(volumeIDs)
	h.selectionMu.Unlock()

	return selected, true
}

func (h *DummyStressHandler) currentSelectionStart() int {
	h.selectionMu.Lock()
	defer h.selectionMu.Unlock()
	return h.nextSelectStart
}

func (h *DummyStressHandler) Execute(
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
	if request.Job.JobType != "" && request.Job.JobType != dummyStressJobType {
		return fmt.Errorf("job type %q is not handled by %s worker", request.Job.JobType, dummyStressJobType)
	}

	volumeID := readInt64Config(request.Job.Parameters, "volume_id", 0)
	if volumeID <= 0 {
		return fmt.Errorf("missing volume_id in job parameters")
	}

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         "dummy stress job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", "dummy stress job accepted"),
		},
	}); err != nil {
		return err
	}

	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}
	volumeList, err := h.loadVolumeList(ctx, masters)
	if err != nil {
		return err
	}

	snapshot := collectDummyVolumeSnapshot(volumeList, uint32(volumeID))
	infoMessage := formatDummyExecutionInfo(snapshot)
	glog.Infof("DUMMY STRESS execute job=%s %s", request.Job.JobId, infoMessage)

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 30,
		Stage:           "inspect",
		Message:         infoMessage,
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("inspect", infoMessage),
		},
	}); err != nil {
		return err
	}

	if err := h.sleep(ctx, dummyStressExecutionDelay); err != nil {
		return err
	}

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 90,
		Stage:           "finishing",
		Message:         "dummy stress work complete",
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("finishing", "dummy stress work complete"),
		},
	}); err != nil {
		return err
	}

	resultSummary := fmt.Sprintf("Dummy stress execution finished for volume %d", volumeID)
	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: request.Job.JobType,
		Success: true,
		Result: &plugin_pb.JobResult{
			Summary: resultSummary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: volumeID},
				},
				"collection": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: snapshot.Collection},
				},
				"size_bytes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(snapshot.Size)},
				},
				"deleted_bytes": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(snapshot.DeletedBytes)},
				},
				"replica_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(snapshot.ReplicaCount)},
				},
				"ec_shard_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(snapshot.ECShardCount)},
				},
				"found_regular": {
					Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: snapshot.FoundRegular},
				},
				"sleep_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(dummyStressExecutionDelay / time.Second)},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *DummyStressHandler) loadVolumeList(
	ctx context.Context,
	masterAddresses []string,
) (*master_pb.VolumeListResponse, error) {
	if h.fetchVolumeList != nil {
		return h.fetchVolumeList(ctx, masterAddresses)
	}

	if len(masterAddresses) == 0 {
		return nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	helper := &VacuumHandler{grpcDialOption: h.grpcDialOption}
	var lastErr error
	for _, masterAddress := range masterAddresses {
		response, err := helper.fetchVolumeList(ctx, masterAddress)
		if err != nil {
			lastErr = err
			glog.Warningf("Dummy stress worker failed master volume list at %s: %v", masterAddress, err)
			continue
		}
		return response, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("failed to load topology from all provided masters")
	}
	return nil, lastErr
}

func (h *DummyStressHandler) sleep(ctx context.Context, duration time.Duration) error {
	if h.sleepWithContext != nil {
		return h.sleepWithContext(ctx, duration)
	}
	if duration <= 0 {
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func collectDummyVolumeIDs(response *master_pb.VolumeListResponse, collectionFilter string) []uint32 {
	if response == nil || response.TopologyInfo == nil {
		return nil
	}

	allowedCollections := parseCollectionFilterSet(collectionFilter)
	volumeIDSet := make(map[uint32]struct{})

	for _, dc := range response.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, disk := range node.DiskInfos {
					for _, volume := range disk.VolumeInfos {
						if volume == nil || volume.Id == 0 {
							continue
						}
						if len(allowedCollections) > 0 {
							if _, allowed := allowedCollections[strings.TrimSpace(volume.Collection)]; !allowed {
								continue
							}
						}
						volumeIDSet[volume.Id] = struct{}{}
					}
					for _, ecShard := range disk.EcShardInfos {
						if ecShard == nil || ecShard.Id == 0 {
							continue
						}
						if len(allowedCollections) > 0 {
							if _, allowed := allowedCollections[strings.TrimSpace(ecShard.Collection)]; !allowed {
								continue
							}
						}
						volumeIDSet[ecShard.Id] = struct{}{}
					}
				}
			}
		}
	}

	volumeIDs := make([]uint32, 0, len(volumeIDSet))
	for volumeID := range volumeIDSet {
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Slice(volumeIDs, func(i, j int) bool { return volumeIDs[i] < volumeIDs[j] })
	return volumeIDs
}

func collectDummyVolumeSnapshot(response *master_pb.VolumeListResponse, volumeID uint32) dummyVolumeSnapshot {
	snapshot := dummyVolumeSnapshot{VolumeID: volumeID}
	if response == nil || response.TopologyInfo == nil || volumeID == 0 {
		return snapshot
	}

	for _, dc := range response.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, disk := range node.DiskInfos {
					for _, volume := range disk.VolumeInfos {
						if volume == nil || volume.Id != volumeID {
							continue
						}
						snapshot.ReplicaCount++
						if !snapshot.FoundRegular {
							snapshot.FoundRegular = true
							snapshot.Collection = volume.Collection
							snapshot.Size = volume.Size
							snapshot.DeletedBytes = volume.DeletedByteCount
							continue
						}
						if snapshot.Collection == "" && volume.Collection != "" {
							snapshot.Collection = volume.Collection
						}
						if volume.Size > snapshot.Size {
							snapshot.Size = volume.Size
						}
						if volume.DeletedByteCount > snapshot.DeletedBytes {
							snapshot.DeletedBytes = volume.DeletedByteCount
						}
					}
					for _, ecShard := range disk.EcShardInfos {
						if ecShard == nil || ecShard.Id != volumeID {
							continue
						}
						shardCount := bits.OnesCount32(ecShard.EcIndexBits)
						if shardCount == 0 && len(ecShard.ShardSizes) > 0 {
							shardCount = len(ecShard.ShardSizes)
						}
						snapshot.ECShardCount += shardCount
						if snapshot.Collection == "" && ecShard.Collection != "" {
							snapshot.Collection = ecShard.Collection
						}
					}
				}
			}
		}
	}

	return snapshot
}

func parseCollectionFilterSet(collectionFilter string) map[string]struct{} {
	filter := strings.TrimSpace(collectionFilter)
	if filter == "" {
		return nil
	}

	out := make(map[string]struct{})
	for _, part := range strings.Split(filter, ",") {
		collection := strings.TrimSpace(part)
		if collection == "" {
			continue
		}
		out[collection] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func formatDummyExecutionInfo(snapshot dummyVolumeSnapshot) string {
	if snapshot.FoundRegular {
		return fmt.Sprintf(
			"volume=%d collection=%s size=%d deleted_bytes=%d replicas=%d total_ec_shards=%d",
			snapshot.VolumeID,
			snapshot.Collection,
			snapshot.Size,
			snapshot.DeletedBytes,
			snapshot.ReplicaCount,
			snapshot.ECShardCount,
		)
	}
	if snapshot.ECShardCount > 0 {
		return fmt.Sprintf(
			"volume=%d regular_volume=not_found total_ec_shards=%d",
			snapshot.VolumeID,
			snapshot.ECShardCount,
		)
	}
	return fmt.Sprintf("volume=%d not_found_in_topology", snapshot.VolumeID)
}
