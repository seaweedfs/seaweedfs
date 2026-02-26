package pluginworker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	ecrepair "github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_repair"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const recentTaskWindowSize = 10

type ecRepairWorkerConfig struct {
	MinIntervalSeconds int
}

// EcRepairHandler is the plugin job handler for EC shard repair.
type EcRepairHandler struct {
	grpcDialOption grpc.DialOption
}

func NewEcRepairHandler(grpcDialOption grpc.DialOption) *EcRepairHandler {
	return &EcRepairHandler{grpcDialOption: grpcDialOption}
}

func (h *EcRepairHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "ec_repair",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "EC Repair",
		Description:             "Repairs missing or inconsistent EC shards",
	}
}

func (h *EcRepairHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "ec_repair",
		DisplayName:       "EC Repair",
		Description:       "Detect and repair missing or inconsistent erasure coding shards",
		Icon:              "fas fa-toolbox",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "ec-repair-admin",
			Title:       "EC Repair Admin Config",
			Description: "Admin-side controls for EC repair detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filters applied before EC repair detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only detect EC repairs for this collection when set.",
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
			FormId:      "ec-repair-worker",
			Title:       "EC Repair Worker Config",
			Description: "Worker-side EC repair controls.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "interval",
					Title:       "Detection Interval",
					Description: "Minimum interval between EC repair scans.",
					Fields: []*plugin_pb.ConfigField{
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
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 300},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      10 * 60,
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           500,
			GlobalExecutionConcurrency:    8,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           30,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 300},
			},
		},
	}
}

func (h *EcRepairHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != "ec_repair" {
		return fmt.Errorf("job type %q is not handled by ec_repair worker", request.JobType)
	}

	workerConfig := deriveEcRepairWorkerConfig(request.GetWorkerConfigValues())
	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), workerConfig.MinIntervalSeconds) {
		minInterval := time.Duration(workerConfig.MinIntervalSeconds) * time.Second
		_ = sender.SendActivity(buildDetectorActivity(
			"skipped_by_interval",
			fmt.Sprintf("EC REPAIR: Detection skipped due to min interval (%s)", minInterval),
			map[string]*plugin_pb.ConfigValue{
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(workerConfig.MinIntervalSeconds)},
				},
			},
		))
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{
			JobType:   "ec_repair",
			Proposals: []*plugin_pb.JobProposal{},
			HasMore:   false,
		}); err != nil {
			return err
		}
		return sender.SendComplete(&plugin_pb.DetectionComplete{
			JobType:        "ec_repair",
			Success:        true,
			TotalProposals: 0,
		})
	}

	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	response, _, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return err
	}

	maxResults := int(request.MaxResults)
	candidates, hasMore, err := ecrepair.Detect(response.TopologyInfo, collectionFilter, maxResults)
	if err != nil {
		return err
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(candidates))
	for _, candidate := range candidates {
		proposal, proposalErr := buildEcRepairProposal(candidate)
		if proposalErr != nil {
			glog.Warningf("Plugin worker skip invalid ec_repair proposal: %v", proposalErr)
			continue
		}
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   "ec_repair",
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        "ec_repair",
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *EcRepairHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != "ec_repair" {
		return fmt.Errorf("job type %q is not handled by ec_repair worker", request.Job.JobType)
	}

	volumeID := readInt64Config(request.Job.Parameters, "volume_id", 0)
	if volumeID <= 0 {
		return fmt.Errorf("missing volume_id in job parameters")
	}
	collection := readStringConfig(request.Job.Parameters, "collection", "")
	diskType := readStringConfig(request.Job.Parameters, "disk_type", "")

	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	if err := sendProgress(sender, request.Job, 0, "assigned", "ec repair job accepted"); err != nil {
		return err
	}

	response, activeTopology, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return err
	}

	plan, err := ecrepair.BuildRepairPlan(response.TopologyInfo, activeTopology, uint32(volumeID), collection, diskType)
	if err != nil {
		return err
	}

	if len(plan.MissingShards) == 0 && len(plan.DeleteByNode) == 0 {
		resultSummary := fmt.Sprintf("EC repair skipped for volume %d (no issues found)", volumeID)
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
				},
			},
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity("completed", resultSummary),
			},
		})
	}

	if err := h.executeRepairPlan(ctx, plan, request.Job, sender); err != nil {
		_ = sendProgress(sender, request.Job, 100, "failed", err.Error())
		return err
	}

	resultSummary := fmt.Sprintf("EC repair completed for volume %d", volumeID)
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
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: plan.Collection},
				},
				"disk_type": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: plan.DiskType},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
	})
}

func (h *EcRepairHandler) executeRepairPlan(ctx context.Context, plan *ecrepair.RepairPlan, job *plugin_pb.JobSpec, sender ExecutionSender) error {
	if plan == nil {
		return fmt.Errorf("repair plan is nil")
	}

	if len(plan.MissingShards) > 0 {
		if err := sendProgress(sender, job, 10, "copy_existing_shards", "copying EC shards to rebuilder"); err != nil {
			return err
		}
		if err := h.copyShardsToRebuilder(ctx, plan); err != nil {
			return err
		}

		if err := sendProgress(sender, job, 40, "rebuild_missing_shards", "rebuilding missing EC shards"); err != nil {
			return err
		}
		rebuilt, err := h.rebuildMissingShards(ctx, plan)
		if err != nil {
			return err
		}

		if err := sendProgress(sender, job, 60, "distribute_shards", "distributing rebuilt EC shards"); err != nil {
			return err
		}
		if err := h.distributeRebuiltShards(ctx, plan, rebuilt); err != nil {
			return err
		}

		if err := sendProgress(sender, job, 75, "cleanup_rebuilder", "cleaning up rebuilder temporary shards"); err != nil {
			return err
		}
		if err := h.cleanupRebuilder(ctx, plan, rebuilt); err != nil {
			return err
		}
	}

	if len(plan.DeleteByNode) > 0 {
		if err := sendProgress(sender, job, 85, "delete_extra_shards", "deleting extra or mismatched shards"); err != nil {
			return err
		}
		if err := h.deleteExtraShards(ctx, plan); err != nil {
			return err
		}
	}

	return sendProgress(sender, job, 100, "completed", "ec repair completed")
}

func (h *EcRepairHandler) copyShardsToRebuilder(ctx context.Context, plan *ecrepair.RepairPlan) error {
	if plan.Rebuilder.NodeAddress == "" {
		return fmt.Errorf("rebuilder node is required")
	}
	if len(plan.CopySources) == 0 {
		return nil
	}

	localShardSet := make(map[uint32]struct{}, len(plan.Rebuilder.LocalShards))
	for _, shardID := range plan.Rebuilder.LocalShards {
		localShardSet[shardID] = struct{}{}
	}

	shardIDs := make([]uint32, 0, len(plan.CopySources))
	for shardID := range plan.CopySources {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })

	copyIndexFiles := true
	for _, shardID := range shardIDs {
		source := strings.TrimSpace(plan.CopySources[shardID])
		if source == "" {
			continue
		}
		if source == plan.Rebuilder.NodeAddress {
			if _, ok := localShardSet[shardID]; ok {
				continue
			}
		}

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(plan.Rebuilder.NodeAddress), h.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       plan.VolumeID,
				Collection:     plan.Collection,
				ShardIds:       []uint32{shardID},
				CopyEcxFile:    copyIndexFiles,
				CopyEcjFile:    copyIndexFiles,
				CopyVifFile:    copyIndexFiles,
				SourceDataNode: source,
				DiskId:         plan.Rebuilder.DiskID,
			})
			return err
		})
		if err != nil {
			return err
		}
		copyIndexFiles = false
	}

	return nil
}

func (h *EcRepairHandler) rebuildMissingShards(ctx context.Context, plan *ecrepair.RepairPlan) ([]uint32, error) {
	var rebuilt []uint32
	if plan.Rebuilder.NodeAddress == "" {
		return nil, fmt.Errorf("rebuilder node is required")
	}
	if err := operation.WithVolumeServerClient(false, pb.ServerAddress(plan.Rebuilder.NodeAddress), h.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.VolumeEcShardsRebuild(ctx, &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   plan.VolumeID,
			Collection: plan.Collection,
		})
		if err != nil {
			return err
		}
		rebuilt = append(rebuilt, resp.RebuiltShardIds...)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(rebuilt) == 0 {
		if len(plan.MissingShards) > 0 {
			glog.V(1).Infof("EC Repair: resp.RebuiltShardIds empty; assuming all missing shards rebuilt for volume %d (%d shards)", plan.VolumeID, len(plan.MissingShards))
		} else {
			glog.V(1).Infof("EC Repair: resp.RebuiltShardIds empty for volume %d with no missing shards declared", plan.VolumeID)
		}
		rebuilt = append(rebuilt, plan.MissingShards...)
	}
	return rebuilt, nil
}

func (h *EcRepairHandler) distributeRebuiltShards(ctx context.Context, plan *ecrepair.RepairPlan, rebuilt []uint32) error {
	if len(plan.Targets) == 0 {
		return nil
	}
	if len(rebuilt) == 0 {
		return nil
	}

	rebuiltSet := make(map[uint32]struct{}, len(rebuilt))
	for _, shardID := range rebuilt {
		rebuiltSet[shardID] = struct{}{}
	}

	targetCopyIndex := make(map[string]bool)
	for _, target := range plan.Targets {
		if target.NodeAddress == "" {
			continue
		}
		var shardIDs []uint32
		for _, shardID := range target.ShardIDs {
			if _, ok := rebuiltSet[shardID]; ok {
				shardIDs = append(shardIDs, shardID)
			}
		}
		if len(shardIDs) == 0 {
			continue
		}
		sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })

		copyIndex := !targetCopyIndex[target.NodeAddress]
		targetCopyIndex[target.NodeAddress] = true

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(target.NodeAddress), h.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			if target.NodeAddress != plan.Rebuilder.NodeAddress {
				_, err := client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       plan.VolumeID,
					Collection:     plan.Collection,
					ShardIds:       shardIDs,
					CopyEcxFile:    copyIndex,
					CopyEcjFile:    copyIndex,
					CopyVifFile:    copyIndex,
					SourceDataNode: plan.Rebuilder.NodeAddress,
					DiskId:         target.DiskID,
				})
				if err != nil {
					return err
				}
			}

			_, err := client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   plan.VolumeID,
				Collection: plan.Collection,
				ShardIds:   shardIDs,
			})
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *EcRepairHandler) cleanupRebuilder(ctx context.Context, plan *ecrepair.RepairPlan, rebuilt []uint32) error {
	if plan.Rebuilder.NodeAddress == "" || len(rebuilt) == 0 {
		return nil
	}

	keep := make(map[uint32]struct{})
	for _, shardID := range plan.Rebuilder.LocalShards {
		keep[shardID] = struct{}{}
	}
	for _, target := range plan.Targets {
		if target.NodeAddress != plan.Rebuilder.NodeAddress {
			continue
		}
		for _, shardID := range target.ShardIDs {
			keep[shardID] = struct{}{}
		}
	}

	var toDelete []uint32
	for _, shardID := range rebuilt {
		if _, ok := keep[shardID]; ok {
			continue
		}
		toDelete = append(toDelete, shardID)
	}
	if len(toDelete) == 0 {
		return nil
	}
	return deleteShardIds(ctx, h.grpcDialOption, plan.Rebuilder.NodeAddress, plan.VolumeID, plan.Collection, toDelete)
}

func (h *EcRepairHandler) deleteExtraShards(ctx context.Context, plan *ecrepair.RepairPlan) error {
	for nodeAddress, shardIDs := range plan.DeleteByNode {
		if len(shardIDs) == 0 {
			continue
		}
		if err := deleteShardIds(ctx, h.grpcDialOption, nodeAddress, plan.VolumeID, plan.Collection, shardIDs); err != nil {
			return err
		}
	}
	return nil
}

func deleteShardIds(ctx context.Context, dialOption grpc.DialOption, nodeAddress string, volumeID uint32, collection string, shardIDs []uint32) error {
	sorted := make([]uint32, len(shardIDs))
	copy(sorted, shardIDs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return operation.WithVolumeServerClient(false, pb.ServerAddress(nodeAddress), dialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: volumeID,
			ShardIds: sorted,
		})
		if err != nil {
			return err
		}
		_, err = client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   volumeID,
			Collection: collection,
			ShardIds:   sorted,
		})
		return err
	})
}

func (h *EcRepairHandler) fetchTopology(ctx context.Context, masterAddresses []string) (*master_pb.VolumeListResponse, *topology.ActiveTopology, error) {
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
		if response == nil || response.TopologyInfo == nil {
			continue
		}
		activeTopology := topology.NewActiveTopology(recentTaskWindowSize)
		if err := activeTopology.UpdateTopology(response.TopologyInfo); err != nil {
			return nil, nil, err
		}
		return response, activeTopology, nil
	}

	return nil, nil, fmt.Errorf("failed to load topology from all provided masters")
}

func (h *EcRepairHandler) fetchVolumeList(ctx context.Context, address string) (*master_pb.VolumeListResponse, error) {
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

func deriveEcRepairWorkerConfig(values map[string]*plugin_pb.ConfigValue) *ecRepairWorkerConfig {
	return &ecRepairWorkerConfig{
		MinIntervalSeconds: int(readInt64Config(values, "min_interval_seconds", 300)),
	}
}

func buildEcRepairProposal(candidate *ecrepair.RepairCandidate) (*plugin_pb.JobProposal, error) {
	if candidate == nil {
		return nil, fmt.Errorf("repair candidate is nil")
	}

	params := &worker_pb.TaskParams{
		VolumeId:   candidate.VolumeID,
		Collection: candidate.Collection,
	}
	payload, err := proto.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal task params: %w", err)
	}

	proposalID := fmt.Sprintf("ec-repair-%d-%d", candidate.VolumeID, time.Now().UnixNano())
	dedupeKey := fmt.Sprintf("ec_repair:%d", candidate.VolumeID)
	if candidate.Collection != "" {
		dedupeKey = dedupeKey + ":" + candidate.Collection
	}
	if candidate.DiskType != "" {
		dedupeKey = dedupeKey + ":" + candidate.DiskType
	}

	summary := fmt.Sprintf("Repair EC volume %d", candidate.VolumeID)
	if candidate.Collection != "" {
		summary = summary + " (" + candidate.Collection + ")"
	}

	detail := fmt.Sprintf("missing shards=%d, extra shards=%d, mismatched shards=%d", candidate.MissingShards, candidate.ExtraShards, candidate.MismatchedShards)

	return &plugin_pb.JobProposal{
		ProposalId: proposalID,
		DedupeKey:  dedupeKey,
		JobType:    "ec_repair",
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Summary:    summary,
		Detail:     detail,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {
				Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload},
			},
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(candidate.VolumeID)},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: candidate.Collection},
			},
			"disk_type": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: candidate.DiskType},
			},
			"missing_shards": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(candidate.MissingShards)},
			},
			"extra_shards": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(candidate.ExtraShards)},
			},
			"mismatched_shards": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(candidate.MismatchedShards)},
			},
		},
		Labels: map[string]string{
			"task_type":         "ec_repair",
			"volume_id":         fmt.Sprintf("%d", candidate.VolumeID),
			"collection":        candidate.Collection,
			"disk_type":         candidate.DiskType,
			"missing_shards":    fmt.Sprintf("%d", candidate.MissingShards),
			"extra_shards":      fmt.Sprintf("%d", candidate.ExtraShards),
			"mismatched_shards": fmt.Sprintf("%d", candidate.MismatchedShards),
		},
	}, nil
}

func sendProgress(sender ExecutionSender, job *plugin_pb.JobSpec, percent float64, stage string, message string) error {
	if sender == nil || job == nil {
		return nil
	}
	return sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           job.JobId,
		JobType:         job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: percent,
		Stage:           stage,
		Message:         message,
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity(stage, message),
		},
	})
}
