package pluginworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	balancetask "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBalanceTimeoutSeconds = int32(10 * 60)
	maxProposalStringLength      = 200
)

// collectionFilterMode controls how collections are handled during balance detection.
type collectionFilterMode string

const (
	collectionFilterAll  collectionFilterMode = "ALL_COLLECTIONS"
	collectionFilterEach collectionFilterMode = "EACH_COLLECTION"
)

// volumeState controls which volumes participate in balance detection.
type volumeState string

const (
	volumeStateAll    volumeState = "ALL"
	volumeStateActive volumeState = "ACTIVE"
	volumeStateFull   volumeState = "FULL"
)

func init() {
	RegisterHandler(HandlerFactory{
		JobType:  "volume_balance",
		Category: CategoryDefault,
		Aliases:  []string{"balance", "volume.balance", "volume-balance"},
		Build: func(opts HandlerBuildOptions) (JobHandler, error) {
			return NewVolumeBalanceHandler(opts.GrpcDialOption), nil
		},
	})
}

type volumeBalanceWorkerConfig struct {
	TaskConfig         *balancetask.Config
	MaxConcurrentMoves int
	BatchSize          int
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
		Weight:                  50,
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
							Description: "Filter collections for balance detection. Use ALL_COLLECTIONS (default) to treat all volumes as one pool, EACH_COLLECTION to run detection separately per collection, or a regex pattern to match specific collections.",
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
								{Value: string(volumeStateAll), Label: "All Volumes"},
								{Value: string(volumeStateActive), Label: "Active (writable)"},
								{Value: string(volumeStateFull), Label: "Full (read-only)"},
							},
						},
						{
							Name:        "data_center_filter",
							Label:       "Data Center Filter",
							Description: "Only balance volumes in matching data centers (comma-separated, wildcards supported). Leave empty for all.",
							Placeholder: "all data centers",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "rack_filter",
							Label:       "Rack Filter",
							Description: "Only balance volumes on matching racks (comma-separated, wildcards supported). Leave empty for all.",
							Placeholder: "all racks",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "node_filter",
							Label:       "Node Filter",
							Description: "Only balance volumes on matching nodes (comma-separated, wildcards supported). Leave empty for all.",
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
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: string(volumeStateAll)},
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
			FormId:      "volume-balance-worker",
			Title:       "Volume Balance Worker Config",
			Description: "Worker-side balance thresholds.",
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
					},
				},
				{
					SectionId:   "batch_execution",
					Title:       "Batch Execution",
					Description: "Controls for running multiple volume moves per job. The worker coordinates moves via gRPC and is not on the data path.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "max_concurrent_moves",
							Label:       "Max Concurrent Moves",
							Description: "Maximum number of volume moves to run concurrently within a single batch job.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 50}},
						},
						{
							Name:        "batch_size",
							Label:       "Batch Size",
							Description: "Maximum number of volume moves to group into a single job. Set to 1 to disable batching.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 100}},
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
				"max_concurrent_moves": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(defaultMaxConcurrentMoves)},
				},
				"batch_size": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 20},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      30 * 60,
			DetectionTimeoutSeconds:       120,
			MaxJobsPerDetection:           100,
			GlobalExecutionConcurrency:    16,
			PerWorkerExecutionConcurrency: 4,
			RetryLimit:                    1,
			RetryBackoffSeconds:           15,
			JobTypeMaxRuntimeSeconds:      1800,
			ExecutionTimeoutSeconds:       1800,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"imbalance_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.2},
			},
			"min_server_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2},
			},
			"max_concurrent_moves": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(defaultMaxConcurrentMoves)},
			},
			"batch_size": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 20},
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
	collectionFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "collection_filter", ""))
	masters := make([]string, 0)
	if request.ClusterContext != nil {
		masters = append(masters, request.ClusterContext.MasterGrpcAddresses...)
	}

	metrics, activeTopology, replicaMap, err := h.collectVolumeMetrics(ctx, masters, collectionFilter)
	if err != nil {
		return err
	}

	volState := volumeState(strings.ToUpper(strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "volume_state", string(volumeStateAll)))))
	metrics = filterMetricsByVolumeState(metrics, volState)

	dataCenterFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "data_center_filter", ""))
	rackFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "rack_filter", ""))
	nodeFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "node_filter", ""))

	if dataCenterFilter != "" || rackFilter != "" || nodeFilter != "" {
		metrics = filterMetricsByLocation(metrics, dataCenterFilter, rackFilter, nodeFilter)
	}

	workerConfig.TaskConfig.DataCenterFilter = dataCenterFilter
	workerConfig.TaskConfig.RackFilter = rackFilter
	workerConfig.TaskConfig.NodeFilter = nodeFilter

	clusterInfo := &workertypes.ClusterInfo{
		ActiveTopology:   activeTopology,
		VolumeReplicaMap: replicaMap,
	}
	maxResults := int(request.MaxResults)

	var results []*workertypes.TaskDetectionResult
	var hasMore bool

	if collectionFilterMode(collectionFilter) == collectionFilterEach {
		// Group metrics by collection in a single pass (O(N) instead of O(C*N))
		metricsByCollection := make(map[string][]*workertypes.VolumeHealthMetrics)
		for _, m := range metrics {
			if m == nil {
				continue
			}
			metricsByCollection[m.Collection] = append(metricsByCollection[m.Collection], m)
		}
		collections := make([]string, 0, len(metricsByCollection))
		for c := range metricsByCollection {
			collections = append(collections, c)
		}
		sort.Strings(collections)

		budget := maxResults
		unlimitedBudget := budget <= 0
		for _, collection := range collections {
			if !unlimitedBudget && budget <= 0 {
				hasMore = true
				break
			}
			perCollectionLimit := budget
			if unlimitedBudget {
				perCollectionLimit = 0 // Detection treats <= 0 as unbounded
			}
			perResults, perHasMore, perErr := balancetask.Detection(metricsByCollection[collection], clusterInfo, workerConfig.TaskConfig, perCollectionLimit)
			if perErr != nil {
				return perErr
			}
			results = append(results, perResults...)
			if !unlimitedBudget {
				budget -= len(perResults)
			}
			if perHasMore {
				hasMore = true
			}
		}
	} else {
		var err error
		results, hasMore, err = balancetask.Detection(metrics, clusterInfo, workerConfig.TaskConfig, maxResults)
		if err != nil {
			return err
		}
	}

	if traceErr := emitVolumeBalanceDetectionDecisionTrace(sender, metrics, activeTopology, workerConfig.TaskConfig, results); traceErr != nil {
		glog.Warningf("Plugin worker failed to emit volume_balance detection trace: %v", traceErr)
	}

	var proposals []*plugin_pb.JobProposal
	if workerConfig.BatchSize > 1 && len(results) > 1 {
		proposals = buildBatchVolumeBalanceProposals(results, workerConfig.BatchSize, workerConfig.MaxConcurrentMoves)
	} else {
		proposals = make([]*plugin_pb.JobProposal, 0, len(results))
		for _, result := range results {
			proposal, proposalErr := buildVolumeBalanceProposal(result)
			if proposalErr != nil {
				glog.Warningf("Plugin worker skip invalid volume_balance proposal: %v", proposalErr)
				continue
			}
			proposals = append(proposals, proposal)
		}
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

func emitVolumeBalanceDetectionDecisionTrace(
	sender DetectionSender,
	metrics []*workertypes.VolumeHealthMetrics,
	activeTopology *topology.ActiveTopology,
	taskConfig *balancetask.Config,
	results []*workertypes.TaskDetectionResult,
) error {
	if sender == nil || taskConfig == nil {
		return nil
	}

	totalVolumes := len(metrics)
	summaryMessage := ""
	if len(results) == 0 {
		summaryMessage = fmt.Sprintf(
			"BALANCE: No tasks created for %d volumes across %d disk type(s). Threshold=%.1f%%, MinServers=%d",
			totalVolumes,
			countBalanceDiskTypes(metrics),
			taskConfig.ImbalanceThreshold*100,
			taskConfig.MinServerCount,
		)
	} else {
		summaryMessage = fmt.Sprintf(
			"BALANCE: Created %d task(s) for %d volumes across %d disk type(s). Threshold=%.1f%%, MinServers=%d",
			len(results),
			totalVolumes,
			countBalanceDiskTypes(metrics),
			taskConfig.ImbalanceThreshold*100,
			taskConfig.MinServerCount,
		)
	}

	if err := sender.SendActivity(BuildDetectorActivity("decision_summary", summaryMessage, map[string]*plugin_pb.ConfigValue{
		"total_volumes": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalVolumes)},
		},
		"selected_tasks": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(results))},
		},
		"imbalance_threshold_percent": {
			Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: taskConfig.ImbalanceThreshold * 100},
		},
		"min_server_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(taskConfig.MinServerCount)},
		},
	})); err != nil {
		return err
	}

	volumesByDiskType := make(map[string][]*workertypes.VolumeHealthMetrics)
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		diskType := strings.TrimSpace(metric.DiskType)
		if diskType == "" {
			diskType = "unknown"
		}
		volumesByDiskType[diskType] = append(volumesByDiskType[diskType], metric)
	}

	diskTypes := make([]string, 0, len(volumesByDiskType))
	for diskType := range volumesByDiskType {
		diskTypes = append(diskTypes, diskType)
	}
	sort.Strings(diskTypes)

	const minVolumeCount = 2
	detailCount := 0
	for _, diskType := range diskTypes {
		diskMetrics := volumesByDiskType[diskType]
		volumeCount := len(diskMetrics)
		if volumeCount < minVolumeCount {
			message := fmt.Sprintf(
				"BALANCE [%s]: No tasks created - cluster too small (%d volumes, need ≥%d)",
				diskType,
				volumeCount,
				minVolumeCount,
			)
			if err := sender.SendActivity(BuildDetectorActivity("decision_disk_type", message, map[string]*plugin_pb.ConfigValue{
				"disk_type": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType},
				},
				"volume_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeCount)},
				},
				"required_min_volume_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: minVolumeCount},
				},
			})); err != nil {
				return err
			}
			detailCount++
			if detailCount >= 3 {
				break
			}
			continue
		}

		// Seed server counts from topology so zero-volume servers are included,
		// matching the same logic used in balancetask.Detection.
		serverVolumeCounts := make(map[string]int)
		if activeTopology != nil {
			topologyInfo := activeTopology.GetTopologyInfo()
			if topologyInfo != nil {
				for _, dc := range topologyInfo.DataCenterInfos {
					for _, rack := range dc.RackInfos {
						for _, node := range rack.DataNodeInfos {
							for diskTypeName := range node.DiskInfos {
								if diskTypeName == diskType {
									serverVolumeCounts[node.Id] = 0
								}
							}
						}
					}
				}
			}
		}
		for _, metric := range diskMetrics {
			serverVolumeCounts[metric.Server]++
		}
		if len(serverVolumeCounts) < taskConfig.MinServerCount {
			message := fmt.Sprintf(
				"BALANCE [%s]: No tasks created - too few servers (%d servers, need ≥%d)",
				diskType,
				len(serverVolumeCounts),
				taskConfig.MinServerCount,
			)
			if err := sender.SendActivity(BuildDetectorActivity("decision_disk_type", message, map[string]*plugin_pb.ConfigValue{
				"disk_type": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType},
				},
				"server_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(serverVolumeCounts))},
				},
				"required_min_server_count": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(taskConfig.MinServerCount)},
				},
			})); err != nil {
				return err
			}
			detailCount++
			if detailCount >= 3 {
				break
			}
			continue
		}

		totalDiskTypeVolumes := len(diskMetrics)
		avgVolumesPerServer := float64(totalDiskTypeVolumes) / float64(len(serverVolumeCounts))
		maxVolumes := 0
		minVolumes := totalDiskTypeVolumes
		maxServer := ""
		minServer := ""
		for server, count := range serverVolumeCounts {
			if count > maxVolumes {
				maxVolumes = count
				maxServer = server
			}
			if count < minVolumes {
				minVolumes = count
				minServer = server
			}
		}

		imbalanceRatio := 0.0
		if avgVolumesPerServer > 0 {
			imbalanceRatio = float64(maxVolumes-minVolumes) / avgVolumesPerServer
		}

		stage := "decision_disk_type"
		message := ""
		if imbalanceRatio <= taskConfig.ImbalanceThreshold {
			message = fmt.Sprintf(
				"BALANCE [%s]: No tasks created - cluster well balanced. Imbalance=%.1f%% (threshold=%.1f%%). Max=%d volumes on %s, Min=%d on %s, Avg=%.1f",
				diskType,
				imbalanceRatio*100,
				taskConfig.ImbalanceThreshold*100,
				maxVolumes,
				maxServer,
				minVolumes,
				minServer,
				avgVolumesPerServer,
			)
		} else {
			stage = "decision_candidate"
			message = fmt.Sprintf(
				"BALANCE [%s]: Candidate detected. Imbalance=%.1f%% (threshold=%.1f%%). Max=%d volumes on %s, Min=%d on %s, Avg=%.1f",
				diskType,
				imbalanceRatio*100,
				taskConfig.ImbalanceThreshold*100,
				maxVolumes,
				maxServer,
				minVolumes,
				minServer,
				avgVolumesPerServer,
			)
		}

		if err := sender.SendActivity(BuildDetectorActivity(stage, message, map[string]*plugin_pb.ConfigValue{
			"disk_type": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType},
			},
			"volume_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalDiskTypeVolumes)},
			},
			"server_count": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(serverVolumeCounts))},
			},
			"imbalance_percent": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: imbalanceRatio * 100},
			},
			"threshold_percent": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: taskConfig.ImbalanceThreshold * 100},
			},
			"max_volumes": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(maxVolumes)},
			},
			"min_volumes": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(minVolumes)},
			},
			"avg_volumes_per_server": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: avgVolumesPerServer},
			},
		})); err != nil {
			return err
		}

		detailCount++
		if detailCount >= 3 {
			break
		}
	}

	return nil
}

// filterMetricsByVolumeState filters volume metrics by state.
// "ACTIVE" keeps volumes with FullnessRatio < 1.01 (writable, below size limit).
// "FULL" keeps volumes with FullnessRatio >= 1.01 (read-only, above size limit).
// "ALL" or any other value returns all metrics unfiltered.
func filterMetricsByVolumeState(metrics []*workertypes.VolumeHealthMetrics, state volumeState) []*workertypes.VolumeHealthMetrics {
	const fullnessThreshold = 1.01

	var predicate func(m *workertypes.VolumeHealthMetrics) bool
	switch state {
	case volumeStateActive:
		predicate = func(m *workertypes.VolumeHealthMetrics) bool {
			return m.FullnessRatio < fullnessThreshold
		}
	case volumeStateFull:
		predicate = func(m *workertypes.VolumeHealthMetrics) bool {
			return m.FullnessRatio >= fullnessThreshold
		}
	default:
		return metrics
	}

	filtered := make([]*workertypes.VolumeHealthMetrics, 0, len(metrics))
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if predicate(m) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func countBalanceDiskTypes(metrics []*workertypes.VolumeHealthMetrics) int {
	diskTypes := make(map[string]struct{})
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		diskType := strings.TrimSpace(metric.DiskType)
		if diskType == "" {
			diskType = "unknown"
		}
		diskTypes[diskType] = struct{}{}
	}
	return len(diskTypes)
}

const (
	defaultMaxConcurrentMoves = 5
	maxConcurrentMovesLimit   = 50
	maxBatchMoves             = 100
)

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

	applyBalanceExecutionDefaults(params)

	// Batch path: if BalanceTaskParams has moves, execute them concurrently
	if bp := params.GetBalanceParams(); bp != nil && len(bp.Moves) > 0 {
		return h.executeBatchMoves(ctx, request, params, sender)
	}

	// Single-move path (backward compatible)
	return h.executeSingleMove(ctx, request, params, sender)
}

func (h *VolumeBalanceHandler) executeSingleMove(
	ctx context.Context,
	request *plugin_pb.ExecuteJobRequest,
	params *worker_pb.TaskParams,
	sender ExecutionSender,
) error {
	if len(params.Sources) == 0 || strings.TrimSpace(params.Sources[0].Node) == "" {
		return fmt.Errorf("volume balance source node is required")
	}
	if len(params.Targets) == 0 || strings.TrimSpace(params.Targets[0].Node) == "" {
		return fmt.Errorf("volume balance target node is required")
	}

	task := balancetask.NewBalanceTask(
		request.Job.JobId,
		params.Sources[0].Node,
		params.VolumeId,
		params.Collection,
		h.grpcDialOption,
	)
	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	task.SetProgressCallback(func(progress float64, stage string) {
		message := fmt.Sprintf("balance progress %.0f%%", progress)
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
		Message:         "volume balance job accepted",
		Activities: []*plugin_pb.ActivityEvent{
			BuildExecutorActivity("assigned", "volume balance job accepted"),
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
			BuildExecutorActivity("completed", resultSummary),
		},
	})
}

// executeBatchMoves runs multiple volume moves concurrently within a single job.
func (h *VolumeBalanceHandler) executeBatchMoves(
	ctx context.Context,
	request *plugin_pb.ExecuteJobRequest,
	params *worker_pb.TaskParams,
	sender ExecutionSender,
) error {
	bp := params.GetBalanceParams()
	if len(bp.Moves) == 0 {
		return fmt.Errorf("batch balance job has no moves")
	}
	if len(bp.Moves) > maxBatchMoves {
		return fmt.Errorf("batch balance job has %d moves, exceeding limit of %d", len(bp.Moves), maxBatchMoves)
	}

	// Filter out nil or incomplete moves before scheduling.
	validMoves := make([]*worker_pb.BalanceMoveSpec, 0, len(bp.Moves))
	for _, m := range bp.Moves {
		if m == nil {
			continue
		}
		if strings.TrimSpace(m.SourceNode) == "" || strings.TrimSpace(m.TargetNode) == "" || m.VolumeId == 0 {
			glog.Warningf("batch balance: skipping invalid move (vol:%d src:%q tgt:%q)", m.VolumeId, m.SourceNode, m.TargetNode)
			continue
		}
		validMoves = append(validMoves, m)
	}
	if len(validMoves) == 0 {
		return fmt.Errorf("batch balance job has no valid moves after validation")
	}
	moves := validMoves

	maxConcurrent := int(bp.MaxConcurrentMoves)
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentMoves
	}
	// Clamp to the worker-side upper bound so a stale or malicious job
	// cannot request unbounded fan-out of concurrent volume moves.
	if maxConcurrent > maxConcurrentMovesLimit {
		maxConcurrent = maxConcurrentMovesLimit
	}

	totalMoves := len(moves)
	glog.Infof("batch volume balance: %d moves, max concurrent %d", totalMoves, maxConcurrent)

	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         fmt.Sprintf("batch volume balance accepted: %d moves", totalMoves),
		Activities: []*plugin_pb.ActivityEvent{
			BuildExecutorActivity("assigned", fmt.Sprintf("batch volume balance: %d moves, concurrency %d", totalMoves, maxConcurrent)),
		},
	}); err != nil {
		return err
	}

	// Derive a cancellable context so we can abort remaining moves if the
	// progress stream breaks (client disconnect, context cancelled).
	batchCtx, batchCancel := context.WithCancel(ctx)
	defer batchCancel()

	// Per-move progress tracking. The mutex serializes both the progress
	// bookkeeping and the sender.SendProgress call, since the underlying
	// gRPC stream is not safe for concurrent writes.
	var progressMu sync.Mutex
	moveProgress := make([]float64, totalMoves)
	var sendErr error // first progress send error

	reportAggregate := func(moveIndex int, progress float64, stage string) {
		progressMu.Lock()
		defer progressMu.Unlock()

		if sendErr != nil {
			return // stream already broken, skip further sends
		}

		moveProgress[moveIndex] = progress
		total := 0.0
		for _, p := range moveProgress {
			total += p
		}

		aggregate := total / float64(totalMoves)
		move := moves[moveIndex]
		message := fmt.Sprintf("[Move %d/%d vol:%d] %s", moveIndex+1, totalMoves, move.VolumeId, stage)

		if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: aggregate,
			Stage:           fmt.Sprintf("move %d/%d", moveIndex+1, totalMoves),
			Message:         message,
			Activities: []*plugin_pb.ActivityEvent{
				BuildExecutorActivity(fmt.Sprintf("move-%d", moveIndex+1), message),
			},
		}); err != nil {
			sendErr = err
			batchCancel() // cancel in-flight and pending moves
		}
	}

	type moveResult struct {
		index    int
		volumeID uint32
		source   string
		target   string
		err      error
	}

	sem := make(chan struct{}, maxConcurrent)
	results := make(chan moveResult, totalMoves)

	for i, move := range moves {
		sem <- struct{}{} // acquire slot
		go func(idx int, m *worker_pb.BalanceMoveSpec) {
			defer func() { <-sem }() // release slot

			task := balancetask.NewBalanceTask(
				fmt.Sprintf("%s-move-%d", request.Job.JobId, idx),
				m.SourceNode,
				m.VolumeId,
				m.Collection,
				h.grpcDialOption,
			)
			task.SetProgressCallback(func(progress float64, stage string) {
				reportAggregate(idx, progress, stage)
			})

			moveParams := buildMoveTaskParams(m, bp)
			err := task.Execute(batchCtx, moveParams)
			results <- moveResult{
				index:    idx,
				volumeID: m.VolumeId,
				source:   m.SourceNode,
				target:   m.TargetNode,
				err:      err,
			}
		}(i, move)
	}

	// Collect all results
	var succeeded, failed int
	var errMessages []string
	for range moves {
		r := <-results
		if r.err != nil {
			failed++
			errMessages = append(errMessages, fmt.Sprintf("volume %d (%s→%s): %v", r.volumeID, r.source, r.target, r.err))
			glog.Warningf("batch balance move %d failed: volume %d %s→%s: %v", r.index, r.volumeID, r.source, r.target, r.err)
		} else {
			succeeded++
		}
	}

	summary := fmt.Sprintf("%d/%d volumes moved successfully", succeeded, totalMoves)
	if failed > 0 {
		summary += fmt.Sprintf("; %d failed", failed)
	}

	// Mark the job as successful if at least one move succeeded. This avoids
	// the standard retry path re-running already-completed moves. The failed
	// move details are available in ErrorMessage and result metadata so a
	// retry mechanism can operate only on the failed items.
	success := succeeded > 0 || failed == 0
	var errMsg string
	if failed > 0 {
		errMsg = strings.Join(errMessages, "; ")
	}

	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:        request.Job.JobId,
		JobType:      request.Job.JobType,
		Success:      success,
		ErrorMessage: errMsg,
		Result: &plugin_pb.JobResult{
			Summary: summary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"total_moves": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(totalMoves)},
				},
				"succeeded": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(succeeded)},
				},
				"failed": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(failed)},
				},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			BuildExecutorActivity("completed", summary),
		},
	})
}

// buildMoveTaskParams constructs a TaskParams for a single move within a batch.
func buildMoveTaskParams(move *worker_pb.BalanceMoveSpec, outerParams *worker_pb.BalanceTaskParams) *worker_pb.TaskParams {
	timeoutSeconds := defaultBalanceTimeoutSeconds
	forceMove := false
	if outerParams != nil {
		if outerParams.TimeoutSeconds > 0 {
			timeoutSeconds = outerParams.TimeoutSeconds
		}
		forceMove = outerParams.ForceMove
	}
	return &worker_pb.TaskParams{
		VolumeId:   move.VolumeId,
		Collection: move.Collection,
		VolumeSize: move.VolumeSize,
		Sources: []*worker_pb.TaskSource{
			{Node: move.SourceNode, VolumeId: move.VolumeId},
		},
		Targets: []*worker_pb.TaskTarget{
			{Node: move.TargetNode, VolumeId: move.VolumeId},
		},
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      forceMove,
				TimeoutSeconds: timeoutSeconds,
			},
		},
	}
}

func (h *VolumeBalanceHandler) collectVolumeMetrics(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, map[uint32][]workertypes.ReplicaLocation, error) {
	return collectVolumeMetricsFromMasters(ctx, masterAddresses, collectionFilter, h.grpcDialOption)
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

	maxConcurrentMoves64 := readInt64Config(values, "max_concurrent_moves", int64(defaultMaxConcurrentMoves))
	if maxConcurrentMoves64 < 1 {
		maxConcurrentMoves64 = 1
	}
	if maxConcurrentMoves64 > 50 {
		maxConcurrentMoves64 = 50
	}
	maxConcurrentMoves := int(maxConcurrentMoves64)

	batchSize64 := readInt64Config(values, "batch_size", 20)
	if batchSize64 < 1 {
		batchSize64 = 1
	}
	if batchSize64 > 100 {
		batchSize64 = 100
	}
	batchSize := int(batchSize64)

	return &volumeBalanceWorkerConfig{
		TaskConfig:         taskConfig,
		MaxConcurrentMoves: maxConcurrentMoves,
		BatchSize:          batchSize,
	}
}

func filterMetricsByLocation(metrics []*workertypes.VolumeHealthMetrics, dcFilter, rackFilter, nodeFilter string) []*workertypes.VolumeHealthMetrics {
	dcMatchers := wildcard.CompileWildcardMatchers(dcFilter)
	rackMatchers := wildcard.CompileWildcardMatchers(rackFilter)
	nodeMatchers := wildcard.CompileWildcardMatchers(nodeFilter)

	filtered := make([]*workertypes.VolumeHealthMetrics, 0, len(metrics))
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if !wildcard.MatchesAnyWildcard(dcMatchers, m.DataCenter) {
			continue
		}
		if !wildcard.MatchesAnyWildcard(rackMatchers, m.Rack) {
			continue
		}
		if !wildcard.MatchesAnyWildcard(nodeMatchers, m.Server) {
			continue
		}
		filtered = append(filtered, m)
	}
	return filtered
}

func buildVolumeBalanceProposal(
	result *workertypes.TaskDetectionResult,
) (*plugin_pb.JobProposal, error) {
	if result == nil {
		return nil, fmt.Errorf("task detection result is nil")
	}
	if result.TypedParams == nil {
		return nil, fmt.Errorf("missing typed params for volume %d", result.VolumeID)
	}

	params := proto.Clone(result.TypedParams).(*worker_pb.TaskParams)
	applyBalanceExecutionDefaults(params)

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

	// Estimate runtime at 5 min/GB (matches vacuum) so the scheduler grants
	// a per-attempt deadline that scales with volume size instead of the
	// 2*DetectionTimeout default.
	volumeSizeGB := int64(result.TypedParams.VolumeSize/1024/1024/1024) + 1
	estimatedRuntimeSeconds := volumeSizeGB * 5 * 60

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
			"estimated_runtime_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: estimatedRuntimeSeconds},
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

// buildBatchVolumeBalanceProposals groups detection results into batch proposals.
// Each batch proposal encodes multiple moves in BalanceTaskParams.Moves.
func buildBatchVolumeBalanceProposals(
	results []*workertypes.TaskDetectionResult,
	batchSize int,
	maxConcurrentMoves int,
) []*plugin_pb.JobProposal {
	if batchSize <= 0 {
		batchSize = 1
	}
	if maxConcurrentMoves <= 0 {
		maxConcurrentMoves = defaultMaxConcurrentMoves
	}

	var proposals []*plugin_pb.JobProposal

	for batchStart := 0; batchStart < len(results); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(results) {
			batchEnd = len(results)
		}
		batch := results[batchStart:batchEnd]

		// If only one result in this batch, emit a single-move proposal
		if len(batch) == 1 {
			proposal, err := buildVolumeBalanceProposal(batch[0])
			if err != nil {
				glog.Warningf("Plugin worker skip invalid volume_balance proposal: %v", err)
				continue
			}
			proposals = append(proposals, proposal)
			continue
		}

		// Build batch proposal with BalanceMoveSpec entries
		moves := make([]*worker_pb.BalanceMoveSpec, 0, len(batch))
		var volumeIDs []string
		var dedupeKeys []string
		highestPriority := workertypes.TaskPriorityLow

		for _, result := range batch {
			if result == nil || result.TypedParams == nil {
				continue
			}
			sourceNode := ""
			targetNode := ""
			if len(result.TypedParams.Sources) > 0 {
				sourceNode = result.TypedParams.Sources[0].Node
			}
			if len(result.TypedParams.Targets) > 0 {
				targetNode = result.TypedParams.Targets[0].Node
			}
			// Skip moves with missing required fields that would fail at execution time.
			if result.VolumeID == 0 || sourceNode == "" || targetNode == "" {
				glog.Warningf("Plugin worker skip invalid batch move: volume=%d source=%q target=%q", result.VolumeID, sourceNode, targetNode)
				continue
			}
			moves = append(moves, &worker_pb.BalanceMoveSpec{
				VolumeId:   uint32(result.VolumeID),
				SourceNode: sourceNode,
				TargetNode: targetNode,
				Collection: result.Collection,
				VolumeSize: result.TypedParams.VolumeSize,
			})
			volumeIDs = append(volumeIDs, fmt.Sprintf("%d", result.VolumeID))

			dedupeKey := fmt.Sprintf("volume_balance:%d", result.VolumeID)
			if result.Collection != "" {
				dedupeKey += ":" + result.Collection
			}
			dedupeKeys = append(dedupeKeys, dedupeKey)

			if result.Priority > highestPriority {
				highestPriority = result.Priority
			}
		}

		if len(moves) == 0 {
			continue
		}

		// After filtering, if only one valid move remains, emit a single-move
		// proposal instead of a batch to preserve the simpler execution path.
		if len(moves) == 1 {
			// Find the matching result for the single valid move
			for _, result := range batch {
				if result != nil && uint32(result.VolumeID) == moves[0].VolumeId {
					proposal, err := buildVolumeBalanceProposal(result)
					if err != nil {
						glog.Warningf("Plugin worker skip invalid volume_balance proposal: %v", err)
					} else {
						proposals = append(proposals, proposal)
					}
					break
				}
			}
			continue
		}

		// Serialize batch params
		taskParams := &worker_pb.TaskParams{
			TaskParams: &worker_pb.TaskParams_BalanceParams{
				BalanceParams: &worker_pb.BalanceTaskParams{
					TimeoutSeconds:     defaultBalanceTimeoutSeconds,
					MaxConcurrentMoves: int32(maxConcurrentMoves),
					Moves:              moves,
				},
			},
		}
		payload, err := proto.Marshal(taskParams)
		if err != nil {
			glog.Warningf("Plugin worker failed to marshal batch balance proposal: %v", err)
			continue
		}

		// Estimate runtime so the scheduler grants a per-attempt deadline
		// large enough for the whole batch. Mirrors vacuum's 5 min/GB budget.
		// Use max(largest single move, total / concurrency) so a skewed batch
		// with one big move isn't underestimated by the average.
		var totalSeconds, maxMoveSeconds int64
		for _, m := range moves {
			gb := int64(m.VolumeSize/1024/1024/1024) + 1
			s := gb * 5 * 60
			totalSeconds += s
			if s > maxMoveSeconds {
				maxMoveSeconds = s
			}
		}
		// Round up to whole scheduling rounds: with N moves and C slots,
		// the busiest slot processes ceil(N/C) moves, not N/C. Using
		// avg * ceil(N/C) avoids underestimating when N is not a multiple
		// of C (e.g. 6 moves at concurrency 5 → 2 rounds, not 1.2).
		avgSeconds := totalSeconds / int64(len(moves))
		numRounds := (int64(len(moves)) + int64(maxConcurrentMoves) - 1) / int64(maxConcurrentMoves)
		estimatedRuntimeSeconds := avgSeconds * numRounds
		if maxMoveSeconds > estimatedRuntimeSeconds {
			estimatedRuntimeSeconds = maxMoveSeconds
		}
		// Floor for orchestration overhead — scales per wave (one round of
		// concurrent moves) rather than per move, since setup/teardown
		// happens once per wave, not once per move.
		if minBudget := numRounds * 60; estimatedRuntimeSeconds < minBudget {
			estimatedRuntimeSeconds = minBudget
		}

		proposalID := fmt.Sprintf("volume-balance-batch-%d-%d", batchStart, time.Now().UnixNano())
		summary := fmt.Sprintf("Batch balance %d volumes (%s)", len(moves), strings.Join(volumeIDs, ","))
		if len(summary) > maxProposalStringLength {
			summary = fmt.Sprintf("Batch balance %d volumes", len(moves))
		}

		// Use composite dedupe key for the batch. When the full key exceeds
		// the length limit, fall back to a deterministic hash of the sorted
		// keys so the same batch always produces the same dedupe key.
		sort.Strings(dedupeKeys)
		compositeDedupeKey := fmt.Sprintf("volume_balance_batch:%s", strings.Join(dedupeKeys, "+"))
		if len(compositeDedupeKey) > maxProposalStringLength {
			h := sha256.Sum256([]byte(strings.Join(dedupeKeys, "+")))
			compositeDedupeKey = fmt.Sprintf("volume_balance_batch:%d-%s", batchStart, hex.EncodeToString(h[:12]))
		}

		proposals = append(proposals, &plugin_pb.JobProposal{
			ProposalId: proposalID,
			DedupeKey:  compositeDedupeKey,
			JobType:    "volume_balance",
			Priority:   mapTaskPriority(highestPriority),
			Summary:    summary,
			Detail:     fmt.Sprintf("Batch of %d volume moves with concurrency %d", len(moves), maxConcurrentMoves),
			Parameters: map[string]*plugin_pb.ConfigValue{
				"task_params_pb": {
					Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: payload},
				},
				"batch_size": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(moves))},
				},
				"estimated_runtime_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: estimatedRuntimeSeconds},
				},
			},
			Labels: map[string]string{
				"task_type":  "balance",
				"batch":      "true",
				"batch_size": fmt.Sprintf("%d", len(moves)),
			},
		})
	}

	return proposals
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

func applyBalanceExecutionDefaults(params *worker_pb.TaskParams) {
	if params == nil {
		return
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		params.TaskParams = &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      false,
				TimeoutSeconds: defaultBalanceTimeoutSeconds,
			},
		}
		return
	}

	if balanceParams.TimeoutSeconds <= 0 {
		balanceParams.TimeoutSeconds = defaultBalanceTimeoutSeconds
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
