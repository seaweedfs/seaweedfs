package plugin

import (
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/protobuf/proto"
)

func enrichTrackedJobParameters(jobType string, parameters map[string]interface{}) map[string]interface{} {
	if len(parameters) == 0 {
		return parameters
	}
	if _, exists := parameters["execution_plan"]; exists {
		return parameters
	}

	taskParams, ok := decodeTaskParamsFromPlainParameters(parameters)
	if !ok || taskParams == nil {
		return parameters
	}

	plan := buildExecutionPlan(strings.TrimSpace(jobType), taskParams)
	if plan == nil {
		return parameters
	}

	enriched := make(map[string]interface{}, len(parameters)+1)
	for key, value := range parameters {
		enriched[key] = value
	}
	enriched["execution_plan"] = plan
	return enriched
}

func decodeTaskParamsFromPlainParameters(parameters map[string]interface{}) (*worker_pb.TaskParams, bool) {
	rawField, ok := parameters["task_params_pb"]
	if !ok || rawField == nil {
		return nil, false
	}

	fieldMap, ok := rawField.(map[string]interface{})
	if !ok {
		return nil, false
	}

	bytesValue, _ := fieldMap["bytes_value"].(string)
	bytesValue = strings.TrimSpace(bytesValue)
	if bytesValue == "" {
		return nil, false
	}

	payload, err := base64.StdEncoding.DecodeString(bytesValue)
	if err != nil {
		return nil, false
	}

	params := &worker_pb.TaskParams{}
	if err := proto.Unmarshal(payload, params); err != nil {
		return nil, false
	}

	return params, true
}

func buildExecutionPlan(jobType string, params *worker_pb.TaskParams) map[string]interface{} {
	if params == nil {
		return nil
	}

	normalizedJobType := strings.TrimSpace(jobType)
	if normalizedJobType == "" && params.GetErasureCodingParams() != nil {
		normalizedJobType = "erasure_coding"
	}

	switch normalizedJobType {
	case "erasure_coding":
		return buildErasureCodingExecutionPlan(params)
	default:
		return nil
	}
}

func buildErasureCodingExecutionPlan(params *worker_pb.TaskParams) map[string]interface{} {
	if params == nil {
		return nil
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return nil
	}

	dataShards := int(ecParams.DataShards)
	if dataShards <= 0 {
		dataShards = int(erasure_coding.DataShardsCount)
	}
	parityShards := int(ecParams.ParityShards)
	if parityShards <= 0 {
		parityShards = int(erasure_coding.ParityShardsCount)
	}
	totalShards := dataShards + parityShards
	if totalShards <= 0 {
		totalShards = int(erasure_coding.TotalShardsCount)
	}

	sources := make([]map[string]interface{}, 0, len(params.Sources))
	for _, source := range params.Sources {
		if source == nil {
			continue
		}
		sources = append(sources, buildExecutionEndpoint(
			source.Node,
			source.DataCenter,
			source.Rack,
			source.VolumeId,
			source.ShardIds,
			dataShards,
		))
	}

	targets := make([]map[string]interface{}, 0, len(params.Targets))
	shardAssignments := make([]map[string]interface{}, 0, totalShards)
	for targetIndex, target := range params.Targets {
		if target == nil {
			continue
		}

		targets = append(targets, buildExecutionEndpoint(
			target.Node,
			target.DataCenter,
			target.Rack,
			target.VolumeId,
			target.ShardIds,
			dataShards,
		))

		for _, shardID := range normalizeShardIDs(target.ShardIds) {
			kind, label := classifyShardID(shardID, dataShards)
			shardAssignments = append(shardAssignments, map[string]interface{}{
				"shard_id":           shardID,
				"kind":               kind,
				"label":              label,
				"target_index":       targetIndex + 1,
				"target_node":        strings.TrimSpace(target.Node),
				"target_data_center": strings.TrimSpace(target.DataCenter),
				"target_rack":        strings.TrimSpace(target.Rack),
				"target_volume_id":   int(target.VolumeId),
			})
		}
	}
	sort.Slice(shardAssignments, func(i, j int) bool {
		left, _ := shardAssignments[i]["shard_id"].(int)
		right, _ := shardAssignments[j]["shard_id"].(int)
		return left < right
	})

	plan := map[string]interface{}{
		"job_type":      "erasure_coding",
		"task_id":       strings.TrimSpace(params.TaskId),
		"volume_id":     int(params.VolumeId),
		"collection":    strings.TrimSpace(params.Collection),
		"data_shards":   dataShards,
		"parity_shards": parityShards,
		"total_shards":  totalShards,
		"sources":       sources,
		"targets":       targets,
		"source_count":  len(sources),
		"target_count":  len(targets),
	}

	if len(shardAssignments) > 0 {
		plan["shard_assignments"] = shardAssignments
	}

	return plan
}

func buildExecutionEndpoint(
	node string,
	dataCenter string,
	rack string,
	volumeID uint32,
	shardIDs []uint32,
	dataShardCount int,
) map[string]interface{} {
	allShards := normalizeShardIDs(shardIDs)
	dataShards := make([]int, 0, len(allShards))
	parityShards := make([]int, 0, len(allShards))
	for _, shardID := range allShards {
		if shardID < dataShardCount {
			dataShards = append(dataShards, shardID)
		} else {
			parityShards = append(parityShards, shardID)
		}
	}

	return map[string]interface{}{
		"node":             strings.TrimSpace(node),
		"data_center":      strings.TrimSpace(dataCenter),
		"rack":             strings.TrimSpace(rack),
		"volume_id":        int(volumeID),
		"shard_ids":        allShards,
		"data_shard_ids":   dataShards,
		"parity_shard_ids": parityShards,
	}
}

func normalizeShardIDs(shardIDs []uint32) []int {
	if len(shardIDs) == 0 {
		return nil
	}

	out := make([]int, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		out = append(out, int(shardID))
	}
	sort.Ints(out)
	return out
}

func classifyShardID(shardID int, dataShardCount int) (kind string, label string) {
	if dataShardCount <= 0 {
		dataShardCount = int(erasure_coding.DataShardsCount)
	}
	if shardID < dataShardCount {
		return "data", "D" + strconv.Itoa(shardID)
	}
	return "parity", strconv.Itoa(shardID)
}
