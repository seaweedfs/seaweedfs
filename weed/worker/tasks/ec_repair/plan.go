package ec_repair

import (
	"fmt"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/placement"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
)

type volumeShardState struct {
	Key         VolumeKey
	ShardMap    map[uint32][]ShardLocation
	MaxShardID  uint32
	SeenTargets map[string]struct{}
}

type shardAnalysis struct {
	Missing     []uint32
	Keep        map[uint32]ShardLocation
	Delete      []ShardLocation
	Mismatched  []ShardLocation
	TotalShards int
}

// Detect scans topology for EC shard issues and returns repair candidates.
func Detect(topoInfo *master_pb.TopologyInfo, collectionFilter string, maxResults int) ([]*RepairCandidate, bool, error) {
	if topoInfo == nil {
		return nil, false, fmt.Errorf("topology info is nil")
	}

	states := collectShardStates(topoInfo, collectionFilter)
	keys := make([]VolumeKey, 0, len(states))
	for key := range states {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].VolumeID != keys[j].VolumeID {
			return keys[i].VolumeID < keys[j].VolumeID
		}
		if keys[i].Collection != keys[j].Collection {
			return keys[i].Collection < keys[j].Collection
		}
		return keys[i].DiskType < keys[j].DiskType
	})

	if maxResults < 0 {
		maxResults = 0
	}

	var candidates []*RepairCandidate
	hasMore := false

	for _, key := range keys {
		state := states[key]
		if state == nil {
			continue
		}
		analysis := analyzeShardState(state)
		if len(analysis.Missing) == 0 && len(analysis.Delete) == 0 {
			continue
		}
		if len(analysis.Missing) > 0 && len(analysis.Keep) < erasure_coding.DataShardsCount {
			// Not enough shards to rebuild missing shards safely.
			continue
		}

		candidates = append(candidates, &RepairCandidate{
			VolumeID:         key.VolumeID,
			Collection:       key.Collection,
			DiskType:         key.DiskType,
			MissingShards:    len(analysis.Missing),
			ExtraShards:      len(analysis.Delete),
			MismatchedShards: len(analysis.Mismatched),
		})

		if maxResults > 0 && len(candidates) >= maxResults {
			if len(candidates) < len(keys) {
				hasMore = true
			}
			break
		}
	}

	return candidates, hasMore, nil
}

// BuildRepairPlan creates a concrete repair plan for one EC volume.
func BuildRepairPlan(
	topoInfo *master_pb.TopologyInfo,
	activeTopology *topology.ActiveTopology,
	volumeID uint32,
	collection string,
	diskType string,
) (*RepairPlan, error) {
	if topoInfo == nil {
		return nil, fmt.Errorf("topology info is nil")
	}
	states := collectShardStates(topoInfo, "")

	var state *volumeShardState
	for key, candidate := range states {
		if key.VolumeID != volumeID {
			continue
		}
		if collection != "" && key.Collection != collection {
			continue
		}
		if diskType != "" && key.DiskType != diskType {
			continue
		}
		state = candidate
		break
	}
	if state == nil {
		return nil, fmt.Errorf("ec volume %d not found in topology", volumeID)
	}

	analysis := analyzeShardState(state)
	if len(analysis.Missing) == 0 && len(analysis.Delete) == 0 {
		return &RepairPlan{
			VolumeID:     volumeID,
			Collection:   state.Key.Collection,
			DiskType:     state.Key.DiskType,
			DeleteByNode: map[string][]uint32{},
		}, nil
	}
	if len(analysis.Missing) > 0 && len(analysis.Keep) < erasure_coding.DataShardsCount {
		return nil, fmt.Errorf("ec volume %d has %d shards, need at least %d to rebuild", volumeID, len(analysis.Keep), erasure_coding.DataShardsCount)
	}

	deleteByNode := groupDeleteByNode(analysis.Delete, activeTopology)

	plan := &RepairPlan{
		VolumeID:      volumeID,
		Collection:    state.Key.Collection,
		DiskType:      state.Key.DiskType,
		MissingShards: analysis.Missing,
		DeleteByNode:  deleteByNode,
		CopySources:   make(map[uint32]string),
	}

	if len(analysis.Keep) > 0 {
		rebuilder := selectRebuilder(analysis.Keep)
		plan.Rebuilder = rebuilder
		plan.CopySources = buildCopySources(analysis.Keep)
	}

	if len(analysis.Missing) > 0 {
		if activeTopology == nil {
			return nil, fmt.Errorf("active topology is required to place missing shards")
		}
		usedNodes := collectUsedNodes(analysis.Keep)
		targets, err := planMissingTargets(activeTopology, analysis.Missing, usedNodes, state.Key.DiskType)
		if err != nil {
			return nil, err
		}
		plan.Targets = targets
	}

	return plan, nil
}

func collectShardStates(topoInfo *master_pb.TopologyInfo, collectionFilter string) map[VolumeKey]*volumeShardState {
	states := make(map[VolumeKey]*volumeShardState)
	filter := strings.TrimSpace(collectionFilter)

	for _, dc := range topoInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				nodeAddress := string(pb.NewServerAddressFromDataNode(node))
				for diskTypeKey, diskInfo := range node.DiskInfos {
					if diskInfo == nil {
						continue
					}
					for _, shardInfo := range diskInfo.EcShardInfos {
						if shardInfo == nil {
							continue
						}
						if filter != "" && shardInfo.Collection != filter {
							continue
						}

						recordedDiskType := strings.TrimSpace(shardInfo.DiskType)
						if recordedDiskType == "" {
							recordedDiskType = diskTypeKey
						}

						key := VolumeKey{
							VolumeID:   shardInfo.Id,
							Collection: shardInfo.Collection,
							DiskType:   recordedDiskType,
						}
						state := states[key]
						if state == nil {
							state = &volumeShardState{
								Key:         key,
								ShardMap:    make(map[uint32][]ShardLocation),
								SeenTargets: make(map[string]struct{}),
							}
							states[key] = state
						}

						shardsInfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo)
						for _, shard := range shardsInfo.AsSlice() {
							shardID := uint32(shard.Id)
							if shardID > state.MaxShardID {
								state.MaxShardID = shardID
							}
							locationKey := fmt.Sprintf("%s:%d:%d", node.Id, shardInfo.DiskId, shardID)
							if _, seen := state.SeenTargets[locationKey]; seen {
								continue
							}
							state.SeenTargets[locationKey] = struct{}{}

							state.ShardMap[shardID] = append(state.ShardMap[shardID], ShardLocation{
								NodeID:      node.Id,
								NodeAddress: nodeAddress,
								DataCenter:  dc.Id,
								Rack:        rack.Id,
								DiskType:    recordedDiskType,
								DiskID:      shardInfo.DiskId,
								ShardID:     shardID,
								Size:        int64(shard.Size),
							})
						}
					}
				}
			}
		}
	}

	return states
}

func analyzeShardState(state *volumeShardState) shardAnalysis {
	analysis := shardAnalysis{
		Keep: make(map[uint32]ShardLocation),
	}
	if state == nil {
		return analysis
	}

	expectedTotal := erasure_coding.TotalShardsCount
	if int(state.MaxShardID)+1 > expectedTotal && int(state.MaxShardID)+1 <= erasure_coding.MaxShardCount {
		expectedTotal = int(state.MaxShardID) + 1
	}
	analysis.TotalShards = expectedTotal

	candidates := make(map[uint32][]ShardLocation, expectedTotal)
	var mismatched []ShardLocation
	for shardID := 0; shardID < expectedTotal; shardID++ {
		locations := state.ShardMap[uint32(shardID)]
		if len(locations) == 0 {
			analysis.Missing = append(analysis.Missing, uint32(shardID))
			continue
		}
		filtered, outliers := splitByCanonicalSize(locations)
		mismatched = append(mismatched, outliers...)
		candidates[uint32(shardID)] = filtered
	}

	analysis.Mismatched = mismatched
	keep := selectDiverseLocations(candidates)
	analysis.Keep = keep

	var deletions []ShardLocation
	for shardID, locations := range candidates {
		keptLocation, ok := keep[shardID]
		for _, location := range locations {
			if ok && location.NodeID == keptLocation.NodeID && location.DiskID == keptLocation.DiskID {
				continue
			}
			deletions = append(deletions, location)
		}
	}
	deletions = append(deletions, mismatched...)
	analysis.Delete = dedupeLocations(deletions)

	return analysis
}

func splitByCanonicalSize(locations []ShardLocation) ([]ShardLocation, []ShardLocation) {
	if len(locations) == 0 {
		return nil, nil
	}

	sizeCounts := make(map[int64]int)
	for _, location := range locations {
		if location.Size > 0 {
			sizeCounts[location.Size]++
		}
	}

	canonicalSize := int64(0)
	if len(sizeCounts) > 0 {
		maxCount := -1
		for size, count := range sizeCounts {
			if count > maxCount || (count == maxCount && size > canonicalSize) {
				maxCount = count
				canonicalSize = size
			}
		}
	}

	var filtered []ShardLocation
	var outliers []ShardLocation
	for _, location := range locations {
		if len(sizeCounts) == 0 {
			filtered = append(filtered, location)
			continue
		}
		if location.Size == canonicalSize {
			filtered = append(filtered, location)
			continue
		}
		outliers = append(outliers, location)
	}
	if len(filtered) == 0 {
		filtered = append(filtered, locations...)
		outliers = nil
	}
	return filtered, outliers
}

func selectDiverseLocations(candidates map[uint32][]ShardLocation) map[uint32]ShardLocation {
	keep := make(map[uint32]ShardLocation, len(candidates))
	if len(candidates) == 0 {
		return keep
	}

	shardIDs := make([]uint32, 0, len(candidates))
	for shardID := range candidates {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Slice(shardIDs, func(i, j int) bool {
		li := len(candidates[shardIDs[i]])
		lj := len(candidates[shardIDs[j]])
		if li != lj {
			return li < lj
		}
		return shardIDs[i] < shardIDs[j]
	})

	dcCount := make(map[string]int)
	rackCount := make(map[string]int)
	nodeCount := make(map[string]int)

	for _, shardID := range shardIDs {
		locations := candidates[shardID]
		if len(locations) == 0 {
			continue
		}
		best := locations[0]
		bestScore := scoreLocation(best, dcCount, rackCount, nodeCount)
		for _, location := range locations[1:] {
			score := scoreLocation(location, dcCount, rackCount, nodeCount)
			if scoreLess(score, bestScore) {
				best = location
				bestScore = score
			}
		}
		keep[shardID] = best

		dcCount[best.DataCenter]++
		rackKey := best.DataCenter + ":" + best.Rack
		rackCount[rackKey]++
		nodeCount[best.NodeID]++
	}

	return keep
}

type locationScore struct {
	dcCount   int
	rackCount int
	nodeCount int
	nodeID    string
	diskID    uint32
}

func scoreLocation(loc ShardLocation, dcCount, rackCount, nodeCount map[string]int) locationScore {
	rackKey := loc.DataCenter + ":" + loc.Rack
	return locationScore{
		dcCount:   dcCount[loc.DataCenter],
		rackCount: rackCount[rackKey],
		nodeCount: nodeCount[loc.NodeID],
		nodeID:    loc.NodeID,
		diskID:    loc.DiskID,
	}
}

func scoreLess(a, b locationScore) bool {
	if a.dcCount != b.dcCount {
		return a.dcCount < b.dcCount
	}
	if a.rackCount != b.rackCount {
		return a.rackCount < b.rackCount
	}
	if a.nodeCount != b.nodeCount {
		return a.nodeCount < b.nodeCount
	}
	if a.nodeID != b.nodeID {
		return a.nodeID < b.nodeID
	}
	return a.diskID < b.diskID
}

func dedupeLocations(locations []ShardLocation) []ShardLocation {
	if len(locations) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(locations))
	out := make([]ShardLocation, 0, len(locations))
	for _, loc := range locations {
		key := fmt.Sprintf("%s:%d:%d", loc.NodeID, loc.DiskID, loc.ShardID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, loc)
	}
	return out
}

func groupDeleteByNode(locations []ShardLocation, activeTopology *topology.ActiveTopology) map[string][]uint32 {
	result := make(map[string]map[uint32]struct{})
	for _, loc := range locations {
		nodeAddress := strings.TrimSpace(loc.NodeAddress)
		if nodeAddress == "" && activeTopology != nil {
			resolved, err := util.ResolveServerAddress(loc.NodeID, activeTopology)
			if err == nil {
				nodeAddress = resolved
			}
		}
		if nodeAddress == "" {
			continue
		}
		if result[nodeAddress] == nil {
			result[nodeAddress] = make(map[uint32]struct{})
		}
		result[nodeAddress][loc.ShardID] = struct{}{}
	}

	final := make(map[string][]uint32, len(result))
	for nodeAddress, shardSet := range result {
		shards := make([]uint32, 0, len(shardSet))
		for shardID := range shardSet {
			shards = append(shards, shardID)
		}
		sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
		final[nodeAddress] = shards
	}
	return final
}

func selectRebuilder(keep map[uint32]ShardLocation) RebuilderPlan {
	var rebuilder RebuilderPlan
	if len(keep) == 0 {
		return rebuilder
	}

	nodeCounts := make(map[string]int)
	diskCounts := make(map[string]map[uint32]int)
	localShards := make(map[string][]uint32)
	for shardID, loc := range keep {
		nodeCounts[loc.NodeAddress]++
		if diskCounts[loc.NodeAddress] == nil {
			diskCounts[loc.NodeAddress] = make(map[uint32]int)
		}
		diskCounts[loc.NodeAddress][loc.DiskID]++
		localShards[loc.NodeAddress] = append(localShards[loc.NodeAddress], shardID)
	}

	var selectedAddr string
	maxCount := -1
	for addr, count := range nodeCounts {
		if count > maxCount || (count == maxCount && addr < selectedAddr) {
			selectedAddr = addr
			maxCount = count
		}
	}

	var selectedDisk uint32
	maxDiskCount := -1
	for diskID, count := range diskCounts[selectedAddr] {
		if count > maxDiskCount || (count == maxDiskCount && diskID < selectedDisk) {
			selectedDisk = diskID
			maxDiskCount = count
		}
	}

	shards := localShards[selectedAddr]
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })

	rebuilder.NodeAddress = selectedAddr
	rebuilder.DiskID = selectedDisk
	rebuilder.LocalShards = shards
	return rebuilder
}

func buildCopySources(keep map[uint32]ShardLocation) map[uint32]string {
	copySources := make(map[uint32]string, len(keep))
	for shardID, loc := range keep {
		copySources[shardID] = loc.NodeAddress
	}
	return copySources
}

func collectUsedNodes(keep map[uint32]ShardLocation) map[string]bool {
	used := make(map[string]bool)
	for _, loc := range keep {
		if loc.NodeID == "" {
			continue
		}
		used[loc.NodeID] = true
	}
	return used
}

func planMissingTargets(activeTopology *topology.ActiveTopology, missing []uint32, usedNodes map[string]bool, diskType string) ([]ShardTarget, error) {
	if activeTopology == nil {
		return nil, fmt.Errorf("active topology is nil")
	}
	if len(missing) == 0 {
		return nil, nil
	}

	selected, err := selectTargetDisks(activeTopology, len(missing), usedNodes, diskType)
	if err != nil {
		selected, err = selectTargetDisks(activeTopology, len(missing), nil, diskType)
		if err != nil {
			return nil, err
		}
	}

	missingIDs := make([]uint32, len(missing))
	copy(missingIDs, missing)
	sort.Slice(missingIDs, func(i, j int) bool { return missingIDs[i] < missingIDs[j] })

	targetMap := make(map[string]*ShardTarget)
	for i, shardID := range missingIDs {
		disk := selected[i]
		address, err := util.ResolveServerAddress(disk.NodeID, activeTopology)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s:%d", disk.NodeID, disk.DiskID)
		target := targetMap[key]
		if target == nil {
			target = &ShardTarget{NodeAddress: address, DiskID: disk.DiskID}
			targetMap[key] = target
		}
		target.ShardIDs = append(target.ShardIDs, shardID)
	}

	targets := make([]ShardTarget, 0, len(targetMap))
	for _, target := range targetMap {
		sort.Slice(target.ShardIDs, func(i, j int) bool { return target.ShardIDs[i] < target.ShardIDs[j] })
		targets = append(targets, *target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].NodeAddress != targets[j].NodeAddress {
			return targets[i].NodeAddress < targets[j].NodeAddress
		}
		return targets[i].DiskID < targets[j].DiskID
	})

	return targets, nil
}

func selectTargetDisks(activeTopology *topology.ActiveTopology, shardCount int, usedNodes map[string]bool, diskType string) ([]*placement.DiskCandidate, error) {
	disks := activeTopology.GetDisksWithEffectiveCapacity(topology.TaskTypeErasureCoding, "", 0)
	if len(disks) == 0 {
		return nil, fmt.Errorf("no disks available for EC repair placement")
	}

	candidates := make([]*topology.DiskInfo, 0, len(disks))
	for _, disk := range disks {
		if disk == nil || disk.DiskInfo == nil {
			continue
		}
		if diskType != "" && !strings.EqualFold(disk.DiskType, diskType) {
			continue
		}
		if usedNodes != nil && usedNodes[disk.NodeID] {
			continue
		}
		candidates = append(candidates, disk)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no disks available for EC repair placement after filtering")
	}

	diskCandidates := diskInfosToCandidates(candidates)
	if len(diskCandidates) == 0 {
		return nil, fmt.Errorf("no candidate disks available for EC repair placement")
	}

	config := placement.PlacementRequest{
		ShardsNeeded:           shardCount,
		MaxShardsPerServer:     0,
		MaxShardsPerRack:       0,
		MaxTaskLoad:            topology.MaxTaskLoadForECPlacement,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	result, err := placement.SelectDestinations(diskCandidates, config)
	if err != nil {
		return nil, err
	}
	if len(result.SelectedDisks) < shardCount {
		return nil, fmt.Errorf("found %d destination disks, need %d", len(result.SelectedDisks), shardCount)
	}
	return result.SelectedDisks, nil
}

func diskInfosToCandidates(disks []*topology.DiskInfo) []*placement.DiskCandidate {
	candidates := make([]*placement.DiskCandidate, 0, len(disks))
	for _, disk := range disks {
		if disk == nil || disk.DiskInfo == nil {
			continue
		}
		freeSlots := int(disk.DiskInfo.MaxVolumeCount - disk.DiskInfo.VolumeCount)
		if freeSlots < 0 {
			freeSlots = 0
		}

		ecShardCount := 0
		if disk.DiskInfo.EcShardInfos != nil {
			for _, shardInfo := range disk.DiskInfo.EcShardInfos {
				if shardInfo.DiskId == disk.DiskID {
					ecShardCount += erasure_coding.GetShardCount(shardInfo)
				}
			}
		}

		candidates = append(candidates, &placement.DiskCandidate{
			NodeID:         disk.NodeID,
			DiskID:         disk.DiskID,
			DataCenter:     disk.DataCenter,
			Rack:           disk.Rack,
			FreeSlots:      freeSlots,
			MaxVolumeCount: disk.DiskInfo.MaxVolumeCount,
			VolumeCount:    disk.DiskInfo.VolumeCount,
			ShardCount:     ecShardCount,
			LoadCount:      disk.LoadCount,
		})
	}
	return candidates
}
