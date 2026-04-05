package volumev2

import (
	"bytes"
	"fmt"
	"slices"
)

// ReplicatedContinuityResult captures one bounded replicated continuity run
// through the current runtime-owned path.
type ReplicatedContinuityResult struct {
	VolumeName            string
	SourcePrimaryNodeID   string
	SelectedPrimaryNodeID string
	ExpectedEpoch         uint64
	Loop2BeforeFailover   Loop2RuntimeSnapshot
	Failover              FailoverResult
	ReadBackLength        uint32
	DataMatch             bool
}

// ExecuteReplicatedContinuity runs one bounded continuity statement through the
// current runtime:
// mirror writes to the bounded participant set -> observe active Loop 2 ->
// fail over to the survivor set -> read back data from the newly selected
// primary. This is a bounded continuity closure, not a full replication product
// claim.
func (m *InProcessRuntimeManager) ExecuteReplicatedContinuity(volumeName, sourcePrimaryNodeID string, expectedEpoch uint64, survivorNodeIDs []string, lba uint64, payload []byte) (ReplicatedContinuityResult, error) {
	if m == nil {
		return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if volumeName == "" {
		return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity volume name is required")
	}
	if sourcePrimaryNodeID == "" {
		return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity source primary node id is required")
	}
	if len(payload) == 0 {
		return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity payload is required")
	}
	if len(survivorNodeIDs) == 0 {
		return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity survivor node ids are required")
	}

	nodeIDs := append([]string{sourcePrimaryNodeID}, survivorNodeIDs...)
	nodeIDs = uniqueSorted(nodeIDs)
	nodes := make([]*Node, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node, err := m.localNode(nodeID)
		if err != nil {
			return ReplicatedContinuityResult{}, err
		}
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		if err := node.WriteLBA(volumeName, lba, payload); err != nil {
			return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity write %s on %s: %w", volumeName, node.NodeID(), err)
		}
	}
	for _, node := range nodes {
		if err := node.SyncCache(volumeName); err != nil {
			return ReplicatedContinuityResult{}, fmt.Errorf("volumev2: continuity sync %s on %s: %w", volumeName, node.NodeID(), err)
		}
	}

	result := ReplicatedContinuityResult{
		VolumeName:          volumeName,
		SourcePrimaryNodeID: sourcePrimaryNodeID,
		ExpectedEpoch:       expectedEpoch,
	}

	loop2Snap, err := m.ObserveLoop2(volumeName, sourcePrimaryNodeID, expectedEpoch, nodeIDs...)
	if err != nil {
		return result, err
	}
	result.Loop2BeforeFailover = loop2Snap

	failover, err := m.ExecuteFailover(volumeName, expectedEpoch, survivorNodeIDs...)
	result.Failover = failover
	if err != nil {
		return result, err
	}
	result.SelectedPrimaryNodeID = failover.Assignment.NodeID

	selectedNode, err := m.localNode(failover.Assignment.NodeID)
	if err != nil {
		return result, err
	}
	readBack, err := selectedNode.ReadLBA(volumeName, lba, uint32(len(payload)))
	if err != nil {
		return result, fmt.Errorf("volumev2: continuity readback %s on %s: %w", volumeName, selectedNode.NodeID(), err)
	}
	result.ReadBackLength = uint32(len(readBack))
	result.DataMatch = bytes.Equal(readBack, payload)
	if !result.DataMatch {
		return result, fmt.Errorf("volumev2: continuity payload mismatch after failover")
	}
	return result, nil
}

func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}
