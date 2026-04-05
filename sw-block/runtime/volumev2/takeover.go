package volumev2

import (
	"fmt"
	"slices"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// ReplicaSummarySource is the bounded Loop 2 query surface a replacement
// primary uses to reconstruct takeover truth from peers.
type ReplicaSummarySource interface {
	QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// PrimaryTakeoverPlan is the minimal input needed for a selected replacement
// primary to prepare takeover locally.
type PrimaryTakeoverPlan struct {
	Assignment masterv2.Assignment
	Peers      []ReplicaSummarySource
}

// ReconstructTakeoverTruth lets the selected replacement primary gather its own
// bounded summary plus peer summaries before it resumes data-control ownership.
// Peers should exclude the current node; duplicate node IDs are ignored.
func (n *Node) ReconstructTakeoverTruth(volumeName string, expectedEpoch uint64, peers []ReplicaSummarySource) (ReconstructedPrimaryTruth, error) {
	if n == nil {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: node is nil")
	}
	req := protocolv2.ReplicaSummaryRequest{
		VolumeName:    volumeName,
		ExpectedEpoch: expectedEpoch,
	}
	self, err := n.QueryReplicaSummary(req)
	if err != nil {
		return ReconstructedPrimaryTruth{}, err
	}

	byNode := map[string]protocolv2.ReplicaSummaryResponse{
		self.NodeID: self,
	}
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		summary, err := peer.QueryReplicaSummary(req)
		if err != nil {
			return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: peer replica summary %s: %w", volumeName, err)
		}
		if summary.NodeID == "" || summary.NodeID == n.id {
			continue
		}
		byNode[summary.NodeID] = summary
	}

	nodeIDs := make([]string, 0, len(byNode))
	for nodeID := range byNode {
		nodeIDs = append(nodeIDs, nodeID)
	}
	slices.Sort(nodeIDs)
	summaries := make([]protocolv2.ReplicaSummaryResponse, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		summaries = append(summaries, byNode[nodeID])
	}
	return ReconstructPrimaryTruth(n.id, summaries)
}

// PreparePrimaryTakeover applies the local primary assignment and reconstructs
// bounded takeover truth from self and peers. It does not decide whether the
// new primary is allowed to activate data control yet.
func (n *Node) PreparePrimaryTakeover(plan PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error) {
	if n == nil {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: node is nil")
	}
	a := plan.Assignment
	if a.Role != "primary" {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: unsupported takeover role %q", a.Role)
	}
	if a.NodeID != "" && a.NodeID != n.id {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: takeover assignment targets %q, node is %q", a.NodeID, n.id)
	}
	if err := n.ApplyAssignments([]masterv2.Assignment{a}); err != nil {
		return ReconstructedPrimaryTruth{}, err
	}
	return n.ReconstructTakeoverTruth(a.Name, a.Epoch, plan.Peers)
}

// GatePrimaryActivation fail-closes activation when the reconstructed truth
// says takeover is degraded, ambiguous, or rebuild-only.
func (n *Node) GatePrimaryActivation(volumeName string, truth ReconstructedPrimaryTruth) error {
	if n == nil {
		return fmt.Errorf("volumev2: node is nil")
	}
	if truth.NeedsRebuild {
		return fmt.Errorf("volumev2: takeover gated for %s: needs rebuild", volumeName)
	}
	if truth.Degraded {
		return fmt.Errorf("volumev2: takeover gated for %s: %s", volumeName, truth.Reason)
	}
	return nil
}

// ApplyPrimaryTakeover is a narrow compatibility wrapper that prepares
// takeover truth and then gates activation.
func (n *Node) ApplyPrimaryTakeover(plan PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error) {
	truth, err := n.PreparePrimaryTakeover(plan)
	if err != nil {
		return ReconstructedPrimaryTruth{}, err
	}
	if err := n.GatePrimaryActivation(plan.Assignment.Name, truth); err != nil {
		return truth, err
	}
	return truth, nil
}
