package volumev2

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// FailoverEvidenceAdapter is the transport/session-facing query surface used by
// failover orchestration. Future remote implementations should satisfy this
// contract without changing failover session logic.
type FailoverEvidenceAdapter interface {
	QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
	QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// FailoverTakeoverAdapter is the selected-primary execution surface used after
// masterv2 authorizes promotion. Future implementations may be local or remote,
// but takeover ownership remains on the selected primary side.
type FailoverTakeoverAdapter interface {
	PreparePrimaryTakeover(PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error)
	GatePrimaryActivation(volumeName string, truth ReconstructedPrimaryTruth) error
}

// FailoverTarget binds one stable node id to the evidence and takeover adapters
// used by failover orchestration.
type FailoverTarget struct {
	NodeID   string
	Evidence FailoverEvidenceAdapter
	Takeover FailoverTakeoverAdapter
}

// NewInProcessFailoverTarget builds the first adapter-backed target from one
// in-process volumev2 node.
func NewInProcessFailoverTarget(node *Node) (FailoverTarget, error) {
	if node == nil {
		return FailoverTarget{}, fmt.Errorf("volumev2: node is nil")
	}
	if node.NodeID() == "" {
		return FailoverTarget{}, fmt.Errorf("volumev2: node id is required")
	}
	return FailoverTarget{
		NodeID:   node.NodeID(),
		Evidence: inProcessFailoverEvidenceAdapter{node: node},
		Takeover: inProcessFailoverTakeoverAdapter{node: node},
	}, nil
}

type inProcessFailoverEvidenceAdapter struct {
	node *Node
}

func (a inProcessFailoverEvidenceAdapter) QueryPromotionEvidence(req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	if a.node == nil {
		return masterv2.PromotionQueryResponse{}, fmt.Errorf("volumev2: in-process evidence node is nil")
	}
	return a.node.QueryPromotionEvidence(req)
}

func (a inProcessFailoverEvidenceAdapter) QueryReplicaSummary(req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	if a.node == nil {
		return protocolv2.ReplicaSummaryResponse{}, fmt.Errorf("volumev2: in-process evidence node is nil")
	}
	return a.node.QueryReplicaSummary(req)
}

type inProcessFailoverTakeoverAdapter struct {
	node *Node
}

func (a inProcessFailoverTakeoverAdapter) PreparePrimaryTakeover(plan PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error) {
	if a.node == nil {
		return ReconstructedPrimaryTruth{}, fmt.Errorf("volumev2: in-process takeover node is nil")
	}
	return a.node.PreparePrimaryTakeover(plan)
}

func (a inProcessFailoverTakeoverAdapter) GatePrimaryActivation(volumeName string, truth ReconstructedPrimaryTruth) error {
	if a.node == nil {
		return fmt.Errorf("volumev2: in-process takeover node is nil")
	}
	return a.node.GatePrimaryActivation(volumeName, truth)
}
