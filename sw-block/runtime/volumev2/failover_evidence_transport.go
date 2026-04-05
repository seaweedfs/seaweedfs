package volumev2

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

// FailoverEvidenceTransport carries failover-time query traffic across a
// transport/session boundary. This is the first transport seam needed for M1:
// promotion evidence and bounded replica summaries.
type FailoverEvidenceTransport interface {
	QueryPromotionEvidence(nodeID string, req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
	QueryReplicaSummary(nodeID string, req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// FailoverEvidenceHandler is the server-side evidence surface registered behind
// a transport implementation.
type FailoverEvidenceHandler interface {
	QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
	QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error)
}

// NewTransportEvidenceAdapter wraps one stable node id behind the supplied
// failover evidence transport.
func NewTransportEvidenceAdapter(nodeID string, transport FailoverEvidenceTransport) (FailoverEvidenceAdapter, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("volumev2: evidence adapter node id is required")
	}
	if transport == nil {
		return nil, fmt.Errorf("volumev2: evidence transport is nil")
	}
	return transportFailoverEvidenceAdapter{
		nodeID:    nodeID,
		transport: transport,
	}, nil
}

// NewHybridInProcessFailoverTarget builds the first transport-backed failover
// target shape: evidence crosses a transport/session seam, while takeover
// remains primary-local on the selected node.
func NewHybridInProcessFailoverTarget(node *Node, transport FailoverEvidenceTransport) (FailoverTarget, error) {
	if node == nil {
		return FailoverTarget{}, fmt.Errorf("volumev2: node is nil")
	}
	evidence, err := NewTransportEvidenceAdapter(node.NodeID(), transport)
	if err != nil {
		return FailoverTarget{}, err
	}
	return FailoverTarget{
		NodeID:   node.NodeID(),
		Evidence: evidence,
		Takeover: inProcessFailoverTakeoverAdapter{node: node},
	}, nil
}

type transportFailoverEvidenceAdapter struct {
	nodeID    string
	transport FailoverEvidenceTransport
}

func (a transportFailoverEvidenceAdapter) QueryPromotionEvidence(req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	if a.transport == nil {
		return masterv2.PromotionQueryResponse{}, fmt.Errorf("volumev2: transport evidence adapter is nil")
	}
	return a.transport.QueryPromotionEvidence(a.nodeID, req)
}

func (a transportFailoverEvidenceAdapter) QueryReplicaSummary(req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	if a.transport == nil {
		return protocolv2.ReplicaSummaryResponse{}, fmt.Errorf("volumev2: transport evidence adapter is nil")
	}
	return a.transport.QueryReplicaSummary(a.nodeID, req)
}

// InMemoryFailoverEvidenceTransport is the first request/response transport-style
// adapter. It is still in-process, but it eliminates direct orchestration-time
// calls to `*Node` for promotion evidence and replica-summary queries.
type InMemoryFailoverEvidenceTransport struct {
	mu       sync.RWMutex
	handlers map[string]FailoverEvidenceHandler
}

// NewInMemoryFailoverEvidenceTransport creates an in-memory request/response
// transport for failover evidence.
func NewInMemoryFailoverEvidenceTransport() *InMemoryFailoverEvidenceTransport {
	return &InMemoryFailoverEvidenceTransport{
		handlers: make(map[string]FailoverEvidenceHandler),
	}
}

// RegisterHandler binds one stable node id to one evidence handler.
func (t *InMemoryFailoverEvidenceTransport) RegisterHandler(nodeID string, handler FailoverEvidenceHandler) error {
	if t == nil {
		return fmt.Errorf("volumev2: evidence transport is nil")
	}
	if nodeID == "" {
		return fmt.Errorf("volumev2: evidence handler node id is required")
	}
	if handler == nil {
		return fmt.Errorf("volumev2: evidence handler %q is nil", nodeID)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[nodeID] = handler
	return nil
}

// UnregisterHandler removes one evidence handler from the in-memory transport.
func (t *InMemoryFailoverEvidenceTransport) UnregisterHandler(nodeID string) {
	if t == nil || nodeID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, nodeID)
}

func (t *InMemoryFailoverEvidenceTransport) QueryPromotionEvidence(nodeID string, req masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	handler, err := t.handler(nodeID)
	if err != nil {
		return masterv2.PromotionQueryResponse{}, err
	}
	return handler.QueryPromotionEvidence(req)
}

func (t *InMemoryFailoverEvidenceTransport) QueryReplicaSummary(nodeID string, req protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	handler, err := t.handler(nodeID)
	if err != nil {
		return protocolv2.ReplicaSummaryResponse{}, err
	}
	return handler.QueryReplicaSummary(req)
}

func (t *InMemoryFailoverEvidenceTransport) handler(nodeID string) (FailoverEvidenceHandler, error) {
	if t == nil {
		return nil, fmt.Errorf("volumev2: evidence transport is nil")
	}
	if nodeID == "" {
		return nil, fmt.Errorf("volumev2: evidence node id is required")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	handler, ok := t.handlers[nodeID]
	if !ok {
		return nil, fmt.Errorf("volumev2: unknown evidence handler %q", nodeID)
	}
	return handler, nil
}
