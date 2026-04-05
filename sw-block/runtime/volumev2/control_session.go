package volumev2

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
)

// ControlSession is the minimal control-plane contract between volumev2 and masterv2.
type ControlSession interface {
	Heartbeat(masterv2.NodeHeartbeat) ([]masterv2.Assignment, error)
}

// PromotionEvidenceSource is the on-demand Loop 1 query surface used during
// failover arbitration.
type PromotionEvidenceSource interface {
	QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error)
}

// InProcessSession is the first in-process control-plane adapter used by the MVP.
type InProcessSession struct {
	master *masterv2.Master
}

// NewInProcessSession creates a control session backed by one in-process masterv2.
func NewInProcessSession(master *masterv2.Master) (*InProcessSession, error) {
	if master == nil {
		return nil, fmt.Errorf("volumev2: master is nil")
	}
	return &InProcessSession{master: master}, nil
}

// Heartbeat sends one periodic heartbeat to masterv2 and returns the assignments to apply.
func (s *InProcessSession) Heartbeat(hb masterv2.NodeHeartbeat) ([]masterv2.Assignment, error) {
	if s == nil || s.master == nil {
		return nil, fmt.Errorf("volumev2: control session is nil")
	}
	return s.master.HandleHeartbeat(hb)
}

// Sync is retained as a narrow compatibility shim for existing tests while the
// three-channel Loop 1 surface settles.
func (s *InProcessSession) Sync(hb masterv2.NodeHeartbeat) ([]masterv2.Assignment, error) {
	return s.Heartbeat(hb)
}
