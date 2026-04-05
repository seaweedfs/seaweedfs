package volumev2

import (
	"fmt"
	"slices"
	"sync"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
)

// InProcessFailoverDriver is the first thin driver that wires one in-process
// masterv2 instance to a set of failover-capable participants. It owns no
// recovery logic; it only resolves participants and constructs sessions.
type InProcessFailoverDriver struct {
	master *masterv2.Master

	mu      sync.RWMutex
	targets map[string]FailoverTarget
}

// NewInProcessFailoverDriver creates a driver for one in-process masterv2.
func NewInProcessFailoverDriver(master *masterv2.Master) (*InProcessFailoverDriver, error) {
	if master == nil {
		return nil, fmt.Errorf("volumev2: master is nil")
	}
	return &InProcessFailoverDriver{
		master:  master,
		targets: make(map[string]FailoverTarget),
	}, nil
}

// RegisterTarget binds one stable node id to one explicit failover target.
func (d *InProcessFailoverDriver) RegisterTarget(target FailoverTarget) error {
	if d == nil {
		return fmt.Errorf("volumev2: failover driver is nil")
	}
	if target.NodeID == "" {
		return fmt.Errorf("volumev2: target node id is required")
	}
	if target.Evidence == nil {
		return fmt.Errorf("volumev2: target %q missing evidence adapter", target.NodeID)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.targets[target.NodeID] = target
	return nil
}

// RegisterParticipant preserves the old convenience path by wrapping one
// legacy failover participant into an explicit target.
func (d *InProcessFailoverDriver) RegisterParticipant(nodeID string, participant FailoverParticipant) error {
	if participant == nil {
		return fmt.Errorf("volumev2: participant %q is nil", nodeID)
	}
	return d.RegisterTarget(FailoverTarget{
		NodeID:   nodeID,
		Evidence: participant,
		Takeover: participant,
	})
}

// UnregisterParticipant removes one node from the in-process driver.
func (d *InProcessFailoverDriver) UnregisterParticipant(nodeID string) {
	if d == nil || nodeID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.targets, nodeID)
}

// ParticipantNodeIDs returns the currently registered node ids in stable order.
func (d *InProcessFailoverDriver) ParticipantNodeIDs() []string {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	nodeIDs := make([]string, 0, len(d.targets))
	for nodeID := range d.targets {
		nodeIDs = append(nodeIDs, nodeID)
	}
	slices.Sort(nodeIDs)
	return nodeIDs
}

// NewSession constructs a FailoverSession using either the requested node ids
// or all currently registered participants when none are specified.
func (d *InProcessFailoverDriver) NewSession(volumeName string, expectedEpoch uint64, nodeIDs ...string) (*FailoverSession, error) {
	if d == nil {
		return nil, fmt.Errorf("volumev2: failover driver is nil")
	}
	targets, err := d.resolveTargets(nodeIDs)
	if err != nil {
		return nil, err
	}
	return NewFailoverSession(d.master, volumeName, expectedEpoch, targets)
}

// Execute runs one failover using the resolved participant set.
func (d *InProcessFailoverDriver) Execute(volumeName string, expectedEpoch uint64, nodeIDs ...string) (FailoverResult, error) {
	session, err := d.NewSession(volumeName, expectedEpoch, nodeIDs...)
	if err != nil {
		return FailoverResult{}, err
	}
	return session.Run()
}

func (d *InProcessFailoverDriver) resolveTargets(nodeIDs []string) ([]FailoverTarget, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.targets) == 0 {
		return nil, fmt.Errorf("volumev2: no failover targets registered")
	}

	resolvedIDs := nodeIDs
	if len(resolvedIDs) == 0 {
		resolvedIDs = make([]string, 0, len(d.targets))
		for nodeID := range d.targets {
			resolvedIDs = append(resolvedIDs, nodeID)
		}
		slices.Sort(resolvedIDs)
	}

	targets := make([]FailoverTarget, 0, len(resolvedIDs))
	for _, nodeID := range resolvedIDs {
		target, ok := d.targets[nodeID]
		if !ok {
			return nil, fmt.Errorf("volumev2: unknown failover target %q", nodeID)
		}
		targets = append(targets, target)
	}
	return targets, nil
}
