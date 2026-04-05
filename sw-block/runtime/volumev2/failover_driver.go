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

	mu           sync.RWMutex
	participants map[string]FailoverParticipant
}

// NewInProcessFailoverDriver creates a driver for one in-process masterv2.
func NewInProcessFailoverDriver(master *masterv2.Master) (*InProcessFailoverDriver, error) {
	if master == nil {
		return nil, fmt.Errorf("volumev2: master is nil")
	}
	return &InProcessFailoverDriver{
		master:       master,
		participants: make(map[string]FailoverParticipant),
	}, nil
}

// RegisterParticipant binds one stable node id to one failover participant.
func (d *InProcessFailoverDriver) RegisterParticipant(nodeID string, participant FailoverParticipant) error {
	if d == nil {
		return fmt.Errorf("volumev2: failover driver is nil")
	}
	if nodeID == "" {
		return fmt.Errorf("volumev2: participant node id is required")
	}
	if participant == nil {
		return fmt.Errorf("volumev2: participant %q is nil", nodeID)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.participants[nodeID] = participant
	return nil
}

// UnregisterParticipant removes one node from the in-process driver.
func (d *InProcessFailoverDriver) UnregisterParticipant(nodeID string) {
	if d == nil || nodeID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.participants, nodeID)
}

// ParticipantNodeIDs returns the currently registered node ids in stable order.
func (d *InProcessFailoverDriver) ParticipantNodeIDs() []string {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	nodeIDs := make([]string, 0, len(d.participants))
	for nodeID := range d.participants {
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
	participants, err := d.resolveParticipants(nodeIDs)
	if err != nil {
		return nil, err
	}
	return NewFailoverSession(d.master, volumeName, expectedEpoch, participants)
}

// Execute runs one failover using the resolved participant set.
func (d *InProcessFailoverDriver) Execute(volumeName string, expectedEpoch uint64, nodeIDs ...string) (FailoverResult, error) {
	session, err := d.NewSession(volumeName, expectedEpoch, nodeIDs...)
	if err != nil {
		return FailoverResult{}, err
	}
	return session.Run()
}

func (d *InProcessFailoverDriver) resolveParticipants(nodeIDs []string) ([]FailoverParticipant, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.participants) == 0 {
		return nil, fmt.Errorf("volumev2: no failover participants registered")
	}

	resolvedIDs := nodeIDs
	if len(resolvedIDs) == 0 {
		resolvedIDs = make([]string, 0, len(d.participants))
		for nodeID := range d.participants {
			resolvedIDs = append(resolvedIDs, nodeID)
		}
		slices.Sort(resolvedIDs)
	}

	participants := make([]FailoverParticipant, 0, len(resolvedIDs))
	for _, nodeID := range resolvedIDs {
		participant, ok := d.participants[nodeID]
		if !ok {
			return nil, fmt.Errorf("volumev2: unknown failover participant %q", nodeID)
		}
		participants = append(participants, participant)
	}
	return participants, nil
}
