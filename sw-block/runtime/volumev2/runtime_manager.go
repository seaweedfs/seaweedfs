package volumev2

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
)

// InProcessRuntimeManager is the first runtime-owned failover manager for the
// new kernel. It wraps the in-process failover driver, owns participant
// registration, and retains the latest failover snapshots/results for
// observability.
type InProcessRuntimeManager struct {
	driver            *InProcessFailoverDriver
	evidenceTransport *InMemoryFailoverEvidenceTransport

	mu              sync.RWMutex
	lastSnapshot    FailoverSnapshot
	lastResult      FailoverResult
	hasLastResult   bool
	snapshotsByName map[string]FailoverSnapshot
	resultsByName   map[string]FailoverResult
}

// NewInProcessRuntimeManager creates a runtime-owned failover manager over one
// in-process masterv2 instance.
func NewInProcessRuntimeManager(master *masterv2.Master) (*InProcessRuntimeManager, error) {
	driver, err := NewInProcessFailoverDriver(master)
	if err != nil {
		return nil, err
	}
	return &InProcessRuntimeManager{
		driver:            driver,
		evidenceTransport: NewInMemoryFailoverEvidenceTransport(),
		snapshotsByName:   make(map[string]FailoverSnapshot),
		resultsByName:     make(map[string]FailoverResult),
	}, nil
}

// RegisterNode registers one concrete volumev2 node under its stable node id.
func (m *InProcessRuntimeManager) RegisterNode(node *Node) error {
	if m == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	if node == nil {
		return fmt.Errorf("volumev2: node is nil")
	}
	if err := m.evidenceTransport.RegisterHandler(node.NodeID(), node); err != nil {
		return err
	}
	target, err := NewHybridInProcessFailoverTarget(node, m.evidenceTransport)
	if err != nil {
		return err
	}
	return m.RegisterTarget(target)
}

// RegisterTarget registers one explicit failover target.
func (m *InProcessRuntimeManager) RegisterTarget(target FailoverTarget) error {
	if m == nil || m.driver == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	return m.driver.RegisterTarget(target)
}

// RegisterParticipant registers one failover-capable participant explicitly.
func (m *InProcessRuntimeManager) RegisterParticipant(nodeID string, participant FailoverParticipant) error {
	if m == nil || m.driver == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	return m.driver.RegisterParticipant(nodeID, participant)
}

// UnregisterParticipant removes one participant from the runtime-owned driver.
func (m *InProcessRuntimeManager) UnregisterParticipant(nodeID string) {
	if m == nil || m.driver == nil {
		return
	}
	if m.evidenceTransport != nil {
		m.evidenceTransport.UnregisterHandler(nodeID)
	}
	m.driver.UnregisterParticipant(nodeID)
}

// ParticipantNodeIDs returns the current runtime-owned participant ids.
func (m *InProcessRuntimeManager) ParticipantNodeIDs() []string {
	if m == nil || m.driver == nil {
		return nil
	}
	return m.driver.ParticipantNodeIDs()
}

// NewFailoverSession resolves participants through the runtime-owned registry.
func (m *InProcessRuntimeManager) NewFailoverSession(volumeName string, expectedEpoch uint64, nodeIDs ...string) (*FailoverSession, error) {
	if m == nil || m.driver == nil {
		return nil, fmt.Errorf("volumev2: runtime manager is nil")
	}
	return m.driver.NewSession(volumeName, expectedEpoch, nodeIDs...)
}

// ExecuteFailover runs one runtime-owned failover and persists the latest
// observable snapshot/result for the volume and the manager as a whole.
func (m *InProcessRuntimeManager) ExecuteFailover(volumeName string, expectedEpoch uint64, nodeIDs ...string) (FailoverResult, error) {
	if m == nil {
		return FailoverResult{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	session, err := m.NewFailoverSession(volumeName, expectedEpoch, nodeIDs...)
	if err != nil {
		return FailoverResult{}, err
	}
	result, runErr := session.Run()
	m.recordSnapshot(volumeName, session.Snapshot(), result)
	return result, runErr
}

// LastFailoverSnapshot returns the most recent runtime-owned failover snapshot.
func (m *InProcessRuntimeManager) LastFailoverSnapshot() (FailoverSnapshot, bool) {
	if m == nil {
		return FailoverSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastSnapshot.VolumeName == "" {
		return FailoverSnapshot{}, false
	}
	return m.lastSnapshot, true
}

// FailoverSnapshot returns the latest snapshot for one volume if present.
func (m *InProcessRuntimeManager) FailoverSnapshot(volumeName string) (FailoverSnapshot, bool) {
	if m == nil {
		return FailoverSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.snapshotsByName[volumeName]
	return snap, ok
}

// LastFailoverResult returns the most recent runtime-owned failover result.
func (m *InProcessRuntimeManager) LastFailoverResult() (FailoverResult, bool) {
	if m == nil {
		return FailoverResult{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.hasLastResult {
		return FailoverResult{}, false
	}
	return m.lastResult, true
}

// FailoverResult returns the latest result for one volume if present.
func (m *InProcessRuntimeManager) FailoverResult(volumeName string) (FailoverResult, bool) {
	if m == nil {
		return FailoverResult{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result, ok := m.resultsByName[volumeName]
	return result, ok
}

func (m *InProcessRuntimeManager) recordSnapshot(volumeName string, snapshot FailoverSnapshot, result FailoverResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSnapshot = snapshot
	m.lastResult = result
	m.hasLastResult = true
	if volumeName != "" {
		m.snapshotsByName[volumeName] = snapshot
		m.resultsByName[volumeName] = result
	}
}
