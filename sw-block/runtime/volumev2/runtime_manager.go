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
	evidenceTransport ManagedFailoverEvidenceTransport

	mu                 sync.RWMutex
	localNodes         map[string]*Node
	lastSnapshot       FailoverSnapshot
	lastResult         FailoverResult
	hasLastResult      bool
	snapshotsByName    map[string]FailoverSnapshot
	resultsByName      map[string]FailoverResult
	lastLoop2Snapshot  Loop2RuntimeSnapshot
	hasLastLoop2       bool
	loop2ByVolume      map[string]Loop2RuntimeSnapshot
	lastContinuity     ReplicatedContinuitySnapshot
	hasLastContinuity  bool
	continuityByVolume map[string]ReplicatedContinuitySnapshot
	frontendExports    map[string]*managedISCSIExport
}

// NewInProcessRuntimeManager creates a runtime-owned failover manager over one
// in-process masterv2 instance.
func NewInProcessRuntimeManager(master *masterv2.Master) (*InProcessRuntimeManager, error) {
	return NewInProcessRuntimeManagerWithEvidenceTransport(master, nil)
}

// NewInProcessRuntimeManagerWithEvidenceTransport creates a runtime-owned
// failover manager over one in-process masterv2 instance using the supplied
// managed evidence transport. When nil, an in-memory transport is used.
func NewInProcessRuntimeManagerWithEvidenceTransport(master *masterv2.Master, transport ManagedFailoverEvidenceTransport) (*InProcessRuntimeManager, error) {
	driver, err := NewInProcessFailoverDriver(master)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		transport = NewInMemoryFailoverEvidenceTransport()
	}
	return &InProcessRuntimeManager{
		driver:             driver,
		evidenceTransport:  transport,
		localNodes:         make(map[string]*Node),
		snapshotsByName:    make(map[string]FailoverSnapshot),
		resultsByName:      make(map[string]FailoverResult),
		loop2ByVolume:      make(map[string]Loop2RuntimeSnapshot),
		continuityByVolume: make(map[string]ReplicatedContinuitySnapshot),
		frontendExports:    make(map[string]*managedISCSIExport),
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
	m.mu.Lock()
	m.localNodes[node.NodeID()] = node
	m.mu.Unlock()
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
	m.mu.Lock()
	delete(m.localNodes, nodeID)
	m.mu.Unlock()
	m.driver.UnregisterParticipant(nodeID)
}

// DisconnectEvidenceNode removes the evidence handler for one node while
// leaving the runtime-owned target and local node registration intact. This is
// useful for bounded live-transport fault injection.
func (m *InProcessRuntimeManager) DisconnectEvidenceNode(nodeID string) {
	if m == nil || m.evidenceTransport == nil {
		return
	}
	m.evidenceTransport.UnregisterHandler(nodeID)
}

// ReconnectEvidenceNode re-registers one local node behind the configured
// evidence transport.
func (m *InProcessRuntimeManager) ReconnectEvidenceNode(nodeID string) error {
	if m == nil || m.evidenceTransport == nil {
		return fmt.Errorf("volumev2: runtime manager is nil")
	}
	node, err := m.localNode(nodeID)
	if err != nil {
		return err
	}
	return m.evidenceTransport.RegisterHandler(nodeID, node)
}

// Close releases runtime-manager owned transport resources.
func (m *InProcessRuntimeManager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	exports := make([]*managedISCSIExport, 0, len(m.frontendExports))
	for volumeName, export := range m.frontendExports {
		exports = append(exports, export)
		delete(m.frontendExports, volumeName)
	}
	m.mu.Unlock()
	for _, export := range exports {
		if export != nil && export.handle != nil {
			_ = export.handle.Close()
		}
	}
	if m.evidenceTransport == nil {
		return nil
	}
	return m.evidenceTransport.Close()
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

// NewLoop2RuntimeSession resolves runtime-owned targets and creates an active
// Loop 2 session for one selected primary.
func (m *InProcessRuntimeManager) NewLoop2RuntimeSession(volumeName, primaryNodeID string, expectedEpoch uint64, nodeIDs ...string) (*Loop2RuntimeSession, error) {
	if m == nil || m.driver == nil {
		return nil, fmt.Errorf("volumev2: runtime manager is nil")
	}
	targets, err := m.driver.resolveTargets(nodeIDs)
	if err != nil {
		return nil, err
	}
	return NewLoop2RuntimeSession(volumeName, primaryNodeID, expectedEpoch, targets)
}

// ObserveLoop2 runs one active Loop 2 observation and persists the latest
// runtime snapshot.
func (m *InProcessRuntimeManager) ObserveLoop2(volumeName, primaryNodeID string, expectedEpoch uint64, nodeIDs ...string) (Loop2RuntimeSnapshot, error) {
	if m == nil {
		return Loop2RuntimeSnapshot{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	session, err := m.NewLoop2RuntimeSession(volumeName, primaryNodeID, expectedEpoch, nodeIDs...)
	if err != nil {
		return Loop2RuntimeSnapshot{}, err
	}
	snapshot, obsErr := session.ObserveOnce()
	m.recordLoop2Snapshot(volumeName, session.Snapshot())
	return snapshot, obsErr
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

// LastLoop2Snapshot returns the most recent active Loop 2 runtime snapshot.
func (m *InProcessRuntimeManager) LastLoop2Snapshot() (Loop2RuntimeSnapshot, bool) {
	if m == nil {
		return Loop2RuntimeSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.hasLastLoop2 {
		return Loop2RuntimeSnapshot{}, false
	}
	return m.lastLoop2Snapshot, true
}

// Loop2Snapshot returns the latest active Loop 2 runtime snapshot for one
// volume if present.
func (m *InProcessRuntimeManager) Loop2Snapshot(volumeName string) (Loop2RuntimeSnapshot, bool) {
	if m == nil {
		return Loop2RuntimeSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot, ok := m.loop2ByVolume[volumeName]
	return snapshot, ok
}

// LastReplicatedContinuitySnapshot returns the most recent bounded continuity
// snapshot observed by the runtime manager.
func (m *InProcessRuntimeManager) LastReplicatedContinuitySnapshot() (ReplicatedContinuitySnapshot, bool) {
	if m == nil {
		return ReplicatedContinuitySnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.hasLastContinuity {
		return ReplicatedContinuitySnapshot{}, false
	}
	return m.lastContinuity, true
}

// ReplicatedContinuitySnapshot returns the latest bounded continuity snapshot
// for one volume if present.
func (m *InProcessRuntimeManager) ReplicatedContinuitySnapshot(volumeName string) (ReplicatedContinuitySnapshot, bool) {
	if m == nil {
		return ReplicatedContinuitySnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot, ok := m.continuityByVolume[volumeName]
	return snapshot, ok
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

func (m *InProcessRuntimeManager) recordLoop2Snapshot(volumeName string, snapshot Loop2RuntimeSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastLoop2Snapshot = snapshot
	m.hasLastLoop2 = true
	if volumeName != "" {
		m.loop2ByVolume[volumeName] = snapshot
	}
}

func (m *InProcessRuntimeManager) recordContinuitySnapshot(volumeName string, snapshot ReplicatedContinuitySnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastContinuity = snapshot
	m.hasLastContinuity = true
	if volumeName != "" {
		m.continuityByVolume[volumeName] = snapshot
	}
}

func (m *InProcessRuntimeManager) localNode(nodeID string) (*Node, error) {
	if m == nil {
		return nil, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if nodeID == "" {
		return nil, fmt.Errorf("volumev2: local node id is required")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.localNodes[nodeID]
	if !ok || node == nil {
		return nil, fmt.Errorf("volumev2: local node %q is not registered", nodeID)
	}
	return node, nil
}
