package volumev2

import (
	"bytes"
	"fmt"
)

type managedISCSIExport struct {
	nodeID string
	handle *ISCSITargetExport
}

// ManagedISCSIExportSnapshot is the runtime-owned serving snapshot for one iSCSI
// export bound to the current runtime path.
type ManagedISCSIExportSnapshot struct {
	VolumeName string
	NodeID     string
	IQN        string
	Address    string
}

// ReplicaRepairResult captures one bounded replica repair/catch-up run through
// the runtime-owned path.
type ReplicaRepairResult struct {
	VolumeName    string
	PrimaryNodeID string
	ReplicaNodeID string
	ExpectedEpoch uint64
	ByteLength    uint32
	DataMatch     bool
	Loop2Before   Loop2RuntimeSnapshot
	Loop2After    Loop2RuntimeSnapshot
}

// ExportVolumeISCSI binds one named volume on one selected node to an iSCSI
// export owned by the runtime manager.
func (m *InProcessRuntimeManager) ExportVolumeISCSI(volumeName, nodeID, listenAddr, iqn string) (ManagedISCSIExportSnapshot, error) {
	if m == nil {
		return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if volumeName == "" {
		return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: iscsi export volume name is required")
	}
	if nodeID == "" {
		return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: iscsi export node id is required")
	}
	node, err := m.localNode(nodeID)
	if err != nil {
		return ManagedISCSIExportSnapshot{}, err
	}
	export, err := node.ExportISCSI(volumeName, listenAddr, iqn)
	if err != nil {
		return ManagedISCSIExportSnapshot{}, err
	}

	m.mu.Lock()
	if prev, ok := m.frontendExports[volumeName]; ok && prev != nil && prev.handle != nil {
		_ = prev.handle.Close()
	}
	m.frontendExports[volumeName] = &managedISCSIExport{
		nodeID: nodeID,
		handle: export,
	}
	m.mu.Unlock()

	return ManagedISCSIExportSnapshot{
		VolumeName: volumeName,
		NodeID:     nodeID,
		IQN:        export.IQN(),
		Address:    export.Address(),
	}, nil
}

// ExportCurrentPrimaryISCSI binds one named volume to the current primary known
// to the runtime manager.
func (m *InProcessRuntimeManager) ExportCurrentPrimaryISCSI(volumeName, listenAddr, iqn string) (ManagedISCSIExportSnapshot, error) {
	if m == nil {
		return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if volumeName == "" {
		return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: current primary export volume name is required")
	}
	if result, ok := m.FailoverResult(volumeName); ok && result.Assignment.NodeID != "" {
		return m.ExportVolumeISCSI(volumeName, result.Assignment.NodeID, listenAddr, iqn)
	}
	if snapshot, ok := m.Loop2Snapshot(volumeName); ok && snapshot.PrimaryNodeID != "" {
		return m.ExportVolumeISCSI(volumeName, snapshot.PrimaryNodeID, listenAddr, iqn)
	}
	return ManagedISCSIExportSnapshot{}, fmt.Errorf("volumev2: current primary for %q is unknown", volumeName)
}

// ISCSIExport returns the current runtime-owned iSCSI export snapshot for one
// volume if present.
func (m *InProcessRuntimeManager) ISCSIExport(volumeName string) (ManagedISCSIExportSnapshot, bool) {
	if m == nil {
		return ManagedISCSIExportSnapshot{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	export, ok := m.frontendExports[volumeName]
	if !ok || export == nil || export.handle == nil {
		return ManagedISCSIExportSnapshot{}, false
	}
	return ManagedISCSIExportSnapshot{
		VolumeName: volumeName,
		NodeID:     export.nodeID,
		IQN:        export.handle.IQN(),
		Address:    export.handle.Address(),
	}, true
}

// RepairReplicaFromPrimary performs one bounded runtime-owned replica repair by
// copying the requested byte range from the selected primary to the selected
// replica and re-observing Loop 2 before and after the repair.
func (m *InProcessRuntimeManager) RepairReplicaFromPrimary(volumeName, primaryNodeID, replicaNodeID string, expectedEpoch uint64, length uint32) (ReplicaRepairResult, error) {
	if m == nil {
		return ReplicaRepairResult{}, fmt.Errorf("volumev2: runtime manager is nil")
	}
	if volumeName == "" {
		return ReplicaRepairResult{}, fmt.Errorf("volumev2: repair volume name is required")
	}
	if primaryNodeID == "" || replicaNodeID == "" {
		return ReplicaRepairResult{}, fmt.Errorf("volumev2: repair primary and replica node ids are required")
	}
	if primaryNodeID == replicaNodeID {
		return ReplicaRepairResult{}, fmt.Errorf("volumev2: repair requires distinct primary and replica nodes")
	}
	if length == 0 {
		return ReplicaRepairResult{}, fmt.Errorf("volumev2: repair byte length is required")
	}

	result := ReplicaRepairResult{
		VolumeName:    volumeName,
		PrimaryNodeID: primaryNodeID,
		ReplicaNodeID: replicaNodeID,
		ExpectedEpoch: expectedEpoch,
		ByteLength:    length,
	}
	before, err := m.ObserveLoop2(volumeName, primaryNodeID, expectedEpoch, primaryNodeID, replicaNodeID)
	if err != nil {
		return result, err
	}
	result.Loop2Before = before

	primary, err := m.localNode(primaryNodeID)
	if err != nil {
		return result, err
	}
	replica, err := m.localNode(replicaNodeID)
	if err != nil {
		return result, err
	}
	payload, err := primary.ReadLBA(volumeName, 0, length)
	if err != nil {
		return result, fmt.Errorf("volumev2: repair readback from %s: %w", primaryNodeID, err)
	}
	if err := replica.WriteLBA(volumeName, 0, payload); err != nil {
		return result, fmt.Errorf("volumev2: repair write to %s: %w", replicaNodeID, err)
	}
	if err := replica.SyncCache(volumeName); err != nil {
		return result, fmt.Errorf("volumev2: repair sync %s: %w", replicaNodeID, err)
	}
	replicaReadBack, err := replica.ReadLBA(volumeName, 0, length)
	if err != nil {
		return result, fmt.Errorf("volumev2: repair verify read %s: %w", replicaNodeID, err)
	}
	result.DataMatch = bytes.Equal(replicaReadBack, payload)
	if !result.DataMatch {
		return result, fmt.Errorf("volumev2: repair payload mismatch after copy")
	}

	after, err := m.ObserveLoop2(volumeName, primaryNodeID, expectedEpoch, primaryNodeID, replicaNodeID)
	if err != nil {
		return result, err
	}
	result.Loop2After = after
	return result, nil
}
