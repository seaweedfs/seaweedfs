package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

// blockReqID extracts a short request ID from context for log correlation.
func blockReqID(ctx context.Context) string {
	id := request_id.Get(ctx)
	if len(id) > 8 {
		return id[:8]
	}
	if id == "" {
		return "no-id"
	}
	return id
}

// CreateBlockVolume picks a volume server, delegates creation, and records
// the mapping in the block volume registry.
func (ms *MasterServer) CreateBlockVolume(ctx context.Context, req *master_pb.CreateBlockVolumeRequest) (*master_pb.CreateBlockVolumeResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.SizeBytes == 0 {
		return nil, fmt.Errorf("size_bytes must be > 0")
	}

	// F2: validate durability mode in gRPC path (authoritative, not bypassable).
	var durMode blockvol.DurabilityMode
	if req.DurabilityMode != "" {
		var err error
		durMode, err = blockvol.ParseDurabilityMode(req.DurabilityMode)
		if err != nil {
			return nil, fmt.Errorf("invalid durability_mode: %w", err)
		}
	}

	// Cross-validate mode + RF (sync_quorum requires RF >= 3).
	replicaFactor := 2
	if req.ReplicaFactor > 0 && req.ReplicaFactor <= 3 {
		replicaFactor = int(req.ReplicaFactor)
	}
	if err := durMode.Validate(replicaFactor); err != nil {
		return nil, fmt.Errorf("durability_mode %q incompatible with replica_factor %d: %w", req.DurabilityMode, replicaFactor, err)
	}

	// Idempotent: if already registered, return existing entry (validate size + mode + RF).
	if entry, ok := ms.blockRegistry.Lookup(req.Name); ok {
		if err := ms.validateIdempotentCreate(entry, req, durMode, replicaFactor); err != nil {
			return nil, err
		}
		return ms.createBlockVolumeResponseFromEntry(entry), nil
	}

	// Per-name inflight lock prevents concurrent creates for the same name.
	if !ms.blockRegistry.AcquireInflight(req.Name) {
		return nil, fmt.Errorf("block volume %q creation already in progress", req.Name)
	}
	defer ms.blockRegistry.ReleaseInflight(req.Name)

	// Double-check after acquiring lock (another goroutine may have finished).
	if entry, ok := ms.blockRegistry.Lookup(req.Name); ok {
		if err := ms.validateIdempotentCreate(entry, req, durMode, replicaFactor); err != nil {
			return nil, err
		}
		return ms.createBlockVolumeResponseFromEntry(entry), nil
	}

	// Get candidate servers.
	servers := ms.blockRegistry.BlockCapableServers()
	if len(servers) == 0 {
		return nil, fmt.Errorf("no block volume servers available")
	}

	// Try up to 3 servers (or all available, whichever is smaller).
	maxRetries := 3
	if len(servers) < maxRetries {
		maxRetries = len(servers)
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		server, err := ms.blockRegistry.PickServer(servers)
		if err != nil {
			return nil, err
		}

		result, err := ms.blockVSAllocate(ctx, server, req.Name, req.SizeBytes, req.DiskType, req.DurabilityMode)
		if err != nil {
			lastErr = fmt.Errorf("server %s: %w", server, err)
			glog.V(0).Infof("[reqID=%s] CreateBlockVolume %q: attempt %d on %s failed: %v", blockReqID(ctx), req.Name, attempt+1, server, err)
			servers = removeServer(servers, server)
			continue
		}

		entry := &BlockVolumeEntry{
			Name:           req.Name,
			VolumeServer:   server,
			Path:           result.Path,
			IQN:            result.IQN,
			ISCSIAddr:      result.ISCSIAddr,
			NvmeAddr:       result.NvmeAddr,
			NQN:            result.NQN,
			SizeBytes:      req.SizeBytes,
			Epoch:          1,
			Role:           blockvol.RoleToWire(blockvol.RolePrimary),
			Status:         StatusActive,
			ReplicaFactor:  replicaFactor,
			DurabilityMode: durMode.String(),
			LeaseTTL:       30 * time.Second,
			LastLeaseGrant: time.Now(), // R2-F1: set BEFORE Register to avoid stale-lease race
		}

		// Create replicaFactor-1 replicas on different servers (F4: partial create OK).
		remaining := removeServer(servers, server)
		for i := 0; i < replicaFactor-1 && len(remaining) > 0; i++ {
			replicaServer := ms.tryCreateOneReplica(ctx, req, entry, result, remaining)
			if replicaServer != "" {
				remaining = removeServer(remaining, replicaServer)
			}
		}
		if len(entry.Replicas) == 0 && replicaFactor > 1 {
			glog.V(0).Infof("[reqID=%s] CreateBlockVolume %q: single-copy mode (replica allocation failed)", blockReqID(ctx), req.Name)
		}

		// F1: strict modes require minimum replicas at create time.
		requiredReplicas := durMode.RequiredReplicas(replicaFactor)
		if len(entry.Replicas) < requiredReplicas {
			ms.cleanupPartialCreate(ctx, entry)
			return nil, fmt.Errorf("durability mode %q requires %d replicas but only %d provisioned",
				durMode.String(), requiredReplicas, len(entry.Replicas))
		}

		// Sync deprecated scalar fields from first replica.
		if len(entry.Replicas) > 0 {
			r0 := &entry.Replicas[0]
			entry.ReplicaServer = r0.Server
			entry.ReplicaPath = r0.Path
			entry.ReplicaIQN = r0.IQN
			entry.ReplicaISCSIAddr = r0.ISCSIAddr
			entry.ReplicaDataAddr = r0.DataAddr
			entry.ReplicaCtrlAddr = r0.CtrlAddr
		}

		// Register in registry as Active (VS confirmed creation).
		if err := ms.blockRegistry.Register(entry); err != nil {
			// Already registered (race condition) — return the existing entry.
			if existing, ok := ms.blockRegistry.Lookup(req.Name); ok {
				return ms.createBlockVolumeResponseFromEntry(existing), nil
			}
			return nil, fmt.Errorf("register block volume: %w", err)
		}

		// Enqueue assignments for primary (and replicas if available).
		leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
		primaryAssignment := blockvol.BlockVolumeAssignment{
			Path:       result.Path,
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs: leaseTTLMs,
		}
		// CP8-2: populate ReplicaAddrs for multi-replica.
		for _, ri := range entry.Replicas {
			primaryAssignment.ReplicaAddrs = append(primaryAssignment.ReplicaAddrs, blockvol.ReplicaAddr{
				DataAddr: ri.DataAddr,
				CtrlAddr: ri.CtrlAddr,
			})
		}
		// Backward compat: also set scalar fields if exactly 1 replica.
		if len(entry.Replicas) == 1 {
			primaryAssignment.ReplicaDataAddr = entry.Replicas[0].DataAddr
			primaryAssignment.ReplicaCtrlAddr = entry.Replicas[0].CtrlAddr
		}
		ms.blockAssignmentQueue.Enqueue(server, primaryAssignment)

		// Enqueue assignments for each replica.
		for _, ri := range entry.Replicas {
			ms.blockAssignmentQueue.Enqueue(ri.Server, blockvol.BlockVolumeAssignment{
				Path:            ri.Path,
				Epoch:           1,
				Role:            blockvol.RoleToWire(blockvol.RoleReplica),
				LeaseTtlMs:      leaseTTLMs,
				ReplicaDataAddr: ri.DataAddr,
				ReplicaCtrlAddr: ri.CtrlAddr,
			})
		}

		// Collect replica server addresses for response.
		var replicaServers []string
		for _, ri := range entry.Replicas {
			replicaServers = append(replicaServers, ri.Server)
		}

		glog.V(0).Infof("[reqID=%s] CreateBlockVolume %q: created on %s (path=%s, iqn=%s, replicas=%v)",
			blockReqID(ctx), req.Name, server, result.Path, result.IQN, replicaServers)
		return ms.createBlockVolumeResponseFromEntry(entry), nil
	}

	return nil, fmt.Errorf("all volume servers failed for %q: %v", req.Name, lastErr)
}

// DeleteBlockVolume removes a block volume from the registry and volume server.
func (ms *MasterServer) DeleteBlockVolume(ctx context.Context, req *master_pb.DeleteBlockVolumeRequest) (*master_pb.DeleteBlockVolumeResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	entry, ok := ms.blockRegistry.Lookup(req.Name)
	if !ok {
		// Idempotent: not found is success.
		return &master_pb.DeleteBlockVolumeResponse{}, nil
	}

	// Call volume server to delete primary.
	if err := ms.blockVSDelete(ctx, entry.VolumeServer, req.Name); err != nil {
		return nil, fmt.Errorf("delete block volume %q on %s: %w", req.Name, entry.VolumeServer, err)
	}

	// CP8-2: Delete ALL replicas (best-effort, don't fail if replica is down).
	for _, ri := range entry.Replicas {
		if err := ms.blockVSDelete(ctx, ri.Server, req.Name); err != nil {
			glog.Warningf("[reqID=%s] DeleteBlockVolume %q: replica delete on %s failed (best-effort): %v",
				blockReqID(ctx), req.Name, ri.Server, err)
		}
	}

	ms.blockRegistry.Unregister(req.Name)
	glog.V(0).Infof("[reqID=%s] DeleteBlockVolume %q: removed from %s (replicas=%d)", blockReqID(ctx), req.Name, entry.VolumeServer, len(entry.Replicas))
	return &master_pb.DeleteBlockVolumeResponse{}, nil
}

// LookupBlockVolume looks up a block volume in the registry.
func (ms *MasterServer) LookupBlockVolume(ctx context.Context, req *master_pb.LookupBlockVolumeRequest) (*master_pb.LookupBlockVolumeResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	entry, ok := ms.blockRegistry.Lookup(req.Name)
	if !ok {
		return nil, fmt.Errorf("block volume %q not found", req.Name)
	}

	replicaServers := replicaServerList(entry)
	rf := entry.ReplicaFactor
	if rf == 0 {
		rf = 2 // default for pre-CP8-2 entries
	}
	durModeStr := entry.DurabilityMode
	if durModeStr == "" {
		durModeStr = "best_effort"
	}
	return &master_pb.LookupBlockVolumeResponse{
		VolumeServer:   entry.VolumeServer,
		IscsiAddr:      entry.ISCSIAddr,
		Iqn:            entry.IQN,
		CapacityBytes:  entry.SizeBytes,
		ReplicaServer:  entry.ReplicaServer, // backward compat
		ReplicaFactor:  uint32(rf),
		ReplicaServers: replicaServers,
		DurabilityMode: durModeStr,
		NvmeAddr:       entry.NvmeAddr,
		Nqn:            entry.NQN,
	}, nil
}

// tryCreateOneReplica attempts to create one replica volume on a different server.
// Returns the replica server address on success, or empty string on failure (F4).
func (ms *MasterServer) tryCreateOneReplica(ctx context.Context, req *master_pb.CreateBlockVolumeRequest, entry *BlockVolumeEntry, primaryResult *blockAllocResult, candidates []string) string {
	for _, replicaServerStr := range candidates {
		replicaResult, err := ms.blockVSAllocate(ctx, replicaServerStr, req.Name, req.SizeBytes, req.DiskType, req.DurabilityMode)
		if err != nil {
			glog.V(0).Infof("[reqID=%s] CreateBlockVolume %q: replica on %s failed: %v", blockReqID(ctx), req.Name, replicaServerStr, err)
			continue
		}
		entry.RebuildListenAddr = primaryResult.RebuildListenAddr
		// CP8-2: populate Replicas[].
		entry.Replicas = append(entry.Replicas, ReplicaInfo{
			Server:        replicaServerStr,
			Path:          replicaResult.Path,
			ISCSIAddr:     replicaResult.ISCSIAddr,
			IQN:           replicaResult.IQN,
			NvmeAddr:      replicaResult.NvmeAddr,
			NQN:           replicaResult.NQN,
			DataAddr:      replicaResult.ReplicaDataAddr,
			CtrlAddr:      replicaResult.ReplicaCtrlAddr,
			Role:          blockvol.RoleToWire(blockvol.RoleReplica),
			LastHeartbeat: time.Now(),
		})
		return replicaServerStr
	}
	glog.Warningf("[reqID=%s] CreateBlockVolume %q: created without replica (replica allocation failed)", blockReqID(ctx), req.Name)
	return ""
}

// CreateBlockSnapshot creates a snapshot on a block volume via the volume server.
func (ms *MasterServer) CreateBlockSnapshot(ctx context.Context, req *master_pb.CreateBlockSnapshotRequest) (*master_pb.CreateBlockSnapshotResponse, error) {
	if req.VolumeName == "" {
		return nil, fmt.Errorf("volume_name is required")
	}

	entry, ok := ms.blockRegistry.Lookup(req.VolumeName)
	if !ok {
		return nil, fmt.Errorf("block volume %q not found", req.VolumeName)
	}

	createdAt, sizeBytes, err := ms.blockVSSnapshot(ctx, entry.VolumeServer, req.VolumeName, req.SnapshotId)
	if err != nil {
		return nil, fmt.Errorf("create snapshot on %s: %w", entry.VolumeServer, err)
	}

	return &master_pb.CreateBlockSnapshotResponse{
		SnapshotId: req.SnapshotId,
		CreatedAt:  createdAt,
		SizeBytes:  sizeBytes,
	}, nil
}

// DeleteBlockSnapshot deletes a snapshot from a block volume via the volume server.
func (ms *MasterServer) DeleteBlockSnapshot(ctx context.Context, req *master_pb.DeleteBlockSnapshotRequest) (*master_pb.DeleteBlockSnapshotResponse, error) {
	if req.VolumeName == "" {
		return nil, fmt.Errorf("volume_name is required")
	}

	entry, ok := ms.blockRegistry.Lookup(req.VolumeName)
	if !ok {
		// Idempotent: volume not found → snapshot doesn't exist either.
		return &master_pb.DeleteBlockSnapshotResponse{}, nil
	}

	if err := ms.blockVSDeleteSnap(ctx, entry.VolumeServer, req.VolumeName, req.SnapshotId); err != nil {
		return nil, fmt.Errorf("delete snapshot on %s: %w", entry.VolumeServer, err)
	}

	return &master_pb.DeleteBlockSnapshotResponse{}, nil
}

// ListBlockSnapshots lists all snapshots on a block volume via the volume server.
func (ms *MasterServer) ListBlockSnapshots(ctx context.Context, req *master_pb.ListBlockSnapshotsRequest) (*master_pb.ListBlockSnapshotsResponse, error) {
	if req.VolumeName == "" {
		return nil, fmt.Errorf("volume_name is required")
	}

	entry, ok := ms.blockRegistry.Lookup(req.VolumeName)
	if !ok {
		return nil, fmt.Errorf("block volume %q not found", req.VolumeName)
	}

	vsInfos, err := ms.blockVSListSnaps(ctx, entry.VolumeServer, req.VolumeName)
	if err != nil {
		return nil, fmt.Errorf("list snapshots on %s: %w", entry.VolumeServer, err)
	}

	resp := &master_pb.ListBlockSnapshotsResponse{}
	for _, si := range vsInfos {
		resp.Snapshots = append(resp.Snapshots, &master_pb.BlockSnapshotInfo{
			SnapshotId:      si.SnapshotId,
			CreatedAt:       si.CreatedAt,
			VolumeSizeBytes: si.VolumeSizeBytes,
		})
	}
	return resp, nil
}

// ExpandBlockVolume expands a block volume. For standalone volumes (no replicas),
// uses direct expand. For replicated volumes, uses coordinated prepare/commit/cancel.
func (ms *MasterServer) ExpandBlockVolume(ctx context.Context, req *master_pb.ExpandBlockVolumeRequest) (*master_pb.ExpandBlockVolumeResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.NewSizeBytes == 0 {
		return nil, fmt.Errorf("new_size_bytes must be > 0")
	}

	entry, ok := ms.blockRegistry.Lookup(req.Name)
	if !ok {
		return nil, fmt.Errorf("block volume %q not found", req.Name)
	}

	// Standalone path: no replicas → direct expand (unchanged behavior).
	if len(entry.Replicas) == 0 {
		capacity, err := ms.blockVSExpand(ctx, entry.VolumeServer, req.Name, req.NewSizeBytes)
		if err != nil {
			return nil, fmt.Errorf("expand on %s: %w", entry.VolumeServer, err)
		}
		if uerr := ms.blockRegistry.UpdateSize(req.Name, capacity); uerr != nil {
			glog.Warningf("[reqID=%s] ExpandBlockVolume %q: registry update failed: %v", blockReqID(ctx), req.Name, uerr)
		}
		return &master_pb.ExpandBlockVolumeResponse{CapacityBytes: capacity}, nil
	}

	// Coordinated expand for replicated volumes.
	expandEpoch := ms.nextExpandEpoch.Add(1)

	if !ms.blockRegistry.AcquireExpandInflight(req.Name, req.NewSizeBytes, expandEpoch) {
		return nil, fmt.Errorf("block volume %q: expand already in progress or failed (requires reconciliation)", req.Name)
	}
	// Only release on clean success or clean cancel (all nodes rolled back).
	// On partial commit failure, MarkExpandFailed keeps the guard up.
	expandClean := false
	defer func() {
		if expandClean {
			ms.blockRegistry.ReleaseExpandInflight(req.Name)
		}
	}()

	// Test-only hook: inject failover between lock acquisition and re-read.
	if ms.expandPreReadHook != nil {
		ms.expandPreReadHook()
	}

	// B-09: Re-read entry after acquiring expand lock. Between the initial
	// Lookup and AcquireExpandInflight, failover may have changed VolumeServer
	// or Replicas. Using the stale snapshot would send PREPARE to dead nodes.
	entry, ok = ms.blockRegistry.Lookup(req.Name)
	if !ok {
		expandClean = true
		return nil, fmt.Errorf("block volume %q disappeared during expand", req.Name)
	}

	// Track prepared nodes for rollback.
	var prepared []string

	// PREPARE: primary.
	if err := ms.blockVSPrepareExpand(ctx, entry.VolumeServer, req.Name, req.NewSizeBytes, expandEpoch); err != nil {
		expandClean = true // nothing to worry about, just release
		return nil, fmt.Errorf("prepare expand on primary %s: %w", entry.VolumeServer, err)
	}
	prepared = append(prepared, entry.VolumeServer)

	// PREPARE: replicas.
	for _, ri := range entry.Replicas {
		if err := ms.blockVSPrepareExpand(ctx, ri.Server, req.Name, req.NewSizeBytes, expandEpoch); err != nil {
			glog.Warningf("[reqID=%s] ExpandBlockVolume %q: prepare on replica %s failed: %v", blockReqID(ctx), req.Name, ri.Server, err)
			// Cancel all prepared nodes.
			for _, ps := range prepared {
				if cerr := ms.blockVSCancelExpand(ctx, ps, req.Name, expandEpoch); cerr != nil {
					glog.Warningf("[reqID=%s] ExpandBlockVolume %q: cancel on %s failed: %v", blockReqID(ctx), req.Name, ps, cerr)
				}
			}
			expandClean = true // all cancelled, safe to release
			return nil, fmt.Errorf("prepare expand on replica %s: %w", ri.Server, err)
		}
		prepared = append(prepared, ri.Server)
	}

	// COMMIT: primary.
	capacity, err := ms.blockVSCommitExpand(ctx, entry.VolumeServer, req.Name, expandEpoch)
	if err != nil {
		// Commit failed on primary — cancel all.
		for _, ps := range prepared {
			if cerr := ms.blockVSCancelExpand(ctx, ps, req.Name, expandEpoch); cerr != nil {
				glog.Warningf("[reqID=%s] ExpandBlockVolume %q: cancel on %s after primary commit fail: %v", blockReqID(ctx), req.Name, ps, cerr)
			}
		}
		expandClean = true // all cancelled, safe to release
		return nil, fmt.Errorf("commit expand on primary %s: %w", entry.VolumeServer, err)
	}

	// COMMIT: replicas.
	allCommitted := true
	for _, ri := range entry.Replicas {
		if _, cerr := ms.blockVSCommitExpand(ctx, ri.Server, req.Name, expandEpoch); cerr != nil {
			glog.Warningf("[reqID=%s] ExpandBlockVolume %q: commit on replica %s failed: %v", blockReqID(ctx), req.Name, ri.Server, cerr)
			allCommitted = false
		}
	}

	if !allCommitted {
		// Primary committed but replica(s) failed. Mark expand as failed:
		// ExpandInProgress stays true → heartbeat won't overwrite SizeBytes.
		// Operator must reconcile (rebuild/re-expand failed replicas) then call ClearExpandFailed.
		ms.blockRegistry.MarkExpandFailed(req.Name)
		return nil, fmt.Errorf("block volume %q: expand committed on primary but failed on one or more replicas (volume degraded, expand locked)", req.Name)
	}

	// All committed: update registry and release cleanly.
	if uerr := ms.blockRegistry.UpdateSize(req.Name, capacity); uerr != nil {
		glog.Warningf("[reqID=%s] ExpandBlockVolume %q: registry update failed: %v", blockReqID(ctx), req.Name, uerr)
	}
	expandClean = true

	return &master_pb.ExpandBlockVolumeResponse{CapacityBytes: capacity}, nil
}

// createBlockVolumeResponseFromEntry builds a CreateBlockVolumeResponse from a registry entry.
func (ms *MasterServer) createBlockVolumeResponseFromEntry(entry *BlockVolumeEntry) *master_pb.CreateBlockVolumeResponse {
	return &master_pb.CreateBlockVolumeResponse{
		VolumeId:       entry.Name,
		VolumeServer:   entry.VolumeServer,
		IscsiAddr:      entry.ISCSIAddr,
		Iqn:            entry.IQN,
		CapacityBytes:  entry.SizeBytes,
		ReplicaServer:  entry.ReplicaServer, // backward compat
		ReplicaServers: replicaServerList(entry),
		NvmeAddr:       entry.NvmeAddr,
		Nqn:            entry.NQN,
	}
}

// validateIdempotentCreate checks that an idempotent create request is consistent
// with an existing entry. Returns nil if compatible, error on mismatch.
func (ms *MasterServer) validateIdempotentCreate(entry *BlockVolumeEntry, req *master_pb.CreateBlockVolumeRequest, durMode blockvol.DurabilityMode, replicaFactor int) error {
	if entry.SizeBytes < req.SizeBytes {
		return fmt.Errorf("block volume %q exists with size %d (requested %d)", req.Name, entry.SizeBytes, req.SizeBytes)
	}
	// Validate durability mode consistency.
	existingMode := entry.DurabilityMode
	if existingMode == "" {
		existingMode = "best_effort"
	}
	if durMode.String() != existingMode {
		return fmt.Errorf("block volume %q exists with durability_mode %q (requested %q)", req.Name, existingMode, durMode.String())
	}
	// Validate replica factor consistency.
	existingRF := entry.ReplicaFactor
	if existingRF == 0 {
		existingRF = 2 // default
	}
	if replicaFactor != existingRF {
		return fmt.Errorf("block volume %q exists with replica_factor %d (requested %d)", req.Name, existingRF, replicaFactor)
	}
	return nil
}

// replicaServerList returns the list of replica server addresses.
// Order matches Replicas[] (append-order), ensuring ReplicaServers[0] == ReplicaServer (legacy).
func replicaServerList(entry *BlockVolumeEntry) []string {
	if len(entry.Replicas) == 0 {
		return nil
	}
	servers := make([]string, len(entry.Replicas))
	for i, ri := range entry.Replicas {
		servers[i] = ri.Server
	}
	return servers
}

// removeServer returns a new slice without the specified server.
func removeServer(servers []string, server string) []string {
	result := make([]string, 0, len(servers)-1)
	for _, s := range servers {
		if s != server {
			result = append(result, s)
		}
	}
	return result
}

// cleanupPartialCreate removes a partially created block volume (primary + any replicas)
// when strict durability mode enforcement fails due to insufficient replicas.
// All operations are best-effort: failures are logged but do not propagate.
func (ms *MasterServer) cleanupPartialCreate(ctx context.Context, entry *BlockVolumeEntry) {
	// Delete primary volume.
	if err := ms.blockVSDelete(ctx, entry.VolumeServer, entry.Name); err != nil {
		glog.Warningf("[reqID=%s] cleanupPartialCreate %q: delete primary on %s: %v",
			blockReqID(ctx), entry.Name, entry.VolumeServer, err)
	}
	// Delete any successfully created replicas.
	for _, ri := range entry.Replicas {
		if err := ms.blockVSDelete(ctx, ri.Server, entry.Name); err != nil {
			glog.Warningf("[reqID=%s] cleanupPartialCreate %q: delete replica on %s: %v",
				blockReqID(ctx), entry.Name, ri.Server, err)
		}
	}
	// Remove from registry if somehow registered (shouldn't be at this point).
	ms.blockRegistry.Unregister(entry.Name)
}
