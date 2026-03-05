package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// CreateBlockVolume picks a volume server, delegates creation, and records
// the mapping in the block volume registry.
func (ms *MasterServer) CreateBlockVolume(ctx context.Context, req *master_pb.CreateBlockVolumeRequest) (*master_pb.CreateBlockVolumeResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.SizeBytes == 0 {
		return nil, fmt.Errorf("size_bytes must be > 0")
	}

	// Idempotent: if already registered, return existing entry (validate size).
	if entry, ok := ms.blockRegistry.Lookup(req.Name); ok {
		if entry.SizeBytes < req.SizeBytes {
			return nil, fmt.Errorf("block volume %q exists with size %d (requested %d)", req.Name, entry.SizeBytes, req.SizeBytes)
		}
		return &master_pb.CreateBlockVolumeResponse{
			VolumeId:      entry.Name,
			VolumeServer:  entry.VolumeServer,
			IscsiAddr:     entry.ISCSIAddr,
			Iqn:           entry.IQN,
			CapacityBytes: entry.SizeBytes,
		}, nil
	}

	// Per-name inflight lock prevents concurrent creates for the same name.
	if !ms.blockRegistry.AcquireInflight(req.Name) {
		return nil, fmt.Errorf("block volume %q creation already in progress", req.Name)
	}
	defer ms.blockRegistry.ReleaseInflight(req.Name)

	// Double-check after acquiring lock (another goroutine may have finished).
	if entry, ok := ms.blockRegistry.Lookup(req.Name); ok {
		return &master_pb.CreateBlockVolumeResponse{
			VolumeId:      entry.Name,
			VolumeServer:  entry.VolumeServer,
			IscsiAddr:     entry.ISCSIAddr,
			Iqn:           entry.IQN,
			CapacityBytes: entry.SizeBytes,
		}, nil
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

		result, err := ms.blockVSAllocate(ctx, server, req.Name, req.SizeBytes, req.DiskType)
		if err != nil {
			lastErr = fmt.Errorf("server %s: %w", server, err)
			glog.V(0).Infof("CreateBlockVolume %q: attempt %d on %s failed: %v", req.Name, attempt+1, server, err)
			servers = removeServer(servers, server)
			continue
		}

		entry := &BlockVolumeEntry{
			Name:           req.Name,
			VolumeServer:   server,
			Path:           result.Path,
			IQN:            result.IQN,
			ISCSIAddr:      result.ISCSIAddr,
			SizeBytes:      req.SizeBytes,
			Epoch:          1,
			Role:           blockvol.RoleToWire(blockvol.RolePrimary),
			Status:         StatusActive,
			LeaseTTL:       30 * time.Second,
			LastLeaseGrant: time.Now(), // R2-F1: set BEFORE Register to avoid stale-lease race
		}

		// Try to create replica on a different server (F4: partial create OK).
		var replicaServer string
		remainingServers := removeServer(servers, server)
		if len(remainingServers) > 0 {
			replicaServer = ms.tryCreateReplica(ctx, req, entry, result, remainingServers)
		} else {
			glog.V(0).Infof("CreateBlockVolume %q: single-copy mode (only 1 server)", req.Name)
		}

		// Register in registry as Active (VS confirmed creation).
		if err := ms.blockRegistry.Register(entry); err != nil {
			// Already registered (race condition) — return the existing entry.
			if existing, ok := ms.blockRegistry.Lookup(req.Name); ok {
				return &master_pb.CreateBlockVolumeResponse{
					VolumeId:      existing.Name,
					VolumeServer:  existing.VolumeServer,
					IscsiAddr:     existing.ISCSIAddr,
					Iqn:           existing.IQN,
					CapacityBytes: existing.SizeBytes,
					ReplicaServer: existing.ReplicaServer,
				}, nil
			}
			return nil, fmt.Errorf("register block volume: %w", err)
		}

		// Enqueue assignments for primary (and replica if available).
		leaseTTLMs := blockvol.LeaseTTLToWire(30 * time.Second)
		ms.blockAssignmentQueue.Enqueue(server, blockvol.BlockVolumeAssignment{
			Path:            result.Path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      leaseTTLMs,
			ReplicaDataAddr: entry.ReplicaDataAddr,
			ReplicaCtrlAddr: entry.ReplicaCtrlAddr,
		})
		if entry.ReplicaServer != "" {
			ms.blockAssignmentQueue.Enqueue(entry.ReplicaServer, blockvol.BlockVolumeAssignment{
				Path:            entry.ReplicaPath,
				Epoch:           1,
				Role:            blockvol.RoleToWire(blockvol.RoleReplica),
				LeaseTtlMs:      leaseTTLMs,
				ReplicaDataAddr: entry.ReplicaDataAddr,
				ReplicaCtrlAddr: entry.ReplicaCtrlAddr,
			})
		}

		glog.V(0).Infof("CreateBlockVolume %q: created on %s (path=%s, iqn=%s, replica=%s)",
			req.Name, server, result.Path, result.IQN, replicaServer)
		return &master_pb.CreateBlockVolumeResponse{
			VolumeId:      req.Name,
			VolumeServer:  server,
			IscsiAddr:     result.ISCSIAddr,
			Iqn:           result.IQN,
			CapacityBytes: req.SizeBytes,
			ReplicaServer: replicaServer,
		}, nil
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

	// R2-F4: Also delete replica (best-effort, don't fail if replica is down).
	if entry.ReplicaServer != "" {
		if err := ms.blockVSDelete(ctx, entry.ReplicaServer, req.Name); err != nil {
			glog.Warningf("DeleteBlockVolume %q: replica delete on %s failed (best-effort): %v",
				req.Name, entry.ReplicaServer, err)
		}
	}

	ms.blockRegistry.Unregister(req.Name)
	glog.V(0).Infof("DeleteBlockVolume %q: removed from %s (replica=%s)", req.Name, entry.VolumeServer, entry.ReplicaServer)
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

	return &master_pb.LookupBlockVolumeResponse{
		VolumeServer:  entry.VolumeServer,
		IscsiAddr:     entry.ISCSIAddr,
		Iqn:           entry.IQN,
		CapacityBytes: entry.SizeBytes,
		ReplicaServer: entry.ReplicaServer,
	}, nil
}

// tryCreateReplica attempts to create a replica volume on a different server.
// Returns the replica server address on success, or empty string on failure (F4).
func (ms *MasterServer) tryCreateReplica(ctx context.Context, req *master_pb.CreateBlockVolumeRequest, entry *BlockVolumeEntry, primaryResult *blockAllocResult, candidates []string) string {
	for _, replicaServerStr := range candidates {
		replicaResult, err := ms.blockVSAllocate(ctx, replicaServerStr, req.Name, req.SizeBytes, req.DiskType)
		if err != nil {
			glog.V(0).Infof("CreateBlockVolume %q: replica on %s failed: %v", req.Name, replicaServerStr, err)
			continue
		}
		entry.ReplicaServer = replicaServerStr
		entry.ReplicaPath = replicaResult.Path
		entry.ReplicaIQN = replicaResult.IQN
		entry.ReplicaISCSIAddr = replicaResult.ISCSIAddr
		entry.ReplicaDataAddr = replicaResult.ReplicaDataAddr
		entry.ReplicaCtrlAddr = replicaResult.ReplicaCtrlAddr
		entry.RebuildListenAddr = primaryResult.RebuildListenAddr
		return replicaServerStr
	}
	glog.Warningf("CreateBlockVolume %q: created without replica (replica allocation failed)", req.Name)
	return ""
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
