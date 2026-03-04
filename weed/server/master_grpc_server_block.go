package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

		path, iqn, iscsiAddr, err := ms.blockVSAllocate(ctx, pb.ServerAddress(server), req.Name, req.SizeBytes, req.DiskType)
		if err != nil {
			lastErr = fmt.Errorf("server %s: %w", server, err)
			glog.V(0).Infof("CreateBlockVolume %q: attempt %d on %s failed: %v", req.Name, attempt+1, server, err)
			servers = removeServer(servers, server)
			continue
		}

		// Register in registry as Active (VS confirmed creation).
		// Heartbeat will update epoch/role fields later.
		if err := ms.blockRegistry.Register(&BlockVolumeEntry{
			Name:         req.Name,
			VolumeServer: server,
			Path:         path,
			IQN:          iqn,
			ISCSIAddr:    iscsiAddr,
			SizeBytes:    req.SizeBytes,
			Status:       StatusActive,
		}); err != nil {
			// Already registered (race condition) — return the existing entry.
			if existing, ok := ms.blockRegistry.Lookup(req.Name); ok {
				return &master_pb.CreateBlockVolumeResponse{
					VolumeId:      existing.Name,
					VolumeServer:  existing.VolumeServer,
					IscsiAddr:     existing.ISCSIAddr,
					Iqn:           existing.IQN,
					CapacityBytes: existing.SizeBytes,
				}, nil
			}
			return nil, fmt.Errorf("register block volume: %w", err)
		}

		glog.V(0).Infof("CreateBlockVolume %q: created on %s (path=%s, iqn=%s)", req.Name, server, path, iqn)
		return &master_pb.CreateBlockVolumeResponse{
			VolumeId:      req.Name,
			VolumeServer:  server,
			IscsiAddr:     iscsiAddr,
			Iqn:           iqn,
			CapacityBytes: req.SizeBytes,
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

	// Call volume server to delete.
	if err := ms.blockVSDelete(ctx, pb.ServerAddress(entry.VolumeServer), req.Name); err != nil {
		return nil, fmt.Errorf("delete block volume %q on %s: %w", req.Name, entry.VolumeServer, err)
	}

	ms.blockRegistry.Unregister(req.Name)
	glog.V(0).Infof("DeleteBlockVolume %q: removed from %s", req.Name, entry.VolumeServer)
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
	}, nil
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
