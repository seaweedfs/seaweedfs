package weed_server

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// mountPeerRegistrySweepInterval is how often the filer evicts expired mount
// registry entries. Eviction is also done lazily on List; the sweep keeps
// memory bounded on long-running filers with high churn.
const mountPeerRegistrySweepInterval = 60 * time.Second

// runMountPeerRegistrySweeper runs for the lifetime of the FilerServer when
// peer registry is enabled.
func (fs *FilerServer) runMountPeerRegistrySweeper() {
	ticker := time.NewTicker(mountPeerRegistrySweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		if fs.mountPeerRegistry == nil {
			return
		}
		if evicted := fs.mountPeerRegistry.Sweep(); evicted > 0 {
			glog.V(2).Infof("peer registry: evicted %d stale entries", evicted)
		}
	}
}

// MountRegister records (or refreshes) the caller as a live mount server in
// the filer's peer registry. Returns an empty response; the caller is
// expected to heartbeat before the TTL expires.
//
// Requests are silently dropped when the registry is disabled (default), so
// clients can safely probe without breaking older filers.
func (fs *FilerServer) MountRegister(ctx context.Context, req *filer_pb.MountRegisterRequest) (*filer_pb.MountRegisterResponse, error) {
	if fs.mountPeerRegistry == nil {
		return &filer_pb.MountRegisterResponse{}, nil
	}
	ttl := time.Duration(req.TtlSeconds) * time.Second
	fs.mountPeerRegistry.Register(req.PeerAddr, req.DataCenter, req.Rack, ttl)
	return &filer_pb.MountRegisterResponse{}, nil
}

// MountList returns the current set of live mounts for callers building
// their HRW seed view.
func (fs *FilerServer) MountList(ctx context.Context, req *filer_pb.MountListRequest) (*filer_pb.MountListResponse, error) {
	if fs.mountPeerRegistry == nil {
		return &filer_pb.MountListResponse{}, nil
	}
	entries := fs.mountPeerRegistry.List()
	resp := &filer_pb.MountListResponse{
		Mounts: make([]*filer_pb.MountInfo, 0, len(entries)),
	}
	for _, e := range entries {
		resp.Mounts = append(resp.Mounts, &filer_pb.MountInfo{
			PeerAddr:   e.PeerAddr,
			Rack:       e.Rack,
			LastSeenNs: e.LastSeenNs,
		})
	}
	return resp, nil
}
