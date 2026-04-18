package mount

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fetchChunkStreamSize is the frame size used when server-streaming a chunk's
// bytes back to a peer. 1 MiB is well above gRPC's 32 KiB preferred frame but
// comfortably under the default 4 MiB message cap, so each Recv on the client
// returns quickly and the chunk is assembled with ~16 Recv calls for typical
// 16 MiB chunks.
const fetchChunkStreamSize = 1 * 1024 * 1024

// PeerGrpcServer is the single mount-to-mount gRPC endpoint. It serves:
//   - ChunkAnnounce / ChunkLookup — the tier-2 directory RPCs, populated
//     by inbound announces and queried by inbound lookups. Each handler
//     is HRW-gated on the caller-side seed view.
//   - FetchChunk (server stream) — serves bytes from the local
//     chunk_cache to peers. Replaces the earlier HTTP-only peer-serve
//     endpoint: one port, one authentication path, one connection pool.
type PeerGrpcServer struct {
	mount_peer_pb.UnimplementedMountPeerServer

	dir      *PeerDirectory
	cache    chunk_cache.ChunkCache
	ownerFor func(fid string) string // HRW owner predicate on current seeds
	selfAddr string
	grpcS    *grpc.Server
	listener net.Listener
	stopped  bool
}

// NewPeerGrpcServer constructs the server. cache is the local chunk_cache
// (used to serve FetchChunk); dir is the local directory shard (used to
// answer ChunkAnnounce / ChunkLookup); ownerFor returns the HRW owner of a
// fid on the current seed view.
func NewPeerGrpcServer(cache chunk_cache.ChunkCache, dir *PeerDirectory, ownerFor func(fid string) string, selfAddr string) *PeerGrpcServer {
	return &PeerGrpcServer{
		cache:    cache,
		dir:      dir,
		ownerFor: ownerFor,
		selfAddr: selfAddr,
	}
}

// Start binds a TCP listener at addr and registers the MountPeer service.
func (s *PeerGrpcServer) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("peer grpc listen %s: %w", addr, err)
	}
	s.listener = ln
	s.grpcS = pb.NewGrpcServer()
	mount_peer_pb.RegisterMountPeerServer(s.grpcS, s)
	go func() {
		if err := s.grpcS.Serve(ln); err != nil && err != grpc.ErrServerStopped {
			glog.Warningf("peer-grpc terminated: %v", err)
		}
	}()
	glog.V(0).Infof("peer-grpc listening on %s", ln.Addr())
	return nil
}

// Stop halts the gRPC server without waiting for in-flight streams.
func (s *PeerGrpcServer) Stop() {
	if s.stopped {
		return
	}
	s.stopped = true
	if s.grpcS != nil {
		s.grpcS.Stop()
	}
}

// Addr returns the bound address (useful when the caller used ":0").
func (s *PeerGrpcServer) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// ChunkAnnounce accepts holder entries for fids this mount owns; rejects
// others so the caller can retry against the correct owner.
func (s *PeerGrpcServer) ChunkAnnounce(ctx context.Context, req *mount_peer_pb.ChunkAnnounceRequest) (*mount_peer_pb.ChunkAnnounceResponse, error) {
	ttl := time.Duration(req.TtlSeconds) * time.Second
	res := s.dir.Announce(req.PeerAddr, req.DataCenter, req.Rack, req.FileIds, ttl, s.ownerPredicate)
	return &mount_peer_pb.ChunkAnnounceResponse{
		RejectedFileIds: res.Rejected,
	}, nil
}

// ChunkLookup returns known holders for each requested fid in LRU order.
func (s *PeerGrpcServer) ChunkLookup(ctx context.Context, req *mount_peer_pb.ChunkLookupRequest) (*mount_peer_pb.ChunkLookupResponse, error) {
	res := s.dir.Lookup(req.FileIds, s.ownerPredicate)
	resp := &mount_peer_pb.ChunkLookupResponse{
		PeersByFid:      make(map[string]*mount_peer_pb.PeerSet, len(res.PeersByFid)),
		NotOwnerFileIds: res.NotOwnerFids,
	}
	for fid, holders := range res.PeersByFid {
		peers := &mount_peer_pb.PeerSet{}
		for _, h := range holders {
			peers.Peers = append(peers.Peers, &mount_peer_pb.PeerInfo{
				PeerAddr:   h.PeerAddr,
				DataCenter: h.DataCenter,
				Rack:       h.Rack,
			})
		}
		resp.PeersByFid[fid] = peers
	}
	return resp, nil
}

// FetchChunk streams bytes of a cached chunk back to the caller. Missing
// fid → gRPC NOT_FOUND. Bytes are framed at fetchChunkStreamSize so gRPC's
// default 4 MiB message cap does not constrain chunk size.
func (s *PeerGrpcServer) FetchChunk(req *mount_peer_pb.FetchChunkRequest, stream mount_peer_pb.MountPeer_FetchChunkServer) error {
	if s.cache == nil {
		return status.Error(codes.Unavailable, "chunk cache not configured")
	}
	fid := req.FileId
	if fid == "" {
		return status.Error(codes.InvalidArgument, "missing file_id")
	}

	maxSize := s.cache.GetMaxFilePartSizeInCache()
	if maxSize == 0 {
		maxSize = 8 * 1024 * 1024
	}
	// mem.Allocate rounds up to the nearest power-of-2 slot backed by a
	// shared sync.Pool; avoids an 8 MiB allocation per FetchChunk call.
	buf := mem.Allocate(int(maxSize))
	defer mem.Free(buf)

	n, err := s.cache.ReadChunkAt(buf, fid, 0)
	if err != nil || n <= 0 {
		return status.Errorf(codes.NotFound, "fid %s not cached", fid)
	}

	for off := 0; off < n; off += fetchChunkStreamSize {
		end := off + fetchChunkStreamSize
		if end > n {
			end = n
		}
		if sendErr := stream.Send(&mount_peer_pb.FetchChunkResponse{
			Data: buf[off:end],
		}); sendErr != nil {
			return sendErr
		}
	}
	return nil
}

func (s *PeerGrpcServer) ownerPredicate(fid string) bool {
	if s.ownerFor == nil {
		return true // no HRW configured → accept all (single-mount mode)
	}
	return s.ownerFor(fid) == s.selfAddr
}

// peerDirectorySweepInterval is how often the mount evicts expired
// directory entries. Lookup no longer deletes inline (it takes only an
// RLock), so this sweeper is the sole memory reclamation path.
const peerDirectorySweepInterval = 60 * time.Second

func (wfs *WFS) runPeerDirectorySweeper() {
	ticker := time.NewTicker(peerDirectorySweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		dir := wfs.peerDirectory
		if dir == nil {
			return
		}
		if evicted := dir.Sweep(); evicted > 0 {
			glog.V(2).Infof("peer directory: evicted %d expired entries", evicted)
		}
	}
}
