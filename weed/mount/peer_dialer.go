package mount

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mount_peer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerConnPool caches one long-lived gRPC client connection per peer
// address. Both the announcer flush loop and the read-path fetcher hit
// the same handful of directory owners repeatedly; without a cache each
// call would pay TCP handshake + HTTP/2 preface cost (and on TLS, an
// additional handshake). The cache makes steady-state owner RPCs
// effectively free after the first call.
//
// Connections in terminal failure states (Shutdown) are transparently
// replaced on next access. Sizing: entries are ~1 KB + the conn itself;
// bounded at maxPeerConnPoolEntries to contain runaway growth.
type PeerConnPool struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// maxPeerConnPoolEntries caps live peer conns per mount. A 10k-mount
// fleet with HRW-sharded directory reaches only ~200 distinct owner
// addresses per mount in the worst case, so this is far above any real
// footprint while still bounding pathological growth.
const maxPeerConnPoolEntries = 4096

// NewPeerConnPool returns an empty pool. dialOpts should carry transport
// credentials matching the server side (production wires
// option.GrpcDialOption, which security.LoadClientTLS populates from
// security.toml). When no options are supplied we fall back to insecure
// cleartext — only safe for in-process tests.
func NewPeerConnPool(dialOpts ...grpc.DialOption) *PeerConnPool {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	return &PeerConnPool{
		dialOpts: dialOpts,
		conns:    map[string]*grpc.ClientConn{},
	}
}

// Dialer returns a MountPeerDialer bound to this pool. The returned
// closeFn is a no-op — the pool owns the connection lifecycle. Tests
// that want per-call dials can keep using the non-pooled variant.
func (p *PeerConnPool) Dialer() MountPeerDialer {
	return func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		conn, err := p.get(peerAddr)
		if err != nil {
			return nil, func() {}, err
		}
		return mount_peer_pb.NewMountPeerClient(conn), func() {}, nil
	}
}

func (p *PeerConnPool) get(peerAddr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[peerAddr]; ok {
		if conn.GetState() != connectivity.Shutdown {
			return conn, nil
		}
		// Pooled conn is unusable — drop it and redial below.
		_ = conn.Close()
		delete(p.conns, peerAddr)
	}
	if len(p.conns) >= maxPeerConnPoolEntries {
		// Evict one arbitrary entry. Simple over LRU: the pool is small
		// in practice, and the victim will be re-dialed if needed.
		for k, c := range p.conns {
			_ = c.Close()
			delete(p.conns, k)
			break
		}
	}
	conn, err := grpc.NewClient(peerAddr, p.dialOpts...)
	if err != nil {
		return nil, err
	}
	p.conns[peerAddr] = conn
	return conn, nil
}

// Close tears down every cached connection. Safe to call multiple times.
func (p *PeerConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		_ = c.Close()
		delete(p.conns, addr)
	}
}

// Size returns the current number of cached connections — useful for
// tests and metrics exports.
func (p *PeerConnPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// DefaultMountPeerDialer returns a per-call dialer (no pooling). Kept for
// tests and for any caller that genuinely wants a fresh connection per
// invocation. Production code should prefer PeerConnPool.Dialer().
//
// Unused options silence dial-time lints when dialOpts is nil.
func DefaultMountPeerDialer(dialOpts ...grpc.DialOption) MountPeerDialer {
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, dialOpts...)
	return func(ctx context.Context, peerAddr string) (mount_peer_pb.MountPeerClient, func(), error) {
		conn, err := grpc.NewClient(peerAddr, opts...)
		if err != nil {
			return nil, func() {}, err
		}
		return mount_peer_pb.NewMountPeerClient(conn), func() { _ = conn.Close() }, nil
	}
}

// peerConnMaxAge lets external callers (or future metrics) decide when a
// pool entry is "stale" for monitoring purposes. The pool itself does not
// expire on age — grpc handles reconnect internally.
var peerConnMaxAge = 10 * time.Minute

var _ = peerConnMaxAge // suppress unused-lint until a metric consumes it
