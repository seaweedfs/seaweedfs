package weed_server

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// writeOwner returns the filer that serializes writes to key, or "" when this
// filer is the serialization point — because it owns the key, or because there
// is no ring and every filer applies locally.
//
// A ring change hands a key to its new owner before that owner has rebuilt the
// locks the prior owner still holds, so the prior owner keeps the key until the
// cooling-off window closes.
func (fs *FilerServer) writeOwner(key string) pb.ServerAddress {
	if fs.filer.Dlm == nil {
		return ""
	}
	owner := fs.filer.Dlm.LockRing.WriteOwner(key)
	if owner == fs.option.Host {
		return ""
	}
	return owner
}

// ownerKeepsOwnStore reports whether key's write owner is another filer that
// is not known to run on this filer's store.
func (fs *FilerServer) ownerKeepsOwnStore(key string) bool {
	owner := fs.writeOwner(key)
	if owner == "" {
		return false
	}
	return fs.filer.MetaAggregator == nil || !fs.filer.MetaAggregator.SharesStore(owner)
}

// forwardDeleteEntry sends the delete to its write owner. handled=false means
// this filer is the owner and applies it.
func (fs *FilerServer) forwardDeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest, fullpath util.FullPath) (*filer_pb.DeleteEntryResponse, bool, error) {
	var ownerResp *filer_pb.DeleteEntryResponse
	handled, err := fs.forwardToWriteOwner(ctx, entryRouteKey(fullpath), func(owner pb.ServerAddress) error {
		if scopeKeepOnly(ctx) {
			return status.Error(codes.PermissionDenied, "filer: scoped DeleteEntry cannot be forwarded to the write owner")
		}
		glog.V(2).InfofCtx(ctx, "DeleteEntry %s: forwarding to owner %s", fullpath, owner)
		req.IsMoved = true
		return pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			forwarded, e := client.DeleteEntry(ctx, req)
			if e != nil {
				return e
			}
			ownerResp = forwarded
			return nil
		})
	})
	if err != nil {
		return &filer_pb.DeleteEntryResponse{}, handled, err
	}
	return ownerResp, handled, nil
}

// forwardToWriteOwner sends the request to key's write owner so a single filer's
// per-path lock arbitrates every writer of that key. handled=false means this
// filer is the owner and the caller should apply the request locally.
//
// An unreachable owner fails the request; it is never re-sent to another filer.
// gRPC reports a response lost in transit as Unavailable, indistinguishable from
// one the owner never saw, so a retry elsewhere could re-apply what the owner
// already committed — and an owner unreachable from here may be partitioned
// rather than down, still serving the key to everyone else. The ring hands the
// key on when the cooling-off window closes, so the outage is bounded.
//
// An owner that refuses the forward as not its own applied nothing: the two
// rings disagree. They converge once a ring update reaches the lagging filer or
// a cooling-off window closes, so the forward is re-routed by this filer's ring
// until shortly after its latest window ends, then the refusal surfaces. It
// also surfaces once this filer's ring names itself: a caller may have checked
// the entry before routing, and that check is stale after the wait.
func (fs *FilerServer) forwardToWriteOwner(ctx context.Context, key string, send func(owner pb.ServerAddress) error) (handled bool, err error) {
	var firstRefusal time.Time
	for wait := notWriteOwnerRetryWait; ; wait = min(2*wait, 10*notWriteOwnerRetryWait) {
		owner := fs.writeOwner(key)
		if owner == "" {
			return err != nil, err
		}
		if err = send(owner); err == nil {
			return true, nil
		}
		glog.V(1).InfofCtx(ctx, "route %s to owner %s: %v", key, owner, err)
		if !isNotWriteOwner(err) {
			return true, err
		}
		if firstRefusal.IsZero() {
			firstRefusal = time.Now()
		}
		deadline := firstRefusal.Add(notWriteOwnerRetrySlack)
		if end := fs.filer.Dlm.LockRing.PriorOwnerWindowEnd().Add(notWriteOwnerRetrySlack); end.After(deadline) {
			deadline = end
		}
		if limit := firstRefusal.Add(notWriteOwnerRetryLimit); deadline.After(limit) {
			deadline = limit
		}
		if time.Now().Add(wait).After(deadline) {
			return true, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return true, err
		case <-timer.C:
		}
	}
}

var (
	notWriteOwnerRetryWait  = 50 * time.Millisecond
	notWriteOwnerRetrySlack = time.Second
	notWriteOwnerRetryLimit = 15 * time.Second
)

// entryRouteKey is the ring key for an entry's writes. It shares the S3
// gateway's namespace so an object's ObjectTransaction and its CreateEntry
// resolve to the same owner, and land on that filer's one per-path lock.
func entryRouteKey(fullpath util.FullPath) string {
	return s3_constants.ObjectWriteRouteKeyPrefix + string(fullpath)
}

// movedFromPeer reports whether an is_moved marker arrived on a connection
// from a ring member, i.e. it marks a genuine forwarded hop.
func (fs *FilerServer) movedFromPeer(ctx context.Context, isMoved bool) bool {
	if !isMoved || fs.filer.Dlm == nil {
		return false
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	peerHost, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return false
	}
	peerIP := net.ParseIP(peerHost)
	if peerIP == nil {
		return false
	}
	for _, ip := range fs.ringMemberIPs(ctx) {
		if ip.Equal(peerIP) {
			return true
		}
	}
	return false
}

// ringPeerIPs caches resolved member addresses of one ring membership. A
// member's hostname may re-resolve under the same ring address, so the cache
// expires rather than trusting the resolution forever.
type ringPeerIPs struct {
	members string
	ips     []net.IP
	expires time.Time
}

const ringPeerIPTTL = 5 * time.Minute

// ringMemberIPs returns the ring members' addresses as IPs. Members can
// advertise hostnames, so resolution is cached per membership to keep DNS off
// each forwarded request; a failed lookup is not cached, so a DNS blip does
// not keep rejecting genuine forwards until the next ring change.
func (fs *FilerServer) ringMemberIPs(ctx context.Context) []net.IP {
	members := fs.filer.Dlm.LockRing.GetSnapshot()
	var sb strings.Builder
	for _, member := range members {
		sb.WriteString(string(member))
		sb.WriteByte(' ')
	}
	key := sb.String()
	if cached := fs.ringPeerIPs.Load(); cached != nil && cached.members == key && time.Now().Before(cached.expires) {
		return cached.ips
	}
	resolved, _, _ := fs.ringResolveGroup.Do(key, func() (any, error) {
		if cached := fs.ringPeerIPs.Load(); cached != nil && cached.members == key && time.Now().Before(cached.expires) {
			return cached.ips, nil
		}
		// Shared by every caller waiting on this key: the lookups outlive the
		// first request's cancellation, and run in parallel so one slow member
		// cannot starve the rest of the shared deadline.
		var ips []net.IP
		var hosts []string
		for _, member := range members {
			host, _, err := net.SplitHostPort(string(member))
			if err != nil {
				continue
			}
			if ip := net.ParseIP(host); ip != nil {
				ips = append(ips, ip)
				continue
			}
			hosts = append(hosts, host)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		failed := false
		for _, host := range hosts {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				defer cancel()
				found, err := net.DefaultResolver.LookupIP(lookupCtx, "ip", host)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failed = true
					return
				}
				ips = append(ips, found...)
			}(host)
		}
		wg.Wait()
		if !failed {
			fs.ringPeerIPs.Store(&ringPeerIPs{members: key, ips: ips, expires: time.Now().Add(ringPeerIPTTL)})
		}
		return ips, nil
	})
	return resolved.([]net.IP)
}

// checkMovedMarker refuses a request whose is_moved marker did not arrive from
// a ring member while this filer is not the key's owner. The marker is
// caller-controlled: applying it would evaluate a conditional mutation under a
// non-owner's lock, and re-forwarding a claimed hop can cycle while rings
// disagree — so an unverifiable marker on a non-owner is refused instead.
// PermissionDenied keeps the refusal distinct from a write condition's
// FailedPrecondition, which callers use to detect a stale stamp.
// owner=="" means this filer is the serialization point and the request can
// be applied locally.
func (fs *FilerServer) checkMovedMarker(ctx context.Context, isMoved bool, owner pb.ServerAddress) error {
	if !isMoved || owner == "" || owner == fs.option.Host || fs.movedFromPeer(ctx, isMoved) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "is_moved not sent by a ring member; the key's owner is %s", owner)
}

// notWriteOwnerReason marks a forwarded write refused by a filer whose own ring
// names another write owner for the key.
const notWriteOwnerReason = "NOT_WRITE_OWNER"

// checkForwardedWrite admits a routed write that arrived with is_moved only on
// the key's write owner in this filer's own ring. While two rings disagree, a
// forward applied here would be checked under a second filer's lock, and
// re-forwarding it could cycle, so it is refused. The refusal is Unavailable
// with a NOT_WRITE_OWNER reason, telling the sender nothing was applied.
func (fs *FilerServer) checkForwardedWrite(ctx context.Context, key string) error {
	owner := fs.writeOwner(key)
	if err := fs.checkMovedMarker(ctx, true, owner); err != nil {
		return err
	}
	if owner == "" {
		return nil
	}
	return notWriteOwnerError(key, owner)
}

func notWriteOwnerError(key string, owner pb.ServerAddress) error {
	st := status.Newf(codes.Unavailable, "not the write owner of %s: %s is", key, owner)
	withReason, err := st.WithDetails(&errdetails.ErrorInfo{Reason: notWriteOwnerReason, Domain: "seaweedfs"})
	if err != nil {
		return st.Err()
	}
	return withReason.Err()
}

// isNotWriteOwner reports whether err is checkForwardedWrite's refusal.
func isNotWriteOwner(err error) bool {
	st, ok := util.ServerStatus(err)
	if !ok || st.Code() != codes.Unavailable {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == notWriteOwnerReason {
			return true
		}
	}
	return false
}
