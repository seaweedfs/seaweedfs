package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const (
	// posixLockSessionTTL is how long a mount's lease survives without a
	// keepalive before its locks are reaped; posixLockSweepInterval is how often
	// each filer checks. The mount renews well within the TTL.
	posixLockSessionTTL    = 15 * time.Second
	posixLockSweepInterval = 5 * time.Second
	// posixCoolingProbeTimeout bounds the dual-read probe to the prior owner so a
	// slow peer can't stall a non-blocking lock call during the cooling window.
	posixCoolingProbeTimeout = 2 * time.Second
	// posixLockWarmup is how long after a (re)start the owner defers would-be
	// grants while mounts re-assert their held locks, so it does not double-grant
	// a lock its fresh, still-empty state has not yet rebuilt. Must exceed the
	// mount keepalive interval and stay below the TTL.
	posixLockWarmup = 10 * time.Second
)

// startPosixLockSweeper periodically reaps the locks of leased sessions (mounts)
// that stopped sending keepalives. Sessions that never renew are never reaped, so
// this is inert until mounts run with -posixLock.
func (fs *FilerServer) startPosixLockSweeper() {
	fs.posixLockReadyAt.Store(time.Now().UnixNano())
	fs.posixLockSweeperStop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(posixLockSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-fs.posixLockSweeperStop:
				return
			case <-ticker.C:
				if reaped := fs.posixLocks.ReapExpired(posixLockSessionTTL); len(reaped) > 0 {
					glog.V(2).Infof("posix lock: reaped %d expired session(s): %v", len(reaped), reaped)
				}
			}
		}
	}()
}

// PosixLock applies one advisory lock operation against the in-memory lock table
// of the inode's owner filer. The owner is resolved from req.Key on the same
// ring the gateway and DLM use; a non-owner filer forwards the request one hop
// (is_moved bounds it), so the owner's table stays the single authority even when
// the caller's ring view is stale. Blocking (SetLkw) is the caller's job — this
// is strictly non-blocking try/release/query.
func (fs *FilerServer) PosixLock(ctx context.Context, req *filer_pb.PosixLockRequest) (*filer_pb.PosixLockResponse, error) {
	if req.Key == "" {
		return &filer_pb.PosixLockResponse{}, fmt.Errorf("key is required")
	}
	if req.Lock == nil {
		return &filer_pb.PosixLockResponse{}, fmt.Errorf("lock is required")
	}

	if !req.IsMoved && fs.filer.Dlm != nil {
		if owner := fs.filer.Dlm.LockRing.GetPrimary(req.Key); owner != "" && owner != fs.option.Host {
			forwarded := &filer_pb.PosixLockRequest{
				Key:     req.Key,
				IsMoved: true,
				Op:      req.Op,
				Lock:    req.Lock,
				Locks:   req.Locks,
			}
			glog.V(4).InfofCtx(ctx, "PosixLock %s op=%v: forwarding to owner %s", req.Key, req.Op, owner)
			var resp *filer_pb.PosixLockResponse
			err := pb.WithFilerClient(false, 0, owner, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				var e error
				resp, e = client.PosixLock(ctx, forwarded)
				return e
			})
			if err != nil {
				return &filer_pb.PosixLockResponse{}, err
			}
			return resp, nil
		}
	}

	lk := posixlock.Range{
		Start:   req.Lock.GetStart(),
		End:     req.Lock.GetEnd(),
		Type:    req.Lock.GetType(),
		Sid:     req.Lock.GetSid(),
		Owner:   req.Lock.GetOwner(),
		Pid:     req.Lock.GetPid(),
		IsFlock: req.Lock.GetIsFlock(),
	}

	resp := &filer_pb.PosixLockResponse{}
	switch req.Op {
	case filer_pb.PosixLockOp_TRY_LOCK:
		// A fresh owner's state may be incomplete (post-restart warm-up, or a ring
		// change whose previous owner can't be reached): report a known conflict,
		// else defer the grant so the client retries rather than risk a
		// double-grant. A deferred grant becomes EAGAIN for non-blocking SetLk and
		// a retry for the blocking SetLkw poll — never a spurious grant.
		if !req.CoolingProbe {
			if c, deferGrant := fs.posixCoolingOrWarmup(ctx, req.Key, lk); c != nil {
				resp.HasConflict = true
				resp.Conflict = c
				break
			} else if deferGrant {
				break
			}
		}
		if c, granted := fs.posixLocks.TryLock(req.Key, lk); granted {
			resp.Granted = true
		} else {
			resp.HasConflict = true
			resp.Conflict = posixRangeToPb(c)
		}
	case filer_pb.PosixLockOp_UNLOCK:
		fs.posixLocks.Unlock(req.Key, lk)
	case filer_pb.PosixLockOp_GET_LK:
		// A query is best-effort: report a known conflict but never defer.
		if !req.CoolingProbe {
			if c, _ := fs.posixCoolingOrWarmup(ctx, req.Key, lk); c != nil {
				resp.HasConflict = true
				resp.Conflict = c
				break
			}
		}
		if c, found := fs.posixLocks.GetLk(req.Key, lk); found {
			resp.HasConflict = true
			resp.Conflict = posixRangeToPb(c)
		}
	case filer_pb.PosixLockOp_RELEASE_POSIX_OWNER:
		fs.posixLocks.ReleasePosixOwner(req.Key, lk.Sid, lk.Owner)
	case filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER:
		fs.posixLocks.ReleaseFlockOwner(req.Key, lk.Sid, lk.Owner)
	case filer_pb.PosixLockOp_KEEP_ALIVE:
		// A re-assertion carries the mount's held locks on this key so the owner
		// can rebuild its in-memory state after an ownership change or restart; a
		// bare keepalive just renews the lease.
		if len(req.Locks) > 0 {
			held := make([]posixlock.Range, 0, len(req.Locks))
			for _, l := range req.Locks {
				held = append(held, posixRangeFromPb(l))
			}
			if conflicts := fs.posixLocks.Reassert(req.Key, lk.Sid, held); len(conflicts) > 0 {
				glog.Warningf("posix reassert %s sid %d: %d lock(s) lost to another session: %+v", req.Key, lk.Sid, len(conflicts), conflicts)
			}
		} else {
			fs.posixLocks.Renew(lk.Sid)
		}
	default:
		return &filer_pb.PosixLockResponse{}, fmt.Errorf("unknown posix lock op %v", req.Op)
	}
	return resp, nil
}

// posixWarmingUp reports whether this filer is still within posixLockWarmup of
// when it began serving POSIX locks. The zero readyAt (e.g. in tests) is never
// warming up.
func (fs *FilerServer) posixWarmingUp() bool {
	readyAt := fs.posixLockReadyAt.Load()
	return readyAt != 0 && time.Since(time.Unix(0, readyAt)) < posixLockWarmup
}

// posixCoolingOrWarmup decides whether a fresh owner can trust its local state
// for key before granting. It returns a known blocking lock (conflict != nil),
// or asks the caller to defer the grant (deferGrant) when the state may be
// incomplete and no conflict can be confirmed. It returns (nil, false) when the
// owner is authoritative and should consult its local table.
//
//   - Warm-up: just after a (re)start the owner is still rebuilding from
//     re-assertions, so only a locally-visible conflict is trustworthy; a
//     would-be grant is deferred.
//   - Ring change (cooling window): the previous owner may still hold a lock this
//     owner hasn't rebuilt. Ask it (marked cooling_probe so it answers locally
//     without recursing), under a short deadline so a slow peer can't stall the
//     non-blocking lock path. If it is unreachable — typically because it
//     crashed, which caused the change — we cannot confirm, so we defer rather
//     than risk a double-grant; re-assertion rebuilds this owner before the
//     window ends.
func (fs *FilerServer) posixCoolingOrWarmup(ctx context.Context, key string, lk posixlock.Range) (conflict *filer_pb.PosixLockRange, deferGrant bool) {
	if fs.posixWarmingUp() {
		if c, found := fs.posixLocks.GetLk(key, lk); found {
			return posixRangeToPb(c), false
		}
		return nil, true
	}
	if fs.filer.Dlm == nil {
		return nil, false
	}
	prior := fs.filer.Dlm.LockRing.PriorOwner(key)
	if prior == "" || prior == fs.option.Host {
		return nil, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, posixCoolingProbeTimeout)
	defer cancel()
	var resp *filer_pb.PosixLockResponse
	err := pb.WithFilerClient(false, 0, prior, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.PosixLock(probeCtx, &filer_pb.PosixLockRequest{
			Key:          key,
			IsMoved:      true,
			CoolingProbe: true,
			Op:           filer_pb.PosixLockOp_GET_LK,
			Lock:         posixRangeToPb(lk),
		})
		return e
	})
	if err != nil {
		// Cannot confirm — defer rather than risk a double-grant (the prior owner
		// likely crashed; re-assertion rebuilds this owner before the window ends).
		glog.V(2).InfofCtx(ctx, "posix cooling probe %s -> %s: %v (deferring)", key, prior, err)
		return nil, true
	}
	if resp.GetHasConflict() {
		return resp.GetConflict(), false
	}
	return nil, false
}

func posixRangeFromPb(l *filer_pb.PosixLockRange) posixlock.Range {
	return posixlock.Range{
		Start:   l.GetStart(),
		End:     l.GetEnd(),
		Type:    l.GetType(),
		Sid:     l.GetSid(),
		Owner:   l.GetOwner(),
		Pid:     l.GetPid(),
		IsFlock: l.GetIsFlock(),
	}
}

func posixRangeToPb(r posixlock.Range) *filer_pb.PosixLockRange {
	return &filer_pb.PosixLockRange{
		Start:   r.Start,
		End:     r.End,
		Type:    r.Type,
		Sid:     r.Sid,
		Owner:   r.Owner,
		Pid:     r.Pid,
		IsFlock: r.IsFlock,
	}
}
