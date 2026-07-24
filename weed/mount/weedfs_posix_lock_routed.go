package mount

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// randomPosixSid returns a process-stable, cluster-unique-enough session id for
// this mount, used to namespace lock owners so the same FUSE owner value on a
// different mount never aliases, and to scope lease-based reaping.
func randomPosixSid() uint64 {
	return binary.BigEndian.Uint64(util.RandomBytes(8))
}

// Routed POSIX locking: when -dlm is set, advisory locks are serialized on the
// inode's owner filer instead of in this mount's local table, so locks are
// honored across mounts. The mount calls its filer and relies on filer-side
// forwarding to reach the owner. Blocking (SetLkw) is client-side polling — there
// is no server-side wait queue.

const (
	posixLockMinBackoff = 5 * time.Millisecond
	posixLockMaxBackoff = 200 * time.Millisecond
	posixLockKeyPrefix  = "s3.fuse.lock:"
	// posixLockReleaseTimeout bounds the background unlock/release RPCs. They run
	// off the syscall path (close/flush) and must not be cancelled by an
	// interrupt, but a deadline keeps a slow or unreachable filer from blocking
	// close indefinitely. It also bounds each keepalive RPC.
	posixLockReleaseTimeout = 5 * time.Second
	// posixKeepaliveInterval renews the session lease well within the filer's
	// posixLockSessionTTL (15s) so a live mount is never reaped.
	posixKeepaliveInterval = 5 * time.Second
)

// posixLockHint records, per inode, which owners this mount has taken fcntl locks
// for. Flush consults it to force a synchronous close and route a release without
// an RPC on every close(). It is a superset hint: an extra sync flush or a no-op
// release RPC is harmless, a missed release is not — so it is added on grant and
// cleared on close (which drops the owner's POSIX locks per POSIX semantics).
type posixLockHint struct {
	mu sync.Mutex
	m  map[uint64]map[uint64]struct{}
}

func newPosixLockHint() *posixLockHint {
	return &posixLockHint{m: make(map[uint64]map[uint64]struct{})}
}

func (h *posixLockHint) add(inode, owner uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	owners := h.m[inode]
	if owners == nil {
		owners = make(map[uint64]struct{})
		h.m[inode] = owners
	}
	owners[owner] = struct{}{}
}

func (h *posixLockHint) has(inode, owner uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.m[inode][owner]
	return ok
}

func (h *posixLockHint) drop(inode, owner uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if owners := h.m[inode]; owners != nil {
		delete(owners, owner)
		if len(owners) == 0 {
			delete(h.m, inode)
		}
	}
}

// posixLockKeyForInode resolves a FUSE inode to its cluster-stable lock identity:
// the HardLinkId for a hardlinked file (so all names share one owner and table),
// else the path. POSIX locks are inode-scoped, so this must never be the FUSE
// NodeId (mount-local) for a multi-named inode.
func (wfs *WFS) posixLockKeyForInode(inode uint64) (string, bool) {
	path, status := wfs.inodeToPath.GetPath(inode)
	if status != fuse.OK {
		return "", false
	}
	if entry, _, st := wfs.maybeLoadEntry(path); st == fuse.OK && entry != nil && len(entry.HardLinkId) > 0 {
		return posixLockKeyPrefix + "hl:" + hex.EncodeToString(entry.HardLinkId), true
	}
	return posixLockKeyPrefix + string(path), true
}

func posixLockTypeToWire(typ uint32) uint32 {
	switch typ {
	case syscall.F_RDLCK:
		return posixlock.Read
	case syscall.F_WRLCK:
		return posixlock.Write
	default:
		return posixlock.Unlock
	}
}

func posixLockTypeFromWire(typ uint32) uint32 {
	switch typ {
	case posixlock.Read:
		return syscall.F_RDLCK
	case posixlock.Write:
		return syscall.F_WRLCK
	default:
		return syscall.F_UNLCK
	}
}

func (wfs *WFS) posixRangeFromLkIn(in *fuse.LkIn) posixlock.Range {
	return posixlock.Range{
		Start:   in.Lk.Start,
		End:     in.Lk.End,
		Type:    posixLockTypeToWire(in.Lk.Typ),
		Sid:     wfs.posixSid,
		Owner:   in.Owner,
		Pid:     in.Lk.Pid,
		IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
	}
}

// posixLockContext derives a context from the FUSE cancel channel so an
// interrupted lock syscall aborts its in-flight RPC. The caller must call the
// returned func to release the watcher goroutine.
func posixLockContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancelFn := context.WithCancel(context.Background())
	if cancel != nil {
		go func() {
			select {
			case <-cancel:
				cancelFn()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancelFn
}

func (wfs *WFS) callPosixLock(ctx context.Context, key string, op filer_pb.PosixLockOp, lk posixlock.Range) (*filer_pb.PosixLockResponse, error) {
	var resp *filer_pb.PosixLockResponse
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var e error
		resp, e = client.PosixLock(ctx, &filer_pb.PosixLockRequest{
			Key: key,
			Op:  op,
			Lock: &filer_pb.PosixLockRange{
				Start: lk.Start, End: lk.End, Type: lk.Type,
				Sid: lk.Sid, Owner: lk.Owner, Pid: lk.Pid, IsFlock: lk.IsFlock,
			},
		})
		return e
	})
	return resp, err
}

func (wfs *WFS) routedGetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	key, ok := wfs.posixLockKeyForInode(in.NodeId)
	if !ok {
		return fuse.EINVAL
	}
	ctx, done := posixLockContext(cancel)
	defer done()
	resp, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_GET_LK, wfs.posixRangeFromLkIn(in))
	if err != nil {
		glog.Warningf("routed GetLk %s: %v", key, err)
		return fuse.EIO
	}
	if resp.GetHasConflict() {
		c := resp.GetConflict()
		out.Lk.Start, out.Lk.End, out.Lk.Pid = c.GetStart(), c.GetEnd(), c.GetPid()
		out.Lk.Typ = posixLockTypeFromWire(c.GetType())
	} else {
		out.Lk.Typ = syscall.F_UNLCK
	}
	return fuse.OK
}

func (wfs *WFS) routedSetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	key, ok := wfs.posixLockKeyForInode(in.NodeId)
	if !ok {
		return fuse.EINVAL
	}
	lk := wfs.posixRangeFromLkIn(in)
	if lk.Type == posixlock.Unlock {
		return wfs.routedUnlock(key, lk)
	}
	ctx, done := posixLockContext(cancel)
	defer done()
	resp, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_TRY_LOCK, lk)
	if err != nil {
		glog.Warningf("routed SetLk %s: %v", key, err)
		return fuse.EIO
	}
	if !resp.GetGranted() {
		return fuse.EAGAIN
	}
	wfs.recordPosixGrant(key, in.NodeId, lk)
	return fuse.OK
}

func (wfs *WFS) routedSetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	key, ok := wfs.posixLockKeyForInode(in.NodeId)
	if !ok {
		return fuse.EINVAL
	}
	lk := wfs.posixRangeFromLkIn(in)
	if lk.Type == posixlock.Unlock {
		return wfs.routedUnlock(key, lk)
	}
	ctx, done := posixLockContext(cancel)
	defer done()
	status := posixPollAcquire(cancel, func() (bool, error) {
		resp, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_TRY_LOCK, lk)
		if err != nil {
			return false, err
		}
		return resp.GetGranted(), nil
	})
	if status == fuse.OK {
		wfs.recordPosixGrant(key, in.NodeId, lk)
	}
	return status
}

// recordPosixGrant notes a newly granted lock: the flush hint (fcntl only) and
// the own-lock mirror, which the keepalive re-asserts to the key's owner so the
// lease is renewed and the owner can rebuild after a takeover or restart.
func (wfs *WFS) recordPosixGrant(key string, inode uint64, lk posixlock.Range) {
	if !lk.IsFlock {
		wfs.posixHint.add(inode, lk.Owner)
	}
	wfs.posixOwn.Track(key, lk)
}

func (wfs *WFS) routedUnlock(key string, lk posixlock.Range) fuse.Status {
	// Drop the range from the mirror first so a keepalive re-assertion can't
	// resurrect it; if the RPC below fails, the next re-assertion reconciles the
	// owner to this (released) state anyway.
	wfs.posixOwn.Unlock(key, lk)
	// A release must complete even if the syscall was interrupted; cancelling it
	// would leak the lock on the owner filer. Bound it so a stuck filer can't
	// hang close() forever.
	ctx, cancel := context.WithTimeout(context.Background(), posixLockReleaseTimeout)
	defer cancel()
	if _, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_UNLOCK, lk); err != nil {
		glog.Warningf("routed unlock %s: %v", key, err)
		return fuse.EIO
	}
	return fuse.OK
}

// posixPollAcquire retries try with capped, jittered backoff until it is granted,
// the cancel channel fires (EINTR), or try errors (EIO). This is the client-side
// stand-in for a server-side wait queue.
func posixPollAcquire(cancel <-chan struct{}, try func() (bool, error)) fuse.Status {
	backoff := posixLockMinBackoff
	for {
		select {
		case <-cancel:
			return fuse.EINTR
		default:
		}
		granted, err := try()
		if err != nil {
			return fuse.EIO
		}
		if granted {
			return fuse.OK
		}
		timer := time.NewTimer(backoff + time.Duration(rand.Int64N(int64(backoff))))
		select {
		case <-cancel:
			timer.Stop()
			return fuse.EINTR
		case <-timer.C:
		}
		if backoff < posixLockMaxBackoff {
			if backoff *= 2; backoff > posixLockMaxBackoff {
				backoff = posixLockMaxBackoff
			}
		}
	}
}

func (wfs *WFS) routedReleasePosixOwner(inode, owner uint64) {
	if !wfs.posixHint.has(inode, owner) {
		return
	}
	key, ok := wfs.posixLockKeyForInode(inode)
	if !ok {
		return
	}
	wfs.posixOwn.ReleasePosixOwner(key, wfs.posixSid, owner)
	ctx, cancel := context.WithTimeout(context.Background(), posixLockReleaseTimeout)
	defer cancel()
	if _, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, posixlock.Range{Sid: wfs.posixSid, Owner: owner}); err != nil {
		// Keep the hint so a later flush retries the release; dropping it on a
		// transient failure would strand the lock until the owner filer's
		// session-lease reaping expires it.
		glog.Warningf("routed release posix owner %s: %v", key, err)
		return
	}
	wfs.posixHint.drop(inode, owner)
}

func (wfs *WFS) routedReleaseFlockOwner(inode, owner uint64) {
	key, ok := wfs.posixLockKeyForInode(inode)
	if !ok {
		return
	}
	wfs.posixOwn.ReleaseFlockOwner(key, wfs.posixSid, owner)
	ctx, cancel := context.WithTimeout(context.Background(), posixLockReleaseTimeout)
	defer cancel()
	if _, err := wfs.callPosixLock(ctx, key, filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER, posixlock.Range{Sid: wfs.posixSid, Owner: owner}); err != nil {
		glog.Warningf("routed release flock owner %s: %v", key, err)
	}
}

// crossMountLocks reports whether advisory locks are routed to the inode's owner
// filer rather than served from the per-mount local table. It tracks -dlm: lock
// coordination rides the same switch as whole-file write coordination, and is
// off under writeback cache (which implies single-writer, so lockClient is nil).
func (wfs *WFS) crossMountLocks() bool {
	return wfs.lockClient != nil
}

// releasePosixOwner / releaseFlockOwner / hasPosixOwner dispatch to the routed
// authority or the local table based on crossMountLocks, keeping the Flush and
// Release call sites flag-agnostic.

func (wfs *WFS) releasePosixOwner(inode, owner uint64) {
	if wfs.crossMountLocks() {
		wfs.routedReleasePosixOwner(inode, owner)
		return
	}
	wfs.posixLocks.ReleasePosixOwner(inode, owner)
}

func (wfs *WFS) releaseFlockOwner(inode, owner uint64) {
	if wfs.crossMountLocks() {
		wfs.routedReleaseFlockOwner(inode, owner)
		return
	}
	wfs.posixLocks.ReleaseFlockOwner(inode, owner)
}

func (wfs *WFS) hasPosixOwner(inode, owner uint64) bool {
	if wfs.crossMountLocks() {
		return owner != 0 && wfs.posixHint.has(inode, owner)
	}
	return wfs.posixLocks.HasPosixOwner(inode, owner)
}

// callPosixReassert sends the mount's held locks on key to the key's current
// owner filer as a KEEP_ALIVE, which renews the lease and lets the owner rebuild
// its in-memory state after a takeover or restart.
func (wfs *WFS) callPosixReassert(ctx context.Context, key string, locks []posixlock.Range) error {
	pbLocks := make([]*filer_pb.PosixLockRange, 0, len(locks))
	for _, l := range locks {
		pbLocks = append(pbLocks, &filer_pb.PosixLockRange{
			Start: l.Start, End: l.End, Type: l.Type,
			Sid: l.Sid, Owner: l.Owner, Pid: l.Pid, IsFlock: l.IsFlock,
		})
	}
	return wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, e := client.PosixLock(ctx, &filer_pb.PosixLockRequest{
			Key:   key,
			Op:    filer_pb.PosixLockOp_KEEP_ALIVE,
			Lock:  &filer_pb.PosixLockRange{Sid: wfs.posixSid},
			Locks: pbLocks,
		})
		return e
	})
}

// posixKeepaliveConcurrency bounds the parallel keepalive RPCs per tick. A mount
// holding locks on many inodes must renew every lease well within the filer TTL;
// dispatching them concurrently keeps the round trip from scaling with lock count.
const posixKeepaliveConcurrency = 32

// loopRenewPosixLeases re-asserts this mount's held locks to the owner filer of
// every key, which renews the session lease and rebuilds the owner's state after
// a ring change or owner restart. A dead mount stops re-asserting and the owners'
// sweepers reclaim its locks after the TTL.
func (wfs *WFS) loopRenewPosixLeases() {
	ticker := time.NewTicker(posixKeepaliveInterval)
	defer ticker.Stop()
	for range ticker.C {
		held := wfs.posixOwn.Snapshot()
		var wg sync.WaitGroup
		sem := make(chan struct{}, posixKeepaliveConcurrency)
		for key, locks := range held {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				// Bound each re-assertion so a stuck filer can't block wg.Wait and
				// stall the whole tick, which would let other keys' leases expire
				// and get reaped.
				ctx, cancel := context.WithTimeout(context.Background(), posixLockReleaseTimeout)
				defer cancel()
				if err := wfs.callPosixReassert(ctx, key, locks); err != nil {
					glog.V(2).Infof("posix reassert %s: %v", key, err)
				}
			}()
		}
		wg.Wait()
	}
}
