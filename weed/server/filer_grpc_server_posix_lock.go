package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

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
		if c, granted := fs.posixLocks.TryLock(req.Key, lk); granted {
			resp.Granted = true
		} else {
			resp.HasConflict = true
			resp.Conflict = posixRangeToPb(c)
		}
	case filer_pb.PosixLockOp_UNLOCK:
		fs.posixLocks.Unlock(req.Key, lk)
	case filer_pb.PosixLockOp_GET_LK:
		if c, found := fs.posixLocks.GetLk(req.Key, lk); found {
			resp.HasConflict = true
			resp.Conflict = posixRangeToPb(c)
		}
	case filer_pb.PosixLockOp_RELEASE_POSIX_OWNER:
		fs.posixLocks.ReleasePosixOwner(req.Key, lk.Sid, lk.Owner)
	case filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER:
		fs.posixLocks.ReleaseFlockOwner(req.Key, lk.Sid, lk.Owner)
	default:
		return &filer_pb.PosixLockResponse{}, fmt.Errorf("unknown posix lock op %v", req.Op)
	}
	return resp, nil
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
