package mount

import (
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// GetLk queries for a conflicting lock on the file.
// If a conflict exists, the conflicting lock is returned in out.
// If no conflict, out.Lk.Typ is set to F_UNLCK.
func (wfs *WFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	proposed := lockRange{
		Start:   in.Lk.Start,
		End:     in.Lk.End,
		Typ:     in.Lk.Typ,
		Owner:   in.Owner,
		Pid:     in.Lk.Pid,
		IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
	}
	wfs.posixLocks.GetLk(in.NodeId, proposed, out)
	return fuse.OK
}

// SetLk sets or clears a POSIX lock (non-blocking).
// Returns EAGAIN if the lock conflicts with an existing lock from another owner.
func (wfs *WFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	lk := lockRange{
		Start:   in.Lk.Start,
		End:     in.Lk.End,
		Typ:     in.Lk.Typ,
		Owner:   in.Owner,
		Pid:     in.Lk.Pid,
		IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
	}
	return wfs.posixLocks.SetLk(in.NodeId, lk)
}

// SetLkw sets a POSIX lock (blocking).
// Waits until the lock can be acquired or the request is cancelled.
func (wfs *WFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	lk := lockRange{
		Start:   in.Lk.Start,
		End:     in.Lk.End,
		Typ:     in.Lk.Typ,
		Owner:   in.Owner,
		Pid:     in.Lk.Pid,
		IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
	}
	if lk.Typ == syscall.F_UNLCK {
		return wfs.posixLocks.SetLk(in.NodeId, lk)
	}
	return wfs.posixLocks.SetLkw(in.NodeId, lk, cancel)
}
