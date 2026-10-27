package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// GetLk queries for a conflicting lock on the file.
// If a conflict exists, the conflicting lock is returned in out.
// If no conflict, out.Lk.Typ is set to F_UNLCK.
func (wfs *WFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	if wfs.crossMountLocks() {
		return wfs.routedGetLk(cancel, in, out)
	}
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
	var status fuse.Status
	if wfs.crossMountLocks() {
		status = wfs.routedSetLk(cancel, in)
	} else {
		lk := lockRange{
			Start:   in.Lk.Start,
			End:     in.Lk.End,
			Typ:     in.Lk.Typ,
			Owner:   in.Owner,
			Pid:     in.Lk.Pid,
			IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
		}
		status = wfs.posixLocks.SetLk(in.NodeId, lk)
	}
	wfs.noteFhLockOwner(in, status)
	return status
}

// SetLkw sets a POSIX lock (blocking).
// Waits until the lock can be acquired or the request is cancelled.
func (wfs *WFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	var status fuse.Status
	if wfs.crossMountLocks() {
		status = wfs.routedSetLkw(cancel, in)
	} else {
		lk := lockRange{
			Start:   in.Lk.Start,
			End:     in.Lk.End,
			Typ:     in.Lk.Typ,
			Owner:   in.Owner,
			Pid:     in.Lk.Pid,
			IsFlock: in.LkFlags&fuse.FUSE_LK_FLOCK != 0,
		}
		if lk.Typ == f_UNLCK {
			status = wfs.posixLocks.SetLk(in.NodeId, lk)
		} else {
			status = wfs.posixLocks.SetLkw(in.NodeId, lk, cancel)
		}
	}
	wfs.noteFhLockOwner(in, status)
	return status
}

// noteFhLockOwner remembers a granted non-flock lock owner under the open it
// was taken on, so Release can drain every lock the file description held —
// the kernel sends no cleanup notice for OFD locks.
func (wfs *WFS) noteFhLockOwner(in *fuse.LkIn, status fuse.Status) {
	if status != fuse.OK || in.Lk.Typ == f_UNLCK || in.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		return
	}
	wfs.fhLocks.add(in.Fh, in.Owner)
}
