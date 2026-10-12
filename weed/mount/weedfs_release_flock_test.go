package mount

import (
	"math"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A close() frees the file description's flock locks even when the handle
// flush is stuck — e.g. a filer that stopped answering. If the locks outlive
// the blocked flush, a restarted process taking the same file's flock sees
// "file already locked" until the mount restarts.
func TestReleaseDropsFlockWhileFlushBlocked(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.posixLocks = NewPosixLockTable()
	startFakeFiler(t, wfs, &fakeFilerServer{})

	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/f"), time.Now().Unix(), false, false, 0, false)
	fh, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "f",
		Attributes: &filer_pb.FuseAttributes{},
	}, 0, 0)
	fh.dirtyMetadata = true

	const owner = uint64(42)
	if st := wfs.posixLocks.SetLk(inode, lockRange{Start: 0, End: math.MaxUint64, Typ: f_WRLCK, Owner: owner, IsFlock: true}); st != fuse.OK {
		t.Fatalf("take flock: %v", st)
	}

	// Stall the release flush at the handle lock flushMetadataToFiler waits on.
	active := wfs.fhLockTable.AcquireLock("test", fh.fh, util.ExclusiveLock)
	released := false
	releaseLock := func() {
		if !released {
			wfs.fhLockTable.ReleaseLock(fh.fh, active)
			released = true
		}
	}
	defer releaseLock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		wfs.Release(nil, &fuse.ReleaseIn{
			InHeader:     fuse.InHeader{NodeId: inode},
			Fh:           uint64(fh.fh),
			LockOwner:    owner,
			ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK,
		})
	}()

	// The dead holder's flock must be gone while the flush is still parked.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var out fuse.LkOut
		wfs.posixLocks.GetLk(inode, lockRange{Start: 0, End: math.MaxUint64, Typ: f_WRLCK, Owner: 7, IsFlock: true}, &out)
		if out.Lk.Typ == f_UNLCK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flock still held while the release flush is blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Let the parked flush through: the live filer answers the metadata update.
	releaseLock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("release never completed after the flush unblocked")
	}
}
