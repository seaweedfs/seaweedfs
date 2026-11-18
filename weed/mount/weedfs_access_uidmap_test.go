package mount

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newSwappedIdMapWFS mounts with a uid/gid map whose local ids are also
// mapped filer ids, and caches a directory and a file owned on the filer by
// 5000:6000, which is the local caller 123:456.
func newSwappedIdMapWFS(t *testing.T) (wfs *WFS, fileInode uint64) {
	t.Helper()
	wfs, _ = newCreateTestWFS(t)
	mapper, err := meta_cache.NewUidGidMapper("123:5000,5000:123", "456:6000,6000:456")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}
	*wfs.option.UidGidMapper = *mapper
	now := time.Now().Unix()
	for _, e := range []*filer_pb.Entry{
		{Name: "owned", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(0o700 | 1<<31), Uid: 5000, Gid: 6000, Mtime: now, Crtime: now}},
		{Name: "owned.txt", Attributes: &filer_pb.FuseAttributes{FileMode: 0o600, Uid: 5000, Gid: 6000, Mtime: now, Crtime: now}},
	} {
		if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", e), 0); err != nil {
			t.Fatalf("cache %s: %v", e.Name, err)
		}
	}
	fileInode = wfs.inodeToPath.Lookup(util.FullPath("/owned.txt"), now, false, false, 0, true)
	return wfs, fileInode
}

func TestPermissionChecksUseLocalOwnerUnderIdMap(t *testing.T) {
	wfs, fileInode := newSwappedIdMapWFS(t)
	caller := fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}}

	if st := wfs.Access(make(chan struct{}), &fuse.AccessIn{
		InHeader: fuse.InHeader{NodeId: fileInode, Caller: caller},
		Mask:     fuse.R_OK | fuse.W_OK,
	}); st != fuse.OK {
		t.Fatalf("Access by the owner = %v, want OK", st)
	}

	fh, _, st := wfs.AcquireHandle(context.Background(), fileInode, syscall.O_RDWR, 123, 456)
	if st != fuse.OK {
		t.Fatalf("open by the owner = %v, want OK", st)
	}
	wfs.ReleaseHandle(fh.fh)

	if _, _, st := wfs.createRegularFile(context.Background(), util.FullPath("/owned"), "new.txt", 0o600, 123, 456, 0, true, true); st != fuse.OK {
		t.Fatalf("create in the owner's directory = %v, want OK", st)
	}

	// A deferred create's cached placeholder answers for its owner too.
	placeholder, created, st := wfs.createRegularFile(context.Background(), util.FullPath("/"), "pending.txt", 0o600, 123, 456, 0, true, true)
	if st != fuse.OK {
		t.Fatalf("deferred create = %v, want OK", st)
	}
	if got := created.GetAttributes(); got.GetUid() != 123 || got.GetGid() != 456 {
		t.Fatalf("deferred create returned %d:%d, want local 123:456", got.GetUid(), got.GetGid())
	}
	wfs.inodeToPath.Lookup(util.FullPath("/pending.txt"), time.Now().Unix(), false, false, placeholder, true)
	if st := wfs.Access(make(chan struct{}), &fuse.AccessIn{
		InHeader: fuse.InHeader{NodeId: placeholder, Caller: caller},
		Mask:     fuse.R_OK | fuse.W_OK,
	}); st != fuse.OK {
		t.Fatalf("Access to a deferred create by its owner = %v, want OK", st)
	}
}
