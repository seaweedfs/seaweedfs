package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The filer sends one event per moved entry, so children follow their parent.
func TestRemoteRenameMovesInodePaths(t *testing.T) {
	dir := util.FullPath("/images")
	wfs := newPagingWFS(t, dir, []string{"f.jpg"}, 0)

	now := time.Now().Unix()
	subInode := wfs.inodeToPath.Lookup(dir.Child("sub"), now, true, false, 0, true)
	childInode := wfs.inodeToPath.Lookup(dir.Child("sub").Child("f.jpg"), now, false, false, 0, true)

	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{
		Path:         dir.Child("sub"),
		RenamedTo:    dir.Child("moved"),
		WasDirectory: true,
	})
	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{
		Path:      dir.Child("sub").Child("f.jpg"),
		RenamedTo: dir.Child("moved").Child("f.jpg"),
	})

	if got, _ := wfs.inodeToPath.GetPath(subInode); got != dir.Child("moved") {
		t.Errorf("renamed directory resolves to %s, want %s", got, dir.Child("moved"))
	}
	if got, _ := wfs.inodeToPath.GetPath(childInode); got != dir.Child("moved").Child("f.jpg") {
		t.Errorf("child resolves to %s, want %s", got, dir.Child("moved").Child("f.jpg"))
	}
	if wfs.inodeToPath.HasPath(dir.Child("sub")) {
		t.Error("pre-rename directory path still in the table")
	}
}

// The handle path and the table move are two halves of one rename. Doing the
// move twice would unlink what the first one just put at the target, leaving
// the renamed inode with no path at all.
func TestRemoteRenameMovesOnceWithAnOpenHandle(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	oldPath, newPath := util.FullPath("/dir/file"), util.FullPath("/dir/renamed")

	inode := wfs.inodeToPath.Lookup(oldPath, time.Now().Unix(), false, false, 0, true)
	wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 88},
	}, 0, 0)

	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: oldPath, RenamedTo: newPath, TsNs: 1000})

	if got, status := wfs.inodeToPath.GetPath(inode); status != fuse.OK || got != newPath {
		t.Errorf("inode resolves to %q (%v), want %s", got, status, newPath)
	}
	if got, found := wfs.inodeToPath.GetInode(newPath); !found || got != inode {
		t.Errorf("%s resolves to %d (found %v), want %d", newPath, got, found, inode)
	}
}

// A rename over a file this mount has open destroys that file, even when the
// source was never visited here. The name must stop resolving to it, and its
// handle must not flush the old content back over what took its place.
func TestRemoteRenameOverAnOpenHandleUnlinksTheTarget(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	source, target := util.FullPath("/dir/unvisited"), util.FullPath("/dir/target")

	targetInode := wfs.inodeToPath.Lookup(target, time.Now().Unix(), false, false, 0, true)
	targetFh, _ := wfs.fhMap.AcquireFileHandle(wfs, targetInode, &filer_pb.Entry{
		Name:       "target",
		Attributes: &filer_pb.FuseAttributes{FileSize: 12},
	}, 0, 0)

	wfs.onEntryInvalidation(meta_cache.EntryInvalidation{Path: source, RenamedTo: target, TsNs: 1000})

	if got, found := wfs.inodeToPath.GetInode(target); found {
		t.Errorf("%s still resolves to %d after being renamed over", target, got)
	}
	if !targetFh.isDeleted {
		t.Error("the replaced file's handle can still flush over the renamed source")
	}
}
