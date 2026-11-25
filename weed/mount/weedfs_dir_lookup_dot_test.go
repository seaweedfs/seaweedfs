package mount

import (
	"syscall"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newDotLookupTestWFS(root string, retain bool) *WFS {
	wfs := &WFS{
		option:      &Option{FilerMountRootPath: root},
		inodeToPath: NewInodeToPath(util.FullPath(root), 0),
	}
	if retain {
		wfs.inodeToPath.RetainForgotten(nfsExportForgottenInodes)
	}
	return wfs
}

func TestResolveDotNameForgottenInode(t *testing.T) {
	for _, retain := range []bool{false, true} {
		wfs := newDotLookupTestWFS("/buckets", retain)
		file := util.FullPath("/buckets/a/b/c.txt")
		inode := wfs.inodeToPath.Lookup(file, 1, false, false, 0, true)
		wfs.inodeToPath.Forget(inode, 1, nil, nil)

		path, gotInode, status := wfs.resolveDotName(inode, ".")
		if !retain {
			if status != fuse.Status(syscall.ESTALE) {
				t.Fatalf("without retention: status %v, want ESTALE", status)
			}
			continue
		}
		if status != fuse.OK || path != file || gotInode != inode {
			t.Fatalf("resolveDotName(.) = %q, %d, %v; want %q, %d, OK", path, gotInode, status, file, inode)
		}
		if again := wfs.inodeToPath.Lookup(file, 2, false, false, gotInode, true); again != inode {
			t.Fatalf("re-lookup gave inode %d, want the handle's %d", again, inode)
		}
		if _, found := wfs.inodeToPath.GetForgottenPath(inode); found {
			t.Fatalf("inode %d still retained after it is live again", inode)
		}
	}
}

func TestResolveDotNameForgottenPathTakenByAnotherInode(t *testing.T) {
	wfs := newDotLookupTestWFS("/", true)
	file := util.FullPath("/a.txt")
	inode := wfs.inodeToPath.Lookup(file, 1, false, false, 0, true)
	wfs.inodeToPath.Forget(inode, 1, nil, nil)
	if other := wfs.inodeToPath.Lookup(file, 1, false, false, inode+100, true); other == inode {
		t.Fatalf("test setup: path kept inode %d", inode)
	}

	if _, _, status := wfs.resolveDotName(inode, "."); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("status %v, want ESTALE", status)
	}
}

func TestResolveDotNameParent(t *testing.T) {
	wfs := newDotLookupTestWFS("/buckets", false)
	sub := wfs.inodeToPath.Lookup("/buckets/a/b", 1, true, false, 0, true)
	top := wfs.inodeToPath.Lookup("/buckets/a", 1, true, false, 0, true)

	for _, tc := range []struct {
		inode uint64
		want  util.FullPath
	}{
		{sub, "/buckets/a"},
		{top, "/buckets"},
		{1, "/buckets"},
	} {
		path, possibleInode, status := wfs.resolveDotName(tc.inode, "..")
		if status != fuse.OK || path != tc.want || possibleInode != 0 {
			t.Errorf("resolveDotName(%d, ..) = %q, %d, %v; want %q, 0, OK", tc.inode, path, possibleInode, status, tc.want)
		}
	}
}

func TestOutputRootEntryTakesARootLookup(t *testing.T) {
	wfs := newDotLookupTestWFS("/", false)
	var out fuse.EntryOut
	wfs.outputRootEntry(&out)
	if out.NodeId != 1 || out.Attr.Ino != 1 {
		t.Fatalf("root entry node %d ino %d, want 1", out.NodeId, out.Attr.Ino)
	}
	wfs.inodeToPath.Forget(1, 1, nil, nil)
	if !wfs.inodeToPath.HasInode(1) {
		t.Fatal("root dropped after forgetting the lookup outputRootEntry took")
	}
}
