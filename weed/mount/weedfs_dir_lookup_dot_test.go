package mount

import (
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
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

func TestSameFileRejectsAReplacement(t *testing.T) {
	file := util.FullPath("/a.txt")
	pathInode := file.AsInode(1)
	for _, tc := range []struct {
		name  string
		inode uint64
		entry *filer.Entry
		want  bool
	}{
		{"same filer inode", 7, &filer.Entry{Attr: filer.Attr{Inode: 7}}, true},
		{"replaced, new filer inode", 7, &filer.Entry{Attr: filer.Attr{Inode: 8}}, false},
		{"path inode, same crtime", pathInode, &filer.Entry{Attr: filer.Attr{Crtime: time.Unix(1, 0)}}, true},
		{"path inode, recreated", pathInode, &filer.Entry{Attr: filer.Attr{Crtime: time.Unix(2, 0)}}, false},
		{"path inode, renamed to a filer inode", util.FullPath("/old.txt").AsInode(1), &filer.Entry{Attr: filer.Attr{Inode: pathInode, Crtime: time.Unix(1, 0)}}, false},
	} {
		if got := sameFile(tc.inode, file, tc.entry); got != tc.want {
			t.Errorf("%s: sameFile = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestLookupHandleLeavesAnotherInodesPath(t *testing.T) {
	table := NewInodeToPath("/", 0)
	table.RetainForgotten(nfsExportForgottenInodes)
	file := util.FullPath("/a.txt")
	inode := table.Lookup(file, 1, false, false, 0, true)
	table.Forget(inode, 1, nil, nil)
	other := table.Lookup(file, 2, false, false, inode+100, true)

	if table.LookupHandle(file, inode, false) {
		t.Fatal("LookupHandle took a path that names another inode")
	}
	if got, _ := table.GetInode(file); got != other {
		t.Fatalf("path maps to %d, want %d", got, other)
	}
	table.Forget(other, 1, nil, nil)
	if table.HasInode(other) {
		t.Fatal("LookupHandle took a reference on the other inode")
	}

	if !table.LookupHandle(file, inode, false) {
		t.Fatal("LookupHandle refused a free path")
	}
	if got, _ := table.GetInode(file); got != inode {
		t.Fatalf("path maps to %d, want the handle's %d", got, inode)
	}
	if _, found := table.GetForgottenPath(inode); found {
		t.Fatal("inode still retained after LookupHandle")
	}
	if !table.LookupHandle("/renamed.txt", inode, false) {
		t.Fatal("LookupHandle refused a live inode")
	}
	table.Forget(inode, 1, nil, nil)
	if !table.HasInode(inode) {
		t.Fatal("live inode lost the reference LookupHandle took")
	}
}

func TestMovePathMovesAForgottenInode(t *testing.T) {
	table := NewInodeToPath("/", 0)
	table.RetainForgotten(2)
	inode := table.Lookup("/a.txt", 1, false, false, 0, true)
	table.Forget(inode, 1, nil, nil)

	table.MovePath("/a.txt", "/b.txt")
	if path, _ := table.GetForgottenPath(inode); path != "/b.txt" {
		t.Fatalf("retained path %q, want /b.txt", path)
	}

	for _, p := range []util.FullPath{"/c.txt", "/d.txt"} {
		table.Forget(table.Lookup(p, 1, false, false, 0, true), 1, nil, nil)
	}
	if _, found := table.GetForgottenPath(inode); found {
		t.Fatal("inode still retained past the bound")
	}
	if len(table.forgottenInodes) != 2 {
		t.Fatalf("%d paths indexed, want 2", len(table.forgottenInodes))
	}
}

func TestLookupHandleRefusesAPathRenamedAway(t *testing.T) {
	table := NewInodeToPath("/", 0)
	table.RetainForgotten(nfsExportForgottenInodes)
	inode := table.Lookup("/a.txt", 1, false, false, 0, true)
	table.Forget(inode, 1, nil, nil)
	table.MovePath("/a.txt", "/b.txt")

	if table.LookupHandle("/a.txt", inode, false) {
		t.Fatal("LookupHandle registered the path the inode was renamed away from")
	}
	if path, _ := table.GetForgottenPath(inode); path != "/b.txt" {
		t.Fatalf("retained path %q, want /b.txt", path)
	}
	if !table.LookupHandle("/b.txt", inode, false) {
		t.Fatal("LookupHandle refused the retained path")
	}
}

func TestMovePathOfAForgottenInodeDropsTheReplacedTarget(t *testing.T) {
	table := NewInodeToPath("/", 0)
	table.RetainForgotten(nfsExportForgottenInodes)
	inode := table.Lookup("/a.txt", 1, false, false, 0, true)
	table.Forget(inode, 1, nil, nil)
	replaced := table.Lookup("/b.txt", 1, false, false, 0, true)

	if source, target := table.MovePath("/a.txt", "/b.txt"); source != 0 || target != replaced {
		t.Fatalf("MovePath = %d, %d; want 0, %d", source, target, replaced)
	}
	if _, found := table.GetInode("/b.txt"); found {
		t.Fatal("replaced inode still mapped at the target")
	}
	if !table.LookupHandle("/b.txt", inode, false) {
		t.Fatal("LookupHandle refused the moved inode at the target")
	}

	if source, target := table.MovePath("/a.txt", "/b.txt"); source != 0 || target != 0 {
		t.Fatalf("redelivered MovePath = %d, %d; want 0, 0", source, target)
	}
	if got, _ := table.GetInode("/b.txt"); got != inode {
		t.Fatalf("redelivery took the target from inode %d (now %d)", inode, got)
	}
}
