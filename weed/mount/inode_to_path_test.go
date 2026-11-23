package mount

import (
	"testing"
	"time"
	"unsafe"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A mount with millions of files is mostly these, so guard the size class.
func TestInodeEntryStaysInSizeClass(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("size classes are 64-bit specific")
	}
	if got := unsafe.Sizeof(InodeEntry{}); got != 32 {
		t.Errorf("sizeof(InodeEntry) = %d, want 32", got)
	}
}

func inodeEntryWith(paths ...util.FullPath) InodeEntry {
	var ie InodeEntry
	for _, p := range paths {
		ie.addPath(p)
	}
	return ie
}

func TestInodeEntry_removeOnePath(t *testing.T) {
	tests := []struct {
		name  string
		entry InodeEntry
		p     util.FullPath
		want  bool
		count int
	}{
		{
			name:  "actual case",
			entry: inodeEntryWith("/pjd/nx", "/pjd/n0"),
			p:     "/pjd/nx",
			want:  true,
			count: 1,
		},
		{
			name:  "empty",
			entry: InodeEntry{},
			p:     "x",
			want:  false,
			count: 0,
		},
		{
			name:  "single",
			entry: inodeEntryWith("/x"),
			p:     "/x",
			want:  true,
			count: 0,
		},
		{
			name:  "first",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/x",
			want:  true,
			count: 2,
		},
		{
			name:  "middle",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/y",
			want:  true,
			count: 2,
		},
		{
			name:  "last",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/z",
			want:  true,
			count: 2,
		},
		{
			name:  "not found",
			entry: inodeEntryWith("/x", "/y", "/z"),
			p:     "/t",
			want:  false,
			count: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.removeOnePath(tt.p); got != tt.want {
				t.Errorf("removeOnePath() = %v, want %v", got, tt.want)
			}
			left := tt.entry.appendPaths(nil)
			if tt.count != len(left) {
				t.Errorf("removeOnePath path count = %v, want %v", len(left), tt.count)
			}
			for i, p := range left {
				if p == tt.p {
					t.Errorf("removeOnePath found path still exists at %v, %v", i, p)
				}
			}
		})
	}
}

// Only directory inodes carry dirState; files never do. This keeps the file
// InodeEntry in the smaller size class, which is the memory win on large mounts.
func TestOnlyDirectoriesGetDirState(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 60)

	file := util.FullPath("/data/file.txt")
	fileInode := itp.Lookup(file, time.Now().Unix(), false, false, 0, true)
	if _, ok := itp.dirStates[fileInode]; ok {
		t.Fatal("file inode must not have a dirState")
	}
	// dir queries against a file are no-ops and must not register one
	itp.GetSubdirCount(file)
	itp.IsChildrenCached(file)
	itp.ShouldReadDirectoryDirect(file)
	itp.MarkChildrenCached(file)
	if _, ok := itp.dirStates[fileInode]; ok {
		t.Fatal("dir queries on a file inode must not create a dirState")
	}

	dir := util.FullPath("/data")
	dirInode := itp.Lookup(dir, time.Now().Unix(), true, false, 0, true)
	if _, ok := itp.dirStates[dirInode]; !ok {
		t.Fatal("directory inode should be registered in dirStates at creation")
	}

	// forgetting the directory drops its dirState
	itp.Forget(dirInode, 1, nil, nil)
	if _, ok := itp.dirStates[dirInode]; ok {
		t.Fatal("forgotten directory must be removed from dirStates")
	}
}

func TestMarkChildrenCachedClearsReadThroughMode(t *testing.T) {
	root := util.FullPath("/")
	dir := util.FullPath("/data")

	inodeToPath := NewInodeToPath(root, 60)
	inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)

	if !inodeToPath.MarkDirectoryReadThrough(dir, time.Now()) {
		t.Fatal("expected read-through flag to be set")
	}
	inodeToPath.MarkChildrenCached(dir)

	if !inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory should be cached after MarkChildrenCached")
	}
	if inodeToPath.ShouldReadDirectoryDirect(dir) {
		t.Fatal("directory should leave read-through mode after caching")
	}
}

// State keyed by an inode number has to be dropped while the table is locked.
// The numbers come from the path, so a lookup racing a forget is handed the
// same one back, and a cleanup running after the unlock would wipe what that
// lookup just stored.
func TestForgetOnReleaseRunsUnderLock(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 60)
	file := util.FullPath("/data/f.txt")
	inode := itp.Lookup(file, time.Now().Unix(), false, false, 0, true)

	itp.Lookup(file, time.Now().Unix(), false, false, 0, true) // nlookup is 2 now

	calls := 0
	onRelease := func(released uint64) {
		calls++
		if released != inode {
			t.Errorf("released inode = %d, want %d", released, inode)
		}
		if itp.TryLock() {
			itp.Unlock()
			t.Error("onRelease ran outside the critical section")
		}
	}

	itp.Forget(inode, 1, onRelease, nil)
	if calls != 0 {
		t.Fatalf("partial forget must not release: onRelease called %d times", calls)
	}

	itp.Forget(inode, 1, onRelease, nil)
	if calls != 1 {
		t.Fatalf("onRelease called %d times, want 1", calls)
	}
}

// A move with no source is not a move. Deciding that inside the lock is what
// keeps two invalidations racing on the same rename from unlinking each other's
// work.
func TestMovePathWithNoSourceLeavesTheTarget(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 0)
	inode := itp.Lookup("/a/f.txt", time.Now().Unix(), false, false, 0, true)

	itp.MovePath("/a/f.txt", "/a/g.txt")
	if sourceInode, targetInode := itp.MovePath("/a/f.txt", "/a/g.txt"); sourceInode != 0 || targetInode != 0 {
		t.Errorf("repeat move reported %d -> %d, want nothing moved", sourceInode, targetInode)
	}

	if got, status := itp.GetPath(inode); status != fuse.OK || got != "/a/g.txt" {
		t.Errorf("inode resolves to %q (%v), want /a/g.txt", got, status)
	}
	if got, found := itp.GetInode("/a/g.txt"); !found || got != inode {
		t.Errorf("/a/g.txt resolves to %d (found %v), want %d", got, found, inode)
	}
}

// A second hard link name found by a lookup belongs to the inode, so it is
// reported with the others and released by the final forget.
func TestLookupRecordsHardLinkNames(t *testing.T) {
	table := NewInodeToPath(util.FullPath("/"), 0)
	inode := table.Lookup("/a", 1, false, true, 42, true)
	if got := table.Lookup("/b", 1, false, true, 42, true); got != inode {
		t.Fatalf("second name got inode %d, want %d", got, inode)
	}
	table.Lookup("/b", 1, false, true, 42, true)
	if got := table.GetAllPaths(inode); len(got) != 2 || got[0] != "/a" || got[1] != "/b" {
		t.Fatalf("paths %v, want [/a /b]", got)
	}
	table.RemovePath("/a", nil)
	if got, status := table.GetPath(inode); status != fuse.OK || got != "/b" {
		t.Fatalf("after removing /a the inode answers to %q (%v), want /b", got, status)
	}
	table.Forget(inode, 3, nil, nil)
	if _, found := table.GetInode("/b"); found {
		t.Fatal("the final forget left /b mapped to a released inode")
	}
}

func TestRemoveAliasOfChecksOwnership(t *testing.T) {
	table := NewInodeToPath(util.FullPath("/"), 0)
	inode := table.Lookup("/a", 1, false, true, 42, true)
	if survivor, ok := table.RemoveAliasOf(inode, "/a"); ok || survivor != "" || !table.HasPath("/a") {
		t.Fatal("removed the only name")
	}
	table.AddPath(inode, "/b")
	if survivor, ok := table.RemoveAliasOf(inode+1, "/a"); ok || survivor != "" {
		t.Fatalf("removed another inode's name: survivor %q", survivor)
	}
	if paths := table.GetAllPaths(inode); len(paths) != 2 || paths[0] != "/a" || paths[1] != "/b" {
		t.Fatalf("ownership mismatch changed inode paths: %v", paths)
	}
	if got, found := table.GetInode("/a"); !found || got != inode {
		t.Fatalf("ownership mismatch changed /a mapping: %d (found %v)", got, found)
	}
	if survivor, ok := table.RemoveAliasOf(inode, "/a"); !ok || survivor != "/b" || table.HasPath("/a") {
		t.Fatalf("RemoveAliasOf(/a) = %q, %v, want /b", survivor, ok)
	}
	if survivor, ok := table.RemoveAliasOf(inode, "/a"); ok || survivor != "" {
		t.Fatal("removed an absent name")
	}
	if got, status := table.GetPath(inode); status != fuse.OK || got != "/b" {
		t.Fatalf("surviving path %q (%v), want /b", got, status)
	}
}

func TestRemoveAliasKeepsSoleName(t *testing.T) {
	table := NewInodeToPath(util.FullPath("/"), 0)
	inode := table.Lookup("/a", 1, false, true, 42, true)
	if _, ok := table.RemoveAlias("/a"); ok || !table.HasPath("/a") {
		t.Fatal("removed the only name")
	}
	table.AddPath(inode, "/b")
	if survivor, ok := table.RemoveAlias("/a"); !ok || survivor != "/b" || table.HasPath("/a") {
		t.Fatalf("RemoveAlias(/a) = %q, %v, want /b", survivor, ok)
	}
	if _, ok := table.RemoveAlias("/b"); ok || !table.HasPath("/b") {
		t.Fatal("removed the surviving name")
	}
}

// AllocateInode must return the first number no inode holds. It used to
// re-check the number it had just stepped off and return the next one
// unchecked, so a file created in the same second as a renamed one could store
// that file's inode number and stat as the same file (GNU mv refused with
// "are the same file").
func TestAllocateInodeReturnsFirstFreeNumber(t *testing.T) {
	itp := NewInodeToPath(util.FullPath("/"), 60)
	tmp := util.FullPath("/run/active.json.tmp")
	now := time.Now().Unix()
	a := tmp.AsInode(now)

	// a and a+2 are held, a+1 is free
	itp.Lookup("/run/x", now, false, false, a, true)
	itp.Lookup("/run/y", now, false, false, a+2, true)
	if got := itp.AllocateInode(tmp, now); got != a+1 {
		t.Fatalf("AllocateInode = a%+d, want a+1", int64(got-a))
	}

	// a, a+1 and a+2 are held
	itp.Lookup("/run/z", now, false, false, a+1, true)
	if got := itp.AllocateInode(tmp, now); got != a+3 {
		t.Fatalf("AllocateInode = a%+d, want a+3", int64(got-a))
	}
}
