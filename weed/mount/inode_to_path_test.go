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

// The same rename can reach the table twice: once for an open handle and once
// for the invalidation behind it. The repeat must leave the moved inode alone
// rather than take apart what the first one put at the target.
func TestMovePathRepeatKeepsTheTarget(t *testing.T) {
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
