package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestFileHandleFullPathFallsBackAfterForget(t *testing.T) {
	wfs := &WFS{
		inodeToPath: NewInodeToPath(util.FullPath("/"), 0),
	}

	fullPath := util.FullPath("/worker_0/subdir_0/test.txt")
	inode := wfs.inodeToPath.Lookup(fullPath, 1, false, false, 0, true)

	fh := &FileHandle{
		inode: inode,
		wfs:   wfs,
	}
	fh.RememberPath(fullPath)

	wfs.inodeToPath.Forget(inode, 1, nil)

	if got := fh.FullPath(); got != fullPath {
		t.Fatalf("FullPath() after forget = %q, want %q", got, fullPath)
	}
}

func TestFileHandleFullPathUsesSavedRenamePathAfterForget(t *testing.T) {
	wfs := &WFS{
		inodeToPath: NewInodeToPath(util.FullPath("/"), 0),
	}

	oldPath := util.FullPath("/worker_0/subdir_0/test.txt")
	newPath := util.FullPath("/worker_0/subdir_1/test.txt")
	inode := wfs.inodeToPath.Lookup(oldPath, 1, false, false, 0, true)

	fh := &FileHandle{
		inode: inode,
		wfs:   wfs,
	}
	fh.RememberPath(oldPath)

	wfs.inodeToPath.MovePath(oldPath, newPath)
	fh.RememberPath(newPath)
	wfs.inodeToPath.Forget(inode, 1, nil)

	if got := fh.FullPath(); got != newPath {
		t.Fatalf("FullPath() after rename+forget = %q, want %q", got, newPath)
	}
}
