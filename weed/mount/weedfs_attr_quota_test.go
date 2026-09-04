package mount

import (
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// TestSetAttrChargesTheGrowth pins the quota accounting for a truncate up: the
// writes that fill the range do not grow the file, so they charge nothing and
// the growth has to be counted where it happens, as Write and Fallocate do.
func TestSetAttrChargesTheGrowth(t *testing.T) {
	atomic.StoreInt64(&uncommittedBytes, 0)
	wfs, fh := newOpenFileHandle(t, 10)
	wfs.option.Quota = 100 << 20

	in := &fuse.SetAttrIn{}
	in.NodeId = fh.inode
	in.Valid = fuse.FATTR_SIZE
	in.Size = 8192
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr size up: got %v, want OK", status)
	}
	if got := wfs.GetUncommittedBytes(); got != 8192-10 {
		t.Fatalf("uncommitted bytes: got %d, want %d", got, 8192-10)
	}

	// Truncating back down frees space rather than consuming it.
	in.Size = 0
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr size down: got %v, want OK", status)
	}
	if got := wfs.GetUncommittedBytes(); got != 8192-10 {
		t.Fatalf("uncommitted bytes after a shrink: got %d, want %d", got, 8192-10)
	}
}
