//go:build !freebsd

package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestSetXAttrCopiesValueBuffer reproduces the bug from
// https://github.com/seaweedfs/seaweedfs/discussions/9275 where `cp -a`
// of a file with an extended attribute leaves the destination with a
// corrupted value. The root cause is that go-fuse hands SetXAttr a
// `data` slice that aliases the per-request input buffer; when the
// buffer is returned to the pool and reused for another FUSE request,
// any reference still held by entry.Extended sees garbage.
//
// The bug only manifested when the file was open during setxattr (the
// open-fh path defers persistence to flush). We simulate that by
// mutating the caller-supplied buffer after SetXAttr returns.
func TestSetXAttrCopiesValueBuffer(t *testing.T) {
	wfs := newCopyRangeTestWFS()

	path := util.FullPath("/aaa.txt")
	inode := wfs.inodeToPath.Lookup(path, 1, false, false, 0, true)
	fh := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name: "aaa.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			Inode:    inode,
		},
	})
	fh.RememberPath(path)

	// Caller buffer aliases a pool that the kernel will overwrite on
	// the very next request. SetXAttr must defensively copy.
	buf := []byte("test,in")
	status := wfs.SetXAttr(make(chan struct{}), &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: inode},
	}, "user.xtags", buf)
	if status != fuse.OK {
		t.Fatalf("SetXAttr status = %v, want OK", status)
	}

	// Simulate the request buffer being recycled and reused for an
	// unrelated payload.
	for i := range buf {
		buf[i] = 0xff
	}

	dest := make([]byte, 64)
	n, status := wfs.GetXAttr(make(chan struct{}), &fuse.InHeader{NodeId: inode}, "user.xtags", dest)
	if status != fuse.OK {
		t.Fatalf("GetXAttr status = %v, want OK", status)
	}
	if got := string(dest[:n]); got != "test,in" {
		t.Fatalf("GetXAttr value = %q, want %q (buffer aliasing leaked into stored xattr)", got, "test,in")
	}
}
