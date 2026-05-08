package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestFlushFileMetadataPreservesUserMtime is a regression test for issue #9363.
//
// Before the fix, flushFileMetadata stamped entry.Attributes.Mtime/Ctime with
// time.Now() on every flush, clobbering the value SetAttr stored on the entry
// when the user ran utimes()/touch -m -d while a file handle was still open.
//
// The flush has no business inventing timestamps: Write and SetAttr already
// maintain mtime/ctime on the entry, and the flush should just persist them.
//
// The test sets a user-chosen mtime far in the past, runs flushFileMetadata
// with no chunks (so it returns before the streamCreateEntry RPC), and asserts
// the entry's mtime is unchanged.
func TestFlushFileMetadataPreservesUserMtime(t *testing.T) {
	wfs := &WFS{
		inodeToPath:  NewInodeToPath(util.FullPath("/"), 0),
		fhLockTable:  util.NewLockTable[FileHandleId](),
		option:       &Option{},
	}

	const inode = uint64(42)
	fullPath := util.FullPath("/dir/sample.txt")
	wfs.inodeToPath.Lookup(fullPath, 1, false, false, inode, true)

	// Use a non-zero nanosecond so the *Ns assertions below catch a regression
	// that zeroes the field instead of preserving it.
	userMtime := time.Date(2020, 1, 15, 12, 34, 56, 123456789, time.UTC)
	entry := &filer_pb.Entry{
		Name: "sample.txt",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:   userMtime.Unix(),
			MtimeNs: int32(userMtime.Nanosecond()),
			Ctime:   userMtime.Unix(),
			CtimeNs: int32(userMtime.Nanosecond()),
		},
	}
	fh := &FileHandle{
		fh:            FileHandleId(1),
		inode:         inode,
		wfs:           wfs,
		entry:         &LockedEntry{Entry: entry},
		dirtyMetadata: true,
	}

	if err := wfs.flushFileMetadata(fh); err != nil {
		t.Fatalf("flushFileMetadata returned error: %v", err)
	}

	if got := entry.Attributes.Mtime; got != userMtime.Unix() {
		t.Errorf("mtime sec changed: got %d, want %d", got, userMtime.Unix())
	}
	if got := entry.Attributes.MtimeNs; got != int32(userMtime.Nanosecond()) {
		t.Errorf("mtime ns changed: got %d, want %d", got, userMtime.Nanosecond())
	}
	if got := entry.Attributes.Ctime; got != userMtime.Unix() {
		t.Errorf("ctime sec changed: got %d, want %d", got, userMtime.Unix())
	}
	if got := entry.Attributes.CtimeNs; got != int32(userMtime.Nanosecond()) {
		t.Errorf("ctime ns changed: got %d, want %d", got, userMtime.Nanosecond())
	}
}
