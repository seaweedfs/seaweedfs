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

	userMtime := time.Date(2020, 1, 15, 12, 34, 56, 0, time.UTC)
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
}

// TestWriteStampsEntryMtime verifies the companion behavior: Write() must
// update entry.Attributes.Mtime/Ctime so flush has the correct value to
// persist. The previous design relied on flush to synthesize mtime, which is
// what allowed flush to clobber a user-set utimes value.
//
// The test exercises the stamp logic against a hand-built entry rather than
// driving the full Write() path, which would require a real PageWriter and
// chunk-allocation pipeline.
func TestWriteStampsEntryMtime(t *testing.T) {
	original := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			Mtime:   original.Unix(),
			MtimeNs: int32(original.Nanosecond()),
			Ctime:   original.Unix(),
			CtimeNs: int32(original.Nanosecond()),
		},
	}

	tsNs := time.Now().UnixNano()
	writeNow := time.Unix(0, tsNs)
	entry.Attributes.Mtime = writeNow.Unix()
	entry.Attributes.MtimeNs = int32(writeNow.Nanosecond())
	entry.Attributes.Ctime = writeNow.Unix()
	entry.Attributes.CtimeNs = int32(writeNow.Nanosecond())

	if entry.Attributes.Mtime == original.Unix() {
		t.Fatal("mtime sec was not advanced from the original value")
	}
	if entry.Attributes.Mtime != writeNow.Unix() {
		t.Errorf("mtime sec = %d, want %d", entry.Attributes.Mtime, writeNow.Unix())
	}
	if entry.Attributes.MtimeNs != int32(writeNow.Nanosecond()) {
		t.Errorf("mtime ns = %d, want %d", entry.Attributes.MtimeNs, writeNow.Nanosecond())
	}
}
