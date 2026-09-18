package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Write() stamps entry.Attributes.FileSize ahead of any chunk upload, so a
// periodic metadata flush could persist a size the stored chunks do not back;
// reads of the unbacked range then zero-fill, and if the mount dies before the
// close-time flush the gap stays committed-but-lost. clampCommittedFileSize
// caps the periodic flush at the offset the uploaded chunks actually cover,
// unless writes beyond that offset already left write-back pages.
func TestClampCommittedFileSize(t *testing.T) {
	newFixture := func(t *testing.T) *FileHandle {
		wfs := &WFS{
			option:      &Option{},
			inodeToPath: NewInodeToPath(util.FullPath("/"), 0),
			fhLockTable: util.NewLockTable[FileHandleId](),
		}
		const inode = uint64(42)
		fullPath := util.FullPath("/dir/sample.txt")
		wfs.inodeToPath.Lookup(fullPath, 1, false, false, inode, true)

		fh := &FileHandle{
			fh:    FileHandleId(1),
			inode: inode,
			wfs:   wfs,
			entry: &LockedEntry{Entry: &filer_pb.Entry{Name: "sample.txt"}},
		}
		fh.dirtyPages = newPageWriter(fh, 1<<20)
		return fh
	}

	addDirtyWrite := func(t *testing.T, fh *FileHandle, offset int64, size int) {
		t.Helper()
		if err := fh.dirtyPages.AddPage(offset, make([]byte, size), true, time.Now().UnixNano()); err != nil {
			t.Fatalf("AddPage: %v", err)
		}
	}

	uploadedChunk := &filer_pb.FileChunk{FileId: "1,01637037d6", Offset: 0, Size: 65536}

	t.Run("dirty pages beyond uploaded coverage get clamped", func(t *testing.T) {
		fh := newFixture(t)
		addDirtyWrite(t, fh, 65536, 10000)
		entry := &filer_pb.Entry{
			Name:       "sample.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 257484},
			Chunks:     []*filer_pb.FileChunk{uploadedChunk},
		}
		clampCommittedFileSize(fh, entry, 65536)
		if got := entry.Attributes.FileSize; got != 65536 {
			t.Errorf("FileSize = %d, want clamped to 65536", got)
		}
	})

	t.Run("sparse extension without dirty pages is preserved", func(t *testing.T) {
		fh := newFixture(t)
		entry := &filer_pb.Entry{
			Name:       "sample.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 1 << 20},
			Chunks:     []*filer_pb.FileChunk{uploadedChunk},
		}
		clampCommittedFileSize(fh, entry, 65536)
		if got := entry.Attributes.FileSize; got != 1<<20 {
			t.Errorf("FileSize = %d, want preserved 1<<20", got)
		}
	})

	t.Run("dirty pages within uploaded coverage are preserved", func(t *testing.T) {
		fh := newFixture(t)
		addDirtyWrite(t, fh, 0, 1000)
		entry := &filer_pb.Entry{
			Name:       "sample.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 100000},
			Chunks:     []*filer_pb.FileChunk{uploadedChunk},
		}
		clampCommittedFileSize(fh, entry, 65536)
		if got := entry.Attributes.FileSize; got != 100000 {
			t.Errorf("FileSize = %d, want preserved 100000", got)
		}
	})

	t.Run("fully uploaded size is preserved", func(t *testing.T) {
		fh := newFixture(t)
		entry := &filer_pb.Entry{
			Name:       "sample.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 65536},
			Chunks:     []*filer_pb.FileChunk{uploadedChunk},
		}
		clampCommittedFileSize(fh, entry, 65536)
		if got := entry.Attributes.FileSize; got != 65536 {
			t.Errorf("FileSize = %d, want preserved 65536", got)
		}
	})

	t.Run("remote entries are left alone", func(t *testing.T) {
		fh := newFixture(t)
		addDirtyWrite(t, fh, 65536, 10000)
		entry := &filer_pb.Entry{
			Name:       "sample.txt",
			Attributes: &filer_pb.FuseAttributes{FileSize: 257484},
			Chunks:     []*filer_pb.FileChunk{uploadedChunk},
			RemoteEntry: &filer_pb.RemoteEntry{
				RemoteSize: 257484,
			},
		}
		clampCommittedFileSize(fh, entry, 65536)
		if got := entry.Attributes.FileSize; got != 257484 {
			t.Errorf("FileSize = %d, want preserved 257484 for remote entry", got)
		}
	})

	t.Run("missing attributes are tolerated", func(t *testing.T) {
		fh := newFixture(t)
		entry := &filer_pb.Entry{Name: "sample.txt"}
		clampCommittedFileSize(fh, entry, 65536)
	})
}
