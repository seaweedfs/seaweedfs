//go:build !freebsd

package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A file unlinked while open, before its deferred create reached the filer,
// must not be created by the periodic metadata flush.
func TestPeriodicFlushSkipsUnlinkedHandle(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	fake.mu.Lock()
	delete(fake.records, "tmp")
	fake.mu.Unlock()
	fh.baseEntry.Store(nil)
	wfs.markHandleDeleted(inode)
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	if err := wfs.flushFileMetadata(fh); err != nil {
		t.Fatalf("periodic flush: %v", err)
	}
	if fake.stored("tmp") != nil {
		t.Fatal("the periodic flush recreated the unlinked name")
	}
}
