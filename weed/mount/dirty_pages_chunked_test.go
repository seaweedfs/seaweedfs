package mount

import (
	"errors"
	"testing"
)

// Once a chunk upload fails its data is gone, so the handle must reject
// further writes instead of buffering the rest of the file — against a full
// cluster that turned cp into an hours-long crawl that only errored at close.
func TestChunkedDirtyPagesFailWritesAfterUploadError(t *testing.T) {
	fh := &FileHandle{wfs: &WFS{option: &Option{}}}
	pages := newMemoryChunkPages(fh, 1024)
	defer pages.Destroy()
	pages.hasWrites = true

	uploadErr := errors.New("assign volume failure: no writable volumes")
	pages.setLastError(uploadErr)

	if err := pages.AddPage(0, []byte("x"), true, 1); !errors.Is(err, uploadErr) {
		t.Fatalf("AddPage after upload failure = %v, want sticky %v", err, uploadErr)
	}
	if err := pages.FlushData(); !errors.Is(err, uploadErr) {
		t.Fatalf("FlushData after upload failure = %v, want wrapped %v", err, uploadErr)
	}
	// First failure wins; later errors must not mask the root cause.
	pages.setLastError(errors.New("later error"))
	if err := pages.LastError(); !errors.Is(err, uploadErr) {
		t.Fatalf("LastError = %v, want first error %v", err, uploadErr)
	}
}
