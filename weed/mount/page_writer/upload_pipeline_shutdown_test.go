package page_writer

import (
	"io"
	"testing"
)

// TestShutdown_ReleasesWritableReservation verifies that calling Shutdown
// on a pipeline with dirty-but-unsealed writable chunks returns those
// chunks' reservations to the accountant. Regression for the Destroy()
// path (truncate / metadata invalidation) which never flushes before
// dropping the pipeline, so writable chunks would otherwise leak their
// write-budget slots permanently.
func TestShutdown_ReleasesWritableReservation(t *testing.T) {
	acc := NewWriteBufferAccountant(1024 * 1024)
	saveFn := func(io.Reader, int64, int64, int64, func()) {}
	up := NewUploadPipeline(nil, 64*1024, saveFn, nil, 16, t.TempDir(), acc)

	// A small partial write lands in writableChunks as a mem chunk and
	// stays there (the chunk is not complete so maybeMoveToSealed is a
	// no-op) — exactly the state Destroy() can hit.
	buf := make([]byte, 128)
	if _, err := up.SaveDataAt(buf, 0, true, 1); err != nil {
		t.Fatalf("SaveDataAt: %v", err)
	}
	if got := acc.Used(); got != 64*1024 {
		t.Fatalf("expected 64KiB reserved after one write, got %d", got)
	}

	up.Shutdown()

	if got := acc.Used(); got != 0 {
		t.Fatalf("expected 0 used after Shutdown, got %d (writable reservation leaked)", got)
	}
}

// TestSealedChunk_FreeReferenceIsIdempotent verifies that FreeReference
// releases the accountant slot exactly once even if it is invoked more
// than once. This guards the path where Shutdown and the async uploader
// goroutine both call FreeReference on the same sealed chunk: the second
// call must be a no-op, not a double-release.
func TestSealedChunk_FreeReferenceIsIdempotent(t *testing.T) {
	acc := NewWriteBufferAccountant(1024 * 1024)
	acc.Reserve(64 * 1024)

	sc := &SealedChunk{
		chunk:            NewMemChunk(0, 64*1024),
		referenceCounter: 1,
		accountant:       acc,
		chunkSize:        64 * 1024,
	}

	sc.FreeReference("first")
	if got := acc.Used(); got != 0 {
		t.Fatalf("expected 0 used after first FreeReference, got %d", got)
	}
	sc.FreeReference("second")
	if got := acc.Used(); got != 0 {
		t.Fatalf("expected 0 used after second FreeReference, got %d (double release)", got)
	}
}
