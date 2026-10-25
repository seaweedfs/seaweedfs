package buffer_pool

import (
	"bytes"
	"testing"
)

// TestSyncPoolPutBuffer_DropsOversized is a regression guard for the volume-side
// retention amplifier in https://github.com/seaweedfs/seaweedfs/issues/6541.
//
// volume.PostHandler -> needle.ParseUpload -> bytes.Buffer.ReadFrom grows the
// buffer to chunk size. If we Put that buffer back as-is, sync.Pool keeps it
// at the grown capacity for the rest of the process's lifetime. With
// concurrent UploadPartCopy load that fills the pool with N × chunk-size
// backing arrays that never shrink — exactly the "RSS never recedes" pattern
// reported in the issue.
//
// We verify a Put + Get round trip can never round-trip a buffer larger than
// maxRetainedBufferCap, regardless of how big it grew while in use.
func TestSyncPoolPutBuffer_DropsOversized(t *testing.T) {
	// Drain the pool so we start from a deterministic point. sync.Pool may
	// still hold cached entries on other Ps, but for the small/big-cap
	// distinction below that doesn't matter — we assert an invariant on
	// every Get, not the identity of a specific buffer.
	for i := 0; i < 64; i++ {
		_ = SyncPoolGetBuffer()
	}

	big := &bytes.Buffer{}
	big.Grow(maxRetainedBufferCap * 4) // simulate a large upload buffer
	if got := big.Cap(); got <= maxRetainedBufferCap {
		t.Fatalf("test setup: big.Cap=%d should exceed threshold %d",
			got, maxRetainedBufferCap)
	}
	SyncPoolPutBuffer(big)

	// Any number of Gets must not return a buffer with cap > threshold.
	// (If big had been retained, sync.Pool's per-P cache would surface it
	// on the very next Get on this goroutine.)
	for i := 0; i < 16; i++ {
		got := SyncPoolGetBuffer()
		if cap := got.Cap(); cap > maxRetainedBufferCap {
			t.Fatalf("Get %d returned buffer with cap=%d, exceeds threshold %d "+
				"(regression: oversized buffers retained in pool?)",
				i, cap, maxRetainedBufferCap)
		}
	}
}

// TestSyncPoolPutBuffer_KeepsRightSized verifies the cap is one-sided: we
// still pool reasonably-sized buffers so the common case (small uploads,
// header parsing) doesn't pay an alloc per request.
func TestSyncPoolPutBuffer_KeepsRightSized(t *testing.T) {
	for i := 0; i < 64; i++ {
		_ = SyncPoolGetBuffer()
	}

	small := &bytes.Buffer{}
	small.Grow(maxRetainedBufferCap / 2)
	smallCap := small.Cap()
	SyncPoolPutBuffer(small)

	// We don't assert pointer identity (sync.Pool can hand back any cached
	// buffer), but the previously-Put buffer should appear among Gets on
	// the same goroutine in the absence of a GC. If our cap-policy ever
	// regresses to dropping right-sized buffers, this test starts seeing
	// only fresh (cap=0) buffers and fails.
	sawPooled := false
	for i := 0; i < 8; i++ {
		got := SyncPoolGetBuffer()
		if got.Cap() == smallCap {
			sawPooled = true
			break
		}
	}
	if !sawPooled {
		t.Fatalf("right-sized buffer (cap=%d) never came back from the pool "+
			"(regression: cap policy too aggressive?)", smallCap)
	}
}

// TestSyncPoolPutBuffer_NilSafe documents that Put tolerates a nil buffer.
// The volume server defers Put on a buffer obtained via Get, but defensive
// callers in other paths may Put(nil); we should not panic.
func TestSyncPoolPutBuffer_NilSafe(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("SyncPoolPutBuffer(nil) panicked: %v", r)
		}
	}()
	SyncPoolPutBuffer(nil)
}
