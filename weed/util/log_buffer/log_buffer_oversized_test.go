package log_buffer

import (
	"testing"
	"time"
)

// An entry larger than BufferSize grows the window array to hold it. Window
// arrays cycle through SealBuffer rather than being freed, so the grown one has
// to be let go once it comes back around -- otherwise every later window is
// allocated and snapshotted at the oversized width.
func TestOversizedEntryDoesNotInflateBufferForever(t *testing.T) {
	lb := NewLogBuffer("oversized", time.Hour, func(_ *LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {}, nil, func() {})
	defer lb.ShutdownLogBuffer()

	if err := lb.AddDataToBuffer(nil, make([]byte, BufferSize+1), time.Now().UnixNano()); err != nil {
		t.Fatalf("add oversized entry: %v", err)
	}
	if len(lb.buf) <= BufferSize {
		t.Fatalf("oversized entry did not grow the buffer: %d bytes", len(lb.buf))
	}

	// One seal per window: the grown array lands in the newest sealed slot and
	// needs a full trip through the rotation to come back as the current buffer.
	for i := 0; i < PreviousBufferCount+2; i++ {
		lb.ForceFlush()
		if err := lb.AddDataToBuffer(nil, []byte("small"), time.Now().UnixNano()); err != nil {
			t.Fatalf("add small entry: %v", err)
		}
	}

	if len(lb.buf) > BufferSize {
		t.Errorf("current buffer still %d bytes, want at most %d", len(lb.buf), BufferSize)
	}
	for i, sealed := range lb.prevBuffers.buffers {
		if len(sealed.buf) > BufferSize {
			t.Errorf("sealed buffer %d still %d bytes, want at most %d", i, len(sealed.buf), BufferSize)
		}
	}
}

// The oversized entry itself still has to survive the round trip.
func TestOversizedEntryStillFlushes(t *testing.T) {
	var flushed [][]byte
	lb := NewLogBuffer("oversized-flush", time.Hour, func(_ *LogBuffer, _, _ time.Time, buf []byte, _, _ int64) {
		flushed = append(flushed, append([]byte(nil), buf...))
	}, nil, func() {})
	defer lb.ShutdownLogBuffer()

	payload := make([]byte, BufferSize+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := lb.AddDataToBuffer(nil, payload, time.Now().UnixNano()); err != nil {
		t.Fatalf("add oversized entry: %v", err)
	}
	lb.ForceFlush()

	var total int
	for _, buf := range flushed {
		total += len(buf)
	}
	if total < len(payload) {
		t.Errorf("flushed %d bytes, want at least the %d byte payload", total, len(payload))
	}
}
