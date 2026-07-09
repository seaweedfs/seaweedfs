package log_buffer

import (
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func addEntries(tb testing.TB, lb *LogBuffer, n int, payloadSize int, tag string) {
	payload := make([]byte, payloadSize)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("/%s/%d", tag, i))
		if err := lb.AddDataToBuffer(key, payload, time.Now().UnixNano()); err != nil {
			tb.Fatal(err)
		}
	}
}

// Sealed-window reads must hand every reader the same shared snapshot rather
// than a fresh copy each.
func TestSealedReadsShareOneSnapshot(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	addEntries(t, lb, 10, 1024, "a")
	lb.ForceFlush() // seal the window
	addEntries(t, lb, 1, 1024, "b")

	pos := NewMessagePosition(1, -2) // far in the past => sealed window hit
	buf1, _, pooled1, err1 := lb.ReadFromBuffer(pos)
	buf2, _, pooled2, err2 := lb.ReadFromBuffer(pos)
	if err1 != nil || err2 != nil {
		t.Fatalf("read errors: %v %v", err1, err2)
	}
	if buf1 == nil || buf2 == nil {
		t.Fatal("expected sealed data in memory")
	}
	if pooled1 || pooled2 {
		t.Fatalf("sealed reads should not be pooled copies: %v %v", pooled1, pooled2)
	}
	b1, b2 := buf1.Bytes(), buf2.Bytes()
	if len(b1) == 0 || len(b1) != len(b2) {
		t.Fatalf("unexpected lengths %d %d", len(b1), len(b2))
	}
	if &b1[0] != &b2[0] {
		t.Fatal("sealed reads made independent copies; expected one shared snapshot")
	}
}

// A shared snapshot must stay intact after its window rotates out and the
// backing array is recycled and overwritten by new writes.
func TestSharedSnapshotSurvivesRecycle(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	addEntries(t, lb, 5, 2048, "keep")
	lb.ForceFlush()

	pos := NewMessagePosition(1, -2)
	buf, _, pooled, err := lb.ReadFromBuffer(pos)
	if err != nil || buf == nil || pooled {
		t.Fatalf("expected shared sealed read, got buf=%v pooled=%v err=%v", buf != nil, pooled, err)
	}
	before := append([]byte(nil), buf.Bytes()...)

	// Rotate the sealed window all the way out so its array is recycled and
	// overwritten with different content.
	for i := 0; i < PreviousBufferCount+1; i++ {
		addEntries(t, lb, 5, 2048, "overwrite")
		lb.ForceFlush()
	}
	addEntries(t, lb, 5, 2048, "overwrite")

	if string(before) != string(buf.Bytes()) {
		t.Fatal("shared snapshot content changed after its window was recycled")
	}
}

// Race test: concurrent lagging readers against a writer that keeps sealing
// and recycling windows. Every delivered entry must unmarshal cleanly with the
// expected payload — corruption here means a reader saw recycled bytes.
func TestSharedSnapshotConcurrentIntegrity(t *testing.T) {
	// The writer below is deliberately unthrottled, so transient marshal garbage
	// scales with host speed; on linux/386 a fast runner can hit the 4GB
	// address-space ceiling. Cap the heap so the GC paces the writer instead.
	prevLimit := debug.SetMemoryLimit(1 << 30)
	defer debug.SetMemoryLimit(prevLimit)

	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	const payloadByte = 0x5A
	payload := make([]byte, 8*1024)
	for i := range payload {
		payload[i] = payloadByte
	}

	var stop sync.WaitGroup
	done := make(chan struct{})
	stop.Add(1)
	go func() { // writer: fill and seal aggressively so arrays recycle under the readers
		defer stop.Done()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := lb.AddDataToBuffer([]byte(fmt.Sprintf("/k/%d", i)), payload, time.Now().UnixNano()); err != nil {
				t.Error(err)
				return
			}
			if i%200 == 0 {
				lb.ForceFlush()
			}
		}
	}()

	var readers sync.WaitGroup
	errCh := make(chan error, 16)
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				start := NewMessagePosition(time.Now().Add(-500*time.Millisecond).UnixNano(), -2)
				lb.LoopProcessLogData(fmt.Sprintf("r%d", r), start, 0,
					func() bool { return time.Now().Before(deadline) },
					func(le *filer_pb.LogEntry) (bool, error) {
						if len(le.Data) != len(payload) {
							err := fmt.Errorf("payload length %d, want %d", len(le.Data), len(payload))
							select {
							case errCh <- err:
							default:
							}
							return true, err
						}
						for _, b := range le.Data {
							if b != payloadByte {
								err := fmt.Errorf("corrupted payload byte %x", b)
								select {
								case errCh <- err:
								default:
								}
								return true, err
							}
						}
						return false, nil
					})
			}
		}(r)
	}
	readers.Wait()
	close(done)
	stop.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

// Current-window reads must share the prefix snapshot: two behind-readers get
// views of the same backing array, and the view content must stay intact while
// the writer keeps appending and eventually seals and recycles the window.
func TestCurrentWindowPrefixSharing(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	addEntries(t, lb, 8, 1024, "cur")

	pos := NewMessagePosition(1, -2) // behind => full current-window read
	buf1, _, pooled1, err1 := lb.ReadFromBuffer(pos)
	buf2, _, pooled2, err2 := lb.ReadFromBuffer(pos)
	if err1 != nil || err2 != nil || buf1 == nil || buf2 == nil {
		t.Fatalf("reads: %v %v %v %v", buf1 != nil, err1, buf2 != nil, err2)
	}
	if pooled1 || pooled2 {
		t.Fatalf("current-window behind-reads should be shared, got pooled %v %v", pooled1, pooled2)
	}
	b1, b2 := buf1.Bytes(), buf2.Bytes()
	if len(b1) == 0 || len(b1) != len(b2) || &b1[0] != &b2[0] {
		t.Fatalf("expected one shared prefix snapshot, lens %d %d", len(b1), len(b2))
	}
	before := append([]byte(nil), b1...)

	// Keep appending: the prefix must be extended, not reallocated or mutated.
	addEntries(t, lb, 8, 1024, "more")
	buf3, _, _, err3 := lb.ReadFromBuffer(pos)
	if err3 != nil || buf3 == nil {
		t.Fatalf("read3: %v %v", buf3 != nil, err3)
	}
	if len(buf3.Bytes()) <= len(before) {
		t.Fatalf("extended read %d should exceed first read %d", len(buf3.Bytes()), len(before))
	}
	if string(buf3.Bytes()[:len(before)]) != string(before) {
		t.Fatal("prefix changed when the snapshot was extended")
	}

	// Seal + rotate the window fully out; earlier views must stay intact.
	for i := 0; i < PreviousBufferCount+1; i++ {
		lb.ForceFlush()
		addEntries(t, lb, 4, 1024, "rotate")
	}
	if string(b1) != string(before) {
		t.Fatal("shared current-window view changed after seal and recycle")
	}
}

// A sealed window whose prefix snapshot was fully extended must reuse it
// rather than re-copying on the first sealed read.
func TestSealHandsOffCompleteSnapshot(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	addEntries(t, lb, 6, 512, "h")
	pos := NewMessagePosition(1, -2)
	buf1, _, _, err := lb.ReadFromBuffer(pos) // extends prefix snapshot to full window
	if err != nil || buf1 == nil {
		t.Fatalf("read1: %v %v", buf1 != nil, err)
	}
	lb.ForceFlush()
	addEntries(t, lb, 1, 512, "next")

	buf2, _, pooled, err := lb.ReadFromBuffer(pos) // sealed read of the same window
	if err != nil || buf2 == nil || pooled {
		t.Fatalf("read2: buf=%v pooled=%v err=%v", buf2 != nil, pooled, err)
	}
	b1, b2 := buf1.Bytes(), buf2.Bytes()
	if len(b1) == 0 || len(b1) > len(b2) || &b1[0] != &b2[0] {
		t.Fatalf("sealed read should reuse the handed-off snapshot (lens %d %d)", len(b1), len(b2))
	}
}

// Sanity: sealed-read content must byte-match what a proto round-trip expects.
func TestSharedSnapshotContentMatches(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer lb.ShutdownLogBuffer()

	addEntries(t, lb, 3, 512, "x")
	lb.ForceFlush()

	buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(1, -2))
	if err != nil || buf == nil {
		t.Fatalf("read: buf=%v err=%v", buf != nil, err)
	}
	data := buf.Bytes()
	count := 0
	for pos := 0; pos+4 < len(data); {
		size, _, err := readTs(data, pos)
		if err != nil {
			t.Fatalf("entry %d: %v", count, err)
		}
		var le filer_pb.LogEntry
		if err := proto.Unmarshal(data[pos+4:pos+4+size], &le); err != nil {
			t.Fatalf("entry %d unmarshal: %v", count, err)
		}
		if want := fmt.Sprintf("/x/%d", count); string(le.Key) != want {
			t.Fatalf("entry %d key %q, want %q", count, le.Key, want)
		}
		pos += 4 + size
		count++
	}
	if count != 3 {
		t.Fatalf("read %d entries, want 3", count)
	}
}
