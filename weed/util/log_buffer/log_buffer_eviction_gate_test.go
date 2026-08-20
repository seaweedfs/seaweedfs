package log_buffer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestEvictionWatermarkTracksSealedRing drives the watermark through the real
// eviction path instead of writing the field: only copyToFlushInternal sees the
// window about to be dropped, and it reads it one statement before SealBuffer
// shifts it out of slot 0. The buffer here never flushes, matching the
// aggregated meta ring, so the watermark is its only emptiness proof.
func TestEvictionWatermarkTracksSealedRing(t *testing.T) {
	lb := NewLogBuffer("evict-watermark", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Each timestamp is further past the previous window's start than the flush
	// interval, so every append seals the window before it.
	base := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
	step := 2 * time.Minute
	at := func(i int) time.Time { return base.Add(time.Duration(i) * step) }

	if got := lb.GetLastEvictedTsNs(); got != 0 {
		t.Fatalf("fresh buffer evicted through %v, want 0", time.Unix(0, got))
	}

	// PreviousBufferCount seals only fill the ring; the next one drops window 0.
	for i := 0; i <= PreviousBufferCount; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(i).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if got := lb.GetLastEvictedTsNs(); got != 0 {
			t.Fatalf("after %d appends evicted through %v, want nothing evicted yet", i+1, time.Unix(0, got))
		}
	}

	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(PreviousBufferCount + 1).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add evicting entry: %v", err)
	}
	// Window 0 held only the first entry, so its stopTime is that entry's ts.
	if got, want := lb.GetLastEvictedTsNs(), at(0).UnixNano(); got != want {
		t.Fatalf("evicted through %v, want the dropped window's stop %v", time.Unix(0, got), time.Unix(0, want))
	}

	// One more eviction advances the watermark; it never regresses.
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(PreviousBufferCount + 2).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add second evicting entry: %v", err)
	}
	if got, want := lb.GetLastEvictedTsNs(), at(1).UnixNano(); got != want {
		t.Fatalf("evicted through %v, want %v", time.Unix(0, got), time.Unix(0, want))
	}
}

// TestGapResumeCursorReadsSingleEntrySealedWindow pins the resume cursor
// against the shape that actually breaks: a sealed window holding one entry,
// where startTime == stopTime. The sealed-buffer lookup only enters a window
// whose stopTime is strictly after the cursor, so a cursor sitting exactly on
// earliest walks straight past such a window and its sole event is never
// delivered. Low-volume metadata windows are routinely one entry.
func TestGapResumeCursorReadsSingleEntrySealedWindow(t *testing.T) {
	lb := NewLogBuffer("gap-resume-sealed", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Timestamps far enough apart that every append seals the window before it,
	// so each sealed window holds exactly one entry.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * 2 * time.Minute).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	earliest := lb.GetEarliestTime()
	if earliest.IsZero() {
		t.Fatal("expected in-memory data")
	}

	// firstTsFrom reports the timestamp of the first entry a read hands back.
	firstTsFrom := func(cursorTsNs int64) int64 {
		buf, _, pooled, err := lb.ReadFromBuffer(NewMessagePosition(cursorTsNs, gapResumeTestOffset))
		if err != nil {
			t.Fatalf("read at %v: %v", time.Unix(0, cursorTsNs), err)
		}
		if buf == nil {
			t.Fatalf("read at %v returned no data", time.Unix(0, cursorTsNs))
		}
		if pooled {
			defer lb.ReleaseMemory(buf)
		}
		_, ts, readErr := readTs(buf.Bytes(), 0)
		if readErr != nil {
			t.Fatalf("decode first entry: %v", readErr)
		}
		return ts
	}

	// The resume the resolvers issue must deliver the earliest entry itself.
	if got := firstTsFrom(earliest.UnixNano() - 1); got != earliest.UnixNano() {
		t.Fatalf("resume just below earliest starts at %v, want the earliest entry %v",
			time.Unix(0, got), earliest)
	}
	// Resuming exactly on earliest is the shape that loses it.
	if got := firstTsFrom(earliest.UnixNano()); got == earliest.UnixNano() {
		t.Fatal("expected a cursor exactly on earliest to skip the single-entry window (precondition)")
	}
}

// gapResumeTestOffset mirrors the sentinel the filer's gap resume carries.
const gapResumeTestOffset = -2

// TestGapResumeCursorReadsFromMemory pins the contract the filer's gap resolver
// depends on: the cursor it resumes with must be one this read actually serves.
// ReadFromBuffer only falls through to memory for a cursor below the in-memory
// window when the offset is a sentinel, and answers ResumeFromDiskError for
// every positive one -- a resume carrying a positive offset would bounce
// straight back to the resolver, which sees no progress and parks a subscriber
// whose data is sitting in the ring.
func TestGapResumeCursorReadsFromMemory(t *testing.T) {
	lb := NewLogBuffer("gap-resume", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * time.Second).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	earliest := lb.GetEarliestTime()
	if earliest.IsZero() {
		t.Fatal("expected in-memory data")
	}

	// The resolver resumes at earliest itself, read inclusively.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano(), -2)); err != nil || buf == nil {
		t.Fatalf("resume at earliest: buf=%v err=%v", buf != nil, err)
	}
	// A sentinel just below it is served too, so the exact boundary is not load-bearing.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano()-1, -2)); err != nil || buf == nil {
		t.Fatalf("resume just below earliest: buf=%v err=%v", buf != nil, err)
	}
	// A positive offset below the window is refused: this is the shape a gap
	// resume must never take.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano()-1, 1)); err != ResumeFromDiskError {
		t.Fatalf("positive-offset cursor below the window: want ResumeFromDiskError, got %v", err)
	}
}

// TestFlushSubscriberContract pins what the filer's gap parks rest on: a flush
// subscriber is woken when a flush lands and the flush watermark is already
// visible at that moment - loopFlush stores lastFlushTsNs before notifying, so
// a waiter that re-checks the watermark on wake-up cannot miss the flush that
// woke it. Appends alone never signal this channel, and unregistering closes
// it so an abandoned waiter is not stranded.
func TestFlushSubscriberContract(t *testing.T) {
	flushed := make(chan struct{}, 16)
	// A flush interval far longer than the test, so appends never auto-seal a
	// window and ForceFlush is the only source of flushes: each round then has
	// exactly one flush, and the token received is known to belong to it.
	lb := NewLogBuffer("flush-sub", time.Hour,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			flushed <- struct{}{}
		}, nil, nil)
	defer lb.ShutdownLogBuffer()

	ch := lb.RegisterFlushSubscriber("s")

	// An append is not a flush: nothing may arrive on the channel. The probe
	// sits just below the first round timestamp: below, so no round entry gets
	// collision-bumped and its stored stopTime stays comparable to the local
	// value each round asserts against; just below, because a gap wider than
	// the flush interval would make round 0's own append auto-seal the probe
	// window - a second flush whose token throws every round off by one.
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: time.Now().Add(-time.Hour - time.Millisecond).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add: %v", err)
	}
	select {
	case <-ch:
		t.Fatal("an append must not wake a flush subscriber")
	case <-time.After(100 * time.Millisecond):
	}

	// A flush wakes it with the watermark already stored - the contract the gap
	// parks re-check on. The store-before-notify ordering itself is pinned by a
	// load-bearing comment at the loopFlush site (a reorder's window is
	// nanoseconds on that goroutine, untestable from here); these round-trips
	// verify the observable contract and catch a notify with no store at all.
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 8; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond).UnixNano()
		// The round entry must be above the buffer head: a bumped timestamp
		// flushes a stopTime far above the local ts compared below, making the
		// round's assertion vacuously true.
		if last := lb.LastTsNs.Load(); last >= ts {
			t.Fatalf("round %d: ts not above buffer head; the assertion would be vacuous", i)
		}
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: ts, Data: []byte("x"), Key: []byte("k")}); err != nil {
			t.Fatalf("add round %d: %v", i, err)
		}
		go lb.ForceFlush()
		select {
		case <-ch:
			if got := lb.GetLastFlushTsNs(); got < ts {
				t.Fatalf("round %d: woken with watermark %v short of the flushed entry %v; a waiter re-checking it on wake-up misses the flush that woke it",
					i, time.Unix(0, got), time.Unix(0, ts))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: a flush must wake the flush subscriber", i)
		}
		select {
		case <-flushed:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: flushFn did not run", i)
		}
	}

	// Unregistering closes the channel so an abandoned waiter unblocks.
	lb.UnregisterFlushSubscriber("s")
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unregister must close the channel, not send on it")
		}
	case <-time.After(time.Second):
		t.Fatal("unregister must close the channel")
	}

	// Unknown ids and double unregisters are harmless.
	lb.UnregisterFlushSubscriber("s")
	lb.UnregisterFlushSubscriber("never-registered")
}

// TestEvictionGatedCursor pins the gated sentinel: below the eviction watermark
// it is refused to disk under the read's own lock - the only place the check is
// atomic with the serve decision - while the plain -2 sentinel keeps master's
// serve-from-earliest behavior for the message queue's readers.
func TestEvictionGatedCursor(t *testing.T) {
	lb := NewLogBuffer("gated-cursor", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Entries far enough apart that every append seals the window before it;
	// wrapping the ring moves the watermark for real.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < PreviousBufferCount+2; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * 2 * time.Minute).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	evicted := lb.GetLastEvictedTsNs()
	if evicted == 0 {
		t.Fatal("precondition: the ring evicted nothing")
	}

	// Below the watermark: gated goes to disk, -2 keeps serving (MQ contract).
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evicted-1, EvictionGatedOffset)); err != ResumeFromDiskError {
		t.Fatalf("gated below watermark: want ResumeFromDiskError, got %v", err)
	}
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evicted-1, -2)); err != nil || buf == nil {
		t.Fatalf("plain sentinel below watermark: buf=%v err=%v, master behavior must hold", buf != nil, err)
	}
	// At and above the watermark the gated cursor serves normally.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evicted, EvictionGatedOffset)); err != nil || buf == nil {
		t.Fatalf("gated at watermark: buf=%v err=%v", buf != nil, err)
	}
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evicted+1, EvictionGatedOffset)); err != nil || buf == nil {
		t.Fatalf("gated above watermark: buf=%v err=%v", buf != nil, err)
	}
}

// TestMarkEvictedThroughGatesYoungRing pins the young-ring gate: a merge-fed
// ring is born empty while its sources hold history, and before its first real
// eviction a below-window gated cursor would otherwise be served the earliest
// retained entry - silently skipping the pre-subscription range that only disk
// holds. MarkEvictedThrough makes that range read as evicted from the start.
func TestMarkEvictedThroughGatesYoungRing(t *testing.T) {
	lb := NewLogBuffer("young-ring", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	seed := time.Now().Add(-time.Minute).UnixNano()
	lb.MarkEvictedThrough(seed)
	if got := lb.GetLastEvictedTsNs(); got != seed {
		t.Fatalf("evicted through %v, want the mark %v", time.Unix(0, got), time.Unix(0, seed))
	}
	if got := lb.GetLastEvictedOriginalTsNs(); got != seed {
		t.Fatalf("original watermark %v, want the mark %v", time.Unix(0, got), time.Unix(0, seed))
	}
	// A lower mark must not regress an established boundary.
	lb.MarkEvictedThrough(seed - int64(time.Hour))
	if got := lb.GetLastEvictedTsNs(); got != seed {
		t.Fatalf("lower mark regressed the watermark to %v", time.Unix(0, got))
	}

	// One post-mark arrival: the ring's window starts well above the mark.
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
		TsNs: time.Now().UnixNano(), Data: []byte("x"), Key: []byte("k"),
	}); err != nil {
		t.Fatal(err)
	}

	// Below the mark the gated cursor goes to disk instead of being handed
	// the earliest retained entry; the plain -2 sentinel keeps the inclusive
	// first-read contract.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(seed-1, EvictionGatedOffset)); err != ResumeFromDiskError {
		t.Fatalf("gated below the mark: want ResumeFromDiskError, got %v", err)
	}
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(seed-1, -2)); err != nil || buf == nil {
		t.Fatalf("plain sentinel below the mark: buf=%v err=%v", buf != nil, err)
	}
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(seed, EvictionGatedOffset)); err != nil || buf == nil {
		t.Fatalf("gated at the mark: buf=%v err=%v", buf != nil, err)
	}
}

// TestEvictionOriginalWatermark pins the second timestamp space. The ring bumps
// an out-of-order arrival past its head, so a bump-heavy interval (peer history
// replay) leaves stopTimes above anything on any peer's disk; a gap gate
// comparing disk cursors against those would park a subscriber that drained
// every peer's log. The original watermark tracks what was actually received.
func TestEvictionOriginalWatermark(t *testing.T) {
	lb := NewLogBuffer("orig-watermark", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Newest original first: it sets the ring head, so every later arrival is
	// bumped. Seals go by bumped time, which advances by nanoseconds, so each
	// window is sealed explicitly; wrapping the ring evicts.
	newest := time.Now().Add(-time.Hour).Truncate(time.Second).UnixNano()
	for i := 0; i < PreviousBufferCount+2; i++ {
		orig := newest - int64(i)*int64(time.Minute)
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: orig, Data: []byte("x"), Key: []byte("k")}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		lb.ForceFlush()
	}

	bumped := lb.GetLastEvictedTsNs()
	original := lb.GetLastEvictedOriginalTsNs()
	if bumped == 0 || original == 0 {
		t.Fatalf("precondition: nothing evicted (bumped=%d original=%d)", bumped, original)
	}
	if original > newest {
		t.Fatalf("original watermark %v exceeds the highest received timestamp %v", time.Unix(0, original), time.Unix(0, newest))
	}
	if bumped <= newest {
		t.Fatalf("bumped watermark %v not past the ring head %v: the spaces did not diverge", time.Unix(0, bumped), time.Unix(0, newest))
	}
	// The punchline: a disk cursor that drained every peer's log sits at the
	// highest received timestamp - clearing the original watermark while still
	// below the bumped one, which no disk timestamp can ever reach.
	diskHead := newest
	if diskHead < original {
		t.Fatalf("disk head %v short of the original watermark %v", time.Unix(0, diskHead), time.Unix(0, original))
	}
	if diskHead >= bumped {
		t.Fatal("disk head reached the bumped watermark; the gate would not have parked and this test proves nothing")
	}
}

// TestOriginalWatermarkWindowAttribution pins which window an entry's received
// timestamp is credited to. An append that rolls the window over seals the
// previous one first; crediting the incoming timestamp before that hands it to
// the sealed window (inflating its watermark: spurious parks) and loses it
// from the new one (deflating: gaps proven empty that are not).
func TestOriginalWatermarkWindowAttribution(t *testing.T) {
	lb := NewLogBuffer("orig-attribution", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	t1 := base.UnixNano()
	t2 := base.Add(2 * time.Minute).UnixNano() // past the flush interval: seals window A

	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: t1, Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add t1: %v", err)
	}
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: t2, Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add t2: %v", err)
	}

	sealed := lb.prevBuffers.buffers[len(lb.prevBuffers.buffers)-1]
	if sealed.size == 0 {
		t.Fatal("precondition: the second append did not seal the first window")
	}
	if got := sealed.maxOriginalTsNs; got != t1 {
		t.Fatalf("sealed window credited with %v, want its own entry %v (t2 belongs to the open window)", time.Unix(0, got), time.Unix(0, t1))
	}
	if got := lb.curWindowMaxOriginalTsNs; got != t2 {
		t.Fatalf("open window holds %v, want the entry that rolled it over %v", time.Unix(0, got), time.Unix(0, t2))
	}
}
