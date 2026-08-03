package weed_server

// End-to-end tests for the metadata subscribe loops. The unit tests in this
// package pin individual helpers; every escaped bug across this PR's review
// rounds lived in the interactions - the loop state machine, the disk/memory
// handoff, and the server/client contract. These tests run the real
// SubscribeLocalMetadata loop against a real filer store, with only the volume
// layer faked, and assert the delivered stream itself: exactly the written
// events, in order, no duplicates from the entry path, and the chunk-mode
// marker never claiming more than the real client code applies.

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// ---- store configuration ----

type testConfig map[string]string

func (c testConfig) GetString(key string) string          { return c[key] }
func (c testConfig) GetBool(key string) bool              { return false }
func (c testConfig) GetInt(key string) int                { return 0 }
func (c testConfig) GetStringSlice(key string) []string   { return nil }
func (c testConfig) SetDefault(key string, v interface{}) {}

// ---- fake gRPC stream ----

type fakeSubscribeStream struct {
	ctx  context.Context
	mu   sync.Mutex
	msgs []*filer_pb.SubscribeMetadataResponse
}

func (s *fakeSubscribeStream) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, resp)
	return nil
}

func (s *fakeSubscribeStream) snapshot() []*filer_pb.SubscribeMetadataResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*filer_pb.SubscribeMetadataResponse(nil), s.msgs...)
}

func (s *fakeSubscribeStream) Context() context.Context     { return s.ctx }
func (s *fakeSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *fakeSubscribeStream) SendMsg(m interface{}) error  { return nil }
func (s *fakeSubscribeStream) RecvMsg(m interface{}) error  { return nil }

// ---- fake volume layer ----

type fakeLogVolumes struct {
	mu     sync.Mutex
	bytes  map[string][]byte // chunk fileId -> raw log bytes (size-prefixed entries)
	dead   map[string]bool
	nextId int
}

func newFakeLogVolumes() *fakeLogVolumes {
	return &fakeLogVolumes{bytes: make(map[string][]byte), dead: make(map[string]bool)}
}

func (v *fakeLogVolumes) put(data []byte) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.nextId++
	id := fmt.Sprintf("t,%d", v.nextId)
	v.bytes[id] = data
	return id
}

func (v *fakeLogVolumes) kill(fileId string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.dead[fileId] = true
}

// notFoundErr matches both the server's and the client's missing-chunk
// predicates, like a real dead log volume does.
func notFoundErr(fileId string) error { return fmt.Errorf("read %s: volume 42 not found", fileId) }

func (v *fakeLogVolumes) get(fileId string) ([]byte, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.dead[fileId] {
		return nil, notFoundErr(fileId)
	}
	data, found := v.bytes[fileId]
	if !found {
		return nil, notFoundErr(fileId)
	}
	return data, nil
}

func decodeLogBytes(data []byte) ([]*filer_pb.LogEntry, error) {
	var entries []*filer_pb.LogEntry
	for pos := 0; pos+4 <= len(data); {
		size := int(util.BytesToUint32(data[pos : pos+4]))
		if pos+4+size > len(data) {
			break // torn tail
		}
		entry := &filer_pb.LogEntry{}
		if err := entry.UnmarshalVT(data[pos+4 : pos+4+size]); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		pos += 4 + size
	}
	return entries, nil
}

// chunkStreamReader mimics the client's whole-file byte stream: sequential,
// erroring at the first dead chunk.
type chunkStreamReader struct {
	vol    *fakeLogVolumes
	chunks []*filer_pb.FileChunk
	buf    []byte
	idx    int
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.idx >= len(r.chunks) {
			return 0, io.EOF
		}
		data, err := r.vol.get(r.chunks[r.idx].GetFileIdString())
		if err != nil {
			return 0, err
		}
		r.buf = data
		r.idx++
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *chunkStreamReader) Close() error { return nil }

// ---- the harness ----

type subscribeHarness struct {
	t    *testing.T
	f    *filer.Filer
	fs   *FilerServer
	vol  *fakeLogVolumes
	base int64 // fixed timestamp origin; a per-call time.Now() shifts across second boundaries mid-test

	// flushGate, when non-nil, blocks the flush function - the "volume outage
	// stalls the metadata log flush" state the whole PR exists to handle.
	gateMu    sync.Mutex
	flushGate chan struct{}
}

const testFilerIdSuffix = "0000abcd"

func newSubscribeHarness(t *testing.T) *subscribeHarness {
	// Shrink the timing knobs so parks and retries run at test speed.
	prevRetry, prevWarn, prevStall := unflushedGapRetryInterval, gapStallWarnInterval, maxGapStall
	unflushedGapRetryInterval, gapStallWarnInterval, maxGapStall = 30*time.Millisecond, 200*time.Millisecond, time.Hour
	t.Cleanup(func() {
		unflushedGapRetryInterval, gapStallWarnInterval, maxGapStall = prevRetry, prevWarn, prevStall
	})

	f := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &leveldb.LevelDBStore{}
	if err := store.Initialize(testConfig{"test.dir": t.TempDir()}, "test."); err != nil {
		t.Fatalf("init store: %v", err)
	}
	f.SetStore(store)

	vol := newFakeLogVolumes()
	restore := filer.SetLogReadHooksForTesting(
		func(chunk *filer_pb.FileChunk) ([]*filer_pb.LogEntry, error) {
			data, err := vol.get(chunk.GetFileIdString())
			if err != nil {
				return nil, err
			}
			return decodeLogBytes(data)
		},
		func(chunks []*filer_pb.FileChunk) io.Reader {
			return &chunkStreamReader{vol: vol, chunks: chunks}
		},
		func(fileId string) error {
			_, err := vol.get(fileId)
			return err
		},
	)
	t.Cleanup(restore)

	h := &subscribeHarness{t: t, f: f, vol: vol,
		base: time.Now().Add(-time.Hour).Truncate(time.Second).UnixNano()}

	// The real buffer, with the flush function writing through the fake volume
	// layer the way logFlushFunc writes through real volumes.
	f.LocalMetaLogBuffer.ShutdownLogBuffer()
	f.LocalMetaLogBuffer = log_buffer.NewLogBuffer("local", time.Minute, h.flushToStore, nil, nil)
	t.Cleanup(f.LocalMetaLogBuffer.ShutdownLogBuffer)

	h.fs = &FilerServer{
		filer:          f,
		option:         &FilerOption{Host: pb.ServerAddress("test:8888")},
		knownListeners: make(map[int32]int32),
	}
	return h
}

func (h *subscribeHarness) blockFlushes() {
	h.gateMu.Lock()
	defer h.gateMu.Unlock()
	if h.flushGate == nil {
		h.flushGate = make(chan struct{})
	}
}

func (h *subscribeHarness) releaseFlushes() {
	h.gateMu.Lock()
	defer h.gateMu.Unlock()
	if h.flushGate != nil {
		close(h.flushGate)
		h.flushGate = nil
	}
}

func (h *subscribeHarness) flushToStore(lb *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
	h.gateMu.Lock()
	gate := h.flushGate
	h.gateMu.Unlock()
	if gate != nil {
		<-gate
	}

	// The same file naming and append shape as logFlushFunc, against the fake
	// volumes: one chunk per flushed window, named for the window start minute.
	startTime, stopTime = startTime.UTC(), stopTime.UTC()
	targetFile := fmt.Sprintf("%s/%04d-%02d-%02d/%02d-%02d.%s", filer.SystemLogDir,
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(), testFilerIdSuffix)
	data := append([]byte(nil), buf...)
	fileId := h.vol.put(data)

	ctx := context.Background()
	fullpath := util.FullPath(targetFile)
	entry, err := h.f.FindEntry(ctx, fullpath)
	var offset int64
	if err == filer_pb.ErrNotFound {
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr:     filer.Attr{Crtime: time.Now(), Mtime: time.Now(), Mode: 0644},
		}
	} else if err != nil {
		h.t.Errorf("find %s: %v", targetFile, err)
		return
	} else {
		offset = int64(filer.TotalSize(entry.GetChunks()))
	}
	entry.Chunks = append(entry.GetChunks(), &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       offset,
		Size:         uint64(len(data)),
		ModifiedTsNs: time.Now().UnixNano(),
	})
	if err := h.f.CreateEntry(ctx, entry, nil, false, false, nil, false, 255); err != nil {
		h.t.Errorf("write log file %s: %v", targetFile, err)
	}
}

// event builds a metadata event log entry the way the filer's notification
// path does, so the loop's real decode and filter code runs.
func testEvent(tsNs int64, name string) *filer_pb.LogEntry {
	data, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{
		Directory:         "/t",
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: name}},
		TsNs:              tsNs,
	})
	if err != nil {
		panic(err)
	}
	return &filer_pb.LogEntry{TsNs: tsNs, Data: data, Key: []byte("/t/" + name)}
}

func (h *subscribeHarness) append(tsNs int64) {
	if err := h.f.LocalMetaLogBuffer.AddLogEntryToBuffer(testEvent(tsNs, fmt.Sprintf("f-%d", tsNs))); err != nil {
		h.t.Fatalf("append: %v", err)
	}
}

type runningSubscribe struct {
	stream   *fakeSubscribeStream
	cancel   context.CancelFunc
	done     chan error
	finished chan struct{} // closed after done is populated; safe to wait repeatedly
}

func (h *subscribeHarness) subscribe(sinceNs int64, mutate func(*filer_pb.SubscribeMetadataRequest)) *runningSubscribe {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &fakeSubscribeStream{ctx: ctx}
	req := &filer_pb.SubscribeMetadataRequest{
		ClientName:  "loop-test",
		ClientId:    7,
		ClientEpoch: 1,
		SinceNs:     sinceNs,
	}
	if mutate != nil {
		mutate(req)
	}
	r := &runningSubscribe{stream: stream, cancel: cancel, done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		r.done <- h.fs.SubscribeLocalMetadata(req, stream)
		close(r.finished)
	}()
	h.t.Cleanup(func() {
		cancel()
		select {
		case <-r.finished:
		case <-time.After(5 * time.Second):
			h.t.Error("subscribe loop did not exit on cancel")
		}
	})
	return r
}

// eventTimestamps extracts delivered metadata events (markers, heartbeats and
// refs excluded).
func eventTimestamps(msgs []*filer_pb.SubscribeMetadataResponse) []int64 {
	var out []int64
	for _, m := range msgs {
		if len(m.LogFileRefs) > 0 || m.EventNotification == nil || m.EventNotification.NewEntry == nil {
			continue
		}
		out = append(out, m.TsNs)
	}
	return out
}

func waitForEvents(t *testing.T, r *runningSubscribe, want []int64, timeout time.Duration) []int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []int64
	for time.Now().Before(deadline) {
		got = eventTimestamps(r.stream.snapshot())
		if len(got) >= len(want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	return got
}

func assertNoEventsFor(t *testing.T, r *runningSubscribe, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	if got := eventTimestamps(r.stream.snapshot()); len(got) > 0 {
		t.Fatalf("delivered %v while the gap was unproven; these events must wait", got)
	}
}

// tsAt returns test timestamps from the harness's fixed base: old enough that
// windows seal on the jump between them, entries 1ms apart within a window.
func (h *subscribeHarness) tsAt(window, i int) int64 {
	return h.base + int64(window)*int64(2*time.Minute) + int64(i)*int64(time.Millisecond)
}

// ---- scenarios ----

// The headline behavior of the whole PR: events evicted from the ring before
// their flush landed must not be skipped. The subscriber parks while the gap
// is unproven and delivers everything once the stalled flush lands.
func TestSubscribeLoop_EvictedUnflushedGapWaitsThenDelivers(t *testing.T) {
	h := newSubscribeHarness(t)
	h.blockFlushes()

	var want []int64
	for w := 0; w < log_buffer.PreviousBufferCount+3; w++ {
		ts := h.tsAt(w, 0)
		want = append(want, ts)
		h.append(ts)
	}
	if h.f.LocalMetaLogBuffer.GetLastEvictedTsNs() == 0 {
		t.Fatal("precondition: the ring evicted nothing")
	}

	r := h.subscribe(0, nil)
	// The evicted windows are nowhere: not in memory, not on disk. Master
	// silently skipped them here; the loop must park instead.
	assertNoEventsFor(t, r, 300*time.Millisecond)

	h.releaseFlushes()
	waitForEvents(t, r, want, 5*time.Second)
}

// A gap that never existed is proven empty and served from memory promptly -
// the guard must not park subscribers on rings that evicted nothing.
func TestSubscribeLoop_NothingEvictedServesFromMemory(t *testing.T) {
	h := newSubscribeHarness(t)
	h.blockFlushes() // no disk at all; memory alone must serve

	want := []int64{h.tsAt(0, 0), h.tsAt(0, 1), h.tsAt(0, 2)}
	for _, ts := range want {
		h.append(ts)
	}
	r := h.subscribe(0, nil)
	waitForEvents(t, r, want, 3*time.Second)
}

// Disk backlog then live tail: the handoff must deliver every event exactly
// once, in order. Timestamps are 1ms-adjacent ACROSS every boundary - window
// to window on disk, and disk to retained memory - so a cursor error of even
// one entry at any handoff shows up as a hole or a duplicate. Flushed windows
// exist on disk AND in the retained ring, which is exactly where
// inclusive/exclusive mistakes on either side used to hide.
func TestSubscribeLoop_BacklogThenLiveExactlyOnce(t *testing.T) {
	h := newSubscribeHarness(t)

	ts := func(i int) int64 { return h.base + int64(i)*int64(time.Millisecond) }

	var want []int64
	n := 0
	for w := 0; w < 3; w++ {
		for i := 0; i < 3; i++ {
			want = append(want, ts(n))
			h.append(ts(n))
			n++
		}
		h.f.LocalMetaLogBuffer.ForceFlush() // adjacent-timestamp window boundary on disk
	}
	// A retained, unflushed tail starting 1ms after the flushed content ends.
	for i := 0; i < 3; i++ {
		want = append(want, ts(n))
		h.append(ts(n))
		n++
	}

	r := h.subscribe(0, nil)
	waitForEvents(t, r, want, 5*time.Second)

	// Live tail on top.
	live := []int64{ts(n), ts(n + 1)}
	for _, l := range live {
		h.append(l)
	}
	waitForEvents(t, r, append(append([]int64(nil), want...), live...), 5*time.Second)
}

// A bounded subscription delivers through its bound and terminates - it must
// not park forever on the gap machinery with its window already served.
func TestSubscribeLoop_BoundedSubscriptionTerminates(t *testing.T) {
	h := newSubscribeHarness(t)

	all := []int64{h.tsAt(0, 0), h.tsAt(0, 1), h.tsAt(1, 0), h.tsAt(1, 1)}
	for _, ts := range all {
		h.append(ts)
	}
	h.f.LocalMetaLogBuffer.ForceFlush()

	until := all[1]
	r := h.subscribe(0, func(req *filer_pb.SubscribeMetadataRequest) { req.UntilNs = until })

	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("bounded subscription failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bounded subscription did not terminate")
	}
	for _, ts := range eventTimestamps(r.stream.snapshot()) {
		if ts > until {
			t.Fatalf("delivered %d past the bound %d", ts, until)
		}
	}
}

// Vacuumed logs: the flush watermark proves a gap empty even though the files
// are gone - the resolver must skip to the retained ring and deliver its
// earliest window intact, including a single-entry window whose start and stop
// coincide. This is the one path where resolveGapResume itself advances the
// stream, and the resume-below-earliest arithmetic is load-bearing.
func TestSubscribeLoop_FlushProvenGapSkipsToRetained(t *testing.T) {
	h := newSubscribeHarness(t)

	lastSealed := log_buffer.PreviousBufferCount
	for w := 0; w < log_buffer.PreviousBufferCount+2; w++ {
		h.append(h.tsAt(w, 0))
	}
	// Through the last sealed window, not just the evicted one: a flush landing
	// after the delete would put that window back on disk, and the disk pass
	// would drag the cursor past the retained windows this test is about.
	waitForFlushedThrough(t, h, h.tsAt(lastSealed, 0))
	if h.f.LocalMetaLogBuffer.GetLastEvictedTsNs() == 0 {
		t.Fatal("precondition: nothing evicted")
	}
	deleteAllLogFiles(t, h)

	earliest := h.f.LocalMetaLogBuffer.GetEarliestTime().UnixNano()
	var retained []int64
	for w := 0; w < log_buffer.PreviousBufferCount+2; w++ {
		if ts := h.tsAt(w, 0); ts >= earliest {
			retained = append(retained, ts)
		}
	}

	r := h.subscribe(0, nil)
	waitForEvents(t, r, retained, 5*time.Second)
}

// waitForFlushedThrough waits until every window up to throughTsNs is on disk.
// Windows flush in seal order, so the watermark reaching it means nothing
// earlier is still queued behind it.
func waitForFlushedThrough(t *testing.T, h *subscribeHarness, throughTsNs int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.f.LocalMetaLogBuffer.GetLastFlushTsNs() >= throughTsNs {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("flushes did not land")
}

func deleteAllLogFiles(t *testing.T, h *subscribeHarness) {
	t.Helper()
	ctx := context.Background()
	days, _, err := h.f.ListDirectoryEntries(ctx, filer.SystemLogDir, "", true, 1000, "", "", "")
	if err != nil {
		t.Fatalf("list log days: %v", err)
	}
	store := h.f.GetStore()
	for _, day := range days {
		if err := store.DeleteFolderChildren(ctx, day.FullPath); err != nil {
			t.Fatalf("delete %s children: %v", day.FullPath, err)
		}
		if err := store.DeleteEntry(ctx, day.FullPath); err != nil {
			t.Fatalf("delete %s: %v", day.FullPath, err)
		}
	}
}

// A permanently wedged flush ends in the loud give-up skip: the loss is
// bounded to the unprovable range, counted, and the stream keeps delivering
// what memory still holds - it must not stay silent forever and must not fail.
func TestSubscribeLoop_GiveUpSkipsAndKeepsStreaming(t *testing.T) {
	h := newSubscribeHarness(t)
	prevStall := maxGapStall
	maxGapStall = 250 * time.Millisecond
	t.Cleanup(func() { maxGapStall = prevStall })

	h.blockFlushes()
	var retained []int64
	for w := 0; w < log_buffer.PreviousBufferCount+3; w++ {
		h.append(h.tsAt(w, 0))
	}
	// What the ring still holds after eviction is what must arrive post-skip.
	earliest := h.f.LocalMetaLogBuffer.GetEarliestTime().UnixNano()
	for w := 0; w < log_buffer.PreviousBufferCount+3; w++ {
		if ts := h.tsAt(w, 0); ts >= earliest {
			retained = append(retained, ts)
		}
	}

	r := h.subscribe(0, nil)
	waitForEvents(t, r, retained, 5*time.Second)
	select {
	case err := <-r.done:
		t.Fatalf("stream ended (%v); the give-up must keep it alive", err)
	default:
	}
}

// Chunk mode, checked against the real client code: everything the marker
// claims must be applied by pb.ReadLogFileRefs over the shipped refs, and the
// inline stream must start strictly after the marker - the contract whose two
// sides drifted in round after round of review.
func TestSubscribeLoop_ChunkModeMarkerMatchesClientReplay(t *testing.T) {
	h := newSubscribeHarness(t)

	var want []int64
	for w := 0; w < 3; w++ {
		for i := 0; i < 3; i++ {
			ts := h.tsAt(w, i)
			want = append(want, ts)
			h.append(ts)
		}
	}
	h.f.LocalMetaLogBuffer.ForceFlush()

	r := h.subscribe(0, func(req *filer_pb.SubscribeMetadataRequest) { req.ClientSupportsMetadataChunks = true })

	// Wait for refs plus their transition marker.
	var refs []*filer_pb.LogFileChunkRef
	var markerTsNs int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		refs = refs[:0]
		markerTsNs = 0
		for _, m := range r.stream.snapshot() {
			if len(m.LogFileRefs) > 0 {
				refs = append(refs, m.LogFileRefs...)
				if markerTsNs != 0 {
					t.Fatal("refs arrived after their batch's marker")
				}
				continue
			}
			if m.EventNotification != nil && m.EventNotification.NewEntry == nil && m.TsNs > 0 && markerTsNs == 0 {
				markerTsNs = m.TsNs
			}
		}
		if markerTsNs != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if markerTsNs == 0 {
		t.Fatal("no transition marker followed the refs; the client would buffer them forever")
	}

	// Apply the refs exactly the way the real client does.
	var applied []int64
	clientLastTs, err := pb.ReadLogFileRefs(refs,
		func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			return &chunkStreamReader{vol: h.vol, chunks: chunks}, nil
		},
		0, 0, pb.PathFilter{},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			applied = append(applied, resp.TsNs)
			return nil
		})
	if err != nil {
		t.Fatalf("client replay: %v", err)
	}
	if markerTsNs > clientLastTs {
		t.Fatalf("marker %d claims more than the client applied through %d; the difference is silently lost", markerTsNs, clientLastTs)
	}
	if fmt.Sprint(applied) != fmt.Sprint(want) {
		t.Fatalf("client applied %v, want %v", applied, want)
	}

	// The inline stream must not re-deliver ref-covered content.
	for _, ts := range eventTimestamps(r.stream.snapshot()) {
		if ts <= markerTsNs {
			t.Fatalf("inline event %d at or below the marker %d duplicates the client's chunk replay", ts, markerTsNs)
		}
	}

	// And a live tail still arrives inline, after the marker.
	live := h.tsAt(4, 0)
	h.append(live)
	waitForEvents(t, r, []int64{live}, 5*time.Second)
}

// Chunk mode with a dead volume mid-file: the marker must stop where the
// client's read stops, the stream must keep working, and the events after the
// dead chunk's file must still arrive.
func TestSubscribeLoop_ChunkModeDeadVolumeAgreesWithClient(t *testing.T) {
	h := newSubscribeHarness(t)

	// Three flushed windows -> three files; kill the middle file's chunk.
	var written []int64
	for w := 0; w < 3; w++ {
		ts := h.tsAt(w, 0)
		written = append(written, ts)
		h.append(ts)
	}
	h.f.LocalMetaLogBuffer.ForceFlush()
	h.vol.kill("t,2")

	r := h.subscribe(0, func(req *filer_pb.SubscribeMetadataRequest) { req.ClientSupportsMetadataChunks = true })

	var refs []*filer_pb.LogFileChunkRef
	var markerTsNs int64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		refs = refs[:0]
		markerTsNs = 0
		for _, m := range r.stream.snapshot() {
			if len(m.LogFileRefs) > 0 {
				refs = append(refs, m.LogFileRefs...)
			} else if m.EventNotification != nil && m.EventNotification.NewEntry == nil && m.TsNs > markerTsNs {
				markerTsNs = m.TsNs
			}
		}
		if markerTsNs != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if markerTsNs == 0 {
		t.Fatal("no transition marker; a dead volume must not block it")
	}

	var applied []int64
	clientLastTs, err := pb.ReadLogFileRefs(refs,
		func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			return &chunkStreamReader{vol: h.vol, chunks: chunks}, nil
		},
		0, 0, pb.PathFilter{},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			applied = append(applied, resp.TsNs)
			return nil
		})
	if err != nil {
		t.Fatalf("client replay: %v", err)
	}
	if markerTsNs > clientLastTs {
		t.Fatalf("marker %d ahead of the client's %d with a dead chunk in between; the suffix is silently lost", markerTsNs, clientLastTs)
	}
	// The client skips the dead file but applies the later one.
	sort.Slice(applied, func(i, j int) bool { return applied[i] < applied[j] })
	appliedStr := fmt.Sprint(applied)
	if !strings.Contains(appliedStr, fmt.Sprint(written[2])) || strings.Contains(appliedStr, fmt.Sprint(written[1])) {
		t.Fatalf("client applied %v; want the dead file %d skipped and the later file %d applied", applied, written[1], written[2])
	}
}
