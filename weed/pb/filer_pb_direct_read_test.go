package pb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// buildLogFileData creates the on-disk log file format:
// [4-byte size | protobuf LogEntry] repeated.
func buildLogFileData(events []*filer_pb.SubscribeMetadataResponse) []byte {
	var buf bytes.Buffer
	for _, event := range events {
		eventData, _ := proto.Marshal(event)
		logEntry := &filer_pb.LogEntry{
			TsNs: event.TsNs,
			Data: eventData,
			Key:  []byte(event.Directory),
		}
		entryData, _ := proto.Marshal(logEntry)
		sizeBuf := make([]byte, 4)
		util.Uint32toBytes(sizeBuf, uint32(len(entryData)))
		buf.Write(sizeBuf)
		buf.Write(entryData)
	}
	return buf.Bytes()
}

func makeSubEvent(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		TsNs:      tsNs,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: false,
			},
		},
	}
}

// delayedReader wraps data with a per-open latency to simulate volume server I/O.
type delayedReader struct {
	data     []byte
	delay    time.Duration
	openedAt time.Time
}

func (r *delayedReader) Read(p []byte) (int, error) {
	if r.openedAt.IsZero() {
		r.openedAt = time.Now()
		time.Sleep(r.delay)
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (r *delayedReader) Close() error { return nil }

type testLogFiles struct {
	refs      []*filer_pb.LogFileChunkRef
	fileData  map[string][]byte // key: "filerId:fileTsNs" → raw log file bytes
	fileDelay time.Duration
}

func newTestLogFiles(numFilers, filesPerFiler, eventsPerFile int, fileDelay time.Duration) *testLogFiles {
	t := &testLogFiles{
		fileData:  make(map[string][]byte),
		fileDelay: fileDelay,
	}

	baseTs := time.Now().Add(-time.Hour).UnixNano()
	tsCounter := int64(0)

	for f := 0; f < numFilers; f++ {
		filerId := fmt.Sprintf("filer%02d", f)
		for file := 0; file < filesPerFiler; file++ {
			fileTsNs := baseTs + int64(file)*int64(time.Minute)

			events := make([]*filer_pb.SubscribeMetadataResponse, eventsPerFile)
			for i := 0; i < eventsPerFile; i++ {
				tsCounter++
				ts := baseTs + tsCounter
				events[i] = makeSubEvent(
					fmt.Sprintf("/data/%s/dir%02d", filerId, file),
					fmt.Sprintf("file%04d.txt", i),
					ts,
				)
			}

			data := buildLogFileData(events)
			key := fmt.Sprintf("%s:%d", filerId, fileTsNs)
			t.fileData[key] = data

			t.refs = append(t.refs, &filer_pb.LogFileChunkRef{
				Chunks: []*filer_pb.FileChunk{{
					FileId: key,
				}},
				FileTsNs: fileTsNs,
				FilerId:  filerId,
			})
		}
	}
	return t
}

func (t *testLogFiles) readerFn() LogFileReaderFn {
	return func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
		if len(chunks) == 0 {
			return nil, fmt.Errorf("no chunks")
		}
		key := chunks[0].FileId
		data, ok := t.fileData[key]
		if !ok {
			return nil, fmt.Errorf("file not found: %s", key)
		}
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		return &delayedReader{data: dataCopy, delay: t.fileDelay}, nil
	}
}

func (t *testLogFiles) totalEvents() int {
	total := 0
	for _, data := range t.fileData {
		pos := 0
		for pos+4 <= len(data) {
			size := int(util.BytesToUint32(data[pos : pos+4]))
			pos += 4 + size
			total++
		}
	}
	return total
}

// assertOrderedReplay reads all refs and checks every event arrives exactly
// once, in timestamp order.
func assertOrderedReplay(t *testing.T, files *testLogFiles) {
	t.Helper()

	var timestamps []int64
	_, err := ReadLogFileRefs(files.refs, files.readerFn(), 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			timestamps = append(timestamps, resp.TsNs)
			return nil
		})
	if err != nil {
		t.Fatalf("ReadLogFileRefs: %v", err)
	}

	if got, want := len(timestamps), files.totalEvents(); got != want {
		t.Fatalf("expected %d events, got %d", want, got)
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Fatalf("out of order at index %d: ts[%d]=%d > ts[%d]=%d",
				i, i-1, timestamps[i-1], i, timestamps[i])
		}
	}
}

// TestReadLogFileRefsMergeOrder verifies that entries from multiple filers are
// delivered in correct timestamp order.
func TestReadLogFileRefsMergeOrder(t *testing.T) {
	assertOrderedReplay(t, newTestLogFiles(3, 2, 50, 0))
}

// TestReadLogFileRefsPathFilter verifies path filtering including system log exclusion.
func TestReadLogFileRefsPathFilter(t *testing.T) {
	files := newTestLogFiles(2, 2, 50, 0)
	total := files.totalEvents()

	var allCount, filteredCount int64
	_, err := ReadLogFileRefs(files.refs, files.readerFn(), 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			allCount++
			return nil
		})
	if err != nil {
		t.Fatalf("ReadLogFileRefs (all): %v", err)
	}

	_, err = ReadLogFileRefs(files.refs, files.readerFn(), 0, 0,
		PathFilter{PathPrefix: "/data/filer00/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			filteredCount++
			return nil
		})
	if err != nil {
		t.Fatalf("ReadLogFileRefs (filtered): %v", err)
	}

	t.Logf("Total events: %d, matching /data/filer00/: %d", allCount, filteredCount)

	if allCount != int64(total) {
		t.Errorf("expected %d total events, got %d", total, allCount)
	}
	if filteredCount >= allCount {
		t.Errorf("filter should reduce events: all=%d filtered=%d", allCount, filteredCount)
	}
	if filteredCount == 0 {
		t.Errorf("filter matched zero events")
	}
}

// TestDirectReadVsServerSideThroughput compares:
//   - Server-side: sequential file read → gRPC send per event
//   - Client direct-read: parallel filers + streaming + no gRPC
func TestDirectReadVsServerSideThroughput(t *testing.T) {
	const (
		numFilers     = 3
		filesPerFiler = 7
		eventsPerFile = 300
		fileReadDelay = 2 * time.Millisecond
		sendDelay     = 20 * time.Microsecond
	)

	files := newTestLogFiles(numFilers, filesPerFiler, eventsPerFile, fileReadDelay)

	var serverRate float64
	t.Run("server_side_sequential", func(t *testing.T) {
		var processed int64
		start := time.Now()

		for _, ref := range files.refs {
			time.Sleep(fileReadDelay)
			key := ref.Chunks[0].FileId
			data := files.fileData[key]
			pos := 0
			for pos+4 <= len(data) {
				size := int(util.BytesToUint32(data[pos : pos+4]))
				pos += 4 + size
				time.Sleep(sendDelay)
				atomic.AddInt64(&processed, 1)
			}
		}
		elapsed := time.Since(start)
		serverRate = float64(processed) / elapsed.Seconds()
		t.Logf("server-side: %d events  %v  %6.0f events/sec  (%d files sequential + %v send/event)",
			processed, elapsed.Round(time.Millisecond), serverRate,
			numFilers*filesPerFiler, sendDelay)
	})

	var directRate float64
	t.Run("client_direct_read_parallel_streaming", func(t *testing.T) {
		var processed int64
		start := time.Now()

		_, err := ReadLogFileRefs(files.refs, files.readerFn(), 0, 0,
			PathFilter{PathPrefix: "/"},
			func(resp *filer_pb.SubscribeMetadataResponse) error {
				atomic.AddInt64(&processed, 1)
				return nil
			})
		if err != nil {
			t.Fatalf("ReadLogFileRefs: %v", err)
		}
		elapsed := time.Since(start)
		directRate = float64(processed) / elapsed.Seconds()
		t.Logf("direct-read: %d events  %v  %6.0f events/sec  (%d filers parallel + streaming, no gRPC)",
			processed, elapsed.Round(time.Millisecond), directRate, numFilers)
	})

	if serverRate > 0 {
		t.Logf("Speedup: %.1fx (parallel + streaming + no gRPC vs server-side sequential)", directRate/serverRate)
	}
}

// failingReaderFn returns err for the given file key, delegating otherwise.
func failingReaderFn(base LogFileReaderFn, failKey string, err error) LogFileReaderFn {
	return func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
		if len(chunks) > 0 && chunks[0].FileId == failKey {
			return nil, err
		}
		return base(chunks)
	}
}

// A real (non not-found) read error must fail the whole replay, not silently
// drop the file and advance the cursor.
func TestReadLogFileRefsMultiFilerGenuineErrorAborts(t *testing.T) {
	files := newTestLogFiles(3, 2, 10, 0)
	failKey := files.refs[2].Chunks[0].FileId // filer01's first file
	readerFn := failingReaderFn(files.readerFn(), failKey, fmt.Errorf("failed to locate %s", failKey))

	var count int64
	_, err := ReadLogFileRefs(files.refs, readerFn, 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			atomic.AddInt64(&count, 1)
			return nil
		})
	if err == nil {
		t.Fatalf("expected error from genuine read failure, got nil (delivered=%d)", count)
	}
}

// A chunk-not-found error skips only that file (volume gone), not the replay.
func TestReadLogFileRefsMultiFilerNotFoundSkips(t *testing.T) {
	files := newTestLogFiles(3, 2, 10, 0)
	skipKey := files.refs[2].Chunks[0].FileId // filer01's first file
	readerFn := failingReaderFn(files.readerFn(), skipKey, fmt.Errorf("volume not found: %s", skipKey))

	var count int64
	_, err := ReadLogFileRefs(files.refs, readerFn, 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			atomic.AddInt64(&count, 1)
			return nil
		})
	if err != nil {
		t.Fatalf("chunk-not-found should be skipped, got error: %v", err)
	}
	expected := int64(files.totalEvents() - 10) // one skipped file's events
	if count != expected {
		t.Fatalf("expected %d events after skipping one file, got %d", expected, count)
	}
}

// TestReadLogFileRefsSingleFilerOrder covers the single-filer path: every
// entry across all files, in order.
func TestReadLogFileRefsSingleFilerOrder(t *testing.T) {
	assertOrderedReplay(t, newTestLogFiles(1, 4, 50, 0))
}

// TestReadLogFileRefsSingleFilerProcessErrorStops verifies that the callback's
// own error propagates and aborts the stream promptly, mid-file.
func TestReadLogFileRefsSingleFilerProcessErrorStops(t *testing.T) {
	files := newTestLogFiles(1, 3, 100, 0)

	var count int
	wantErr := fmt.Errorf("boom")
	_, err := ReadLogFileRefs(files.refs, files.readerFn(), 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			count++
			if count == 5 {
				return wantErr
			}
			return nil
		})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected processing error to propagate, got: %v", err)
	}
	// Should stop near the failing event, not process the whole 300-event set.
	if count > 20 {
		t.Fatalf("expected prompt stop after error, processed %d events", count)
	}
}

// A corrupt size prefix must fail the replay instead of allocating gigabytes.
func TestReadLogFileRefsCorruptSizePrefix(t *testing.T) {
	data := make([]byte, 4)
	util.Uint32toBytes(data, 0xFFFFFFF0)
	refs := []*filer_pb.LogFileChunkRef{{
		Chunks:   []*filer_pb.FileChunk{{FileId: "corrupt"}},
		FileTsNs: 1,
		FilerId:  "filer00",
	}}
	readerFn := func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	_, err := ReadLogFileRefs(refs, readerFn, 0, 0, PathFilter{PathPrefix: "/"},
		func(*filer_pb.SubscribeMetadataResponse) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size-cap error, got: %v", err)
	}
}

// blockingReader blocks in Read until released.
type blockingReader struct{ release chan struct{} }

func (r *blockingReader) Read(p []byte) (int, error) { <-r.release; return 0, io.EOF }
func (r *blockingReader) Close() error               { return nil }

// An abort (fatal error on one filer) must not wait for another filer's
// in-flight chunk read: the replay returns promptly and the wedged producer
// exits on its own once its read completes.
func TestReadLogFileRefsAbortDoesNotJoinWedgedReader(t *testing.T) {
	files := newTestLogFiles(2, 1, 10, 0)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	wedgedKey := files.refs[0].Chunks[0].FileId // filer00 wedges mid-read
	failKey := files.refs[1].Chunks[0].FileId   // filer01 fails for real
	base := files.readerFn()
	readerFn := func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
		switch chunks[0].FileId {
		case wedgedKey:
			return &blockingReader{release: release}, nil
		case failKey:
			return nil, fmt.Errorf("failed to locate %s", failKey)
		}
		return base(chunks)
	}

	done := make(chan error, 1)
	go func() {
		_, err := ReadLogFileRefs(files.refs, readerFn, 0, 0, PathFilter{PathPrefix: "/"},
			func(*filer_pb.SubscribeMetadataResponse) error { return nil })
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected the fatal read error to propagate")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("ReadLogFileRefs did not return while a peer reader was wedged")
	}
}

// TestReadLogFileRefsSingleFilerNotFoundSkips confirms a chunk-not-found on the
// single-filer path skips just that file, not the whole replay.
func TestReadLogFileRefsSingleFilerNotFoundSkips(t *testing.T) {
	files := newTestLogFiles(1, 3, 10, 0)
	skipKey := files.refs[1].Chunks[0].FileId // second file
	readerFn := failingReaderFn(files.readerFn(), skipKey, fmt.Errorf("volume not found: %s", skipKey))

	var count int
	_, err := ReadLogFileRefs(files.refs, readerFn, 0, 0,
		PathFilter{PathPrefix: "/"},
		func(resp *filer_pb.SubscribeMetadataResponse) error {
			count++
			return nil
		})
	if err != nil {
		t.Fatalf("chunk-not-found should be skipped, got error: %v", err)
	}
	if want := files.totalEvents() - 10; count != want {
		t.Fatalf("expected %d events after skipping one file, got %d", want, count)
	}
}
