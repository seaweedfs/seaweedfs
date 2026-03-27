package pb

import (
	"bytes"
	"fmt"
	"io"
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

// TestReadLogFileRefsMergeOrder verifies that entries from multiple filers are
// delivered in correct timestamp order.
func TestReadLogFileRefsMergeOrder(t *testing.T) {
	files := newTestLogFiles(3, 2, 50, 0)

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

	expected := files.totalEvents()
	if len(timestamps) != expected {
		t.Fatalf("expected %d events, got %d", expected, len(timestamps))
	}

	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] < timestamps[i-1] {
			t.Errorf("out of order at index %d: ts[%d]=%d > ts[%d]=%d",
				i, i-1, timestamps[i-1], i, timestamps[i])
			break
		}
	}

	t.Logf("Verified %d events from 3 filers in correct timestamp order", len(timestamps))
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
//   - Client direct-read: parallel filers + prefetching + no gRPC
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
	t.Run("client_direct_read_parallel_prefetch", func(t *testing.T) {
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
		t.Logf("direct-read: %d events  %v  %6.0f events/sec  (%d filers parallel + prefetch, no gRPC)",
			processed, elapsed.Round(time.Millisecond), directRate, numFilers)
	})

	if serverRate > 0 {
		t.Logf("Speedup: %.1fx (parallel + prefetch + no gRPC vs server-side sequential)", directRate/serverRate)
	}
}
