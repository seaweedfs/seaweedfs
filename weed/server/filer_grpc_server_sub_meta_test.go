package weed_server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// slowStream simulates a gRPC stream with configurable per-Send latency.
// It counts individual events including those packed inside batches.
// Atomic counters use atomic.Int64 so they stay 8-byte aligned on 32-bit
// architectures (386, ARMv7, mips32) where a bare int64 struct field is
// only 4-byte aligned and panics under atomic.AddInt64.
type slowStream struct {
	sends      atomic.Int64 // number of stream.Send() calls
	eventsSent atomic.Int64 // total events (1 + len(Events) per Send)
	sendDelay  time.Duration
}

func (s *slowStream) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	time.Sleep(s.sendDelay)
	s.sends.Add(1)
	s.eventsSent.Add(1 + int64(len(msg.Events)))
	return nil
}

type collectingStream struct {
	messages []*filer_pb.SubscribeMetadataResponse
}

func (s *collectingStream) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	s.messages = append(s.messages, msg)
	return nil
}

func makeEvent(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
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

// makeOldEvents creates events with timestamps far in the past (triggers batch mode).
func makeOldEvents(n int) []*filer_pb.SubscribeMetadataResponse {
	baseTs := time.Now().Add(-time.Hour).UnixNano() // 1 hour ago → well past batchBehindThreshold
	events := make([]*filer_pb.SubscribeMetadataResponse, n)
	for i := range events {
		events[i] = makeEvent("/bucket/dir", fmt.Sprintf("file%06d.txt", i), baseTs+int64(i))
	}
	return events
}

// makeRecentEvents creates events with timestamps close to now (sends one-by-one).
func makeRecentEvents(n int) []*filer_pb.SubscribeMetadataResponse {
	baseTs := time.Now().UnixNano()
	events := make([]*filer_pb.SubscribeMetadataResponse, n)
	for i := range events {
		events[i] = makeEvent("/bucket/dir", fmt.Sprintf("file%06d.txt", i), baseTs+int64(i))
	}
	return events
}

// TestPipelinedSenderThroughput compares direct (blocking) stream.Send with
// the pipelinedSender with adaptive batching.
//
// Simulates realistic backlog catch-up: the reader loads one log file at a time
// from a volume server (fileReadDelay per file), producing a burst of ~300
// events. The sender has per-Send gRPC overhead (sendDelay).
//
//   - Direct: serial — each event: send one-by-one between file reads
//   - Pipelined+batched: file I/O overlaps with batched sending
func TestPipelinedSenderThroughput(t *testing.T) {
	const (
		eventsPerFile = 300                      // events in one minute-log file
		numFiles      = 7                        // files to process
		totalEvents   = eventsPerFile * numFiles // 2100
		fileReadDelay = 5 * time.Millisecond     // volume server read per log file
		sendDelay     = 50 * time.Microsecond    // gRPC round-trip per Send()
	)

	// Partition old events into file-sized bursts
	files := make([][]*filer_pb.SubscribeMetadataResponse, numFiles)
	baseTs := time.Now().Add(-time.Hour).UnixNano()
	for f := 0; f < numFiles; f++ {
		files[f] = make([]*filer_pb.SubscribeMetadataResponse, eventsPerFile)
		for i := 0; i < eventsPerFile; i++ {
			idx := f*eventsPerFile + i
			files[f][i] = makeEvent("/bucket/dir", fmt.Sprintf("file%06d.txt", idx), baseTs+int64(idx))
		}
	}

	// --- Direct (old behavior): read file, send events one-by-one, repeat ---
	var directRate float64
	t.Run("direct_send", func(t *testing.T) {
		stream := &slowStream{sendDelay: sendDelay}

		start := time.Now()
		for _, file := range files {
			time.Sleep(fileReadDelay) // read log file from volume server
			for _, ev := range file {
				if err := stream.Send(ev); err != nil {
					t.Fatalf("send error: %v", err)
				}
			}
		}
		elapsed := time.Since(start)

		directRate = float64(stream.eventsSent.Load()) / elapsed.Seconds()
		t.Logf("direct:          %d events  %4d sends  %v  %6.0f events/sec",
			stream.eventsSent.Load(), stream.sends.Load(), elapsed.Round(time.Millisecond), directRate)
	})

	// --- Pipelined + batched (new behavior): file reads overlap with batched sends ---
	var batchedRate float64
	t.Run("pipelined_batched_send", func(t *testing.T) {
		stream := &slowStream{sendDelay: sendDelay}
		sender := newPipelinedSender(stream, 1024, true)

		start := time.Now()
		for _, file := range files {
			time.Sleep(fileReadDelay) // read log file from volume server
			for _, ev := range file {
				if err := sender.Send(ev); err != nil {
					t.Fatalf("send error: %v", err)
				}
			}
		}
		if err := sender.Close(); err != nil {
			t.Fatalf("close error: %v", err)
		}
		elapsed := time.Since(start)

		batchedRate = float64(stream.eventsSent.Load()) / elapsed.Seconds()
		t.Logf("pipelined+batch: %d events  %4d sends  %v  %6.0f events/sec",
			stream.eventsSent.Load(), stream.sends.Load(), elapsed.Round(time.Millisecond), batchedRate)
	})

	if directRate > 0 {
		t.Logf("Speedup: %.1fx (pipelined+batched vs direct)", batchedRate/directRate)
	}
}

func TestEachEventNotificationFnMatchesRenameTargetsForAllWatchTypes(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{Host: pb.ServerAddress("127.0.0.1:8888")},
		filer:  &filer.Filer{Signature: 123},
	}

	tests := []struct {
		name string
		req  *filer_pb.SubscribeMetadataRequest
	}{
		{
			name: "additional path prefix",
			req: &filer_pb.SubscribeMetadataRequest{
				ClientName:   "test",
				PathPrefix:   "/data/",
				PathPrefixes: []string{"/etc/remote"},
			},
		},
		{
			name: "directory watch",
			req: &filer_pb.SubscribeMetadataRequest{
				ClientName:  "test",
				PathPrefix:  "/data/",
				Directories: []string{"/etc/iam/identities"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &collectingStream{}
			eachEventFn := fs.eachEventNotificationFn(tt.req, stream, "client")

			newDir := "/etc/remote"
			if len(tt.req.Directories) > 0 {
				newDir = tt.req.Directories[0]
			}
			err := eachEventFn("/tmp", &filer_pb.EventNotification{
				OldEntry:      &filer_pb.Entry{Name: "old"},
				NewEntry:      &filer_pb.Entry{Name: "new"},
				NewParentPath: newDir,
			}, time.Now().UnixNano())
			if err != nil {
				t.Fatalf("eachEventFn: %v", err)
			}
			if len(stream.messages) != 1 {
				t.Fatalf("messages sent = %d, want 1", len(stream.messages))
			}
		})
	}
}

// TestBatchingAdaptive verifies the adaptive behavior: old events are batched,
// recent events are sent one-by-one.
func TestBatchingAdaptive(t *testing.T) {
	const numEvents = 500

	t.Run("old_events_are_batched", func(t *testing.T) {
		stream := &slowStream{sendDelay: 10 * time.Microsecond}
		sender := newPipelinedSender(stream, 1024, true)

		// Push all events at once (no read delay) so the sender can batch aggressively
		for _, ev := range makeOldEvents(numEvents) {
			sender.Send(ev)
		}
		sender.Close()

		sends := stream.sends.Load()
		events := stream.eventsSent.Load()
		t.Logf("old events: %d events in %d sends (avg batch size: %.1f)",
			events, sends, float64(events)/float64(sends))

		if sends >= int64(numEvents) {
			t.Errorf("expected batching to reduce sends below %d, got %d", numEvents, sends)
		}
	})

	t.Run("recent_events_sent_individually", func(t *testing.T) {
		stream := &slowStream{sendDelay: 10 * time.Microsecond}
		sender := newPipelinedSender(stream, 1024, true)

		for _, ev := range makeRecentEvents(numEvents) {
			sender.Send(ev)
		}
		sender.Close()

		sends := stream.sends.Load()
		events := stream.eventsSent.Load()
		t.Logf("recent events: %d events in %d sends (avg batch size: %.1f)",
			events, sends, float64(events)/float64(sends))

		if sends != int64(numEvents) {
			t.Errorf("expected 1:1 sends for recent events, got %d sends for %d events", sends, numEvents)
		}
	})
}

// errorStreamImpl is a metadataStreamSender that returns an error after N sends.
// count uses atomic.Int64 so it stays 8-byte aligned on 32-bit architectures
// (386, ARMv7, mips32) where a bare int64 struct field after smaller fields
// is only 4-byte aligned and panics under atomic.AddInt64.
type errorStreamImpl struct {
	count     atomic.Int64
	failAfter int
	err       error
}

func (s *errorStreamImpl) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	n := s.count.Add(1)
	if int(n) > s.failAfter {
		return s.err
	}
	return nil
}

// TestPipelinedSenderErrorPropagation verifies that when stream.Send fails,
// the error propagates to pipelinedSender.Send callers and Close.
func TestPipelinedSenderErrorPropagation(t *testing.T) {
	sendErr := fmt.Errorf("connection reset")

	t.Run("send_returns_error", func(t *testing.T) {
		// Stream fails after 5 successful sends
		stream := &errorStreamImpl{failAfter: 5, err: sendErr}
		sender := newPipelinedSender(stream, 4, true)

		var lastErr error
		for i := 0; i < 100; i++ {
			ev := makeOldEvents(1)[0]
			if err := sender.Send(ev); err != nil {
				lastErr = err
				break
			}
		}

		if lastErr == nil {
			t.Fatal("expected Send to return an error after stream failure")
		}
		t.Logf("Send returned error after stream failure: %v", lastErr)
	})

	t.Run("close_returns_error_if_not_consumed", func(t *testing.T) {
		// Stream fails on the very first send — error surfaces via Close
		// since Send may have already returned before the sender goroutine
		// processes the message.
		stream := &errorStreamImpl{failAfter: 0, err: sendErr}
		sender := newPipelinedSender(stream, 1024, true)

		ev := makeOldEvents(1)[0]
		sender.Send(ev)

		closeErr := sender.Close()
		if closeErr == nil {
			t.Log("Close returned nil (error was consumed by Send)")
		} else {
			t.Logf("Close returned error: %v", closeErr)
		}
	})
}

// TestPipelinedSingleVsParallelStreams shows 1 pipelined+batched stream vs
// N parallel pipelined+batched streams, using the realistic burst-read pattern.
func TestPipelinedSingleVsParallelStreams(t *testing.T) {
	const (
		numDirs       = 10
		filesPerDir   = 7                                     // log files per directory
		eventsPerFile = 300                                   // events per log file
		totalEvents   = numDirs * filesPerDir * eventsPerFile // 21000
		fileReadDelay = 5 * time.Millisecond
		sendDelay     = 50 * time.Microsecond
	)

	// Generate partitioned OLD events grouped into file-sized bursts
	baseTs := time.Now().Add(-time.Hour).UnixNano()
	type logFile []*filer_pb.SubscribeMetadataResponse
	// partitions[dir][file][event]
	partitions := make([][]logFile, numDirs)
	var allFiles []logFile
	idx := 0
	for d := 0; d < numDirs; d++ {
		dir := fmt.Sprintf("/bucket/dir%03d", d)
		for f := 0; f < filesPerDir; f++ {
			file := make(logFile, eventsPerFile)
			for i := 0; i < eventsPerFile; i++ {
				file[i] = makeEvent(dir, fmt.Sprintf("file%06d.txt", idx), baseTs+int64(idx))
				idx++
			}
			partitions[d] = append(partitions[d], file)
			allFiles = append(allFiles, file)
		}
	}

	// simulatePipeline: read files with I/O delay, push events, send via pipelinedSender
	simulatePipeline := func(files []logFile) (eventsSent, sends int64, elapsed time.Duration, err error) {
		stream := &slowStream{sendDelay: sendDelay}
		sender := newPipelinedSender(stream, 1024, true)

		start := time.Now()
	outer:
		for _, file := range files {
			time.Sleep(fileReadDelay) // volume server read
			for _, ev := range file {
				if err = sender.Send(ev); err != nil {
					break outer
				}
			}
		}
		if closeErr := sender.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		elapsed = time.Since(start)
		eventsSent = stream.eventsSent.Load()
		sends = stream.sends.Load()
		return
	}

	var singleRate float64
	t.Run("1_pipelined_stream", func(t *testing.T) {
		eventsSent, sends, elapsed, err := simulatePipeline(allFiles)
		if err != nil {
			t.Fatalf("pipeline error: %v", err)
		}
		singleRate = float64(eventsSent) / elapsed.Seconds()
		t.Logf("1 stream:    %5d events  %4d sends  %v  %7.0f events/sec",
			eventsSent, sends, elapsed.Round(time.Millisecond), singleRate)
	})

	var parallelRate float64
	t.Run("10_pipelined_streams", func(t *testing.T) {
		// atomic.Int64 guarantees 8-byte alignment on 32-bit architectures where
		// a local int64 variable's address is only 4-byte aligned and atomic
		// 64-bit operations panic with "unaligned 64-bit atomic operation".
		var totalEventsSent, totalSends atomic.Int64
		var wg sync.WaitGroup

		start := time.Now()
		for d := 0; d < numDirs; d++ {
			wg.Add(1)
			go func(files []logFile) {
				defer wg.Done()
				eventsSent, sends, _, _ := simulatePipeline(files)
				totalEventsSent.Add(eventsSent)
				totalSends.Add(sends)
			}(partitions[d])
		}
		wg.Wait()
		elapsed := time.Since(start)

		totalEvents := totalEventsSent.Load()
		parallelRate = float64(totalEvents) / elapsed.Seconds()
		t.Logf("%d streams:  %5d events  %4d sends  %v  %7.0f events/sec",
			numDirs, totalEvents, totalSends.Load(), elapsed.Round(time.Millisecond), parallelRate)
	})

	if singleRate > 0 && parallelRate > 0 {
		t.Logf("Speedup: %.1fx (%d parallel pipelined streams vs 1)", parallelRate/singleRate, numDirs)
	}
}
