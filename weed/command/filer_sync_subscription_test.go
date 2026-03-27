package command

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// createFileEvent creates a SubscribeMetadataResponse for a file creation.
func createFileEvent(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
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

// partitionedEvents creates file creation events spread across numDirs directories.
func partitionedEvents(numDirs, filesPerDir int) (partitions [][]*filer_pb.SubscribeMetadataResponse, all []*filer_pb.SubscribeMetadataResponse) {
	baseTs := time.Now().UnixNano()
	partitions = make([][]*filer_pb.SubscribeMetadataResponse, numDirs)
	for d := 0; d < numDirs; d++ {
		dir := fmt.Sprintf("/bucket/dir%03d", d)
		for f := 0; f < filesPerDir; f++ {
			tsNs := baseTs + int64(d*filesPerDir+f) + 1
			event := createFileEvent(dir, fmt.Sprintf("file%06d.txt", f), tsNs)
			partitions[d] = append(partitions[d], event)
			all = append(all, event)
		}
	}
	return
}

// runSingleStream feeds all events through one MetadataProcessor with a per-event
// stream delivery delay (simulating a single gRPC SubscribeMetadata stream).
func runSingleStream(events []*filer_pb.SubscribeMetadataResponse, concurrency int, streamDelay, processDelay time.Duration) (processed int64, elapsed time.Duration) {
	var wg sync.WaitGroup

	processFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		defer wg.Done()
		time.Sleep(processDelay)
		atomic.AddInt64(&processed, 1)
		return nil
	}

	processor := NewMetadataProcessor(processFn, concurrency, 0)

	start := time.Now()
	for _, event := range events {
		if streamDelay > 0 {
			time.Sleep(streamDelay)
		}
		wg.Add(1)
		processor.AddSyncJob(event)
	}
	wg.Wait()
	elapsed = time.Since(start)
	return
}

// runParallelStreams feeds partitioned events through separate MetadataProcessors,
// each in its own goroutine (simulating parallel per-directory gRPC streams).
func runParallelStreams(partitions [][]*filer_pb.SubscribeMetadataResponse, concurrency int, streamDelay, processDelay time.Duration) (processed int64, elapsed time.Duration) {
	var outerWg sync.WaitGroup

	start := time.Now()
	for _, dirEvents := range partitions {
		outerWg.Add(1)
		go func(events []*filer_pb.SubscribeMetadataResponse) {
			defer outerWg.Done()

			var wg sync.WaitGroup
			processFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
				defer wg.Done()
				time.Sleep(processDelay)
				atomic.AddInt64(&processed, 1)
				return nil
			}

			processor := NewMetadataProcessor(processFn, concurrency, 0)
			for _, event := range events {
				if streamDelay > 0 {
					time.Sleep(streamDelay)
				}
				wg.Add(1)
				processor.AddSyncJob(event)
			}
			wg.Wait()
		}(dirEvents)
	}
	outerWg.Wait()
	elapsed = time.Since(start)
	return
}

// TestStreamDeliveryBottleneck demonstrates that a single serial event stream
// is the primary throughput bottleneck, and N parallel streams achieve N× throughput.
//
// Reproduces discussion #8771: single filer.sync "/" achieves ~80 events/sec,
// while N parallel processes for individual directories achieve N × ~80 events/sec.
//
// The bottleneck is the serial gRPC metadata stream, NOT conflict detection or
// processing concurrency.
func TestStreamDeliveryBottleneck(t *testing.T) {
	const (
		numDirs     = 10
		filesPerDir = 200
		// Per-event stream delivery overhead (server-side log read + gRPC round-trip).
		// Production: ~10-12ms giving ~80-100 events/sec. Scaled down for test speed.
		streamDelay  = 50 * time.Microsecond
		processDelay = 200 * time.Microsecond
	)

	partitions, allEvents := partitionedEvents(numDirs, filesPerDir)

	singleCount, singleElapsed := runSingleStream(allEvents, 256, streamDelay, processDelay)
	singleRate := float64(singleCount) / singleElapsed.Seconds()
	t.Logf("1 stream:    %4d events  %v  %6.0f events/sec",
		singleCount, singleElapsed.Round(time.Millisecond), singleRate)

	parallelCount, parallelElapsed := runParallelStreams(partitions, 256, streamDelay, processDelay)
	parallelRate := float64(parallelCount) / parallelElapsed.Seconds()
	t.Logf("%d streams:  %4d events  %v  %6.0f events/sec",
		numDirs, parallelCount, parallelElapsed.Round(time.Millisecond), parallelRate)

	speedup := parallelRate / singleRate
	t.Logf("Speedup: %.1fx (%d parallel streams vs 1 stream)", speedup, numDirs)

	if singleCount != int64(numDirs*filesPerDir) {
		t.Errorf("single: expected %d events, got %d", numDirs*filesPerDir, singleCount)
	}
	if parallelCount != int64(numDirs*filesPerDir) {
		t.Errorf("parallel: expected %d events, got %d", numDirs*filesPerDir, parallelCount)
	}
	// Parallel should be significantly faster
	if speedup < float64(numDirs)*0.4 {
		t.Errorf("expected at least %.1fx speedup, got %.1fx", float64(numDirs)*0.4, speedup)
	}
}

// TestConcurrencyIneffectiveOnStreamBottleneck shows that increasing the
// -concurrency flag has no effect when the stream delivery rate is the bottleneck.
//
// Matches the user observation: "-concurrency=256 is little better than default
// but increasing it to 1024 doesn't do anything."
func TestConcurrencyIneffectiveOnStreamBottleneck(t *testing.T) {
	const (
		numDirs      = 10
		filesPerDir  = 100
		streamDelay  = 50 * time.Microsecond
		processDelay = 200 * time.Microsecond
	)

	_, allEvents := partitionedEvents(numDirs, filesPerDir)

	var rates []float64
	for _, concurrency := range []int{32, 128, 512} {
		count, elapsed := runSingleStream(allEvents, concurrency, streamDelay, processDelay)
		rate := float64(count) / elapsed.Seconds()
		rates = append(rates, rate)
		t.Logf("concurrency=%3d: %d events  %v  %.0f events/sec",
			concurrency, count, elapsed.Round(time.Millisecond), rate)
	}

	if len(rates) >= 2 {
		ratio := rates[len(rates)-1] / rates[0]
		t.Logf("concurrency 512 vs 32: %.2fx (expected ~1.0x when stream-limited)", ratio)
		// Should be within 50% — concurrency doesn't help a stream bottleneck
		if ratio > 1.5 || ratio < 0.5 {
			t.Errorf("unexpected ratio %.2f: concurrency should not affect stream-limited throughput", ratio)
		}
	}
}

// TestLogBufferSubscriptionThroughput uses the real LogBuffer and LoopProcessLogData
// to demonstrate that a single subscriber's callback is called serially (blocking
// the event loop), while N parallel subscribers process events concurrently.
//
// This directly reproduces the server-side pipeline: SubscribeMetadata reads events
// from the LogBuffer via LoopProcessLogData, and for each event calls stream.Send()
// which blocks until the client acknowledges. A slow client stalls the entire
// event loop for that subscriber.
func TestLogBufferSubscriptionThroughput(t *testing.T) {
	const (
		numDirs      = 10
		filesPerDir  = 200
		totalEvents  = numDirs * filesPerDir
		processDelay = 200 * time.Microsecond
	)

	lb := log_buffer.NewLogBuffer("test-subscription", time.Hour, nil, nil, func() {})

	// Populate buffer with events across directories
	baseTs := time.Now().UnixNano()
	var firstTsNs, lastTsNs int64
	for d := 0; d < numDirs; d++ {
		dir := fmt.Sprintf("/data/dir%03d", d)
		for f := 0; f < filesPerDir; f++ {
			tsNs := baseTs + int64(d*filesPerDir+f) + 1
			if firstTsNs == 0 {
				firstTsNs = tsNs
			}
			lastTsNs = tsNs
			event := createFileEvent(dir, fmt.Sprintf("file%06d.txt", f), tsNs)
			data, err := proto.Marshal(event)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := lb.AddDataToBuffer([]byte(dir), data, tsNs); err != nil {
				t.Fatalf("add to buffer: %v", err)
			}
		}
	}

	startPos := log_buffer.NewMessagePosition(firstTsNs-1, -2)

	// --- Single subscriber: all events go through one callback serially ---
	var singleProcessed int64
	var singleRate float64
	t.Run("single_subscriber_root", func(t *testing.T) {
		done := make(chan struct{})
		start := time.Now()
		go func() {
			defer close(done)
			lb.LoopProcessLogData("single-root", startPos, lastTsNs,
				func() bool { return true },
				func(logEntry *filer_pb.LogEntry) (bool, error) {
					event := &filer_pb.SubscribeMetadataResponse{}
					if err := proto.Unmarshal(logEntry.Data, event); err != nil {
						return false, err
					}
					// All events match "/" — process all
					time.Sleep(processDelay)
					atomic.AddInt64(&singleProcessed, 1)
					return false, nil
				})
		}()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("timed out")
		}
		elapsed := time.Since(start)
		singleRate = float64(singleProcessed) / elapsed.Seconds()
		t.Logf("1 subscriber (/): %4d events  %v  %6.0f events/sec",
			singleProcessed, elapsed.Round(time.Millisecond), singleRate)

		if singleProcessed != int64(totalEvents) {
			t.Errorf("expected %d events, got %d", totalEvents, singleProcessed)
		}
	})

	// --- N parallel subscribers, each filtering for one directory ---
	var parallelProcessed int64
	var parallelRate float64
	t.Run("parallel_subscribers_per_dir", func(t *testing.T) {
		var wg sync.WaitGroup

		start := time.Now()
		for d := 0; d < numDirs; d++ {
			wg.Add(1)
			prefix := fmt.Sprintf("/data/dir%03d/", d)
			name := fmt.Sprintf("parallel-dir%03d", d)

			go func(pfx, readerName string) {
				defer wg.Done()
				lb.LoopProcessLogData(readerName, startPos, lastTsNs,
					func() bool { return true },
					func(logEntry *filer_pb.LogEntry) (bool, error) {
						event := &filer_pb.SubscribeMetadataResponse{}
						if err := proto.Unmarshal(logEntry.Data, event); err != nil {
							return false, err
						}
						fullpath := event.Directory
						if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
							fullpath += "/" + event.EventNotification.NewEntry.Name
						}
						if !strings.HasPrefix(fullpath, pfx) {
							return false, nil // skip non-matching — no delay
						}
						time.Sleep(processDelay)
						atomic.AddInt64(&parallelProcessed, 1)
						return false, nil
					})
			}(prefix, name)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("timed out")
		}
		elapsed := time.Since(start)
		parallelRate = float64(parallelProcessed) / elapsed.Seconds()
		t.Logf("%d subscribers:    %4d events  %v  %6.0f events/sec",
			numDirs, parallelProcessed, elapsed.Round(time.Millisecond), parallelRate)

		if parallelProcessed != int64(totalEvents) {
			t.Errorf("expected %d events, got %d", totalEvents, parallelProcessed)
		}
	})

	if singleRate > 0 && parallelRate > 0 {
		speedup := parallelRate / singleRate
		t.Logf("LogBuffer speedup: %.1fx (%d parallel subscribers vs 1)", speedup, numDirs)
	}
}
