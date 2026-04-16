package page_writer

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestWriteBufferCap_SharedAcrossPipelines exercises the full write-budget
// plumbing end-to-end: one WriteBufferAccountant shared across several
// UploadPipeline instances (modeling multiple open file handles in a
// single mount), a deliberately stalled saveFn (modeling the reported
// "volume server rejecting uploads" condition), and many concurrent
// writers.  Without the cap this scenario is what filled /tmp to 1.8 TB;
// with the cap, Used() must never exceed the configured budget and
// writers must backpressure instead of allocating unboundedly.
func TestWriteBufferCap_SharedAcrossPipelines(t *testing.T) {
	const (
		chunkSize    = 64 * 1024
		capChunks    = 4 // global budget: 4 chunks across all pipelines
		numPipelines = 3
		numWriters   = 6
		writesEach   = 8 // more than enough to exceed capChunks if unbounded
	)
	capBytes := int64(capChunks * chunkSize)
	acc := NewWriteBufferAccountant(capBytes)

	// Gate every upload so sealed chunks cannot drain until the test says
	// so — this is what reproduces the "stalled volume server" scenario.
	var inflight atomic.Int64
	var maxInflight atomic.Int64
	release := make(chan struct{})
	saveFn := func(_ io.Reader, _, _, _ int64, cleanup func()) {
		cur := inflight.Add(1)
		for {
			prev := maxInflight.Load()
			if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		if cleanup != nil {
			cleanup()
		}
	}

	// Sample Used() frequently on a background goroutine so we can catch
	// any moment the accountant exceeds the cap.
	var observedMax atomic.Int64
	stopSampling := make(chan struct{})
	var samplerWg sync.WaitGroup
	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		for {
			select {
			case <-stopSampling:
				return
			default:
			}
			u := acc.Used()
			for {
				prev := observedMax.Load()
				if u <= prev || observedMax.CompareAndSwap(prev, u) {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Build N pipelines, each with its own uploader executor so chunks
	// can actually enter the upload stage and hit the gated saveFn.
	pipelines := make([]*UploadPipeline, numPipelines)
	for i := range pipelines {
		up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(4), chunkSize, saveFn, nil, 2, t.TempDir(), acc)
		pipelines[i] = up
	}

	// Launch numWriters goroutines, round-robin across pipelines. Each
	// writer writes writesEach full chunks, on distinct logical chunk
	// indices so every write triggers a new reservation.
	var writerWg sync.WaitGroup
	writerStart := make(chan struct{})
	writerWg.Add(numWriters)
	for w := 0; w < numWriters; w++ {
		w := w
		go func() {
			defer writerWg.Done()
			<-writerStart
			up := pipelines[w%numPipelines]
			data := make([]byte, chunkSize)
			for k := 0; k < writesEach; k++ {
				// Offset each writer into a distinct region of the file
				// so chunks don't collide on the same logicChunkIndex.
				off := int64(w)*int64(writesEach)*chunkSize + int64(k)*chunkSize
				if _, err := up.SaveDataAt(data, off, true, int64(k+1)); err != nil {
					t.Errorf("writer %d: SaveDataAt: %v", w, err)
					return
				}
			}
		}()
	}
	close(writerStart)

	// Give the writers time to pile up against the cap. Any unbounded
	// allocation would show up as Used() > capBytes; the sampler catches
	// that. Writers should be blocked on Reserve well before writesEach
	// chunks are created per pipeline.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if acc.Used() >= capBytes {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := acc.Used(); got < capBytes/2 {
		t.Fatalf("expected writers to fill the budget, used=%d cap=%d", got, capBytes)
	}

	// While the gate is closed and writers are blocked, Used() must stay
	// at or below the cap. Sample explicitly for a short window to catch
	// any overshoot.
	for i := 0; i < 50; i++ {
		if u := acc.Used(); u > capBytes {
			t.Fatalf("Used()=%d exceeded cap=%d while writers were stalled", u, capBytes)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Open the gate: every sealed chunk completes, writers drain.
	close(release)

	// Wait for all writers to finish.
	writerDone := make(chan struct{})
	go func() {
		writerWg.Wait()
		close(writerDone)
	}()
	select {
	case <-writerDone:
	case <-time.After(10 * time.Second):
		close(stopSampling)
		samplerWg.Wait()
		t.Fatalf("writers did not drain after release; used=%d", acc.Used())
	}

	// Shut down all pipelines and stop sampling.
	for _, up := range pipelines {
		up.Shutdown()
	}
	close(stopSampling)
	samplerWg.Wait()

	if got := observedMax.Load(); got > capBytes {
		t.Fatalf("observed Used()=%d exceeded cap=%d", got, capBytes)
	}
	if got := acc.Used(); got != 0 {
		t.Fatalf("expected 0 used after shutdown, got %d", got)
	}

	t.Logf("peak inflight uploads=%d, peak Used()=%d bytes (cap=%d)",
		maxInflight.Load(), observedMax.Load(), capBytes)
}
