package protocol

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// partitionReader maintains a persistent connection to a single topic-partition
// and streams records forward, eliminating repeated offset lookups
// Pre-fetches and buffers records for instant serving
type partitionReader struct {
	topicName     string
	partitionID   int32
	currentOffset int64
	fetchChan     chan *partitionFetchRequest
	closeChan     chan struct{}

	// Pre-fetch buffer support
	recordBuffer chan *bufferedRecords // Buffered pre-fetched records
	bufferMu     sync.Mutex            // Protects offset access

	handler *Handler
	connCtx *ConnectionContext
}

// bufferedRecords represents a batch of pre-fetched records
type bufferedRecords struct {
	recordBatch []byte
	startOffset int64
	endOffset   int64
}

// partitionFetchRequest represents a request to fetch data from this partition
type partitionFetchRequest struct {
	requestedOffset int64
	maxBytes        int32
	maxWaitMs       int32 // MaxWaitTime from Kafka fetch request
	resultChan      chan *partitionFetchResult
	isSchematized   bool
	apiVersion      uint16
}

// newPartitionReader creates and starts a new partition reader with pre-fetch buffering
func newPartitionReader(ctx context.Context, handler *Handler, connCtx *ConnectionContext, topicName string, partitionID int32, startOffset int64) *partitionReader {
	pr := &partitionReader{
		topicName:     topicName,
		partitionID:   partitionID,
		currentOffset: startOffset,
		fetchChan:     make(chan *partitionFetchRequest, 200), // Buffer 200 requests to handle Schema Registry's rapid polling in slow CI environments
		closeChan:     make(chan struct{}),
		recordBuffer:  make(chan *bufferedRecords, 5), // Buffer 5 batches of records
		handler:       handler,
		connCtx:       connCtx,
	}

	// Start the pre-fetch goroutine that continuously fetches ahead
	go pr.preFetchLoop(ctx)

	// Start the request handler goroutine
	go pr.handleRequests(ctx)

	glog.V(4).Infof("[%s] Created partition reader for %s[%d] starting at offset %d (sequential with ch=200)",
		connCtx.ConnectionID, topicName, partitionID, startOffset)

	return pr
}

// preFetchLoop is disabled for SMQ backend to prevent subscriber storms
// SMQ reads from disk and creating multiple concurrent subscribers causes
// broker overload and partition shutdowns. Fetch requests are handled
// on-demand in serveFetchRequest instead.
func (pr *partitionReader) preFetchLoop(ctx context.Context) {
	defer func() {
		glog.V(4).Infof("[%s] Pre-fetch loop exiting for %s[%d]",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		close(pr.recordBuffer)
	}()

	// Wait for shutdown - no continuous pre-fetching to avoid overwhelming the broker
	select {
	case <-ctx.Done():
		return
	case <-pr.closeChan:
		return
	}
}

// handleRequests serves fetch requests SEQUENTIALLY to prevent subscriber storm
// Sequential processing is essential for SMQ backend because:
// 1. GetStoredRecords may create a new subscriber on each call
// 2. Concurrent calls create multiple subscribers for the same partition
// 3. This overwhelms the broker and causes partition shutdowns
func (pr *partitionReader) handleRequests(ctx context.Context) {
	defer func() {
		glog.V(4).Infof("[%s] Request handler exiting for %s[%d]",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.closeChan:
			return
		case req := <-pr.fetchChan:
			// Process sequentially to prevent subscriber storm
			pr.serveFetchRequest(ctx, req)
		}
	}
}

// serveFetchRequest fetches data on-demand (no pre-fetching)
func (pr *partitionReader) serveFetchRequest(ctx context.Context, req *partitionFetchRequest) {
	startTime := time.Now()
	result := &partitionFetchResult{}

	defer func() {
		result.fetchDuration = time.Since(startTime)

		// Send result back to client
		select {
		case req.resultChan <- result:
			// Successfully sent
		case <-ctx.Done():
			glog.Warningf("[%s] Context cancelled while sending result for %s[%d]",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		case <-time.After(50 * time.Millisecond):
			glog.Warningf("[%s] Timeout sending result for %s[%d] - CLIENT MAY HAVE DISCONNECTED",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		}
	}()

	// Get high water mark
	hwmUnknown := false
	hwm, hwmErr := pr.handler.seaweedMQHandler.GetLatestOffset(pr.topicName, pr.partitionID)
	if hwmErr != nil {
		// HWM lookup can fail when the partition has been deactivated between consumer
		// sessions. Proceed with the fetch anyway — the broker will return the correct
		// data (or empty) based on its own state. Use math.MaxInt64 as sentinel so
		// FetchMultipleBatches doesn't artificially cap recordsAvailable, and we
		// don't hit the early-return below. The actual HWM will be derived from
		// the fetch result (newOffset) after the read.
		glog.Warningf("[%s] HWM lookup failed for %s[%d]: %v — will attempt fetch anyway",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, hwmErr)
		hwm = math.MaxInt64
		hwmUnknown = true
	}
	result.highWaterMark = hwm

	glog.V(2).Infof("[%s] HWM for %s[%d]: %d (requested: %d)",
		pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, hwm, req.requestedOffset)

	// If requested offset >= HWM, return immediately with empty result
	// This prevents overwhelming the broker with futile read attempts when no data is available
	if req.requestedOffset >= hwm {
		result.recordBatch = []byte{}
		glog.V(3).Infof("[%s] Requested offset %d >= HWM %d, returning empty",
			pr.connCtx.ConnectionID, req.requestedOffset, hwm)
		return
	}

	// Update tracking offset to match requested offset
	pr.bufferMu.Lock()
	if req.requestedOffset != pr.currentOffset {
		glog.V(3).Infof("[%s] Updating currentOffset for %s[%d]: %d -> %d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, pr.currentOffset, req.requestedOffset)
		pr.currentOffset = req.requestedOffset
	}
	pr.bufferMu.Unlock()

	// Fetch on-demand - no pre-fetching to avoid overwhelming the broker
	recordBatch, newOffset := pr.readRecords(ctx, req.requestedOffset, req.maxBytes, req.maxWaitMs, hwm)

	// When HWM was unknown, derive a reasonable value from the fetch result
	// so the client sees a meaningful high water mark instead of MaxInt64.
	if hwmUnknown {
		if newOffset > req.requestedOffset {
			result.highWaterMark = newOffset // best estimate: end of what we read
		} else {
			result.highWaterMark = req.requestedOffset // no data found
		}
	}

	// Log what we got back - DETAILED for diagnostics
	if len(recordBatch) == 0 {
		glog.V(2).Infof("[%s] FETCH %s[%d]: readRecords returned EMPTY (offset=%d, hwm=%d)",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, req.requestedOffset, result.highWaterMark)
		result.recordBatch = []byte{}
	} else {
		result.recordBatch = recordBatch
		pr.bufferMu.Lock()
		pr.currentOffset = newOffset
		pr.bufferMu.Unlock()
	}
}

// readRecords reads records forward using the multi-batch fetcher
func (pr *partitionReader) readRecords(ctx context.Context, fromOffset int64, maxBytes int32, maxWaitMs int32, highWaterMark int64) ([]byte, int64) {
	fetchStartTime := time.Now()

	// Create context with timeout based on Kafka fetch request's MaxWaitTime
	// This ensures we wait exactly as long as the client requested
	fetchCtx := ctx
	if maxWaitMs > 0 {
		var cancel context.CancelFunc
		// Use 1.5x the client timeout to account for internal processing overhead
		// This prevents legitimate slow reads from being killed by client timeout
		internalTimeoutMs := int32(float64(maxWaitMs) * 1.5)
		if internalTimeoutMs > 5000 {
			internalTimeoutMs = 5000 // Cap at 5 seconds
		}
		fetchCtx, cancel = context.WithTimeout(ctx, time.Duration(internalTimeoutMs)*time.Millisecond)
		defer cancel()
	}

	// Use multi-batch fetcher for better MaxBytes compliance
	multiFetcher := NewMultiBatchFetcher(pr.handler)
	startTime := time.Now()
	fetchResult, err := multiFetcher.FetchMultipleBatches(
		fetchCtx,
		pr.topicName,
		pr.partitionID,
		fromOffset,
		highWaterMark,
		maxBytes,
	)
	fetchDuration := time.Since(startTime)

	// Log slow fetches (potential hangs)
	if fetchDuration > 2*time.Second {
		glog.Warningf("[%s] SLOW FETCH for %s[%d]: offset=%d took %.2fs (maxWait=%dms, HWM=%d)",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, fromOffset, fetchDuration.Seconds(), maxWaitMs, highWaterMark)
	}

	if err == nil && fetchResult.TotalSize > 0 {
		glog.V(4).Infof("[%s] Multi-batch fetch for %s[%d]: %d batches, %d bytes, offset %d -> %d (duration: %v)",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			fetchResult.BatchCount, fetchResult.TotalSize, fromOffset, fetchResult.NextOffset, fetchDuration)
		return fetchResult.RecordBatches, fetchResult.NextOffset
	}

	// Multi-batch failed - try single batch with a fresh timeout
	glog.Warningf("[%s] Multi-batch fetch failed for %s[%d] offset=%d after %v, falling back to single-batch (err: %v)",
		pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, fromOffset, fetchDuration, err)

	// Compute the remaining time budget for the fallback. If the parent
	// context carries a deadline, honour it; otherwise derive remaining
	// from maxWaitMs minus elapsed time. This prevents the fallback from
	// restarting the full budget after the multi-batch fetch already
	// consumed part of it.
	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	} else {
		remaining = time.Duration(maxWaitMs)*time.Millisecond - time.Since(fetchStartTime)
	}
	if remaining <= 0 {
		// Budget exhausted — skip the fallback entirely.
		glog.V(2).Infof("[%s] No remaining budget for fallback on %s[%d] (maxWait=%dms, elapsed=%v)",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, maxWaitMs, time.Since(fetchStartTime))
		return []byte{}, fromOffset
	}
	// Clamp: floor of 2s so disk reads via gRPC have a realistic chance,
	// but never exceed 10s to bound data-plane blocking.
	fallbackTimeout := remaining
	if fallbackTimeout < 2*time.Second {
		fallbackTimeout = 2 * time.Second
	}
	if fallbackTimeout > 10*time.Second {
		fallbackTimeout = 10 * time.Second
	}
	fallbackCtx, fallbackCancel := context.WithTimeout(ctx, fallbackTimeout)
	defer fallbackCancel()
	fallbackStartTime := time.Now()
	smqRecords, err := pr.handler.seaweedMQHandler.GetStoredRecords(fallbackCtx, pr.topicName, pr.partitionID, fromOffset, 10)
	fallbackDuration := time.Since(fallbackStartTime)

	if fallbackDuration > 2*time.Second {
		glog.Warningf("[%s] SLOW FALLBACK for %s[%d]: offset=%d took %.2fs",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, fromOffset, fallbackDuration.Seconds())
	}

	if err != nil {
		glog.Errorf("[%s] CRITICAL: Both multi-batch AND fallback failed for %s[%d] offset=%d: %v",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, fromOffset, err)
		return []byte{}, fromOffset
	}

	if len(smqRecords) > 0 {
		recordBatch := pr.handler.constructRecordBatchFromSMQ(pr.topicName, fromOffset, smqRecords)
		nextOffset := fromOffset + int64(len(smqRecords))
		glog.V(3).Infof("[%s] Fallback succeeded: got %d records for %s[%d] offset %d -> %d (total: %v)",
			pr.connCtx.ConnectionID, len(smqRecords), pr.topicName, pr.partitionID, fromOffset, nextOffset, time.Since(fetchStartTime))
		return recordBatch, nextOffset
	}

	// No records available
	glog.V(3).Infof("[%s] No records available for %s[%d] offset=%d after multi-batch and fallback (total: %v)",
		pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, fromOffset, time.Since(fetchStartTime))
	return []byte{}, fromOffset
}

// close signals the reader to shut down
func (pr *partitionReader) close() {
	close(pr.closeChan)
}
