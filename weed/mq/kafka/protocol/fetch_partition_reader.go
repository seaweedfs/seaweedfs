package protocol

import (
	"context"
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
	recordBatch   []byte
	startOffset   int64
	endOffset     int64
	highWaterMark int64
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

	glog.V(2).Infof("[%s] Created partition reader for %s[%d] starting at offset %d (sequential with ch=200)",
		connCtx.ConnectionID, topicName, partitionID, startOffset)

	return pr
}

// preFetchLoop is disabled for SMQ backend to prevent subscriber storms
// SMQ reads from disk and creating multiple concurrent subscribers causes
// broker overload and partition shutdowns. Fetch requests are handled
// on-demand in serveFetchRequest instead.
func (pr *partitionReader) preFetchLoop(ctx context.Context) {
	defer func() {
		glog.V(2).Infof("[%s] Pre-fetch loop exiting for %s[%d]",
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
// CRITICAL: Sequential processing is essential for SMQ backend because:
// 1. GetStoredRecords may create a new subscriber on each call
// 2. Concurrent calls create multiple subscribers for the same partition
// 3. This overwhelms the broker and causes partition shutdowns
func (pr *partitionReader) handleRequests(ctx context.Context) {
	defer func() {
		glog.V(2).Infof("[%s] Request handler exiting for %s[%d]",
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
		select {
		case req.resultChan <- result:
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			glog.Warningf("[%s] Timeout sending result for %s[%d]",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		}
	}()

	// Get high water mark
	hwm, hwmErr := pr.handler.seaweedMQHandler.GetLatestOffset(pr.topicName, pr.partitionID)
	result.highWaterMark = hwm

	// Debug logging for _schemas topic
	isSchemasTopic := pr.topicName == "_schemas"
	if isSchemasTopic {
		glog.Infof("[SCHEMAS DEBUG] Fetch request: topic=%s partition=%d requestedOffset=%d hwm=%d hwmErr=%v",
			pr.topicName, pr.partitionID, req.requestedOffset, hwm, hwmErr)
	}

	// CRITICAL: If requested offset >= HWM, return immediately with empty result
	// This prevents overwhelming the broker with futile read attempts when no data is available
	if req.requestedOffset >= hwm {
		result.recordBatch = []byte{}
		if isSchemasTopic {
			glog.Infof("[SCHEMAS DEBUG] No data: requestedOffset=%d >= hwm=%d", req.requestedOffset, hwm)
		}
		glog.V(3).Infof("[%s] No data available for %s[%d]: offset=%d >= hwm=%d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, req.requestedOffset, hwm)
		return
	}

	// Update tracking offset to match requested offset
	pr.bufferMu.Lock()
	if req.requestedOffset != pr.currentOffset {
		glog.V(2).Infof("[%s] Offset seek for %s[%d]: requested=%d current=%d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, req.requestedOffset, pr.currentOffset)
		pr.currentOffset = req.requestedOffset
	}
	pr.bufferMu.Unlock()

	// Fetch on-demand - no pre-fetching to avoid overwhelming the broker
	// Pass the requested offset and maxWaitMs directly to avoid race conditions
	recordBatch, newOffset := pr.readRecords(ctx, req.requestedOffset, req.maxBytes, req.maxWaitMs, hwm)
	if len(recordBatch) > 0 && newOffset > pr.currentOffset {
		result.recordBatch = recordBatch
		pr.bufferMu.Lock()
		pr.currentOffset = newOffset
		pr.bufferMu.Unlock()
		glog.V(2).Infof("[%s] On-demand fetch for %s[%d]: offset %d->%d, %d bytes",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			req.requestedOffset, newOffset, len(recordBatch))
	} else {
		result.recordBatch = []byte{}
	}
}

// readRecords reads records forward using the multi-batch fetcher
func (pr *partitionReader) readRecords(ctx context.Context, fromOffset int64, maxBytes int32, maxWaitMs int32, highWaterMark int64) ([]byte, int64) {
	isSchemasTopic := pr.topicName == "_schemas"

	// Create context with timeout based on Kafka fetch request's MaxWaitTime
	// This ensures we wait exactly as long as the client requested
	fetchCtx := ctx
	if maxWaitMs > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(ctx, time.Duration(maxWaitMs)*time.Millisecond)
		defer cancel()
	}

	// Use multi-batch fetcher for better MaxBytes compliance
	multiFetcher := NewMultiBatchFetcher(pr.handler)
	fetchResult, err := multiFetcher.FetchMultipleBatches(
		fetchCtx,
		pr.topicName,
		pr.partitionID,
		fromOffset,
		highWaterMark,
		maxBytes,
	)

	if isSchemasTopic {
		glog.Infof("[SCHEMAS DEBUG] Multi-batch fetch result: fromOffset=%d hwm=%d err=%v totalSize=%d batchCount=%d nextOffset=%d",
			fromOffset, highWaterMark, err, fetchResult.TotalSize, fetchResult.BatchCount, fetchResult.NextOffset)
	}

	if err == nil && fetchResult.TotalSize > 0 {
		glog.V(2).Infof("[%s] Multi-batch fetch for %s[%d]: %d batches, %d bytes, offset %d -> %d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			fetchResult.BatchCount, fetchResult.TotalSize, fromOffset, fetchResult.NextOffset)
		return fetchResult.RecordBatches, fetchResult.NextOffset
	}

	// Fallback to single batch (pass context to respect timeout)
	smqRecords, err := pr.handler.seaweedMQHandler.GetStoredRecords(fetchCtx, pr.topicName, pr.partitionID, fromOffset, 10)
	if err == nil && len(smqRecords) > 0 {
		recordBatch := pr.handler.constructRecordBatchFromSMQ(pr.topicName, fromOffset, smqRecords)
		nextOffset := fromOffset + int64(len(smqRecords))
		glog.V(2).Infof("[%s] Single-batch fetch for %s[%d]: %d records, %d bytes, offset %d -> %d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			len(smqRecords), len(recordBatch), fromOffset, nextOffset)
		return recordBatch, nextOffset
	}

	// No records available
	return []byte{}, fromOffset
}

// close signals the reader to shut down
func (pr *partitionReader) close() {
	close(pr.closeChan)
}
