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
		fetchChan:     make(chan *partitionFetchRequest, 10), // Buffer 10 requests
		closeChan:     make(chan struct{}),
		recordBuffer:  make(chan *bufferedRecords, 5), // Buffer 5 batches of records
		handler:       handler,
		connCtx:       connCtx,
	}

	// Start the pre-fetch goroutine that continuously fetches ahead
	go pr.preFetchLoop(ctx)

	// Start the request handler goroutine
	go pr.handleRequests(ctx)

	glog.V(2).Infof("[%s] Created partition reader for %s[%d] starting at offset %d (pre-fetch buffer)",
		connCtx.ConnectionID, topicName, partitionID, startOffset)

	return pr
}

// preFetchLoop continuously fetches records ahead and fills the buffer
func (pr *partitionReader) preFetchLoop(ctx context.Context) {
	defer func() {
		glog.V(2).Infof("[%s] Pre-fetch loop exiting for %s[%d]",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		close(pr.recordBuffer)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.closeChan:
			return
		default:
			// Try to fetch next batch if buffer has space
			pr.bufferMu.Lock()

			// Check if topic exists
			if !pr.handler.seaweedMQHandler.TopicExists(pr.topicName) {
				pr.bufferMu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Get high water mark
			highWaterMark, err := pr.handler.seaweedMQHandler.GetLatestOffset(pr.topicName, pr.partitionID)
			if err != nil {
				pr.bufferMu.Unlock()
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Fetch next batch if there's data available
			if pr.currentOffset < highWaterMark {
				recordBatch, newOffset := pr.readRecords(ctx, 1024*1024, highWaterMark) // Fetch 1MB batches

				// CRITICAL: Don't buffer empty results to avoid channel saturation
				// If readRecords returns empty (no data fetched), skip buffering and backoff
				if len(recordBatch) == 0 || newOffset == pr.currentOffset {
					pr.bufferMu.Unlock()
					glog.V(3).Infof("[%s] Pre-fetch returned empty for %s[%d] (offset=%d, HWM=%d), backing off",
						pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, pr.currentOffset, highWaterMark)
					time.Sleep(200 * time.Millisecond) // Longer backoff for empty results
					continue
				}

				buffered := &bufferedRecords{
					recordBatch:   recordBatch,
					startOffset:   pr.currentOffset,
					endOffset:     newOffset,
					highWaterMark: highWaterMark,
				}
				pr.currentOffset = newOffset
				pr.bufferMu.Unlock()

				// Send to buffer (blocks if buffer is full)
				select {
				case pr.recordBuffer <- buffered:
					glog.V(2).Infof("[%s] Buffered records for %s[%d]: offset %d->%d, %d bytes",
						pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
						buffered.startOffset, buffered.endOffset, len(recordBatch))
				case <-ctx.Done():
					return
				case <-pr.closeChan:
					return
				}
			} else {
				pr.bufferMu.Unlock()
				// No data available, wait a bit before checking again
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

// handleRequests serves fetch requests from the pre-fetched buffer
func (pr *partitionReader) handleRequests(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.closeChan:
			return
		case req := <-pr.fetchChan:
			pr.serveFetchRequest(ctx, req)
		}
	}
}

// serveFetchRequest serves a fetch request from the buffer or waits for pre-fetch
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
	hwm, _ := pr.handler.seaweedMQHandler.GetLatestOffset(pr.topicName, pr.partitionID)
	result.highWaterMark = hwm

	// Check if offset seek is needed
	pr.bufferMu.Lock()
	needSeek := req.requestedOffset < pr.currentOffset
	if needSeek {
		// Offset rewind - drain buffer and reset
		glog.V(2).Infof("[%s] Offset seek for %s[%d]: requested=%d current=%d, draining buffer",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, req.requestedOffset, pr.currentOffset)
		// Drain the buffer
		for len(pr.recordBuffer) > 0 {
			<-pr.recordBuffer
		}
		// Reset offset - subscriber will be recreated by GetStoredRecords with the new offset
		pr.currentOffset = req.requestedOffset
	}
	pr.bufferMu.Unlock()

	// Try to get buffered records (with reasonable timeout for pre-fetch to complete)
	select {
	case buffered, ok := <-pr.recordBuffer:
		if !ok {
			// Buffer closed
			result.recordBatch = []byte{}
			return
		}
		// Check if buffered offset matches request (allowing for forward reads)
		if buffered.startOffset <= req.requestedOffset && req.requestedOffset < buffered.endOffset {
			// Perfect match - serve from buffer
			result.recordBatch = buffered.recordBatch
			glog.V(2).Infof("[%s] Served from buffer for %s[%d]: %d bytes (offset %d->%d)",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
				len(buffered.recordBatch), buffered.startOffset, buffered.endOffset)
		} else if buffered.endOffset <= req.requestedOffset {
			// Buffer is behind - return empty (pre-fetch will catch up)
			result.recordBatch = []byte{}
			glog.V(2).Infof("[%s] Buffer behind for %s[%d] (buffered %d->%d, requested %d), returning empty",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
				buffered.startOffset, buffered.endOffset, req.requestedOffset)
		} else {
			// Buffer is ahead - return empty (client will retry)
			result.recordBatch = []byte{}
			glog.V(2).Infof("[%s] Buffer ahead for %s[%d] (buffered %d->%d, requested %d), returning empty",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
				buffered.startOffset, buffered.endOffset, req.requestedOffset)
		}
	case <-time.After(30 * time.Millisecond):
		// Buffer empty or taking too long, return empty result
		result.recordBatch = []byte{}
		glog.V(2).Infof("[%s] Buffer timeout for %s[%d], returning empty",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
	case <-ctx.Done():
		result.recordBatch = []byte{}
	}
}

// readRecords reads records forward using the multi-batch fetcher
func (pr *partitionReader) readRecords(ctx context.Context, maxBytes int32, highWaterMark int64) ([]byte, int64) {
	// Use multi-batch fetcher for better MaxBytes compliance
	multiFetcher := NewMultiBatchFetcher(pr.handler)
	fetchResult, err := multiFetcher.FetchMultipleBatches(
		pr.topicName,
		pr.partitionID,
		pr.currentOffset,
		highWaterMark,
		maxBytes,
	)

	if err == nil && fetchResult.TotalSize > 0 {
		glog.V(2).Infof("[%s] Multi-batch fetch for %s[%d]: %d batches, %d bytes, offset %d -> %d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			fetchResult.BatchCount, fetchResult.TotalSize, pr.currentOffset, fetchResult.NextOffset)
		return fetchResult.RecordBatches, fetchResult.NextOffset
	}

	// Fallback to single batch
	smqRecords, err := pr.handler.seaweedMQHandler.GetStoredRecords(pr.topicName, pr.partitionID, pr.currentOffset, 10)
	if err == nil && len(smqRecords) > 0 {
		recordBatch := pr.handler.constructRecordBatchFromSMQ(pr.topicName, pr.currentOffset, smqRecords)
		nextOffset := pr.currentOffset + int64(len(smqRecords))
		glog.V(2).Infof("[%s] Single-batch fetch for %s[%d]: %d records, %d bytes, offset %d -> %d",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID,
			len(smqRecords), len(recordBatch), pr.currentOffset, nextOffset)
		return recordBatch, nextOffset
	}

	// No records available
	return []byte{}, pr.currentOffset
}

// close signals the reader to shut down
func (pr *partitionReader) close() {
	close(pr.closeChan)
}
