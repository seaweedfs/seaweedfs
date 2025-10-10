package protocol

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
)

// partitionReader maintains a persistent connection to a single topic-partition
// and streams records forward, eliminating repeated offset lookups
// Now supports concurrent request processing via worker pool
type partitionReader struct {
	topicName     string
	partitionID   int32
	currentOffset int64
	subscriber    *integration.BrokerSubscriberSession
	fetchChan     chan *partitionFetchRequest
	closeChan     chan struct{}

	// Concurrent processing support
	workerSem    chan struct{} // Semaphore to limit concurrent workers
	subscriberMu sync.Mutex    // Protects subscriber and offset access

	handler *Handler
	connCtx *ConnectionContext
}

// partitionFetchRequest represents a request to fetch data from this partition
type partitionFetchRequest struct {
	requestedOffset int64
	maxBytes        int32
	resultChan      chan *partitionFetchResult
	isSchematized   bool
	apiVersion      uint16
}

// newPartitionReader creates and starts a new partition reader with concurrent worker pool
func newPartitionReader(ctx context.Context, handler *Handler, connCtx *ConnectionContext, topicName string, partitionID int32, startOffset int64) *partitionReader {
	pr := &partitionReader{
		topicName:     topicName,
		partitionID:   partitionID,
		currentOffset: startOffset,
		fetchChan:     make(chan *partitionFetchRequest, 20), // Buffer 20 requests
		closeChan:     make(chan struct{}),
		workerSem:     make(chan struct{}, 10), // Allow 10 concurrent workers per partition
		handler:       handler,
		connCtx:       connCtx,
	}

	// Start the dispatcher goroutine
	go pr.dispatcher(ctx)

	glog.V(1).Infof("[%s] Created partition reader for %s[%d] starting at offset %d (10 workers)",
		connCtx.ConnectionID, topicName, partitionID, startOffset)

	return pr
}

// dispatcher is the main loop that spawns worker goroutines for concurrent request processing
func (pr *partitionReader) dispatcher(ctx context.Context) {
	defer func() {
		glog.V(1).Infof("[%s] Partition reader dispatcher exiting for %s[%d]",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		pr.closeSubscriber()
	}()

	for {
		select {
		case <-ctx.Done():
			// Connection closed
			return
		case <-pr.closeChan:
			// Explicit close
			return
		case req := <-pr.fetchChan:
			// Spawn worker goroutine to handle this request concurrently
			go pr.worker(ctx, req)
		}
	}
}

// worker processes a single fetch request with semaphore-based concurrency control
func (pr *partitionReader) worker(ctx context.Context, req *partitionFetchRequest) {
	// Acquire semaphore slot (block until available or context cancelled)
	select {
	case pr.workerSem <- struct{}{}:
		// Got slot, will release on exit
		defer func() { <-pr.workerSem }()
	case <-ctx.Done():
		// Context cancelled, return empty
		req.resultChan <- &partitionFetchResult{}
		return
	}

	startTime := time.Now()
	result := &partitionFetchResult{}
	defer func() {
		result.fetchDuration = time.Since(startTime)
		select {
		case req.resultChan <- result:
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
			// Timeout sending result, drop it
			glog.Warningf("[%s] Timeout sending result for %s[%d]",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
		}
	}()

	// Lock for subscriber and offset access
	pr.subscriberMu.Lock()
	defer pr.subscriberMu.Unlock()

	// Check if offset rewind is needed
	if req.requestedOffset < pr.currentOffset {
		glog.V(1).Infof("[%s] Offset rewind detected for %s[%d]: current=%d requested=%d, recreating subscriber",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, pr.currentOffset, req.requestedOffset)
		pr.closeSubscriber()
		pr.currentOffset = req.requestedOffset
	}

	// Ensure we have a subscriber
	if pr.subscriber == nil {
		var err error
		pr.subscriber, err = pr.createSubscriber(ctx, pr.currentOffset)
		if err != nil {
			glog.Errorf("[%s] Failed to create subscriber for %s[%d]: %v",
				pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, err)
			result.errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			return
		}
	}

	// Get high water mark
	highWaterMark, err := pr.handler.seaweedMQHandler.GetLatestOffset(pr.topicName, pr.partitionID)
	if err != nil {
		glog.V(1).Infof("[%s] Failed to get HWM for %s[%d]: %v",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, err)
		highWaterMark = pr.currentOffset
	}
	result.highWaterMark = highWaterMark

	// Check if topic exists
	if !pr.handler.seaweedMQHandler.TopicExists(pr.topicName) {
		result.errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
		return
	}

	// Read records forward
	if pr.currentOffset < highWaterMark {
		recordBatch, newOffset := pr.readRecords(ctx, req.maxBytes, highWaterMark)
		result.recordBatch = recordBatch
		if newOffset > pr.currentOffset {
			pr.currentOffset = newOffset
		}
	} else {
		// No data available
		result.recordBatch = []byte{}
	}
}

// createSubscriber creates a new subscriber for this partition
func (pr *partitionReader) createSubscriber(ctx context.Context, startOffset int64) (*integration.BrokerSubscriberSession, error) {
	// Get the broker client from connection context
	brokerClient, ok := pr.connCtx.BrokerClient.(*integration.BrokerClient)
	if !ok || brokerClient == nil {
		return nil, fmt.Errorf("broker client not available")
	}

	subscriber, err := brokerClient.GetOrCreateSubscriber(pr.topicName, pr.partitionID, startOffset)
	if err != nil {
		return nil, fmt.Errorf("create subscriber: %w", err)
	}

	glog.V(1).Infof("[%s] Created subscriber for %s[%d] at offset %d",
		pr.connCtx.ConnectionID, pr.topicName, pr.partitionID, startOffset)

	return subscriber, nil
}

// closeSubscriber closes the current subscriber if it exists
func (pr *partitionReader) closeSubscriber() {
	if pr.subscriber != nil {
		if pr.subscriber.Stream != nil {
			_ = pr.subscriber.Stream.CloseSend()
		}
		pr.subscriber = nil
		glog.V(1).Infof("[%s] Closed subscriber for %s[%d]",
			pr.connCtx.ConnectionID, pr.topicName, pr.partitionID)
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
