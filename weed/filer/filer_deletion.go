package filer

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

const (
	// Maximum number of retry attempts for failed deletions
	MaxRetryAttempts = 10
	// Initial retry delay (will be doubled with each attempt)
	InitialRetryDelay = 5 * time.Minute
	// Maximum retry delay
	MaxRetryDelay = 6 * time.Hour
	// Interval for checking retry queue for ready items
	DeletionRetryPollInterval = 1 * time.Minute
	// Maximum number of items to process per retry iteration
	DeletionRetryBatchSize = 1000
	// Maximum number of error details to include in log messages
	MaxLoggedErrorDetails = 10
	// Interval for polling the deletion queue for new items
	DeletionPollInterval = 1123 * time.Millisecond
)

// DeletionRetryItem represents a file deletion that failed and needs to be retried
type DeletionRetryItem struct {
	FileId      string
	RetryCount  int
	NextRetryAt time.Time
	LastError   string
	heapIndex   int // index in the heap (for heap.Interface)
}

// retryHeap implements heap.Interface for DeletionRetryItem
// Items are ordered by NextRetryAt (earliest first)
type retryHeap []*DeletionRetryItem

// Compile-time assertion that retryHeap implements heap.Interface
var _ heap.Interface = (*retryHeap)(nil)

func (h retryHeap) Len() int { return len(h) }

func (h retryHeap) Less(i, j int) bool {
	return h[i].NextRetryAt.Before(h[j].NextRetryAt)
}

func (h retryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *retryHeap) Push(x any) {
	item := x.(*DeletionRetryItem)
	item.heapIndex = len(*h)
	*h = append(*h, item)
}

func (h *retryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil      // avoid memory leak
	item.heapIndex = -1 // mark as removed
	*h = old[0 : n-1]
	return item
}

// DeletionRetryQueue manages the queue of failed deletions that need to be retried.
// Uses a min-heap ordered by NextRetryAt for efficient retrieval of ready items.
//
// LIMITATION: Current implementation stores retry queue in memory only.
// On filer restart, all pending retries are lost. With MaxRetryDelay up to 6 hours,
// process restarts during this window will cause retry state loss.
//
// TODO: Consider persisting retry queue to durable storage for production resilience:
//   - Option 1: Leverage existing Filer store (KV operations)
//   - Option 2: Periodic snapshots to disk with recovery on startup
//   - Option 3: Write-ahead log for retry queue mutations
//   - Trade-offs: Performance vs durability, complexity vs reliability
//
// For now, accepting in-memory storage as pragmatic initial implementation.
// Lost retries will be eventually consistent as files remain in deletion queue.
type DeletionRetryQueue struct {
	heap      retryHeap
	itemIndex map[string]*DeletionRetryItem // for O(1) lookup by FileId
	lock      sync.Mutex
}

// NewDeletionRetryQueue creates a new retry queue
func NewDeletionRetryQueue() *DeletionRetryQueue {
	q := &DeletionRetryQueue{
		heap:      make(retryHeap, 0),
		itemIndex: make(map[string]*DeletionRetryItem),
	}
	heap.Init(&q.heap)
	return q
}

// calculateBackoff calculates the exponential backoff delay for a given retry count.
// Uses exponential backoff formula: InitialRetryDelay * 2^(retryCount-1)
// The first retry (retryCount=1) uses InitialRetryDelay, second uses 2x, third uses 4x, etc.
// Includes overflow protection and caps at MaxRetryDelay.
func calculateBackoff(retryCount int) time.Duration {
	// The first retry is attempt 1, but shift should start at 0
	shiftAmount := uint(retryCount - 1)
	if shiftAmount > 63 {
		// Prevent overflow: use max delay directly
		return MaxRetryDelay
	}

	delay := InitialRetryDelay * time.Duration(1<<shiftAmount)

	// Additional safety check for overflow: if delay wrapped around to negative or zero
	if delay <= 0 || delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}

	return delay
}

// AddOrUpdate adds a new failed deletion or updates an existing one
// Time complexity: O(log N) for insertion/update
func (q *DeletionRetryQueue) AddOrUpdate(fileId string, errorMsg string) {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Check if item already exists
	if item, exists := q.itemIndex[fileId]; exists {
		item.RetryCount++
		item.LastError = errorMsg
		delay := calculateBackoff(item.RetryCount)
		item.NextRetryAt = time.Now().Add(delay)
		// Re-heapify since NextRetryAt changed
		heap.Fix(&q.heap, item.heapIndex)
		glog.V(2).Infof("updated retry for %s: attempt %d, next retry in %v", fileId, item.RetryCount, delay)
		return
	}

	// Add new item
	delay := InitialRetryDelay
	item := &DeletionRetryItem{
		FileId:      fileId,
		RetryCount:  1,
		NextRetryAt: time.Now().Add(delay),
		LastError:   errorMsg,
	}
	heap.Push(&q.heap, item)
	q.itemIndex[fileId] = item
	glog.V(2).Infof("added retry for %s: next retry in %v", fileId, delay)
}

// RequeueForRetry re-adds a previously failed item back to the queue with incremented retry count.
// This method MUST be used when re-queuing items from processRetryBatch to preserve retry state.
// Time complexity: O(log N) for insertion
func (q *DeletionRetryQueue) RequeueForRetry(item *DeletionRetryItem, errorMsg string) {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Increment retry count
	item.RetryCount++
	item.LastError = errorMsg

	// Calculate next retry time with exponential backoff
	delay := calculateBackoff(item.RetryCount)
	item.NextRetryAt = time.Now().Add(delay)
	glog.V(2).Infof("requeued retry for %s: attempt %d, next retry in %v", item.FileId, item.RetryCount, delay)

	// Re-add to heap and index
	heap.Push(&q.heap, item)
	q.itemIndex[item.FileId] = item
}

// GetReadyItems returns items that are ready to be retried and removes them from the queue
// Time complexity: O(K log N) where K is the number of ready items
// Items are processed in order of NextRetryAt (earliest first)
func (q *DeletionRetryQueue) GetReadyItems(maxItems int) []*DeletionRetryItem {
	q.lock.Lock()
	defer q.lock.Unlock()

	now := time.Now()
	var readyItems []*DeletionRetryItem

	// Peek at items from the top of the heap (earliest NextRetryAt)
	for len(q.heap) > 0 && len(readyItems) < maxItems {
		item := q.heap[0]

		// If the earliest item is not ready yet, no other items are ready either
		if item.NextRetryAt.After(now) {
			break
		}

		// Remove from heap and index
		heap.Pop(&q.heap)
		delete(q.itemIndex, item.FileId)

		if item.RetryCount < MaxRetryAttempts {
			readyItems = append(readyItems, item)
		} else {
			// Max attempts reached, log and discard
			glog.Warningf("max retry attempts (%d) reached for %s, last error: %s", MaxRetryAttempts, item.FileId, item.LastError)
		}
	}

	return readyItems
}

// Size returns the current size of the retry queue
func (q *DeletionRetryQueue) Size() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return len(q.heap)
}

func LookupByMasterClientFn(masterClient *wdclient.MasterClient) func(vids []string) (map[string]*operation.LookupResult, error) {
	return func(vids []string) (map[string]*operation.LookupResult, error) {
		m := make(map[string]*operation.LookupResult)
		for _, vid := range vids {
			locs, _ := masterClient.GetVidLocations(vid)
			var locations []operation.Location
			for _, loc := range locs {
				locations = append(locations, operation.Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
					GrpcPort:  loc.GrpcPort,
				})
			}
			m[vid] = &operation.LookupResult{
				VolumeOrFileId: vid,
				Locations:      locations,
			}
		}
		return m, nil
	}
}

func (f *Filer) loopProcessingDeletion() {

	lookupFunc := LookupByMasterClientFn(f.MasterClient)

	DeletionBatchSize := 100000 // roughly 20 bytes cost per file id.

	// Create retry queue
	retryQueue := NewDeletionRetryQueue()

	// Start retry processor in a separate goroutine
	go f.loopProcessingDeletionRetry(lookupFunc, retryQueue)

	ticker := time.NewTicker(DeletionPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.deletionQuit:
			glog.V(0).Infof("deletion processor shutting down")
			return
		case <-ticker.C:
			f.fileIdDeletionQueue.Consume(func(fileIds []string) {
				for len(fileIds) > 0 {
					var toDeleteFileIds []string
					if len(fileIds) > DeletionBatchSize {
						toDeleteFileIds = fileIds[:DeletionBatchSize]
						fileIds = fileIds[DeletionBatchSize:]
					} else {
						toDeleteFileIds = fileIds
						fileIds = fileIds[:0]
					}
					f.processDeletionBatch(toDeleteFileIds, lookupFunc, retryQueue)
				}
			})
		}
	}
}

// processDeletionBatch handles deletion of a batch of file IDs and processes results.
// It classifies errors into retryable and permanent categories, adds retryable failures
// to the retry queue, and logs appropriate messages.
func (f *Filer) processDeletionBatch(toDeleteFileIds []string, lookupFunc func([]string) (map[string]*operation.LookupResult, error), retryQueue *DeletionRetryQueue) {
	results := operation.DeleteFileIdsWithLookupVolumeId(f.GrpcDialOption, toDeleteFileIds, lookupFunc)

	// Process individual results for better error tracking
	var successCount, notFoundCount, retryableErrorCount, permanentErrorCount int
	var errorDetails []string

	for _, result := range results {
		if result.Error == "" {
			successCount++
		} else if result.Error == "not found" || strings.Contains(result.Error, storage.ErrorDeleted.Error()) {
			// Already deleted - acceptable
			notFoundCount++
		} else if isRetryableError(result.Error) {
			// Retryable error - add to retry queue
			retryableErrorCount++
			retryQueue.AddOrUpdate(result.FileId, result.Error)
			if len(errorDetails) < MaxLoggedErrorDetails {
				errorDetails = append(errorDetails, result.FileId+": "+result.Error+" (will retry)")
			}
		} else {
			// Permanent error - log but don't retry
			permanentErrorCount++
			if len(errorDetails) < MaxLoggedErrorDetails {
				errorDetails = append(errorDetails, result.FileId+": "+result.Error+" (permanent)")
			}
		}
	}

	if successCount > 0 || notFoundCount > 0 {
		glog.V(2).Infof("deleted %d files successfully, %d already deleted (not found)", successCount, notFoundCount)
	}

	totalErrors := retryableErrorCount + permanentErrorCount
	if totalErrors > 0 {
		logMessage := fmt.Sprintf("failed to delete %d/%d files (%d retryable, %d permanent)",
			totalErrors, len(toDeleteFileIds), retryableErrorCount, permanentErrorCount)
		if totalErrors > MaxLoggedErrorDetails {
			logMessage += fmt.Sprintf(" (showing first %d)", MaxLoggedErrorDetails)
		}
		glog.V(0).Infof("%s: %v", logMessage, strings.Join(errorDetails, "; "))
	}

	if retryQueue.Size() > 0 {
		glog.V(2).Infof("retry queue size: %d", retryQueue.Size())
	}
}

// isRetryableError determines if an error is retryable based on its message.
//
// Current implementation uses string matching which is brittle and may break
// if error messages change in dependencies. This is acceptable for the initial
// implementation but should be improved in the future.
//
// TODO: Consider these improvements for more robust error handling:
//   - Pass DeleteResult instead of just error string to access Status codes
//   - Use HTTP status codes (503 Service Unavailable, 429 Too Many Requests, etc.)
//   - Implement structured error types that can be checked with errors.Is/errors.As
//   - Extract and check gRPC status codes for better classification
//   - Add error wrapping in the deletion pipeline to preserve error context
//
// For now, we use conservative string matching for known transient error patterns.
func isRetryableError(errorMsg string) bool {
	// Empty errors are not retryable
	if errorMsg == "" {
		return false
	}

	// Known patterns that indicate temporary/transient conditions.
	// These are based on actual error messages from the deletion pipeline.
	retryablePatterns := []string{
		"is read only",              // Volume temporarily read-only (tiering, maintenance)
		"error reading from server", // Network I/O errors
		"connection reset by peer",  // Network connection issues
		"closed network connection", // Network connection closed unexpectedly
		"connection refused",        // Server temporarily unavailable
		"timeout",                   // Operation timeout (network or server)
		"deadline exceeded",         // Context deadline exceeded
		"context canceled",          // Context cancellation (may be transient)
		"lookup error",              // Volume lookup failures
		"lookup failed",             // Volume server discovery issues
		"too many requests",         // Rate limiting / backpressure
		"service unavailable",       // HTTP 503 errors
		"temporarily unavailable",   // Temporary service issues
		"try again",                 // Explicit retry suggestion
		"i/o timeout",               // Network I/O timeout
		"broken pipe",               // Connection broken during operation
	}

	errorLower := strings.ToLower(errorMsg)
	for _, pattern := range retryablePatterns {
		if strings.Contains(errorLower, pattern) {
			return true
		}
	}
	return false
}

// loopProcessingDeletionRetry processes the retry queue for failed deletions
func (f *Filer) loopProcessingDeletionRetry(lookupFunc func([]string) (map[string]*operation.LookupResult, error), retryQueue *DeletionRetryQueue) {

	ticker := time.NewTicker(DeletionRetryPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.deletionQuit:
			glog.V(0).Infof("retry processor shutting down, %d items remaining in queue", retryQueue.Size())
			return
		case <-ticker.C:
			// Get items that are ready to retry
			readyItems := retryQueue.GetReadyItems(DeletionRetryBatchSize)

			if len(readyItems) == 0 {
				continue
			}

			glog.V(1).Infof("retrying deletion of %d files", len(readyItems))
			f.processRetryBatch(readyItems, lookupFunc, retryQueue)
		}
	}
}

// processRetryBatch attempts to retry deletion of files and processes results.
// Successfully deleted items are removed from tracking, retryable failures are
// re-queued with updated retry counts, and permanent errors are logged and discarded.
func (f *Filer) processRetryBatch(readyItems []*DeletionRetryItem, lookupFunc func([]string) (map[string]*operation.LookupResult, error), retryQueue *DeletionRetryQueue) {
	// Extract file IDs from retry items
	fileIds := make([]string, 0, len(readyItems))
	itemsByFileId := make(map[string]*DeletionRetryItem, len(readyItems))
	for _, item := range readyItems {
		fileIds = append(fileIds, item.FileId)
		itemsByFileId[item.FileId] = item
	}

	// Attempt deletion
	results := operation.DeleteFileIdsWithLookupVolumeId(f.GrpcDialOption, fileIds, lookupFunc)

	// Process results
	var successCount, notFoundCount, retryCount int
	for _, result := range results {
		item := itemsByFileId[result.FileId]

		if result.Error == "" {
			successCount++
			glog.V(2).Infof("retry successful for %s after %d attempts", result.FileId, item.RetryCount)
		} else if result.Error == "not found" || strings.Contains(result.Error, storage.ErrorDeleted.Error()) {
			// Already deleted - success
			notFoundCount++
		} else if isRetryableError(result.Error) {
			// Still failing, re-queue with preserved retry count
			retryCount++
			retryQueue.RequeueForRetry(item, result.Error)
		} else {
			// Permanent error on retry - give up
			glog.Warningf("permanent error on retry for %s after %d attempts: %s", result.FileId, item.RetryCount, result.Error)
		}
	}

	if successCount > 0 || notFoundCount > 0 {
		glog.V(1).Infof("retry: deleted %d files successfully, %d already deleted", successCount, notFoundCount)
	}
	if retryCount > 0 {
		glog.V(1).Infof("retry: %d files still failing, will retry again later", retryCount)
	}
}

func (f *Filer) DeleteUncommittedChunks(ctx context.Context, chunks []*filer_pb.FileChunk) {
	f.doDeleteChunks(ctx, chunks)
}

func (f *Filer) DeleteChunks(ctx context.Context, fullpath util.FullPath, chunks []*filer_pb.FileChunk) {
	rule := f.FilerConf.MatchStorageRule(string(fullpath))
	if rule.DisableChunkDeletion {
		return
	}
	f.doDeleteChunks(ctx, chunks)
}

func (f *Filer) doDeleteChunks(ctx context.Context, chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
			continue
		}
		dataChunks, manifestResolveErr := ResolveOneChunkManifest(ctx, f.MasterClient.LookupFileId, chunk)
		if manifestResolveErr != nil {
			glog.V(0).InfofCtx(ctx, "failed to resolve manifest %s: %v", chunk.FileId, manifestResolveErr)
		}
		for _, dChunk := range dataChunks {
			f.fileIdDeletionQueue.EnQueue(dChunk.GetFileIdString())
		}
		f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
	}
}

func (f *Filer) DeleteChunksNotRecursive(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
	}
}

func (f *Filer) deleteChunksIfNotNew(ctx context.Context, oldEntry, newEntry *Entry) {
	var oldChunks, newChunks []*filer_pb.FileChunk
	if oldEntry != nil {
		oldChunks = oldEntry.GetChunks()
	}
	if newEntry != nil {
		newChunks = newEntry.GetChunks()
	}

	toDelete, err := MinusChunks(ctx, f.MasterClient.GetLookupFileIdFunction(), oldChunks, newChunks)
	if err != nil {
		glog.ErrorfCtx(ctx, "Failed to resolve old entry chunks when delete old entry chunks. new: %s, old: %s", newChunks, oldChunks)
		return
	}
	f.DeleteChunksNotRecursive(toDelete)
}
