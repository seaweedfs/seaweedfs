package filer

import (
	"container/heap"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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
	// Using a prime number to de-synchronize with other periodic tasks
	DeletionPollInterval = 1123 * time.Millisecond
	// Maximum number of file IDs to delete per batch (roughly 20 bytes per file ID)
	DeletionBatchSize = 100000
)

// retryablePatterns contains transient conditions specific to the deletion
// pipeline, on top of the network and service failures util.IsTransientErrorMessage
// already covers.
//
// Context cancellation counts as retryable here but not in util: this decides
// whether to requeue the deletion, not whether to retry a call, and a cancelled
// batch leaves the file still needing deletion.
var retryablePatterns = []string{
	"is read only",              // Volume temporarily read-only (tiering, maintenance)
	"error reading from server", // Network I/O errors
	"closed network connection", // Network connection closed unexpectedly
	"timeout",                   // Operation timeout (network or server)
	"deadline exceeded",         // Context deadline exceeded
	"context canceled",          // Context cancellation
	"lookup error",              // Volume lookup failures
	"lookup failed",             // Volume server discovery issues
	"too many requests",         // Rate limiting / backpressure
	"try again",                 // Explicit retry suggestion
	// A volume server in maintenance mode refuses BatchDelete until it is switched off.
	// The other replicas already dropped the needle, so a delete that is not retried
	// leaves this replica live for good.
	"is in maintenance mode",
}

// DeletionRetryItem represents a file deletion that failed and needs to be retried
type DeletionRetryItem struct {
	FileId      string
	RetryCount  int
	NextRetryAt time.Time
	LastError   string
	ledgerEpoch uint64 // pendingDeletions epoch when the item was queued; expiry forgets only a matching epoch
	// goneSince and goneSeen track consecutive retries the master answered
	// "volume not found" for (volumeGoneConfirmed).
	goneSince time.Time
	goneSeen  int
	heapIndex int  // index in the heap (for heap.Interface)
	inFlight  bool // true when item is being processed, prevents duplicate additions
	manifest  *filer_pb.FileChunk
	fsync     bool
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
	if retryCount <= 1 {
		return InitialRetryDelay
	}

	shiftAmount := uint(retryCount - 1)

	// time.Duration is an int64. A left shift of 63 or more will result in a
	// negative number or zero. The multiplication can also overflow much earlier
	// (around a shift of 25 for a 5-minute initial delay).
	// The `delay <= 0` check below correctly catches all these overflow cases.
	delay := InitialRetryDelay << shiftAmount

	if delay <= 0 || delay > MaxRetryDelay {
		return MaxRetryDelay
	}

	return delay
}

// AddOrUpdate adds a new failed deletion or updates an existing one
// Time complexity: O(log N) for insertion/update
func (q *DeletionRetryQueue) AddOrUpdate(fileId string, errorMsg string, ledgerEpoch uint64) {
	q.addOrUpdate(fileId, errorMsg, ledgerEpoch, nil, false)
}

func (q *DeletionRetryQueue) addOrUpdate(fileId string, errorMsg string, ledgerEpoch uint64, manifest *filer_pb.FileChunk, fsync bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Check if item already exists (including in-flight items)
	if item, exists := q.itemIndex[fileId]; exists {
		// Item is already in the queue or being processed. Just update the error.
		// The existing retry schedule should proceed.
		// RetryCount is only incremented in RequeueForRetry when an actual retry is performed.
		item.LastError = errorMsg
		item.fsync = item.fsync || fsync
		// Keep the recorded epoch while the item is in flight: an expiry or
		// permanent outcome from that attempt must forget only the record it
		// started with, not a newer enqueue for the same id. This also keeps
		// the field immutable once the worker can read it without the lock.
		if !item.inFlight {
			item.ledgerEpoch = ledgerEpoch
		}
		if item.inFlight {
			glog.V(2).Infof("retry for %s in-flight: attempt %d, will preserve retry state", fileId, item.RetryCount)
		} else {
			glog.V(2).Infof("retry for %s already scheduled: attempt %d, next retry in %v", fileId, item.RetryCount, time.Until(item.NextRetryAt))
		}
		return
	}

	// Add new item
	delay := InitialRetryDelay
	item := &DeletionRetryItem{
		FileId:      fileId,
		RetryCount:  1,
		NextRetryAt: time.Now().Add(delay),
		LastError:   errorMsg,
		ledgerEpoch: ledgerEpoch,
		inFlight:    false,
		manifest:    manifest,
		fsync:       fsync,
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
	item.inFlight = false // Clear in-flight flag
	glog.V(2).Infof("requeued retry for %s: attempt %d, next retry in %v", item.FileId, item.RetryCount, delay)

	// Re-add to heap (item still in itemIndex)
	heap.Push(&q.heap, item)
}

// GetReadyItems returns items that are ready to be retried and marks them as in-flight.
// Expired reports fileIds dropped for exceeding MaxRetryAttempts — permanent
// dropouts the caller must also forget in the durable deletion ledger.
// Time complexity: O(K log N) where K is the number of ready items
// Items are processed in order of NextRetryAt (earliest first)
func (q *DeletionRetryQueue) GetReadyItems(maxItems int) (ready []*DeletionRetryItem, expired []*DeletionRetryItem) {
	q.lock.Lock()
	defer q.lock.Unlock()

	now := time.Now()

	// Peek at items from the top of the heap (earliest NextRetryAt)
	for len(q.heap) > 0 && len(ready) < maxItems {
		item := q.heap[0]

		// If the earliest item is not ready yet, no other items are ready either
		if item.NextRetryAt.After(now) {
			break
		}

		// Remove from heap but keep in itemIndex with inFlight flag
		heap.Pop(&q.heap)

		if item.RetryCount <= MaxRetryAttempts {
			item.inFlight = true // Mark as being processed
			ready = append(ready, item)
		} else {
			// Max attempts reached, log and discard completely
			delete(q.itemIndex, item.FileId)
			expired = append(expired, item)
			glog.Warningf("max retry attempts (%d) reached for %s, last error: %s", MaxRetryAttempts, item.FileId, item.LastError)
		}
	}

	return ready, expired
}

// Remove removes an item from the queue (called when deletion succeeds or fails permanently)
// Time complexity: O(1)
func (q *DeletionRetryQueue) Remove(item *DeletionRetryItem) {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Item was already removed from heap by GetReadyItems, just remove from index
	delete(q.itemIndex, item.FileId)
}

func (q *DeletionRetryQueue) fsyncOf(item *DeletionRetryItem) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return item.fsync
}

func (q *DeletionRetryQueue) removeUnlessFsyncChanged(item *DeletionRetryItem, fsync bool) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	if item.fsync != fsync {
		return false
	}
	delete(q.itemIndex, item.FileId)
	return true
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
			m[vid] = &operation.LookupResult{
				VolumeOrFileId: vid,
				Locations:      toOperationLocations(locs),
			}
		}
		return m, nil
	}
}

func toOperationLocations(locs []wdclient.Location) []operation.Location {
	var locations []operation.Location
	for _, loc := range locs {
		locations = append(locations, operation.Location{
			Url:       loc.Url,
			PublicUrl: loc.PublicUrl,
			GrpcPort:  loc.GrpcPort,
		})
	}
	return locations
}

// volumeGoneError starts the lookup error of a volume the master answered
// "not found" for. That is what a deleted volume (a dropped collection, a
// removed bucket) answers, but also a live one whose server is disconnected
// for now, or one a follower master has not cached yet; so the retry loop
// ends such a deletion only once the answer has held (volumeGoneConfirmed).
const volumeGoneError = "volume not found by the master"

const (
	// masterLookupTimeout bounds the retry loop's master lookup.
	masterLookupTimeout = 5 * time.Second
	// VolumeGoneGrace and VolumeGoneMinAnswers: how long, and over how many
	// retries in a row, the master must answer "not found" for a volume
	// before its pending chunk deletions count as done. A volume server that
	// is down gets the same answer, so the grace outlasts a maintenance
	// window rather than a reconnect.
	VolumeGoneGrace      = MaxRetryDelay
	VolumeGoneMinAnswers = 3
)

// retryLookupFn is the retry loop's lookup: the client's cache, then the
// master for the volumes the cache does not know. Without it a chunk on a
// deleted volume reached no volume server, got no deletion result, and was
// re-queued forever: the durable ledger reloads it with a fresh retry count
// on every restart.
func retryLookupFn(masterClient *wdclient.MasterClient) func(vids []string) (map[string]*operation.LookupResult, error) {
	return lookupWithMasterFallback(masterClient.GetVidLocations, func(vids []string) (*master_pb.LookupVolumeResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), masterLookupTimeout)
		defer cancel()
		var resp *master_pb.LookupVolumeResponse
		err := masterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
			r, err := client.LookupVolume(ctx, &master_pb.LookupVolumeRequest{VolumeOrFileIds: vids})
			resp = r
			return err
		})
		return resp, err
	})
}

// lookupWithMasterFallback resolves locations from the cache and asks the
// master about the rest. A volume the master has no location for and answers
// "... not found" carries volumeGoneError; any failure to ask leaves the
// volume without locations, as before (no result, retried).
func lookupWithMasterFallback(cached func(vid string) ([]wdclient.Location, error),
	askMaster func(vids []string) (*master_pb.LookupVolumeResponse, error)) func(vids []string) (map[string]*operation.LookupResult, error) {
	return func(vids []string) (map[string]*operation.LookupResult, error) {
		m := make(map[string]*operation.LookupResult)
		var missing []string
		for _, vid := range vids {
			locs, _ := cached(vid)
			m[vid] = &operation.LookupResult{VolumeOrFileId: vid, Locations: toOperationLocations(locs)}
			if len(m[vid].Locations) == 0 {
				missing = append(missing, vid)
			}
		}
		if len(missing) == 0 {
			return m, nil
		}
		resp, err := askMaster(missing)
		if err != nil || resp == nil {
			glog.V(1).Infof("deletion retry: master lookup of %d uncached volumes: %v", len(missing), err)
			return m, nil
		}
		for _, vl := range resp.VolumeIdLocations {
			vid := vl.VolumeOrFileId
			if i := strings.Index(vid, ","); i > 0 {
				vid = vid[:i]
			}
			r, ok := m[vid]
			if !ok || len(r.Locations) > 0 {
				continue
			}
			for _, loc := range vl.Locations {
				r.Locations = append(r.Locations, operation.Location{Url: loc.Url, PublicUrl: loc.PublicUrl, GrpcPort: int(loc.GrpcPort)})
			}
			if len(r.Locations) == 0 && strings.HasSuffix(vl.Error, "not found") {
				r.Error = volumeGoneError + ": " + vl.Error
			}
		}
		return m, nil
	}
}

// volumeGoneConfirmed records one more "volume not found" answer for the item
// and reports whether the answers have held long enough to treat the chunk as
// gone with its volume. Any other outcome resets the streak.
func volumeGoneConfirmed(item *DeletionRetryItem, now time.Time) bool {
	if item.goneSeen == 0 {
		item.goneSince = now
	}
	item.goneSeen++
	return item.goneSeen >= VolumeGoneMinAnswers && now.Sub(item.goneSince) >= VolumeGoneGrace
}

func (f *Filer) loopProcessingDeletion() {

	lookupFunc := LookupByMasterClientFn(f.MasterClient)

	// Start retry processor in a separate goroutine
	go f.loopProcessingDeletionRetry(retryLookupFn(f.MasterClient))

	ticker := time.NewTicker(DeletionPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.deletionQuit:
			glog.V(0).Infof("deletion processor shutting down")
			return
		case <-ticker.C:
			f.FlushFileIdDeletionQueue(context.Background(), lookupFunc)
		}
	}
}

func (f *Filer) FlushFileIdDeletionQueue(ctx context.Context, lookupFunc func([]string) (map[string]*operation.LookupResult, error)) (consumed []string) {
	f.FileIdDeletionQueue.Consume(func(fileIds []string) {
		consumed = fileIds
		for i := 0; i < len(fileIds); i += DeletionBatchSize {
			end := i + DeletionBatchSize
			if end > len(fileIds) {
				end = len(fileIds)
			}
			f.processDeletionBatch(ctx, fileIds[i:end], lookupFunc)
		}
	})
	return consumed
}

// processDeletionBatch handles deletion of a batch of file IDs and processes results.
// It classifies errors into retryable and permanent categories, adds retryable failures
// to the retry queue, and logs appropriate messages.
func (f *Filer) processDeletionBatch(ctx context.Context, toDeleteFileIds []string, lookupFunc func([]string) (map[string]*operation.LookupResult, error)) {
	// Deduplicate file IDs to prevent incorrect retry count increments for the same file ID within a single batch.
	uniqueFileIdsSlice := make([]string, 0, len(toDeleteFileIds))
	processed := make(map[string]struct{}, len(toDeleteFileIds))
	for _, fileId := range toDeleteFileIds {
		if _, found := processed[fileId]; !found {
			processed[fileId] = struct{}{}
			uniqueFileIdsSlice = append(uniqueFileIdsSlice, fileId)
		}
	}

	if len(uniqueFileIdsSlice) == 0 {
		return
	}

	// Remember each id's ledger epoch before the remote deletes run: a
	// permanent outcome below must not erase a record re-queued mid-flight.
	epochs := make(map[string]uint64, len(uniqueFileIdsSlice))
	for _, fileId := range uniqueFileIdsSlice {
		epochs[fileId] = f.deletionEpoch(fileId)
	}

	// Delete files and classify outcomes
	fsyncIds := f.fsyncDeletionIds(uniqueFileIdsSlice)
	outcomes := deleteFilesAndClassify(ctx, f.volumeGrpcDialOption(), uniqueFileIdsSlice, fsyncIds, lookupFunc)

	// Process outcomes
	var successCount, notFoundCount, retryableErrorCount, permanentErrorCount int
	var errorDetails []string

	for _, fileId := range uniqueFileIdsSlice {
		outcome := outcomes[fileId]
		_, fsynced := fsyncIds[fileId]

		switch outcome.status {
		case deletionOutcomeSuccess:
			successCount++
			f.forgetDeletion(fileId, fsynced) // confirmed gone: drop from durable ledger
		case deletionOutcomeNotFound:
			notFoundCount++
			f.forgetDeletion(fileId, fsynced) // already gone: drop from durable ledger
		case deletionOutcomeRetryable, deletionOutcomeNoResult, deletionOutcomeVolumeGone:
			retryableErrorCount++
			// Keep in the durable ledger: not confirmed, must survive restarts.
			f.DeletionRetryQueue.AddOrUpdate(fileId, outcome.errorMsg, f.deletionEpoch(fileId))
			if len(errorDetails) < MaxLoggedErrorDetails {
				errorDetails = append(errorDetails, fileId+": "+outcome.errorMsg+" (will retry)")
			}
		case deletionOutcomePermanent:
			permanentErrorCount++
			// gave up: drop the record, unless the id was re-queued meanwhile
			f.forgetDeletionEpoch(fileId, epochs[fileId])
			if len(errorDetails) < MaxLoggedErrorDetails {
				errorDetails = append(errorDetails, fileId+": "+outcome.errorMsg+" (permanent)")
			}
		}
	}

	if successCount > 0 || notFoundCount > 0 {
		glog.V(2).Infof("deleted %d files successfully, %d already deleted (not found)", successCount, notFoundCount)
	}

	totalErrors := retryableErrorCount + permanentErrorCount
	if totalErrors > 0 {
		logMessage := fmt.Sprintf("failed to delete %d/%d files (%d retryable, %d permanent)",
			totalErrors, len(uniqueFileIdsSlice), retryableErrorCount, permanentErrorCount)
		if len(errorDetails) > 0 {
			if totalErrors > MaxLoggedErrorDetails {
				logMessage += fmt.Sprintf(" (showing first %d)", len(errorDetails))
			}
			glog.V(0).Infof("%s: %v", logMessage, strings.Join(errorDetails, "; "))
		} else {
			glog.V(0).Info(logMessage)
		}
	}

	if f.DeletionRetryQueue.Size() > 0 {
		glog.V(2).Infof("retry queue size: %d", f.DeletionRetryQueue.Size())
	}
}

const (
	deletionOutcomeSuccess    = "success"
	deletionOutcomeNotFound   = "not_found"
	deletionOutcomeRetryable  = "retryable"
	deletionOutcomePermanent  = "permanent"
	deletionOutcomeNoResult   = "no_result"
	deletionOutcomeVolumeGone = "volume_gone"
)

// deletionOutcome represents the result of classifying deletion results for a file
type deletionOutcome struct {
	status   string // One of the deletionOutcome* constants
	errorMsg string
}

// deleteFilesAndClassify performs deletion and classifies outcomes for a list of file IDs.
// Ids in fsyncIds are deleted durably.
func deleteFilesAndClassify(ctx context.Context, grpcDialOption grpc.DialOption, fileIds []string, fsyncIds map[string]struct{}, lookupFunc func([]string) (map[string]*operation.LookupResult, error)) map[string]deletionOutcome {
	var plainIds, syncedIds []string
	for _, fileId := range fileIds {
		if _, ok := fsyncIds[fileId]; ok {
			syncedIds = append(syncedIds, fileId)
		} else {
			plainIds = append(plainIds, fileId)
		}
	}
	var results []*volume_server_pb.DeleteResult
	if len(plainIds) > 0 {
		results = operation.DeleteFileIdsWithLookupVolumeId(ctx, grpcDialOption, plainIds, false, lookupFunc)
	}
	if len(syncedIds) > 0 {
		results = append(results, operation.DeleteFileIdsWithLookupVolumeId(ctx, grpcDialOption, syncedIds, true, lookupFunc)...)
	}

	// Group results by file ID to handle multiple results for replicated volumes
	resultsByFileId := make(map[string][]*volume_server_pb.DeleteResult)
	for _, result := range results {
		resultsByFileId[result.FileId] = append(resultsByFileId[result.FileId], result)
	}

	// Classify outcome for each file
	outcomes := make(map[string]deletionOutcome, len(fileIds))
	for _, fileId := range fileIds {
		_, durable := fsyncIds[fileId]
		outcomes[fileId] = classifyDeletionOutcome(fileId, resultsByFileId, durable)
	}

	return outcomes
}

// classifyDeletionOutcome examines all deletion results for a file ID and determines the overall outcome
// Uses a single pass through results with early return for permanent errors (highest priority)
// Priority: Permanent > Retryable > Success > Not Found
// A durable delete the volume server could not sync is rolled back and
// answered with a 5xx, so for a durable delete that answer stays retryable.
func classifyDeletionOutcome(fileId string, resultsByFileId map[string][]*volume_server_pb.DeleteResult, durable bool) deletionOutcome {
	fileIdResults, found := resultsByFileId[fileId]
	if !found || len(fileIdResults) == 0 {
		return deletionOutcome{
			status:   deletionOutcomeNoResult,
			errorMsg: "no deletion result from volume server",
		}
	}

	var firstRetryableError string
	hasSuccess := false

	for _, res := range fileIdResults {
		if strings.HasPrefix(res.Error, volumeGoneError) {
			return deletionOutcome{status: deletionOutcomeVolumeGone, errorMsg: res.Error}
		}
		if res.Error == "" {
			hasSuccess = true
			continue
		}
		if strings.Contains(res.Error, storage.ErrorDeleted.Error()) || res.Error == "not found" {
			continue
		}

		if isRetryableError(res.Error) || (durable && res.Status >= http.StatusInternalServerError) {
			if firstRetryableError == "" {
				firstRetryableError = res.Error
			}
		} else {
			// Permanent error takes highest precedence - return immediately
			return deletionOutcome{status: deletionOutcomePermanent, errorMsg: res.Error}
		}
	}

	if firstRetryableError != "" {
		return deletionOutcome{status: deletionOutcomeRetryable, errorMsg: firstRetryableError}
	}

	if hasSuccess {
		return deletionOutcome{status: deletionOutcomeSuccess, errorMsg: ""}
	}

	// If we are here, all results were "not found"
	return deletionOutcome{status: deletionOutcomeNotFound, errorMsg: ""}
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

	if util.IsTransientErrorMessage(errorMsg) {
		return true
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
func (f *Filer) loopProcessingDeletionRetry(lookupFunc func([]string) (map[string]*operation.LookupResult, error)) {

	ticker := time.NewTicker(DeletionRetryPollInterval)
	defer ticker.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-f.deletionQuit
		cancel()
	}()

	for {
		select {
		case <-f.deletionQuit:
			glog.V(0).Infof("retry processor shutting down, %d items remaining in queue", f.DeletionRetryQueue.Size())
			return
		case <-ticker.C:
			// Process all ready items in batches until queue is empty
			totalProcessed := 0
			for {
				readyItems, expired := f.DeletionRetryQueue.GetReadyItems(DeletionRetryBatchSize)
				for _, item := range expired {
					// Permanently discarded — drop the ledger record, but only
					// if the id was not re-queued after this retry item was
					// recorded. A surviving newer record still needs work, so
					// it goes back through the hot queue.
					if !f.forgetDeletionEpoch(item.FileId, item.ledgerEpoch) {
						f.queueDeletions(false, item.FileId)
					}
				}
				if len(readyItems) == 0 {
					break
				}

				f.processRetryBatch(readyItems, lookupFunc)
				totalProcessed += len(readyItems)
			}

			if totalProcessed > 0 {
				glog.V(1).Infof("retried deletion of %d files", totalProcessed)
			}
			f.processManifestRetries(ctx)
		}
	}
}

// processRetryBatch attempts to retry deletion of files and processes results.
// Successfully deleted items are removed from tracking, retryable failures are
// re-queued with updated retry counts, and permanent errors are logged and discarded.
func (f *Filer) processRetryBatch(readyItems []*DeletionRetryItem, lookupFunc func([]string) (map[string]*operation.LookupResult, error)) {
	// Extract file IDs from retry items
	fileIds := make([]string, 0, len(readyItems))
	for _, item := range readyItems {
		fileIds = append(fileIds, item.FileId)
	}

	// Delete files and classify outcomes
	fsyncIds := f.fsyncDeletionIds(fileIds)
	outcomes := deleteFilesAndClassify(context.Background(), f.volumeGrpcDialOption(), fileIds, fsyncIds, lookupFunc)

	// Process outcomes - iterate over readyItems to ensure all items are accounted for
	var successCount, notFoundCount, retryCount, permanentErrorCount, volumeGoneCount int
	for _, item := range readyItems {
		outcome := outcomes[item.FileId]
		_, fsynced := fsyncIds[item.FileId]

		if outcome.status != deletionOutcomeVolumeGone {
			item.goneSeen = 0
		}
		if outcome.status == deletionOutcomeVolumeGone && volumeGoneConfirmed(item, time.Now()) {
			// the master has not known the volume for VolumeGoneGrace: the
			// chunk went with it
			volumeGoneCount++
			outcome.status = deletionOutcomeNotFound
		}
		switch outcome.status {
		case deletionOutcomeSuccess:
			successCount++
			f.DeletionRetryQueue.Remove(item)      // Remove from queue (success)
			f.forgetDeletion(item.FileId, fsynced) // confirmed gone: drop from durable ledger
			glog.V(2).Infof("retry successful for %s after %d attempts", item.FileId, item.RetryCount)
		case deletionOutcomeNotFound:
			notFoundCount++
			f.DeletionRetryQueue.Remove(item)      // Remove from queue (already deleted)
			f.forgetDeletion(item.FileId, fsynced) // already gone: drop from durable ledger
		case deletionOutcomeRetryable, deletionOutcomeNoResult, deletionOutcomeVolumeGone:
			retryCount++
			if outcome.status == deletionOutcomeNoResult {
				glog.Warningf("no deletion result for retried file %s, re-queuing to avoid loss", item.FileId)
			}
			// Keep in the durable ledger: still not confirmed.
			f.DeletionRetryQueue.RequeueForRetry(item, outcome.errorMsg)
		case deletionOutcomePermanent:
			permanentErrorCount++
			f.DeletionRetryQueue.Remove(item) // Remove from queue (permanent failure)
			// gave up: drop the record, unless the id was re-queued meanwhile
			// — a surviving newer record goes back through the hot queue.
			if !f.forgetDeletionEpoch(item.FileId, item.ledgerEpoch) {
				f.queueDeletions(false, item.FileId)
			}
			glog.Warningf("permanent error on retry for %s after %d attempts: %s", item.FileId, item.RetryCount, outcome.errorMsg)
		}
	}

	if successCount > 0 || notFoundCount > 0 {
		glog.V(1).Infof("retry: deleted %d files successfully, %d already deleted", successCount, notFoundCount)
	}
	if retryCount > 0 {
		glog.V(1).Infof("retry: %d files still failing, will retry again later", retryCount)
	}
	if volumeGoneCount > 0 {
		glog.V(0).Infof("retry: %d files were on volumes the master has not known for %v, dropped as deleted with their volume", volumeGoneCount, VolumeGoneGrace)
	}
	if permanentErrorCount > 0 {
		glog.Warningf("retry: %d files failed with permanent errors", permanentErrorCount)
	}
}

func (f *Filer) DeleteUncommittedChunks(ctx context.Context, chunks []*filer_pb.FileChunk) {
	f.doDeleteChunks(ctx, chunks, false)
}

func (f *Filer) DeleteChunks(ctx context.Context, fullpath util.FullPath, chunks []*filer_pb.FileChunk) {
	rule := f.FilerConf.MatchStorageRule(string(fullpath))
	if rule.DisableChunkDeletion {
		return
	}
	f.doDeleteChunks(ctx, chunks, rule.GetFsync())
}

func (f *Filer) doDeleteChunks(ctx context.Context, chunks []*filer_pb.FileChunk, fsync bool) {
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			f.queueDeletions(fsync, chunk.GetFileIdString())
			continue
		}
		// The manifest needle is the only record of its sub-chunks, so it
		// stays until they are queued.
		fileIds, err := f.manifestFileIds(ctx, chunk)
		if err != nil {
			glog.WarningfCtx(ctx, "failed to resolve manifest %s, will retry: %v", chunk.GetFileIdString(), err)
			f.manifestRetries().addOrUpdate(chunk.GetFileIdString(), err.Error(), 0, chunk, fsync)
			continue
		}
		f.queueDeletions(fsync, fileIds...)
	}
}

// manifestFileIds resolves a manifest and any nested manifests, listing each
// sub-chunk ahead of the manifest that holds it.
func (f *Filer) manifestFileIds(ctx context.Context, manifest *filer_pb.FileChunk) ([]string, error) {
	dataChunks, err := ResolveOneChunkManifest(ctx, f.MasterClient.GetLookupFileIdFunction(), manifest, f.MasterClient)
	if err != nil {
		return nil, err
	}
	var fileIds []string
	for _, chunk := range dataChunks {
		if !chunk.IsChunkManifest {
			fileIds = append(fileIds, chunk.GetFileIdString())
			continue
		}
		nested, err := f.manifestFileIds(ctx, chunk)
		if err != nil {
			return nil, err
		}
		fileIds = append(fileIds, nested...)
	}
	return append(fileIds, manifest.GetFileIdString()), nil
}

func (f *Filer) manifestRetries() *DeletionRetryQueue {
	f.manifestRetryOnce.Do(func() {
		f.manifestRetryQueue = NewDeletionRetryQueue()
	})
	return f.manifestRetryQueue
}

// processManifestRetries resolves manifests that failed to resolve on delete.
// One that never resolves within MaxRetryAttempts is deleted alone, leaving
// its sub-chunks to volume.fsck. A pass stops after manifestRetryTimeout and
// leaves the rest queued, so the shared retry loop gets its turn.
func (f *Filer) processManifestRetries(ctx context.Context) {
	q := f.manifestRetries()
	passCtx, cancel := context.WithTimeout(ctx, manifestRetryTimeout)
	defer cancel()
	for passCtx.Err() == nil {
		ready, expired := q.GetReadyItems(1)
		for _, item := range expired {
			f.queueDeletions(item.fsync, item.FileId)
		}
		if len(ready) == 0 {
			if len(expired) == 0 {
				return
			}
			continue
		}
		f.retryManifest(ctx, q, ready[0])
	}
}

// manifestRetryTimeout bounds one manifest read so a stalled volume server
// cannot hold up the shared retry loop.
var manifestRetryTimeout = 1 * time.Minute

// retryManifest queues the resolved ids again, without rereading the manifest
// it already queued, when a durable delete arrived during the attempt.
func (f *Filer) retryManifest(ctx context.Context, q *DeletionRetryQueue, item *DeletionRetryItem) {
	attemptCtx, cancel := context.WithTimeout(ctx, manifestRetryTimeout)
	fileIds, err := f.manifestFileIds(attemptCtx, item.manifest)
	cancel()
	if err != nil {
		q.RequeueForRetry(item, err.Error())
		return
	}
	for {
		fsync := q.fsyncOf(item)
		f.queueDeletions(fsync, fileIds...)
		if q.removeUnlessFsyncChanged(item, fsync) {
			return
		}
	}
}

func (f *Filer) DeleteChunksNotRecursive(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		f.queueDeletions(false, chunk.GetFileIdString())
	}
}

func (f *Filer) deleteChunksIfNotNew(ctx context.Context, oldEntry, newEntry *Entry) {
	if f.hardLinkStillHeld(ctx, oldEntry, newEntry) {
		// newEntry replaced one name of a hard-linked file; the other names
		// still read every chunk oldEntry has
		return
	}
	var oldChunks, newChunks []*filer_pb.FileChunk
	if oldEntry != nil {
		oldChunks = oldEntry.GetChunks()
	}
	if newEntry != nil {
		newChunks = newEntry.GetChunks()
	}

	toDelete, err := MinusChunks(ctx, f.MasterClient.GetLookupFileIdFunction(), oldChunks, newChunks, f.MasterClient)
	if err != nil {
		glog.ErrorfCtx(ctx, "Failed to resolve old entry chunks when delete old entry chunks. new: %v, old: %v", filer_pb.FileIdsString(newChunks), filer_pb.FileIdsString(oldChunks))
		return
	}
	f.DeleteChunksNotRecursive(toDelete)
}
