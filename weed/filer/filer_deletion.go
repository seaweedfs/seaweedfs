package filer

import (
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
)

// DeletionRetryItem represents a file deletion that failed and needs to be retried
type DeletionRetryItem struct {
	FileId      string
	RetryCount  int
	NextRetryAt time.Time
	LastError   string
}

// DeletionRetryQueue manages the queue of failed deletions that need to be retried
type DeletionRetryQueue struct {
	items     map[string]*DeletionRetryItem
	itemsLock sync.RWMutex
}

// NewDeletionRetryQueue creates a new retry queue
func NewDeletionRetryQueue() *DeletionRetryQueue {
	return &DeletionRetryQueue{
		items: make(map[string]*DeletionRetryItem),
	}
}

// AddOrUpdate adds a new failed deletion or updates an existing one
func (q *DeletionRetryQueue) AddOrUpdate(fileId string, errorMsg string) {
	q.itemsLock.Lock()
	defer q.itemsLock.Unlock()

	// Check if item already exists
	if item, exists := q.items[fileId]; exists {
		item.RetryCount++
		item.LastError = errorMsg
		// Calculate next retry time with exponential backoff
		delay := InitialRetryDelay * time.Duration(1<<uint(item.RetryCount-1))
		if delay > MaxRetryDelay {
			delay = MaxRetryDelay
		}
		item.NextRetryAt = time.Now().Add(delay)
		glog.V(2).Infof("updated retry for %s: attempt %d, next retry in %v", fileId, item.RetryCount, delay)
		return
	}

	// Add new item
	delay := InitialRetryDelay
	q.items[fileId] = &DeletionRetryItem{
		FileId:      fileId,
		RetryCount:  1,
		NextRetryAt: time.Now().Add(delay),
		LastError:   errorMsg,
	}
	glog.V(2).Infof("added retry for %s: next retry in %v", fileId, delay)
}

// GetReadyItems returns items that are ready to be retried and removes them from the queue
func (q *DeletionRetryQueue) GetReadyItems(maxItems int) []*DeletionRetryItem {
	q.itemsLock.Lock()
	defer q.itemsLock.Unlock()

	now := time.Now()
	var readyItems []*DeletionRetryItem

	for fileId, item := range q.items {
		if len(readyItems) < maxItems && item.NextRetryAt.Before(now) {
			if item.RetryCount < MaxRetryAttempts {
				readyItems = append(readyItems, item)
				delete(q.items, fileId)
			} else {
				// Max attempts reached, log and discard
				glog.Warningf("max retry attempts (%d) reached for %s, last error: %s", MaxRetryAttempts, item.FileId, item.LastError)
				delete(q.items, fileId)
			}
		}
	}

	return readyItems
}

// Size returns the current size of the retry queue
func (q *DeletionRetryQueue) Size() int {
	q.itemsLock.RLock()
	defer q.itemsLock.RUnlock()
	return len(q.items)
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

	ticker := time.NewTicker(1123 * time.Millisecond)
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
							if len(errorDetails) < 10 {
								errorDetails = append(errorDetails, result.FileId+": "+result.Error+" (will retry)")
							}
						} else {
							// Permanent error - log but don't retry
							permanentErrorCount++
							if len(errorDetails) < 10 {
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
						if totalErrors > 10 {
							logMessage += " (showing first 10)"
						}
						glog.V(0).Infof("%s: %v", logMessage, strings.Join(errorDetails, "; "))
					}

					if retryQueue.Size() > 0 {
						glog.V(2).Infof("retry queue size: %d", retryQueue.Size())
					}
				}
			})
		}
	}
}

// isRetryableError determines if an error is retryable
func isRetryableError(errorMsg string) bool {
	// Errors that indicate temporary conditions
	retryablePatterns := []string{
		"is read only",
		"error reading from server",
		"connection reset by peer",
		"closed network connection",
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

			// Extract file IDs from retry items
			var fileIds []string
			itemsByFileId := make(map[string]*DeletionRetryItem)
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
					// Still failing, add back to retry queue
					retryCount++
					retryQueue.AddOrUpdate(result.FileId, result.Error)
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
