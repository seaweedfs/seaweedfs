package filer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// chunkPipeResult represents a prefetched chunk streaming through a pipe.
// The fetch goroutine writes data into the pipeWriter; the consumer reads from pipeReader.
type chunkPipeResult struct {
	chunkView  *ChunkView
	reader     *io.PipeReader
	fetchErr   error         // final error from fetch goroutine
	written    int64         // bytes written by fetch goroutine
	done       chan struct{} // closed when fetch goroutine finishes
	urlStrings []string      // snapshot of URLs at dispatch time (for retry logic)
}

// streamChunksPrefetched streams chunks with concurrent prefetch using io.Pipe.
//
// For each chunk in file order, a goroutine is launched (bounded by a semaphore)
// that establishes an HTTP connection to the volume server and streams data through
// an io.Pipe. The consumer reads from pipes in order, writing to the response.
//
// Memory usage is minimal: pipes are synchronous (no buffering), and only one
// reusable copy buffer is allocated for the consumer.
func streamChunksPrefetched(
	ctx context.Context,
	writer io.Writer,
	chunkViews *IntervalList[*ChunkView],
	fileId2Url map[string][]string,
	jwtFunc VolumeServerJwtFunction,
	masterClient wdclient.HasLookupFileIdFunction,
	offset int64,
	size int64,
	downloadMaxBytesPs int64,
	prefetchAhead int,
) error {
	downloadThrottler := util.NewWriteThrottler(downloadMaxBytesPs)

	// Create a local cancellable context so the consumer can stop the producer
	// and all in-flight fetch goroutines on error (e.g., client disconnect).
	localCtx, localCancel := context.WithCancel(ctx)
	defer localCancel()

	// Ordered channel: one entry per chunk, in file order.
	// Capacity = prefetchAhead so the producer can run ahead.
	// Uses pointer to avoid copying the struct while fetch goroutines write to it.
	results := make(chan *chunkPipeResult, prefetchAhead)

	// Semaphore to limit concurrent fetch goroutines (and thus HTTP connections).
	sem := make(chan struct{}, prefetchAhead)

	// Producer: walks chunk list, launches fetch goroutines, sends results in order.
	// The producer only reads from fileId2Url (populated before streaming starts),
	// so there is no concurrent map access — the consumer never writes to it.
	var producerWg sync.WaitGroup
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		defer close(results)

		for x := chunkViews.Front(); x != nil; x = x.Next {
			chunkView := x.Value

			// Check context before starting new fetch
			select {
			case <-localCtx.Done():
				return
			default:
			}

			// Acquire semaphore slot (bounds concurrent HTTP connections)
			select {
			case sem <- struct{}{}:
			case <-localCtx.Done():
				return
			}

			pr, pw := io.Pipe()
			urlStrings := fileId2Url[chunkView.FileId]
			jwt := jwtFunc(chunkView.FileId)

			result := &chunkPipeResult{
				chunkView:  chunkView,
				reader:     pr,
				done:       make(chan struct{}),
				urlStrings: urlStrings,
			}

			// Launch fetch goroutine
			go func(cv *ChunkView, urls []string, jwt string, pw *io.PipeWriter, res *chunkPipeResult) {
				defer func() { <-sem }() // release semaphore
				defer close(res.done)

				written, err := retriedStreamFetchChunkData(
					localCtx, pw, urls, jwt,
					cv.CipherKey, cv.IsGzipped, cv.IsFullChunk(),
					cv.OffsetInChunk, int(cv.ViewSize),
				)
				res.written = written
				res.fetchErr = err

				if err != nil {
					pw.CloseWithError(err)
				} else {
					pw.Close()
				}
			}(chunkView, urlStrings, jwt, pw, result)

			// Send result to consumer (blocks if channel full, back-pressuring producer)
			select {
			case results <- result:
			case <-localCtx.Done():
				// Consumer gone; close the pipe so fetch goroutine unblocks
				pr.Close()
				return
			}
		}
	}()

	// Consumer: reads from results channel in order, writes to response writer.
	// Use the SeaweedFS memory pool for the copy buffer to reduce GC pressure.
	copyBuf := mem.Allocate(256 * 1024)
	defer mem.Free(copyBuf)
	remaining := size

	var consumeErr error
	for result := range results {
		chunkView := result.chunkView

		// Handle gap before this chunk (zero-fill)
		if offset < chunkView.ViewOffset {
			gap := chunkView.ViewOffset - offset
			remaining -= gap
			glog.V(4).InfofCtx(ctx, "prefetch zero [%d,%d)", offset, chunkView.ViewOffset)
			if err := writeZero(writer, gap); err != nil {
				consumeErr = fmt.Errorf("write zero [%d,%d): %w", offset, chunkView.ViewOffset, err)
				result.reader.Close()
				break
			}
			offset = chunkView.ViewOffset
		}

		// Stream chunk data from pipe to response
		start := time.Now()
		copied, copyErr := io.CopyBuffer(writer, result.reader, copyBuf)
		result.reader.Close()

		// Wait for fetch goroutine to finish to get final error
		<-result.done

		// Determine the effective error
		err := copyErr
		if err == nil && result.fetchErr != nil && copied == 0 {
			err = result.fetchErr
		}

		// If read failed with no data, try cache invalidation + re-fetch (same as sequential path)
		if err != nil && copied == 0 {
			if err := localCtx.Err(); err != nil {
				consumeErr = err
				break
			}
			retryErr := retryWithCacheInvalidation(localCtx, writer, chunkView, result.urlStrings, jwtFunc, masterClient)
			if retryErr != nil {
				stats.FilerHandlerCounter.WithLabelValues("chunkDownloadError").Inc()
				consumeErr = fmt.Errorf("read chunk: %w", retryErr)
				break
			}
			// Retry succeeded
			err = nil
		} else if err != nil {
			if localCtx.Err() != nil {
				consumeErr = localCtx.Err()
			} else {
				stats.FilerHandlerCounter.WithLabelValues("chunkDownloadError").Inc()
				consumeErr = fmt.Errorf("read chunk: %w", err)
			}
			break
		}

		offset += int64(chunkView.ViewSize)
		remaining -= int64(chunkView.ViewSize)
		stats.FilerRequestHistogram.WithLabelValues("chunkDownload").Observe(time.Since(start).Seconds())
		stats.FilerHandlerCounter.WithLabelValues("chunkDownload").Inc()
		downloadThrottler.MaybeSlowdown(int64(chunkView.ViewSize))
	}

	// Cancel the local context to stop the producer and any in-flight fetchers early.
	// This ensures goroutines don't linger after the consumer exits (e.g., on write error).
	localCancel()

	// Drain remaining results to close pipes and unblock fetch goroutines
	for result := range results {
		result.reader.Close()
		<-result.done
	}

	// Wait for producer to finish
	producerWg.Wait()

	if consumeErr != nil {
		return consumeErr
	}

	// Handle trailing zero-fill
	if remaining > 0 {
		glog.V(4).InfofCtx(ctx, "prefetch zero [%d,%d)", offset, offset+remaining)
		if err := writeZero(writer, remaining); err != nil {
			return fmt.Errorf("write zero [%d,%d): %w", offset, offset+remaining, err)
		}
	}

	return nil
}

// retryWithCacheInvalidation attempts to re-fetch a chunk after invalidating the URL cache.
// This mirrors the retry logic in PrepareStreamContentWithThrottler's sequential path.
func retryWithCacheInvalidation(
	ctx context.Context,
	writer io.Writer,
	chunkView *ChunkView,
	oldUrlStrings []string,
	jwtFunc VolumeServerJwtFunction,
	masterClient wdclient.HasLookupFileIdFunction,
) error {
	invalidator, ok := masterClient.(CacheInvalidator)
	if !ok {
		return fmt.Errorf("read chunk %s failed and no cache invalidator available", chunkView.FileId)
	}

	glog.V(0).InfofCtx(ctx, "prefetch read chunk %s failed, invalidating cache and retrying", chunkView.FileId)
	invalidator.InvalidateCache(chunkView.FileId)

	newUrlStrings, lookupErr := masterClient.GetLookupFileIdFunction()(ctx, chunkView.FileId)
	if lookupErr != nil {
		glog.WarningfCtx(ctx, "failed to re-lookup chunk %s after cache invalidation: %v", chunkView.FileId, lookupErr)
		return fmt.Errorf("re-lookup chunk %s: %w", chunkView.FileId, lookupErr)
	}
	if len(newUrlStrings) == 0 {
		glog.WarningfCtx(ctx, "re-lookup for chunk %s returned no locations, skipping retry", chunkView.FileId)
		return fmt.Errorf("re-lookup chunk %s: no locations", chunkView.FileId)
	}

	if urlSlicesEqual(oldUrlStrings, newUrlStrings) {
		glog.V(0).InfofCtx(ctx, "re-lookup returned same locations for chunk %s, skipping retry", chunkView.FileId)
		return fmt.Errorf("read chunk %s failed, same locations after cache invalidation", chunkView.FileId)
	}

	glog.V(0).InfofCtx(ctx, "retrying read chunk %s with new locations: %v", chunkView.FileId, newUrlStrings)
	jwt := jwtFunc(chunkView.FileId)
	_, err := retriedStreamFetchChunkData(
		ctx, writer, newUrlStrings, jwt,
		chunkView.CipherKey, chunkView.IsGzipped, chunkView.IsFullChunk(),
		chunkView.OffsetInChunk, int(chunkView.ViewSize),
	)
	return err
}
