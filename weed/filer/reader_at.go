package filer

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// DefaultPrefetchCount is the default number of chunks to prefetch ahead during
// sequential reads. This value is used when prefetch count is not explicitly
// configured (e.g., WebDAV, query engine, message queue). For mount operations,
// the prefetch count is derived from the -concurrentReaders option.
const DefaultPrefetchCount = 4

// minReadConcurrency is the minimum number of parallel chunk fetches.
// This ensures at least some parallelism even when prefetchCount is low,
// improving throughput for reads spanning multiple chunks.
const minReadConcurrency = 4

type ChunkReadAt struct {
	masterClient  *wdclient.MasterClient
	chunkViews    *IntervalList[*ChunkView]
	fileSize      int64
	readerCache   *ReaderCache
	readerPattern *ReaderPattern
	lastChunkFid  string
	prefetchCount int             // Number of chunks to prefetch ahead during sequential reads
	ctx           context.Context // Context used for cancellation during chunk read operations
}

var _ = io.ReaderAt(&ChunkReadAt{})
var _ = io.Closer(&ChunkReadAt{})

// LookupFn creates a basic volume location lookup function with simple caching.
//
// Deprecated: Use wdclient.FilerClient instead. This function has several limitations compared to wdclient.FilerClient:
//   - Simple bounded cache (10k entries, no eviction policy or TTL for stale entries)
//   - No singleflight deduplication (concurrent requests for same volume will duplicate work)
//   - No cache history for volume moves (no fallback chain when volumes migrate)
//   - No high availability (single filer address, no automatic failover)
//
// For NEW code, especially mount operations, use wdclient.FilerClient instead:
//
//	filerClient := wdclient.NewFilerClient(filerAddresses, grpcDialOption, dataCenter, opts)
//	lookupFn := filerClient.GetLookupFileIdFunction()
//
// This provides:
//   - Bounded cache with configurable size
//   - Singleflight deduplication of concurrent lookups
//   - Cache history when volumes move
//   - Battle-tested vidMap with cache chain
//
// This function is kept for backward compatibility with existing code paths
// (shell commands, streaming, etc.) but should be avoided in long-running processes
// or multi-tenant deployments where unbounded memory growth is a concern.
//
// Maximum recommended cache entries: ~10,000 volumes per process.
// Beyond this, consider migrating to wdclient.FilerClient.
func LookupFn(filerClient filer_pb.FilerClient) wdclient.LookupFileIdFunctionType {

	vidCache := make(map[string]*filer_pb.Locations)
	var vidCacheLock sync.RWMutex
	cacheSize := 0
	const maxCacheSize = 10000 // Simple bound to prevent unbounded growth

	return func(ctx context.Context, fileId string) (targetUrls []string, err error) {
		vid := VolumeId(fileId)
		vidCacheLock.RLock()
		locations, found := vidCache[vid]
		vidCacheLock.RUnlock()

		if !found {
			util.Retry("lookup volume "+vid, func() error {
				err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
					resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
						VolumeIds: []string{vid},
					})
					if err != nil {
						return err
					}

					locations = resp.LocationsMap[vid]
					if locations == nil || len(locations.Locations) == 0 {
						glog.V(0).InfofCtx(ctx, "failed to locate %s", fileId)
						return fmt.Errorf("failed to locate %s", fileId)
					}
					vidCacheLock.Lock()
					// Simple size limit to prevent unbounded growth
					// For proper cache management, use wdclient.FilerClient instead
					if cacheSize < maxCacheSize {
						vidCache[vid] = locations
						cacheSize++
					} else if cacheSize == maxCacheSize {
						glog.Warningf("filer.LookupFn cache reached limit of %d volumes, not caching new entries. Consider migrating to wdclient.FilerClient for bounded cache management.", maxCacheSize)
						cacheSize++ // Only log once
					}
					vidCacheLock.Unlock()

					return nil
				})
				return err
			})
		}

		if err != nil {
			return nil, err
		}

		fcDataCenter := filerClient.GetDataCenter()
		var sameDcTargetUrls, otherTargetUrls []string
		for _, loc := range locations.Locations {
			volumeServerAddress := filerClient.AdjustedUrl(loc)
			targetUrl := fmt.Sprintf("http://%s/%s", volumeServerAddress, fileId)
			if fcDataCenter == "" || fcDataCenter != loc.DataCenter {
				otherTargetUrls = append(otherTargetUrls, targetUrl)
			} else {
				sameDcTargetUrls = append(sameDcTargetUrls, targetUrl)
			}
		}
		rand.Shuffle(len(sameDcTargetUrls), func(i, j int) {
			sameDcTargetUrls[i], sameDcTargetUrls[j] = sameDcTargetUrls[j], sameDcTargetUrls[i]
		})
		rand.Shuffle(len(otherTargetUrls), func(i, j int) {
			otherTargetUrls[i], otherTargetUrls[j] = otherTargetUrls[j], otherTargetUrls[i]
		})
		// Prefer same data center
		targetUrls = append(sameDcTargetUrls, otherTargetUrls...)
		return
	}
}

func NewChunkReaderAtFromClient(ctx context.Context, readerCache *ReaderCache, chunkViews *IntervalList[*ChunkView], fileSize int64, prefetchCount int) *ChunkReadAt {

	return &ChunkReadAt{
		chunkViews:    chunkViews,
		fileSize:      fileSize,
		readerCache:   readerCache,
		readerPattern: NewReaderPattern(),
		prefetchCount: prefetchCount,
		ctx:           ctx,
	}
}

func (c *ChunkReadAt) Size() int64 {
	return c.fileSize
}

func (c *ChunkReadAt) Close() error {
	c.readerCache.destroy()
	return nil
}

func (c *ChunkReadAt) ReadAt(p []byte, offset int64) (n int, err error) {

	c.readerPattern.MonitorReadAt(offset, len(p))

	c.chunkViews.Lock.RLock()
	defer c.chunkViews.Lock.RUnlock()

	// glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	n, _, err = c.doReadAt(c.ctx, p, offset)
	return
}

func (c *ChunkReadAt) ReadAtWithTime(ctx context.Context, p []byte, offset int64) (n int, ts int64, err error) {

	c.readerPattern.MonitorReadAt(offset, len(p))

	c.chunkViews.Lock.RLock()
	defer c.chunkViews.Lock.RUnlock()

	// glog.V(4).Infof("ReadAt [%d,%d) of total file size %d bytes %d chunk views", offset, offset+int64(len(p)), c.fileSize, len(c.chunkViews))
	return c.doReadAt(ctx, p, offset)
}

// chunkReadTask represents a single chunk read operation for parallel processing
type chunkReadTask struct {
	chunk        *ChunkView
	bufferStart  int64  // start position in the output buffer
	bufferEnd    int64  // end position in the output buffer
	chunkOffset  uint64 // offset within the chunk to read from
	bytesRead    int
	modifiedTsNs int64
}

func (c *ChunkReadAt) doReadAt(ctx context.Context, p []byte, offset int64) (n int, ts int64, err error) {

	// Collect all chunk read tasks
	var tasks []*chunkReadTask
	var gaps []struct{ start, length int64 } // gaps that need zero-filling

	startOffset, remaining := offset, int64(len(p))
	var lastChunk *Interval[*ChunkView]

	for x := c.chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value
		if remaining <= 0 {
			break
		}
		lastChunk = x

		// Handle gap before this chunk
		if startOffset < chunk.ViewOffset {
			gap := chunk.ViewOffset - startOffset
			gaps = append(gaps, struct{ start, length int64 }{startOffset - offset, gap})
			startOffset, remaining = chunk.ViewOffset, remaining-gap
			if remaining <= 0 {
				break
			}
		}

		chunkStart, chunkStop := max(chunk.ViewOffset, startOffset), min(chunk.ViewOffset+int64(chunk.ViewSize), startOffset+remaining)
		if chunkStart >= chunkStop {
			continue
		}

		bufferOffset := chunkStart - chunk.ViewOffset + chunk.OffsetInChunk
		tasks = append(tasks, &chunkReadTask{
			chunk:       chunk,
			bufferStart: startOffset - offset,
			bufferEnd:   chunkStop - chunkStart + startOffset - offset,
			chunkOffset: uint64(bufferOffset),
		})

		startOffset, remaining = chunkStop, remaining-(chunkStop-chunkStart)
	}

	// Zero-fill gaps
	for _, gap := range gaps {
		glog.V(4).Infof("zero [%d,%d)", offset+gap.start, offset+gap.start+gap.length)
		n += zero(p, gap.start, gap.length)
	}

	// If only one chunk or random access mode, use sequential reading
	if len(tasks) <= 1 || c.readerPattern.IsRandomMode() {
		for _, task := range tasks {
			copied, readErr := c.readChunkSliceAt(ctx, p[task.bufferStart:task.bufferEnd], task.chunk, nil, task.chunkOffset)
			ts = max(ts, task.chunk.ModifiedTsNs)
			if readErr != nil {
				glog.Errorf("fetching chunk %+v: %v\n", task.chunk, readErr)
				return n + copied, ts, readErr
			}
			n += copied
		}
	} else {
		// Parallel chunk fetching for multiple chunks
		// This significantly improves throughput when chunks are on different volume servers
		g, gCtx := errgroup.WithContext(ctx)

		// Limit concurrency to avoid overwhelming the system
		concurrency := c.prefetchCount
		if concurrency < minReadConcurrency {
			concurrency = minReadConcurrency
		}
		if concurrency > len(tasks) {
			concurrency = len(tasks)
		}
		g.SetLimit(concurrency)

		for _, task := range tasks {
			g.Go(func() error {
				// Read directly into the correct position in the output buffer
				copied, readErr := c.readChunkSliceAtForParallel(gCtx, p[task.bufferStart:task.bufferEnd], task.chunk, task.chunkOffset)
				task.bytesRead = copied
				task.modifiedTsNs = task.chunk.ModifiedTsNs
				return readErr
			})
		}

		// Wait for all chunk reads to complete
		if waitErr := g.Wait(); waitErr != nil {
			err = waitErr
		}

		// Aggregate results (order is preserved since we read directly into buffer positions)
		for _, task := range tasks {
			n += task.bytesRead
			ts = max(ts, task.modifiedTsNs)
		}

		if err != nil {
			return n, ts, err
		}
	}

	// Trigger prefetch for sequential reads
	if lastChunk != nil && lastChunk.Next != nil && c.prefetchCount > 0 && !c.readerPattern.IsRandomMode() {
		c.readerCache.MaybeCache(lastChunk.Next, c.prefetchCount)
	}

	// Zero the remaining bytes if a gap exists at the end
	if remaining > 0 {
		var delta int64
		if c.fileSize >= startOffset {
			delta = min(remaining, c.fileSize-startOffset)
			bufStart := startOffset - offset
			if delta > 0 {
				glog.V(4).Infof("zero2 [%d,%d) of file size %d bytes", startOffset, startOffset+delta, c.fileSize)
				n += zero(p, bufStart, delta)
			}
		}
	}

	if err == nil && offset+int64(len(p)) >= c.fileSize {
		err = io.EOF
	}

	return
}

func (c *ChunkReadAt) readChunkSliceAt(ctx context.Context, buffer []byte, chunkView *ChunkView, nextChunkViews *Interval[*ChunkView], offset uint64) (n int, err error) {

	if c.readerPattern.IsRandomMode() {
		n, err := c.readerCache.chunkCache.ReadChunkAt(buffer, chunkView.FileId, offset)
		if n > 0 {
			return n, err
		}
		return fetchChunkRange(ctx, buffer, c.readerCache.lookupFileIdFn, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset))
	}

	shouldCache := (uint64(chunkView.ViewOffset) + chunkView.ChunkSize) <= c.readerCache.chunkCache.GetMaxFilePartSizeInCache()
	n, err = c.readerCache.ReadChunkAt(ctx, buffer, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset), int(chunkView.ChunkSize), shouldCache)
	if c.lastChunkFid != chunkView.FileId {
		if chunkView.OffsetInChunk == 0 { // start of a new chunk
			if c.lastChunkFid != "" {
				c.readerCache.UnCache(c.lastChunkFid)
			}
			if nextChunkViews != nil && c.prefetchCount > 0 {
				// Prefetch multiple chunks ahead for better sequential read throughput
				// This keeps the network pipeline full with parallel chunk fetches
				c.readerCache.MaybeCache(nextChunkViews, c.prefetchCount)
			}
		}
	}
	c.lastChunkFid = chunkView.FileId
	return
}

// readChunkSliceAtForParallel is a simplified version for parallel chunk fetching
// It doesn't update lastChunkFid or trigger prefetch (handled by the caller)
func (c *ChunkReadAt) readChunkSliceAtForParallel(ctx context.Context, buffer []byte, chunkView *ChunkView, offset uint64) (n int, err error) {
	shouldCache := (uint64(chunkView.ViewOffset) + chunkView.ChunkSize) <= c.readerCache.chunkCache.GetMaxFilePartSizeInCache()
	return c.readerCache.ReadChunkAt(ctx, buffer, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int64(offset), int(chunkView.ChunkSize), shouldCache)
}

func zero(buffer []byte, start, length int64) int {
	if length <= 0 {
		return 0
	}
	end := min(start+length, int64(len(buffer)))
	start = max(start, 0)

	// zero the bytes
	for o := start; o < end; o++ {
		buffer[o] = 0
	}
	return int(end - start)
}
