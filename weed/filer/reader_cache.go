package filer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type ReaderCache struct {
	chunkCache     chunk_cache.ChunkCache
	lookupFileIdFn wdclient.LookupFileIdFunctionType
	sync.Mutex
	downloaders map[string]*SingleChunkCacher
	limit       int
}

type SingleChunkCacher struct {
	completedTimeNew int64
	sync.Mutex
	parent         *ReaderCache
	chunkFileId    string
	data           []byte
	err            error
	cipherKey      []byte
	isGzipped      bool
	chunkSize      int
	shouldCache    bool
	wg             sync.WaitGroup
	cacheStartedCh chan struct{}
	done           chan struct{} // signals when download is complete
}

func NewReaderCache(limit int, chunkCache chunk_cache.ChunkCache, lookupFileIdFn wdclient.LookupFileIdFunctionType) *ReaderCache {
	return &ReaderCache{
		limit:          limit,
		chunkCache:     chunkCache,
		lookupFileIdFn: lookupFileIdFn,
		downloaders:    make(map[string]*SingleChunkCacher),
	}
}

// MaybeCache prefetches up to 'count' chunks ahead in parallel.
// This improves read throughput for sequential reads by keeping the
// network pipeline full with parallel chunk fetches.
func (rc *ReaderCache) MaybeCache(chunkViews *Interval[*ChunkView], count int) {
	if rc.lookupFileIdFn == nil {
		return
	}
	if count <= 0 {
		count = 1
	}

	rc.Lock()
	defer rc.Unlock()

	if len(rc.downloaders) >= rc.limit {
		return
	}

	cached := 0
	for x := chunkViews; x != nil && cached < count; x = x.Next {
		chunkView := x.Value
		if _, found := rc.downloaders[chunkView.FileId]; found {
			continue
		}
		if rc.chunkCache.IsInCache(chunkView.FileId, true) {
			glog.V(4).Infof("%s is in cache", chunkView.FileId)
			continue
		}

		if len(rc.downloaders) >= rc.limit {
			// abort when slots are filled
			return
		}

		// glog.V(4).Infof("prefetch %s offset %d", chunkView.FileId, chunkView.ViewOffset)
		// cache this chunk if not yet
		shouldCache := (uint64(chunkView.ViewOffset) + chunkView.ChunkSize) <= rc.chunkCache.GetMaxFilePartSizeInCache()
		cacher := newSingleChunkCacher(rc, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int(chunkView.ChunkSize), shouldCache)
		go cacher.startCaching()
		<-cacher.cacheStartedCh
		rc.downloaders[chunkView.FileId] = cacher
		cached++
	}

	return
}

func (rc *ReaderCache) ReadChunkAt(ctx context.Context, buffer []byte, fileId string, cipherKey []byte, isGzipped bool, offset int64, chunkSize int, shouldCache bool) (int, error) {
	rc.Lock()

	if cacher, found := rc.downloaders[fileId]; found {
		rc.Unlock()
		n, err := cacher.readChunkAt(ctx, buffer, offset)
		if n > 0 || err != nil {
			return n, err
		}
		// If n=0 and err=nil, the cacher couldn't provide data for this offset.
		// Fall through to try chunkCache.
		rc.Lock()
	}
	if shouldCache || rc.lookupFileIdFn == nil {
		n, err := rc.chunkCache.ReadChunkAt(buffer, fileId, uint64(offset))
		if n > 0 {
			rc.Unlock()
			return n, err
		}
	}

	// clean up old downloaders
	if len(rc.downloaders) >= rc.limit {
		oldestFid, oldestTime := "", time.Now().UnixNano()
		for fid, downloader := range rc.downloaders {
			completedTime := atomic.LoadInt64(&downloader.completedTimeNew)
			if completedTime > 0 && completedTime < oldestTime {
				oldestFid, oldestTime = fid, completedTime
			}
		}
		if oldestFid != "" {
			oldDownloader := rc.downloaders[oldestFid]
			delete(rc.downloaders, oldestFid)
			oldDownloader.destroy()
		}
	}

	// glog.V(4).Infof("cache1 %s", fileId)

	cacher := newSingleChunkCacher(rc, fileId, cipherKey, isGzipped, chunkSize, shouldCache)
	go cacher.startCaching()
	<-cacher.cacheStartedCh
	rc.downloaders[fileId] = cacher
	rc.Unlock()

	return cacher.readChunkAt(ctx, buffer, offset)
}

func (rc *ReaderCache) UnCache(fileId string) {
	rc.Lock()
	defer rc.Unlock()
	// glog.V(4).Infof("uncache %s", fileId)
	if downloader, found := rc.downloaders[fileId]; found {
		downloader.destroy()
		delete(rc.downloaders, fileId)
	}
}

func (rc *ReaderCache) destroy() {
	rc.Lock()
	defer rc.Unlock()

	for _, downloader := range rc.downloaders {
		downloader.destroy()
	}

}

func newSingleChunkCacher(parent *ReaderCache, fileId string, cipherKey []byte, isGzipped bool, chunkSize int, shouldCache bool) *SingleChunkCacher {
	return &SingleChunkCacher{
		parent:         parent,
		chunkFileId:    fileId,
		cipherKey:      cipherKey,
		isGzipped:      isGzipped,
		chunkSize:      chunkSize,
		shouldCache:    shouldCache,
		cacheStartedCh: make(chan struct{}),
		done:           make(chan struct{}),
	}
}

// startCaching downloads the chunk data in the background.
// It does NOT hold the lock during the HTTP download to allow concurrent readers
// to wait efficiently using the done channel.
func (s *SingleChunkCacher) startCaching() {
	s.wg.Add(1)
	defer s.wg.Done()
	defer close(s.done) // guarantee completion signal even on panic

	s.cacheStartedCh <- struct{}{} // signal that we've started

	// Note: We intentionally use context.Background() here, NOT a request-specific context.
	// The downloaded chunk is a shared resource - multiple concurrent readers may be waiting
	// for this same download to complete. If we used a request context and that request was
	// cancelled, it would abort the download and cause errors for all other waiting readers.
	// The download should always complete once started to serve all potential consumers.

	// Lookup file ID without holding the lock
	urlStrings, err := s.parent.lookupFileIdFn(context.Background(), s.chunkFileId)
	if err != nil {
		s.Lock()
		s.err = fmt.Errorf("operation LookupFileId %s failed, err: %v", s.chunkFileId, err)
		s.Unlock()
		return
	}

	// Allocate buffer and download without holding the lock
	// This allows multiple downloads to proceed in parallel
	data := mem.Allocate(s.chunkSize)
	_, fetchErr := util_http.RetriedFetchChunkData(context.Background(), data, urlStrings, s.cipherKey, s.isGzipped, true, 0, s.chunkFileId)

	// Now acquire lock to update state
	s.Lock()
	if fetchErr != nil {
		mem.Free(data)
		s.err = fetchErr
	} else {
		s.data = data
		if s.shouldCache {
			s.parent.chunkCache.SetChunk(s.chunkFileId, s.data)
		}
		atomic.StoreInt64(&s.completedTimeNew, time.Now().UnixNano())
	}
	s.Unlock()
}

func (s *SingleChunkCacher) destroy() {
	// wait for all reads to finish before destroying the data
	s.wg.Wait()
	s.Lock()
	defer s.Unlock()

	if s.data != nil {
		mem.Free(s.data)
		s.data = nil
	}
}

// readChunkAt reads data from the cached chunk.
// It waits for the download to complete if it's still in progress.
// The ctx parameter allows the reader to cancel its wait (but the download continues
// for other readers - see comment in startCaching about shared resource semantics).
func (s *SingleChunkCacher) readChunkAt(ctx context.Context, buf []byte, offset int64) (int, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	// Wait for download to complete, but allow reader cancellation.
	// Prioritize checking done first - if data is already available,
	// return it even if context is also cancelled.
	select {
	case <-s.done:
		// Download already completed, proceed immediately
	default:
		// Download not complete, wait for it or context cancellation
		select {
		case <-s.done:
			// Download completed
		case <-ctx.Done():
			// Reader cancelled while waiting - download continues for other readers
			return 0, ctx.Err()
		}
	}

	s.Lock()
	defer s.Unlock()

	if s.err != nil {
		return 0, s.err
	}

	if len(s.data) <= int(offset) {
		return 0, nil
	}

	return copy(buf, s.data[offset:]), nil
}
