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

type CacheInvalidator interface {
	InvalidateCache(fileId string)
}

type fetchChunkDataFnType func(ctx context.Context, buffer []byte, urlStrings []string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, fileId string, refreshUrls util_http.RefreshUrlsFunc) (n int, err error)

type ReaderCache struct {
	chunkCache       chunk_cache.ChunkCache
	lookupFileIdFn   wdclient.LookupFileIdFunctionType
	cacheInvalidator CacheInvalidator
	fetchChunkDataFn fetchChunkDataFnType
	sync.Mutex
	downloaders map[string]*SingleChunkCacher
	limit       int
	budget      *ReaderCacheBudget
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

func NewReaderCache(limit int, chunkCache chunk_cache.ChunkCache, lookupFileIdFn wdclient.LookupFileIdFunctionType, cacheInvalidator CacheInvalidator, budgets ...*ReaderCacheBudget) *ReaderCache {
	var budget *ReaderCacheBudget
	if len(budgets) > 0 {
		budget = budgets[0]
	}
	if budget == nil {
		budget = NewReaderCacheBudget(DefaultReaderCacheMemoryLimit)
	}
	return &ReaderCache{
		limit:            limit,
		budget:           budget,
		chunkCache:       chunkCache,
		lookupFileIdFn:   lookupFileIdFn,
		cacheInvalidator: cacheInvalidator,
		fetchChunkDataFn: util_http.RetriedFetchChunkData,
		downloaders:      make(map[string]*SingleChunkCacher),
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
retry:
	rc.Lock()

	for {
		if cacher, found := rc.downloaders[fileId]; found {
			if cacher.hasCompletedError() {
				delete(rc.downloaders, fileId)
				rc.Unlock()
				cacher.destroy()
				rc.Lock()
				continue
			}
			// Count this read on the cacher before releasing the map lock, so a
			// concurrent destroy() (error eviction here, LRU, or UnCache) cannot
			// start wg.Wait() on a zero counter while this read is about to register.
			cacher.wg.Add(1)
			rc.Unlock()
			n, err := cacher.readChunkAt(ctx, buffer, offset)
			if n > 0 || err != nil {
				return n, err
			}
			// If n=0 and err=nil, the cacher couldn't provide data for this offset.
			// Fall through to try chunkCache.
			rc.Lock()
		}
		break
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
			rc.Unlock()
			oldDownloader.destroy()
			goto retry
		}
	}

	// glog.V(4).Infof("cache1 %s", fileId)

	cacher := newSingleChunkCacher(rc, fileId, cipherKey, isGzipped, chunkSize, shouldCache)
	go cacher.startCaching()
	<-cacher.cacheStartedCh
	rc.downloaders[fileId] = cacher
	cacher.wg.Add(1)
	rc.Unlock()

	return cacher.readChunkAt(ctx, buffer, offset)
}

func (rc *ReaderCache) UnCache(fileId string) {
	rc.Lock()
	downloader := rc.downloaders[fileId]
	delete(rc.downloaders, fileId)
	rc.Unlock()
	if downloader != nil {
		downloader.destroy()
	}
}

func (rc *ReaderCache) remove(downloader *SingleChunkCacher) {
	rc.Lock()
	if rc.downloaders[downloader.chunkFileId] == downloader {
		delete(rc.downloaders, downloader.chunkFileId)
	}
	rc.Unlock()
	downloader.destroy()
}

func (rc *ReaderCache) destroy() {
	rc.Lock()
	downloaders := rc.downloaders
	rc.downloaders = make(map[string]*SingleChunkCacher)
	rc.Unlock()
	for _, downloader := range downloaders {
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

// startCaching downloads a chunk shared by concurrent readers.
func (s *SingleChunkCacher) startCaching() {
	s.wg.Add(1)
	defer func() {
		close(s.done)
		s.wg.Done()
		if s.hasCompletedError() {
			s.parent.remove(s)
		} else {
			s.parent.budget.complete(s)
		}
	}()

	s.cacheStartedCh <- struct{}{}
	if err := s.parent.budget.reserve(s); err != nil {
		s.setError(err)
		return
	}

	// Intentionally use context.Background(), not a request-specific context.
	// The downloaded chunk is a shared resource: multiple concurrent readers may
	// wait on this same download via s.done. A request-scoped context that got
	// cancelled would abort the download and error every other waiting reader.
	// The download always runs to completion once started; readers that cancel
	// individually drop out via readChunkAt's select on ctx.Done().
	urlStrings, err := s.parent.lookupFileIdFn(context.Background(), s.chunkFileId)
	if err != nil {
		s.setError(fmt.Errorf("operation LookupFileId %s failed, err: %v", s.chunkFileId, err))
		return
	}
	if len(urlStrings) == 0 {
		s.setError(fmt.Errorf("operation LookupFileId %s failed, err: urls not found", s.chunkFileId))
		return
	}

	data, fetchErr := s.fetchChunkData(context.Background(), urlStrings)
	if fetchErr != nil {
		data, fetchErr = s.retryFetchAfterCacheInvalidation(context.Background(), urlStrings, fetchErr)
	}

	// Now acquire lock to update state
	s.Lock()
	atomic.StoreInt64(&s.completedTimeNew, time.Now().UnixNano())
	if fetchErr != nil {
		s.err = fetchErr
	} else {
		s.data = data
		if s.shouldCache {
			s.parent.chunkCache.SetChunk(s.chunkFileId, s.data)
		}
	}
	s.Unlock()
}

func (s *SingleChunkCacher) setError(err error) {
	s.Lock()
	defer s.Unlock()
	s.err = err
	atomic.StoreInt64(&s.completedTimeNew, time.Now().UnixNano())
}

func (s *SingleChunkCacher) hasCompletedError() bool {
	if atomic.LoadInt64(&s.completedTimeNew) == 0 {
		return false
	}
	s.Lock()
	defer s.Unlock()
	return s.err != nil
}

func (s *SingleChunkCacher) fetchChunkData(ctx context.Context, urlStrings []string) ([]byte, error) {
	// Allocate buffer and download without holding the lock.
	// This allows multiple downloads to proceed in parallel.
	data := mem.Allocate(s.chunkSize)
	_, fetchErr := s.parent.fetchChunkDataFn(ctx, data, urlStrings, s.cipherKey, s.isGzipped, true, 0, s.chunkFileId, refreshUrls(ctx, s.parent.cacheInvalidator, s.parent.lookupFileIdFn, s.chunkFileId))
	if fetchErr != nil {
		mem.Free(data)
		return nil, fetchErr
	}
	return data, nil
}

func (s *SingleChunkCacher) retryFetchAfterCacheInvalidation(ctx context.Context, oldUrlStrings []string, originalErr error) ([]byte, error) {
	var data []byte
	err := retryFetchWithFreshLocations(ctx, s.parent.cacheInvalidator, s.parent.lookupFileIdFn, s.chunkFileId, oldUrlStrings, originalErr, func(newUrls []string) error {
		var fetchErr error
		data, fetchErr = s.fetchChunkData(ctx, newUrls)
		return fetchErr
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *SingleChunkCacher) destroy() {
	// wait for all reads to finish before destroying the data
	s.wg.Wait()
	s.Lock()
	if s.data != nil {
		mem.Free(s.data)
		s.data = nil
	}
	s.Unlock()
	s.parent.budget.release(s)
}

// readChunkAt reads data from the cached chunk.
// It waits for the download to complete if it's still in progress.
// The ctx parameter allows the reader to cancel its wait (but the download continues
// for other readers - see comment in startCaching about shared resource semantics).
// The caller must s.wg.Add(1) under the ReaderCache lock before calling; this only releases it.
func (s *SingleChunkCacher) readChunkAt(ctx context.Context, buf []byte, offset int64) (int, error) {
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
