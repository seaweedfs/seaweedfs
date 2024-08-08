package filer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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
}

func NewReaderCache(limit int, chunkCache chunk_cache.ChunkCache, lookupFileIdFn wdclient.LookupFileIdFunctionType) *ReaderCache {
	return &ReaderCache{
		limit:          limit,
		chunkCache:     chunkCache,
		lookupFileIdFn: lookupFileIdFn,
		downloaders:    make(map[string]*SingleChunkCacher),
	}
}

func (rc *ReaderCache) MaybeCache(chunkViews *Interval[*ChunkView]) {
	if rc.lookupFileIdFn == nil {
		return
	}

	rc.Lock()
	defer rc.Unlock()

	if len(rc.downloaders) >= rc.limit {
		return
	}

	for x := chunkViews; x != nil; x = x.Next {
		chunkView := x.Value
		if _, found := rc.downloaders[chunkView.FileId]; found {
			continue
		}

		if len(rc.downloaders) >= rc.limit {
			// abort when slots are filled
			return
		}

		// glog.V(4).Infof("prefetch %s offset %d", chunkView.FileId, chunkView.ViewOffset)
		// cache this chunk if not yet
		cacher := newSingleChunkCacher(rc, chunkView.FileId, chunkView.CipherKey, chunkView.IsGzipped, int(chunkView.ChunkSize), false)
		go cacher.startCaching()
		<-cacher.cacheStartedCh
		rc.downloaders[chunkView.FileId] = cacher

	}

	return
}

func (rc *ReaderCache) ReadChunkAt(buffer []byte, fileId string, cipherKey []byte, isGzipped bool, offset int64, chunkSize int, shouldCache bool) (int, error) {
	rc.Lock()

	if cacher, found := rc.downloaders[fileId]; found {
		if n, err := cacher.readChunkAt(buffer, offset); n != 0 && err == nil {
			rc.Unlock()
			return n, err
		}
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

	return cacher.readChunkAt(buffer, offset)
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
	}
}

func (s *SingleChunkCacher) startCaching() {
	s.wg.Add(1)
	defer s.wg.Done()
	s.Lock()
	defer s.Unlock()

	s.cacheStartedCh <- struct{}{} // means this has been started

	urlStrings, err := s.parent.lookupFileIdFn(s.chunkFileId)
	if err != nil {
		s.err = fmt.Errorf("operation LookupFileId %s failed, err: %v", s.chunkFileId, err)
		return
	}

	s.data = mem.Allocate(s.chunkSize)

	_, s.err = util_http.RetriedFetchChunkData(s.data, urlStrings, s.cipherKey, s.isGzipped, true, 0)
	if s.err != nil {
		mem.Free(s.data)
		s.data = nil
		return
	}

	if s.shouldCache {
		s.parent.chunkCache.SetChunk(s.chunkFileId, s.data)
	}
	atomic.StoreInt64(&s.completedTimeNew, time.Now().UnixNano())

	return
}

func (s *SingleChunkCacher) destroy() {
	// wait for all reads to finish before destroying the data
	s.wg.Wait()
	s.Lock()
	defer s.Unlock()

	if s.data != nil {
		mem.Free(s.data)
		s.data = nil
		close(s.cacheStartedCh)
	}
}

func (s *SingleChunkCacher) readChunkAt(buf []byte, offset int64) (int, error) {
	s.wg.Add(1)
	defer s.wg.Done()
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
