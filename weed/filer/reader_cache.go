package filer

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util/chunk_cache"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"sync"
	"time"
)

type ReaderCache struct {
	chunkCache     chunk_cache.ChunkCache
	lookupFileIdFn wdclient.LookupFileIdFunctionType
	sync.Mutex
	downloaders map[string]*SingleChunkCacher
	limit       int
}

type SingleChunkCacher struct {
	sync.RWMutex
	parent        *ReaderCache
	chunkFileId   string
	data          []byte
	err           error
	cipherKey     []byte
	isGzipped     bool
	chunkSize     int
	shouldCache   bool
	wg            sync.WaitGroup
	completedTime time.Time
}

func newReaderCache(limit int, chunkCache chunk_cache.ChunkCache, lookupFileIdFn wdclient.LookupFileIdFunctionType) *ReaderCache {
	return &ReaderCache{
		limit:          limit,
		chunkCache:     chunkCache,
		lookupFileIdFn: lookupFileIdFn,
		downloaders:    make(map[string]*SingleChunkCacher),
	}
}

func (rc *ReaderCache) MaybeCache(fileId string, cipherKey []byte, isGzipped bool, chunkSize int) {
	rc.Lock()
	defer rc.Unlock()
	if _, found := rc.downloaders[fileId]; found {
		return
	}
	if rc.lookupFileIdFn == nil {
		return
	}

	// if too many, delete one of them?
	glog.V(0).Infof("downloader2 %d", len(rc.downloaders))
	if len(rc.downloaders) >= rc.limit {
		oldestFid, oldestTime := "", time.Now()
		for fid, downloader := range rc.downloaders {
			if !downloader.completedTime.IsZero() {
				if downloader.completedTime.Before(oldestTime) {
					oldestFid, oldestTime = fid, downloader.completedTime
				}
			}
		}
		if oldestFid != "" {
			oldDownloader := rc.downloaders[oldestFid]
			delete(rc.downloaders, oldestFid)
			oldDownloader.destroy()
		} else {
			// if still no slots, return
			return
		}
	}

	cacher := newSingleChunkCacher(rc, fileId, cipherKey, isGzipped, chunkSize, false)
	cacher.wg.Add(1)
	go cacher.startCaching()
	cacher.wg.Wait()
	rc.downloaders[fileId] = cacher

	return
}

func (rc *ReaderCache) ReadChunkAt(buffer []byte, fileId string, cipherKey []byte, isGzipped bool, offset int64, chunkSize int, shouldCache bool) (int, error) {
	rc.Lock()
	defer rc.Unlock()
	if cacher, found := rc.downloaders[fileId]; found {
		return cacher.readChunkAt(buffer, offset)
	}
	if shouldCache || rc.lookupFileIdFn == nil {
		n, err := rc.chunkCache.ReadChunkAt(buffer, fileId, uint64(offset))
		if n > 0 {
			return n, err
		}
	}

	glog.V(0).Infof("downloader1 %d", len(rc.downloaders))
	if len(rc.downloaders) >= rc.limit {
		oldestFid, oldestTime := "", time.Now()
		for fid, downloader := range rc.downloaders {
			if !downloader.completedTime.IsZero() {
				if downloader.completedTime.Before(oldestTime) {
					oldestFid, oldestTime = fid, downloader.completedTime
				}
			}
		}
		if oldestFid != "" {
			oldDownloader := rc.downloaders[oldestFid]
			delete(rc.downloaders, oldestFid)
			oldDownloader.destroy()
		}
	}

	cacher := newSingleChunkCacher(rc, fileId, cipherKey, isGzipped, chunkSize, shouldCache)
	cacher.wg.Add(1)
	go cacher.startCaching()
	cacher.wg.Wait()
	rc.downloaders[fileId] = cacher

	return cacher.readChunkAt(buffer, offset)
}

func (rc *ReaderCache) destroy() {
	rc.Lock()
	defer rc.Unlock()

	for _, downloader := range rc.downloaders {
		downloader.destroy()
	}

}

func newSingleChunkCacher(parent *ReaderCache, fileId string, cipherKey []byte, isGzipped bool, chunkSize int, shouldCache bool) *SingleChunkCacher {
	t := &SingleChunkCacher{
		parent:      parent,
		chunkFileId: fileId,
		cipherKey:   cipherKey,
		isGzipped:   isGzipped,
		chunkSize:   chunkSize,
		shouldCache: shouldCache,
	}
	return t
}

func (s *SingleChunkCacher) startCaching() {
	s.Lock()
	defer s.Unlock()

	s.wg.Done() // means this has been started

	urlStrings, err := s.parent.lookupFileIdFn(s.chunkFileId)
	if err != nil {
		s.err = fmt.Errorf("operation LookupFileId %s failed, err: %v", s.chunkFileId, err)
		return
	}

	s.data = mem.Allocate(s.chunkSize)

	_, s.err = retriedFetchChunkData(s.data, urlStrings, s.cipherKey, s.isGzipped, true, 0)
	if s.err != nil {
		mem.Free(s.data)
		s.data = nil
		return
	}

	s.completedTime = time.Now()
	if s.shouldCache {
		s.parent.chunkCache.SetChunk(s.chunkFileId, s.data)
	}

	return
}

func (s *SingleChunkCacher) destroy() {
	if s.data != nil {
		mem.Free(s.data)
		s.data = nil
	}
}

func (s *SingleChunkCacher) readChunkAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	return copy(buf, s.data[offset:]), s.err

}
