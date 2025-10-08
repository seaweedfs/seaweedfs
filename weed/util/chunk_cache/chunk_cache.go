package chunk_cache

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var ErrorOutOfBounds = errors.New("attempt to read out of bounds")

const cacheHeaderSize = 8 // 4 bytes volumeId + 4 bytes cookie

// parseCacheHeader extracts volume ID and cookie from the 8-byte cache header
func parseCacheHeader(header []byte) (needle.VolumeId, types.Cookie) {
	volumeId := needle.VolumeId(binary.BigEndian.Uint32(header[0:4]))
	cookie := types.BytesToCookie(header[4:8])
	return volumeId, cookie
}

type ChunkCache interface {
	ReadChunkAt(data []byte, fileId string, offset uint64) (n int, err error)
	SetChunk(fileId string, data []byte)
	IsInCache(fileId string, lockNeeded bool) (answer bool)
	GetMaxFilePartSizeInCache() (answer uint64)
}

// a global cache for recently accessed file chunks
type TieredChunkCache struct {
	memCache   *ChunkCacheInMemory
	diskCaches []*OnDiskCacheLayer
	sync.RWMutex
	onDiskCacheSizeLimit0  uint64
	onDiskCacheSizeLimit1  uint64
	onDiskCacheSizeLimit2  uint64
	maxFilePartSizeInCache uint64
}

var _ ChunkCache = &TieredChunkCache{}

func NewTieredChunkCache(maxEntries int64, dir string, diskSizeInUnit int64, unitSize int64) *TieredChunkCache {

	c := &TieredChunkCache{
		memCache: NewChunkCacheInMemory(maxEntries),
	}
	c.diskCaches = make([]*OnDiskCacheLayer, 3)
	c.onDiskCacheSizeLimit0 = uint64(unitSize)
	c.onDiskCacheSizeLimit1 = 4 * c.onDiskCacheSizeLimit0
	c.onDiskCacheSizeLimit2 = 2 * c.onDiskCacheSizeLimit1
	c.diskCaches[0] = NewOnDiskCacheLayer(dir, "c0_2", diskSizeInUnit*unitSize/8, 2)
	c.diskCaches[1] = NewOnDiskCacheLayer(dir, "c1_3", diskSizeInUnit*unitSize/4+diskSizeInUnit*unitSize/8, 3)
	c.diskCaches[2] = NewOnDiskCacheLayer(dir, "c2_2", diskSizeInUnit*unitSize/2, 2)
	c.maxFilePartSizeInCache = uint64(unitSize*diskSizeInUnit) / 4

	return c
}

func (c *TieredChunkCache) GetMaxFilePartSizeInCache() (answer uint64) {
	if c == nil {
		return 0
	}
	return c.maxFilePartSizeInCache
}

func (c *TieredChunkCache) IsInCache(fileId string, lockNeeded bool) (answer bool) {
	if c == nil {
		return false
	}

	if lockNeeded {
		c.RLock()
		defer c.RUnlock()
	}

	item := c.memCache.cache.Get(fileId)
	if item != nil {
		glog.V(4).Infof("fileId %s is in memcache", fileId)
		return true
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.V(4).Infof("failed to parse file id %s", fileId)
		return false
	}

	// Check disk cache with volume ID and cookie validation
	for i, diskCacheLayer := range c.diskCaches {
		for k, v := range diskCacheLayer.diskCaches {
			if nv, ok := v.nm.Get(fid.Key); ok {
				// Read cache header to check volume ID and cookie
				headerBytes := make([]byte, cacheHeaderSize)
				if readN, readErr := v.DataBackend.ReadAt(headerBytes, nv.Offset.ToActualOffset()); readErr == nil && readN == cacheHeaderSize {
					// Parse volume ID and cookie from header
					storedVolumeId, storedCookie := parseCacheHeader(headerBytes)

					if storedVolumeId == fid.VolumeId && storedCookie == fid.Cookie {
						glog.V(4).Infof("fileId %s is in diskCaches[%d].volume[%d]", fileId, i, k)
						return true
					}
					glog.V(4).Infof("fileId %s header mismatch in diskCaches[%d].volume[%d]: stored volume %d cookie %x, expected volume %d cookie %x",
						fileId, i, k, storedVolumeId, storedCookie, fid.VolumeId, fid.Cookie)
				}
			}
		}
	}
	return false
}

func (c *TieredChunkCache) ReadChunkAt(data []byte, fileId string, offset uint64) (n int, err error) {
	if c == nil {
		return 0, nil
	}

	c.RLock()
	defer c.RUnlock()

	minSize := offset + uint64(len(data))
	if minSize <= c.onDiskCacheSizeLimit0 {
		n, err = c.memCache.readChunkAt(data, fileId, offset)
		if err != nil {
			glog.Errorf("failed to read from memcache: %s", err)
		}
		if n == int(len(data)) {
			return n, nil
		}
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return 0, nil
	}

	// Try disk caches with volume ID and cookie validation
	if minSize <= c.onDiskCacheSizeLimit0 {
		n, err = c.readChunkAtWithHeaderValidation(data, fid, offset, 0)
		if n == int(len(data)) {
			return
		}
	}
	if minSize <= c.onDiskCacheSizeLimit1 {
		n, err = c.readChunkAtWithHeaderValidation(data, fid, offset, 1)
		if n == int(len(data)) {
			return
		}
	}
	{
		n, err = c.readChunkAtWithHeaderValidation(data, fid, offset, 2)
		if n == int(len(data)) {
			return
		}
	}

	return 0, nil

}

func (c *TieredChunkCache) SetChunk(fileId string, data []byte) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	glog.V(4).Infof("SetChunk %s size %d\n", fileId, len(data))
	if c.IsInCache(fileId, false) {
		glog.V(4).Infof("fileId %s is already in cache", fileId)
		return
	}

	c.doSetChunk(fileId, data)
}

func (c *TieredChunkCache) doSetChunk(fileId string, data []byte) {
	// Disk cache format: [4-byte volumeId][4-byte cookie][chunk data]
	// Memory cache format: full fileId as key -> raw data (unchanged)

	// Memory cache unchanged - uses full fileId
	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.memCache.SetChunk(fileId, data)
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}

	// Prepend volume ID and cookie to data for disk cache
	// Format: [4-byte volumeId][4-byte cookie][chunk data]
	headerBytes := make([]byte, cacheHeaderSize)
	// Store volume ID in first 4 bytes using big-endian
	binary.BigEndian.PutUint32(headerBytes[0:4], uint32(fid.VolumeId))
	// Store cookie in next 4 bytes
	types.CookieToBytes(headerBytes[4:8], fid.Cookie)
	dataWithHeader := append(headerBytes, data...)

	// Store with volume ID and cookie header in disk cache
	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.diskCaches[0].setChunk(fid.Key, dataWithHeader)
	} else if len(data) <= int(c.onDiskCacheSizeLimit1) {
		c.diskCaches[1].setChunk(fid.Key, dataWithHeader)
	} else {
		c.diskCaches[2].setChunk(fid.Key, dataWithHeader)
	}

}

func (c *TieredChunkCache) Shutdown() {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, diskCache := range c.diskCaches {
		diskCache.shutdown()
	}
}

// readChunkAtWithHeaderValidation reads from disk cache with volume ID and cookie validation
func (c *TieredChunkCache) readChunkAtWithHeaderValidation(data []byte, fid *needle.FileId, offset uint64, cacheLevel int) (n int, err error) {
	// Step 1: Read and validate header (volume ID + cookie)
	headerBuffer := make([]byte, cacheHeaderSize)
	headerRead, err := c.diskCaches[cacheLevel].readChunkAt(headerBuffer, fid.Key, 0)

	if err != nil {
		glog.V(4).Infof("failed to read header for %s from cache level %d: %v",
			fid.String(), cacheLevel, err)
		return 0, err
	}

	if headerRead < cacheHeaderSize {
		glog.V(4).Infof("insufficient data for header validation for %s from cache level %d: read %d bytes",
			fid.String(), cacheLevel, headerRead)
		return 0, nil // Not enough data for header - likely old format, treat as cache miss
	}

	// Parse volume ID and cookie from header
	storedVolumeId, storedCookie := parseCacheHeader(headerBuffer)

	// Validate both volume ID and cookie
	if storedVolumeId != fid.VolumeId || storedCookie != fid.Cookie {
		glog.V(4).Infof("header mismatch for %s in cache level %d: stored volume %d cookie %x, expected volume %d cookie %x (possible old format)",
			fid.String(), cacheLevel, storedVolumeId, storedCookie, fid.VolumeId, fid.Cookie)
		return 0, nil // Treat as cache miss - could be old format or actual mismatch
	}

	// Step 2: Read actual data from the offset position (after header)
	// The disk cache has format: [4-byte volumeId][4-byte cookie][actual chunk data]
	// We want to read from position: cacheHeaderSize + offset
	dataOffset := cacheHeaderSize + offset
	n, err = c.diskCaches[cacheLevel].readChunkAt(data, fid.Key, dataOffset)

	if err != nil {
		glog.V(4).Infof("failed to read data at offset %d for %s from cache level %d: %v",
			offset, fid.String(), cacheLevel, err)
		return 0, err
	}

	return n, nil
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
