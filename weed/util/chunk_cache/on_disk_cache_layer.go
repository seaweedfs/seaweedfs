package chunk_cache

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"path"
	"slices"
)

type OnDiskCacheLayer struct {
	diskCaches []*ChunkCacheVolume
}

func NewOnDiskCacheLayer(dir, namePrefix string, diskSize int64, segmentCount int) *OnDiskCacheLayer {

	volumeCount, volumeSize := int(diskSize/(30000*1024*1024)), int64(30000*1024*1024)
	if volumeCount < segmentCount {
		volumeCount, volumeSize = segmentCount, diskSize/int64(segmentCount)
	}

	c := &OnDiskCacheLayer{}
	for i := 0; i < volumeCount; i++ {
		fileName := path.Join(dir, fmt.Sprintf("%s_%d", namePrefix, i))
		diskCache, err := LoadOrCreateChunkCacheVolume(fileName, volumeSize)
		if err != nil {
			glog.Errorf("failed to add cache %s : %v", fileName, err)
		} else {
			c.diskCaches = append(c.diskCaches, diskCache)
		}
	}

	// keep newest cache to the front
	slices.SortFunc(c.diskCaches, func(a, b *ChunkCacheVolume) int {
		return b.lastModTime.Compare(a.lastModTime)
	})
	return c
}

func (c *OnDiskCacheLayer) setChunk(needleId types.NeedleId, data []byte) {

	if len(c.diskCaches) == 0 {
		return
	}

	if c.diskCaches[0].fileSize+int64(len(data)) > c.diskCaches[0].sizeLimit {
		t, resetErr := c.diskCaches[len(c.diskCaches)-1].Reset()
		if resetErr != nil {
			glog.Errorf("failed to reset cache file %s", c.diskCaches[len(c.diskCaches)-1].fileName)
			return
		}
		for i := len(c.diskCaches) - 1; i > 0; i-- {
			c.diskCaches[i] = c.diskCaches[i-1]
		}
		c.diskCaches[0] = t
	}

	if err := c.diskCaches[0].WriteNeedle(needleId, data); err != nil {
		glog.V(0).Infof("cache write %v size %d: %v", needleId, len(data), err)
	}

}

func (c *OnDiskCacheLayer) getChunk(needleId types.NeedleId) (data []byte) {

	var err error

	for _, diskCache := range c.diskCaches {
		data, err = diskCache.GetNeedle(needleId)
		if err == storage.ErrorNotFound {
			continue
		}
		if err != nil {
			glog.Errorf("failed to read cache file %s id %d", diskCache.fileName, needleId)
			continue
		}
		if len(data) != 0 {
			return
		}
	}

	return nil

}

func (c *OnDiskCacheLayer) getChunkSlice(needleId types.NeedleId, offset, length uint64) (data []byte) {

	var err error

	for _, diskCache := range c.diskCaches {
		data, err = diskCache.getNeedleSlice(needleId, offset, length)
		if err == storage.ErrorNotFound {
			continue
		}
		if err != nil {
			glog.Warningf("failed to read cache file %s id %d: %v", diskCache.fileName, needleId, err)
			continue
		}
		if len(data) != 0 {
			return
		}
	}

	return nil

}

func (c *OnDiskCacheLayer) readChunkAt(buffer []byte, needleId types.NeedleId, offset uint64) (n int, err error) {

	for _, diskCache := range c.diskCaches {
		n, err = diskCache.readNeedleSliceAt(buffer, needleId, offset)
		if err == storage.ErrorNotFound {
			continue
		}
		if err != nil {
			glog.Warningf("failed to read cache file %s id %d: %v", diskCache.fileName, needleId, err)
			continue
		}
		if n > 0 {
			return
		}
	}

	return

}

func (c *OnDiskCacheLayer) shutdown() {

	for _, diskCache := range c.diskCaches {
		diskCache.Shutdown()
	}

}
