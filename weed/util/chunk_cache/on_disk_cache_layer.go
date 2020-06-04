package chunk_cache

import (
	"fmt"
	"path"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

type OnDiskCacheLayer struct {
	diskCaches []*ChunkCacheVolume
}

func NewOnDiskCacheLayer(dir, namePrefix string, diskSizeMB int64, segmentCount int) *OnDiskCacheLayer {

	volumeCount, volumeSize := int(diskSizeMB/30000), int64(30000)
	if volumeCount < segmentCount {
		volumeCount, volumeSize = segmentCount, diskSizeMB/int64(segmentCount)
	}

	c := &OnDiskCacheLayer{}
	for i := 0; i < volumeCount; i++ {
		fileName := path.Join(dir, fmt.Sprintf("%s_%d", namePrefix, i))
		diskCache, err := LoadOrCreateChunkCacheVolume(fileName, volumeSize*1024*1024)
		if err != nil {
			glog.Errorf("failed to add cache %s : %v", fileName, err)
		} else {
			c.diskCaches = append(c.diskCaches, diskCache)
		}
	}

	// keep newest cache to the front
	sort.Slice(c.diskCaches, func(i, j int) bool {
		return c.diskCaches[i].lastModTime.After(c.diskCaches[j].lastModTime)
	})

	return c
}

func (c *OnDiskCacheLayer) setChunk(needleId types.NeedleId, data []byte) {

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

	c.diskCaches[0].WriteNeedle(needleId, data)

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

func (c *OnDiskCacheLayer) shutdown() {

	for _, diskCache := range c.diskCaches {
		diskCache.Shutdown()
	}

}
