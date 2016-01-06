package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/util"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	rp                       *storage.ReplicaPlacement
	storageType2VolumeLayout *util.ConcurrentReadMap
}

func NewCollection(name string, rp *storage.ReplicaPlacement, volumeSizeLimit uint64) *Collection {
	c := &Collection{Name: name, volumeSizeLimit: volumeSizeLimit, rp: rp}
	c.storageType2VolumeLayout = util.NewConcurrentReadMap()
	return c
}

func (c *Collection) String() string {
	return fmt.Sprintf("Name:%s, volumeSizeLimit:%d, storageType2VolumeLayout:%v", c.Name, c.volumeSizeLimit, c.storageType2VolumeLayout)
}

func (c *Collection) GetOrCreateVolumeLayout(ttl *storage.TTL) *VolumeLayout {
	keyString := ""
	if ttl != nil {
		keyString += ttl.String()
	}
	vl := c.storageType2VolumeLayout.Get(keyString, func() interface{} {
		return NewVolumeLayout(c.rp, ttl, c.volumeSizeLimit)
	})
	return vl.(*VolumeLayout)
}

func (c *Collection) Lookup(vid storage.VolumeId) *VolumeLocationList {
	for _, vl := range c.storageType2VolumeLayout.Items {
		if vl != nil {
			if list := vl.(*VolumeLayout).Lookup(vid); list != nil {
				return list
			}
		}
	}
	return nil
}

func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	for _, vl := range c.storageType2VolumeLayout.Items {
		if vl != nil {
			if list := vl.(*VolumeLayout).ListVolumeServers(); list != nil {
				nodes = append(nodes, list...)
			}
		}
	}
	return
}
