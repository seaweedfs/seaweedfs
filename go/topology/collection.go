package topology

import (
	"github.com/aszxqw/weed-fs/go/storage"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	storageType2VolumeLayout map[string]*VolumeLayout
}

func NewCollection(name string, volumeSizeLimit uint64) *Collection {
	c := &Collection{Name: name, volumeSizeLimit: volumeSizeLimit}
	c.storageType2VolumeLayout = make(map[string]*VolumeLayout)
	return c
}

func (c *Collection) GetOrCreateVolumeLayout(rp *storage.ReplicaPlacement, ttl *storage.TTL) *VolumeLayout {
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if c.storageType2VolumeLayout[keyString] == nil {
		c.storageType2VolumeLayout[keyString] = NewVolumeLayout(rp, ttl, c.volumeSizeLimit)
	}
	return c.storageType2VolumeLayout[keyString]
}

func (c *Collection) Lookup(vid storage.VolumeId) []*DataNode {
	for _, vl := range c.storageType2VolumeLayout {
		if vl != nil {
			if list := vl.Lookup(vid); list != nil {
				return list
			}
		}
	}
	return nil
}

func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	for _, vl := range c.storageType2VolumeLayout {
		if vl != nil {
			if list := vl.ListVolumeServers(); list != nil {
				nodes = append(nodes, list...)
			}
		}
	}
	return
}
