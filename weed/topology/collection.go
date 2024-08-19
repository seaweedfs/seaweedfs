package topology

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	replicationAsMin         bool
	storageType2VolumeLayout *util.ConcurrentReadMap
}

func NewCollection(name string, volumeSizeLimit uint64, replicationAsMin bool) *Collection {
	c := &Collection{
		Name:             name,
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
	c.storageType2VolumeLayout = util.NewConcurrentReadMap()
	return c
}

func (c *Collection) String() string {
	return fmt.Sprintf("Name:%s, volumeSizeLimit:%d, storageType2VolumeLayout:%v", c.Name, c.volumeSizeLimit, c.storageType2VolumeLayout)
}

func (c *Collection) GetOrCreateVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) *VolumeLayout {
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}
	vl := c.storageType2VolumeLayout.Get(keyString, func() interface{} {
		return NewVolumeLayout(rp, ttl, diskType, c.volumeSizeLimit, c.replicationAsMin)
	})
	return vl.(*VolumeLayout)
}

func (c *Collection) GetVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) (*VolumeLayout, bool) {
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}
	vl, ok := c.storageType2VolumeLayout.Find(keyString)
	return vl.(*VolumeLayout), ok
}

func (c *Collection) GetAllVolumeLayouts() []*VolumeLayout {
	var vls []*VolumeLayout
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			vls = append(vls, vl.(*VolumeLayout))
		}
	}
	return vls
}

func (c *Collection) DeleteVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) {
	keyString := rp.String()
	if ttl != nil {
		keyString += ttl.String()
	}
	if diskType != types.HardDriveType {
		keyString += string(diskType)
	}
	c.storageType2VolumeLayout.Delete(keyString)
}

func (c *Collection) Lookup(vid needle.VolumeId) []*DataNode {
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			if list := vl.(*VolumeLayout).Lookup(vid); list != nil {
				return list
			}
		}
	}
	return nil
}

func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	for _, vl := range c.storageType2VolumeLayout.Items() {
		if vl != nil {
			if list := vl.(*VolumeLayout).ListVolumeServers(); list != nil {
				nodes = append(nodes, list...)
			}
		}
	}
	return
}
