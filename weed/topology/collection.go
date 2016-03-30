package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	rp                       *storage.ReplicaPlacement
	storageType2VolumeLayout *util.ConcurrentMap
}

func NewCollection(name string, rp *storage.ReplicaPlacement, volumeSizeLimit uint64) *Collection {
	c := &Collection{Name: name, volumeSizeLimit: volumeSizeLimit, rp: rp}
	c.storageType2VolumeLayout = util.NewConcurrentMap()
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
	vl := c.storageType2VolumeLayout.GetOrNew(keyString, func() interface{} {
		return NewVolumeLayout(c.rp, ttl, c.volumeSizeLimit)
	})
	return vl.(*VolumeLayout)
}

func (c *Collection) Lookup(vid storage.VolumeId) (vll *VolumeLocationList) {
	c.storageType2VolumeLayout.Walk(func(k string, vl interface{}) (e error) {
		if vl != nil {
			if vl != nil {
				if vll = vl.(*VolumeLayout).Lookup(vid); vll != nil {
					return util.ErrBreakWalk
				}
			}
		}
		return nil
	})
	return
}

func (c *Collection) ListVolumeServers() (nodes []*DataNode) {
	c.storageType2VolumeLayout.Walk(func(k string, vl interface{}) (e error) {
		if vl != nil {
			if list := vl.(*VolumeLayout).ListVolumeServers(); list != nil {
				nodes = append(nodes, list...)
			}
		}
		return nil
	})
	return

}
