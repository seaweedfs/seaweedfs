package topology

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
)

type Collection struct {
	Name                     string
	volumeSizeLimit          uint64
	replicaType2VolumeLayout []*VolumeLayout
}

func NewCollection(name string, volumeSizeLimit uint64) *Collection {
	c := &Collection{Name: name, volumeSizeLimit: volumeSizeLimit}
	c.replicaType2VolumeLayout = make([]*VolumeLayout, storage.LengthRelicationType)
	return c
}

func (c *Collection) GetOrCreateVolumeLayout(repType storage.ReplicationType) *VolumeLayout {
	replicationTypeIndex := repType.GetReplicationLevelIndex()
	if c.replicaType2VolumeLayout[replicationTypeIndex] == nil {
		glog.V(0).Infoln("collection", c.Name, "adding replication type", repType)
		c.replicaType2VolumeLayout[replicationTypeIndex] = NewVolumeLayout(repType, c.volumeSizeLimit)
	}
	return c.replicaType2VolumeLayout[replicationTypeIndex]
}

func (c *Collection) Lookup(vid storage.VolumeId) []*DataNode {
	for _, vl := range c.replicaType2VolumeLayout {
		if vl != nil {
			if list := vl.Lookup(vid); list != nil {
				return list
			}
		}
	}
	return nil
}
