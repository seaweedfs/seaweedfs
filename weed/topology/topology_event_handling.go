package topology

import (
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (t *Topology) StartRefreshWritableVolumes(garbageThreshold string, preallocate int64) {
	go func() {
		for {
			if t.IsLeader() {
				freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func(garbageThreshold string) {
		c := time.Tick(15 * time.Minute)
		for _ = range c {
			if t.IsLeader() {
				t.Vacuum(garbageThreshold, preallocate)
			}
		}
	}(garbageThreshold)
	go func() {
		for {
			select {
			case v := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(v)
			}
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}
	for _, dn := range vl.vid2location[volumeInfo.Id].list {
		if !volumeInfo.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	return true
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}
