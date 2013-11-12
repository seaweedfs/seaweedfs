package topology

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"math/rand"
	"time"
)

func (t *Topology) StartRefreshWritableVolumes(garbageThreshold string) {
	go func() {
		for {
			freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
			t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func(garbageThreshold string) {
		c := time.Tick(15 * time.Minute)
		for _ = range c {
			t.Vacuum(garbageThreshold)
		}
	}(garbageThreshold)
	go func() {
		for {
			select {
			case v := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(v)
			case dn := <-t.chanRecoveredDataNodes:
				t.RegisterRecoveredDataNode(dn)
				glog.V(0).Infoln("DataNode", dn, "is back alive!")
			case dn := <-t.chanDeadDataNodes:
				t.UnRegisterDataNode(dn)
				glog.V(0).Infoln("DataNode", dn, "is dead!")
			}
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.RepType)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}
	for _, dn := range vl.vid2location[volumeInfo.Id].list {
		dn.UpAdjustActiveVolumeCountDelta(-1)
	}
	return true
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn)
		vl := t.GetVolumeLayout(v.Collection, v.RepType)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	dn.Parent().UnlinkChildNode(dn.Id())
}
func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		vl := t.GetVolumeLayout(v.Collection, v.RepType)
		if vl.isWritable(&v) {
			vl.SetVolumeAvailable(dn, v.Id)
		}
	}
}
