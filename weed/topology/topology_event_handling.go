package topology

import (
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (t *Topology) StartRefreshWritableVolumes() {
	go func() {
		for {
			if t.IsLeader() {
				freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func() {
		c := time.Tick(15 * time.Minute)
		if t.IsLeader() {
			for range c {
				t.Vacuum("")
				//				t.CheckReplicate()
			}
		}
	}()
	go func() {
		for {
			select {
			case v := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(v)
			case dn := <-t.chanRecoveredDataNodes:
				t.RegisterRecoveredDataNode(dn)
				glog.V(0).Infoln("DataNode", dn.Id(), "is back alive!")
				glog.V(4).Infoln("DataNode", dn, "is back alive!")
			case dn := <-t.chanDeadDataNodes:
				t.UnRegisterDataNode(dn)
				glog.V(0).Infoln("DataNode", dn.Id(), "is dead!")
				glog.V(4).Infoln("DataNode", dn, "is dead!")
			}
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.Ttl)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}
	vll := vl.Lookup(volumeInfo.Id)
	if vll != nil {
		return false
	}
	for _, dn := range vll.AllDataNode() {
		if !volumeInfo.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	return true
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.Volumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn)
		vl := t.GetVolumeLayout(v.Collection, v.Ttl)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	dn.Parent().UnlinkChildNode(dn.Id())
}
func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.Volumes() {
		vl := t.GetVolumeLayout(v.Collection, v.Ttl)
		if vl.IsWritable(v) {
			vl.SetVolumeAvailable(dn, v.Id)
		}
	}
}
