package topology

import (
	"fmt"
	"math/rand"
	"pkg/storage"
	"time"
)

func (t *Topology) StartRefreshWritableVolumes() {
	go func() {
		for {
			freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
			t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func() {
		for {
			select {
			case v := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(v)
				fmt.Println("Volume", v, "is full!")
			case dn := <-t.chanRecoveredDataNodes:
				t.RegisterRecoveredDataNode(dn)
				fmt.Println("DataNode", dn, "is back alive!")
			case dn := <-t.chanDeadDataNodes:
				t.UnRegisterDataNode(dn)
				fmt.Println("DataNode", dn, "is dead!")
			}
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo *storage.VolumeInfo) {
	vl := t.GetVolumeLayout(volumeInfo.RepType)
	vl.SetVolumeCapacityFull(volumeInfo.Id)
	for _, dn := range vl.vid2location[volumeInfo.Id].list {
		dn.UpAdjustActiveVolumeCountDelta(-1)
	}
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		fmt.Println("Removing Volume", v.Id, "from the dead volume server", dn)
		vl := t.GetVolumeLayout(v.RepType)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	dn.Parent().UnlinkChildNode(dn.Id())
}
func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		if uint64(v.Size) < t.volumeSizeLimit {
			vl := t.GetVolumeLayout(v.RepType)
			vl.SetVolumeAvailable(dn, v.Id)
		}
	}
}
