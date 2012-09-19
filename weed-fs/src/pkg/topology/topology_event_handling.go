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
			freshThreshHold := time.Now().Unix() - 3*t.pulse //5 times of sleep interval
			t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func() {
		for {
			select {
			case v := <-t.chanIncomplemteVolumes:
				fmt.Println("Volume", v, "is incomplete!")
			case v := <-t.chanRecoveredVolumes:
				fmt.Println("Volume", v, "is recovered!")
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
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		fmt.Println("Removing Volume", v.Id, "from the dead volume server", dn)
		vl := t.GetVolumeLayout(v.RepType)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
}
func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		if uint64(v.Size) < t.volumeSizeLimit {
			vl := t.GetVolumeLayout(v.RepType)
			vl.SetVolumeAvailable(dn, v.Id)
		}
	}
}
