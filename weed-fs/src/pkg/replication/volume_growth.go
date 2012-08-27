package replication

import (
	"pkg/topology"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowth struct {
	copy1factor int
	copy2factor int
	copy3factor int
	copyAll     int
}

func (vg *VolumeGrowth) GrowVolumeCopy(copyLevel int, topo topology.Topology) {
	if copyLevel == 1 {
		for i := 0; i <vg.copy1factor; i++ {
			topo.RandomlyCreateOneVolume()
		}
	}

}
