package replication

import (
	"fmt"
	"math/rand"
	"pkg/storage"
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

func (vg *VolumeGrowth) GrowVolumeCopy(copyLevel int, topo *topology.Topology) {
	switch copyLevel {
	case 1:
		for i := 0; i < vg.copy1factor; i++ {
			ret, server, vid := topo.RandomlyReserveOneVolume()
			if ret {
				vg.Grow(vid, server)
			}
		}
	case 20:
		for i := 0; i < vg.copy2factor; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.Server
				for _, n := range picked {
					if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
						servers = append(servers, server)
					}
				}
				if len(servers) == 2 {
					vg.Grow(vid, servers[0], servers[1])
				}
			}
		}
	case 30:
		for i := 0; i < vg.copy3factor; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(3)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.Server
				for _, n := range picked {
					if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
						servers = append(servers, server)
					}
				}
				if len(servers) == 3 {
					vg.Grow(vid, servers[0], servers[1], servers[2])
				}
			}
		}
	case 02:
		for i := 0; i < vg.copy2factor; i++ {
			//randomly pick one server, and then choose from the same rack
			ret, server1, vid := topo.RandomlyReserveOneVolume()
			if ret {
				rack := server1.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[server1.String()] = server1
				newNodeList := topology.NewNodeList(rack.Children(), exclusion)
				ret2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), vid)
				if ret2 {
					vg.Grow(vid, server1, server2)
				}
			}
		}
	case 12:
		for i := 0; i < vg.copy3factor; i++ {
		}
	}

}
func (vg *VolumeGrowth) Grow(vid storage.VolumeId, servers ...*topology.Server) {
	for _, server := range servers {
		vi := &storage.VolumeInfo{Id: vid, Size: 0}
		server.AddVolume(vi)
	}
	fmt.Println("Assigning", vid, "to", servers)
}
