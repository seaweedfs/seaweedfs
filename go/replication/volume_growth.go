package replication

import (
	"errors"
	"fmt"
	"math/rand"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"sync"
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

	accessLock sync.Mutex
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{copy1factor: 7, copy2factor: 6, copy3factor: 3}
}

func (vg *VolumeGrowth) GrowByType(repType storage.ReplicationType, topo *topology.Topology) (int, error) {
	switch repType {
	case storage.Copy000:
		return vg.GrowByCountAndType(vg.copy1factor, repType, topo)
	case storage.Copy001:
		return vg.GrowByCountAndType(vg.copy2factor, repType, topo)
	case storage.Copy010:
		return vg.GrowByCountAndType(vg.copy2factor, repType, topo)
	case storage.Copy100:
		return vg.GrowByCountAndType(vg.copy2factor, repType, topo)
	case storage.Copy110:
		return vg.GrowByCountAndType(vg.copy3factor, repType, topo)
	case storage.Copy200:
		return vg.GrowByCountAndType(vg.copy3factor, repType, topo)
	}
	return 0, errors.New("Unknown Replication Type!")
}
func (vg *VolumeGrowth) GrowByCountAndType(count int, repType storage.ReplicationType, topo *topology.Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	counter = 0
	switch repType {
	case storage.Copy000:
		for i := 0; i < count; i++ {
			if ok, server, vid := topo.RandomlyReserveOneVolume(); ok {
				if err = vg.grow(topo, *vid, repType, server); err == nil {
					counter++
				}
			}
		}
	case storage.Copy001:
		for i := 0; i < count; i++ {
			//randomly pick one server, and then choose from the same rack
			if ok, server1, vid := topo.RandomlyReserveOneVolume(); ok {
				rack := server1.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[server1.String()] = server1
				newNodeList := topology.NewNodeList(rack.Children(), exclusion)
				if newNodeList.FreeSpace() > 0 {
					if ok2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), *vid); ok2 {
						if err = vg.grow(topo, *vid, repType, server1, server2); err == nil {
							counter++
						}
					}
				}
			}
		}
	case storage.Copy010:
		for i := 0; i < count; i++ {
			//randomly pick one server, and then choose from the same rack
			if ok, server1, vid := topo.RandomlyReserveOneVolume(); ok {
				rack := server1.Parent()
				dc := rack.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[rack.String()] = rack
				newNodeList := topology.NewNodeList(dc.Children(), exclusion)
				if newNodeList.FreeSpace() > 0 {
					if ok2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), *vid); ok2 {
						if err = vg.grow(topo, *vid, repType, server1, server2); err == nil {
							counter++
						}
					}
				}
			}
		}
	case storage.Copy100:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2, 1)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 2 {
					if err = vg.grow(topo, vid, repType, servers...); err == nil {
						counter++
					}
				}
			}
		}
	case storage.Copy110:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2, 2)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				dc1, dc2 := picked[0], picked[1]
				if dc2.FreeSpace() > dc1.FreeSpace() {
					dc1, dc2 = dc2, dc1
				}
				if dc1.FreeSpace() > 0 {
					if ok, server1 := dc1.ReserveOneVolume(rand.Intn(dc1.FreeSpace()), vid); ok {
						servers = append(servers, server1)
						rack := server1.Parent()
						exclusion := make(map[string]topology.Node)
						exclusion[rack.String()] = rack
						newNodeList := topology.NewNodeList(dc1.Children(), exclusion)
						if newNodeList.FreeSpace() > 0 {
							if ok2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), vid); ok2 {
								servers = append(servers, server2)
							}
						}
					}
				}
				if dc2.FreeSpace() > 0 {
					if ok, server := dc2.ReserveOneVolume(rand.Intn(dc2.FreeSpace()), vid); ok {
						servers = append(servers, server)
					}
				}
				if len(servers) == 3 {
					if err = vg.grow(topo, vid, repType, servers...); err == nil {
						counter++
					}
				}
			}
		}
	case storage.Copy200:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(3, 1)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 3 {
					if err = vg.grow(topo, vid, repType, servers...); err == nil {
						counter++
					}
				}
			}
		}
	}
	return
}
func (vg *VolumeGrowth) grow(topo *topology.Topology, vid storage.VolumeId, repType storage.ReplicationType, servers ...*topology.DataNode) error {
	for _, server := range servers {
		if err := operation.AllocateVolume(server, vid, repType); err == nil {
			vi := storage.VolumeInfo{Id: vid, Size: 0, RepType: repType, Version: storage.CurrentVersion}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(&vi, server)
			fmt.Println("Created Volume", vid, "on", server)
		} else {
			fmt.Println("Failed to assign", vid, "to", servers)
			return errors.New("Failed to assign " + vid.String())
		}
	}
	return nil
}
