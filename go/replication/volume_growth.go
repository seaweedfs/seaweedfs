package replication

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"errors"
	"fmt"
	"math/rand"
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

func (vg *VolumeGrowth) AutomaticGrowByType(collection string, repType storage.ReplicationType, dataCenter string, topo *topology.Topology) (count int, err error) {
	factor := 1
	switch repType {
	case storage.Copy000:
		factor = 1
		count, err = vg.GrowByCountAndType(vg.copy1factor, collection, repType, dataCenter, topo)
	case storage.Copy001:
		factor = 2
		count, err = vg.GrowByCountAndType(vg.copy2factor, collection, repType, dataCenter, topo)
	case storage.Copy010:
		factor = 2
		count, err = vg.GrowByCountAndType(vg.copy2factor, collection, repType, dataCenter, topo)
	case storage.Copy100:
		factor = 2
		count, err = vg.GrowByCountAndType(vg.copy2factor, collection, repType, dataCenter, topo)
	case storage.Copy110:
		factor = 3
		count, err = vg.GrowByCountAndType(vg.copy3factor, collection, repType, dataCenter, topo)
	case storage.Copy200:
		factor = 3
		count, err = vg.GrowByCountAndType(vg.copy3factor, collection, repType, dataCenter, topo)
	default:
		err = errors.New("Unknown Replication Type!")
	}
	if count > 0 && count%factor == 0 {
		return count, nil
	}
	return count, err
}
func (vg *VolumeGrowth) GrowByCountAndType(count int, collection string, repType storage.ReplicationType, dataCenter string, topo *topology.Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	counter = 0
	switch repType {
	case storage.Copy000:
		for i := 0; i < count; i++ {
			if ok, server, vid := topo.RandomlyReserveOneVolume(dataCenter); ok {
				if err = vg.grow(topo, *vid, collection, repType, server); err == nil {
					counter++
				} else {
					return counter, err
				}
			} else {
				return counter, fmt.Errorf("Failed to grown volume for data center %s", dataCenter)
			}
		}
	case storage.Copy001:
		for i := 0; i < count; i++ {
			//randomly pick one server from the datacenter, and then choose from the same rack
			if ok, server1, vid := topo.RandomlyReserveOneVolume(dataCenter); ok {
				rack := server1.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[server1.String()] = server1
				newNodeList := topology.NewNodeList(rack.Children(), exclusion)
				if newNodeList.FreeSpace() > 0 {
					if ok2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), *vid); ok2 {
						if err = vg.grow(topo, *vid, collection, repType, server1, server2); err == nil {
							counter++
						}
					}
				}
			}
		}
	case storage.Copy010:
		for i := 0; i < count; i++ {
			//randomly pick one server from the datacenter, and then choose from the a different rack
			if ok, server1, vid := topo.RandomlyReserveOneVolume(dataCenter); ok {
				rack := server1.Parent()
				dc := rack.Parent()
				exclusion := make(map[string]topology.Node)
				exclusion[rack.String()] = rack
				newNodeList := topology.NewNodeList(dc.Children(), exclusion)
				if newNodeList.FreeSpace() > 0 {
					if ok2, server2 := newNodeList.ReserveOneVolume(rand.Intn(newNodeList.FreeSpace()), *vid); ok2 {
						if err = vg.grow(topo, *vid, collection, repType, server1, server2); err == nil {
							counter++
						}
					}
				}
			}
		}
	case storage.Copy100:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2, 1, dataCenter)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid, ""); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 2 {
					if err = vg.grow(topo, vid, collection, repType, servers...); err == nil {
						counter++
					}
				}
			} else {
				return counter, fmt.Errorf("Failed to grown volume on data center %s and another data center", dataCenter)
			}
		}
	case storage.Copy110:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(2, 2, dataCenter)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				dc1, dc2 := picked[0], picked[1]
				if dc2.FreeSpace() > dc1.FreeSpace() {
					dc1, dc2 = dc2, dc1
				}
				if dc1.FreeSpace() > 0 {
					if ok, server1 := dc1.ReserveOneVolume(rand.Intn(dc1.FreeSpace()), vid, ""); ok {
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
					if ok, server := dc2.ReserveOneVolume(rand.Intn(dc2.FreeSpace()), vid, ""); ok {
						servers = append(servers, server)
					}
				}
				if len(servers) == 3 {
					if err = vg.grow(topo, vid, collection, repType, servers...); err == nil {
						counter++
					}
				}
			}
		}
	case storage.Copy200:
		for i := 0; i < count; i++ {
			nl := topology.NewNodeList(topo.Children(), nil)
			picked, ret := nl.RandomlyPickN(3, 1, dataCenter)
			vid := topo.NextVolumeId()
			if ret {
				var servers []*topology.DataNode
				for _, n := range picked {
					if n.FreeSpace() > 0 {
						if ok, server := n.ReserveOneVolume(rand.Intn(n.FreeSpace()), vid, ""); ok {
							servers = append(servers, server)
						}
					}
				}
				if len(servers) == 3 {
					if err = vg.grow(topo, vid, collection, repType, servers...); err == nil {
						counter++
					}
				}
			}
		}
	}
	return
}
func (vg *VolumeGrowth) grow(topo *topology.Topology, vid storage.VolumeId, collection string, repType storage.ReplicationType, servers ...*topology.DataNode) error {
	for _, server := range servers {
		if err := operation.AllocateVolume(server, vid, collection, repType); err == nil {
			vi := storage.VolumeInfo{Id: vid, Size: 0, Collection: collection, RepType: repType, Version: storage.CurrentVersion}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(&vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server)
		} else {
			glog.V(0).Infoln("Failed to assign", vid, "to", servers, "error", err)
			return errors.New("Failed to assign " + vid.String() + ", " + err.Error())
		}
	}
	return nil
}
