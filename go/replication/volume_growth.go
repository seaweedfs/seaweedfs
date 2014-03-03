package replication

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"errors"
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
	accessLock sync.Mutex
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count int) {
	switch copyCount {
	case 1:
		count = 7
	case 2:
		count = 6
	case 3:
		count = 3
	default:
		count = 1
	}
	return
}

func (vg *VolumeGrowth) AutomaticGrowByType(collection string, rp *storage.ReplicaPlacement, preferredDataCenter string, topo *topology.Topology) (count int, err error) {
	count, err = vg.GrowByCountAndType(vg.findVolumeCount(rp.GetCopyCount()), collection, rp, preferredDataCenter, topo)
	if count > 0 && count%rp.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}
func (vg *VolumeGrowth) GrowByCountAndType(targetCount int, collection string, rp *storage.ReplicaPlacement, preferredDataCenter string, topo *topology.Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if c, e := vg.findAndGrow(topo, preferredDataCenter, collection, rp); e == nil {
			counter += c
		} else {
			return counter, e
		}
	}
	return
}

func (vg *VolumeGrowth) findAndGrow(topo *topology.Topology, preferredDataCenter string, collection string, rp *storage.ReplicaPlacement) (int, error) {
	servers, e := vg.findEmptySlotsForOneVolume(topo, preferredDataCenter, rp)
	if e != nil {
		return 0, e
	}
	vid := topo.NextVolumeId()
	err := vg.grow(topo, vid, collection, rp, servers...)
	return len(servers), err
}

func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *topology.Topology, preferredDataCenter string, rp *storage.ReplicaPlacement) (servers []*topology.DataNode, err error) {
	//find main datacenter and other data centers
	mainDataCenter, otherDataCenters, dc_err := topo.RandomlyPickNodes(rp.DiffDataCenterCount+1, func(node topology.Node) bool {
		if preferredDataCenter != "" && node.IsDataCenter() && node.Id() != topology.NodeId(preferredDataCenter) {
			return false
		}
		return node.FreeSpace() > rp.DiffRackCount+rp.SameRackCount+1
	})
	if dc_err != nil {
		return nil, dc_err
	}

	//find main rack and other racks
	mainRack, otherRacks, rack_err := mainDataCenter.(*topology.DataCenter).RandomlyPickNodes(rp.DiffRackCount+1, func(node topology.Node) bool {
		return node.FreeSpace() > rp.SameRackCount+1
	})
	if rack_err != nil {
		return nil, rack_err
	}

	//find main rack and other racks
	mainServer, otherServers, server_err := mainRack.(*topology.Rack).RandomlyPickNodes(rp.SameRackCount+1, func(node topology.Node) bool {
		return node.FreeSpace() > 1
	})
	if server_err != nil {
		return nil, server_err
	}

	servers = append(servers, mainServer.(*topology.DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*topology.DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Intn(rack.FreeSpace())
		if server, e := rack.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	for _, datacenter := range otherDataCenters {
		r := rand.Intn(datacenter.FreeSpace())
		if server, e := datacenter.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	return
}

func (vg *VolumeGrowth) grow(topo *topology.Topology, vid storage.VolumeId, collection string, rp *storage.ReplicaPlacement, servers ...*topology.DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, vid, collection, rp); err == nil {
			vi := storage.VolumeInfo{Id: vid, Size: 0, Collection: collection, ReplicaPlacement: rp, Version: storage.CurrentVersion}
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
