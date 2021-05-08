package topology

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowRequest struct {
	Option *VolumeGrowOption
	Count  int
	ErrCh  chan error
}

type VolumeGrowOption struct {
	Collection         string                        `json:"collection,omitempty"`
	ReplicaPlacement   *super_block.ReplicaPlacement `json:"replication,omitempty"`
	Ttl                *needle.TTL                   `json:"ttl,omitempty"`
	DiskType           types.DiskType                `json:"disk,omitempty"`
	Preallocate        int64                         `json:"preallocate,omitempty"`
	DataCenter         string                        `json:"dataCenter,omitempty"`
	Rack               string                        `json:"rack,omitempty"`
	DataNode           string                        `json:"dataNode,omitempty"`
	MemoryMapMaxSizeMb uint32                        `json:"memoryMapMaxSizeMb,omitempty"`
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

func (o *VolumeGrowOption) String() string {
	blob, _ := json.Marshal(o)
	return string(blob)
}

func (o *VolumeGrowOption) Threshold() float64 {
	v := util.GetViper()
	return v.GetFloat64("master.volume_growth.threshold")
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count int) {
	v := util.GetViper()
	switch copyCount {
	case 1:
		count = v.GetInt("master.volume_growth.copy_1")
	case 2:
		count = v.GetInt("master.volume_growth.copy_2")
	case 3:
		count = v.GetInt("master.volume_growth.copy_3")
	default:
		count = v.GetInt("master.volume_growth.copy_other")
	}
	return
}

func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, grpcDialOption grpc.DialOption, topo *Topology, targetCount int) (count int, err error) {
	if targetCount == 0 {
		targetCount = vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount())
	}
	count, err = vg.GrowByCountAndType(grpcDialOption, targetCount, option, topo)
	if count > 0 && count%option.ReplicaPlacement.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}
func (vg *VolumeGrowth) GrowByCountAndType(grpcDialOption grpc.DialOption, targetCount int, option *VolumeGrowOption, topo *Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if c, e := vg.findAndGrow(grpcDialOption, topo, option); e == nil {
			counter += c
		} else {
			glog.V(0).Infof("create %d volume, created %d: %v", targetCount, counter, e)
			return counter, e
		}
	}
	return
}

func (vg *VolumeGrowth) findAndGrow(grpcDialOption grpc.DialOption, topo *Topology, option *VolumeGrowOption) (int, error) {
	servers, e := vg.findEmptySlotsForOneVolume(topo, option)
	if e != nil {
		return 0, e
	}
	vid, raftErr := topo.NextVolumeId()
	if raftErr != nil {
		return 0, raftErr
	}
	err := vg.grow(grpcDialOption, topo, vid, option, servers...)
	return len(servers), err
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption) (servers []*DataNode, err error) {
	//find main datacenter and other data centers
	rp := option.ReplicaPlacement
	mainDataCenter, otherDataCenters, dc_err := topo.PickNodesByWeight(rp.DiffDataCenterCount+1, option, func(node Node) error {
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}
		if node.AvailableSpaceFor(option) < int64(rp.DiffRackCount+rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), rp.DiffRackCount+rp.SameRackCount+1)
		}
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if n.AvailableSpaceFor(option) >= 1 {
					possibleDataNodesCount++
				}
			}
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				possibleRacksCount++
			}
		}
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		return nil
	})
	if dc_err != nil {
		return nil, dc_err
	}

	//find main rack and other racks
	mainRack, otherRacks, rackErr := mainDataCenter.(*DataCenter).PickNodesByWeight(rp.DiffRackCount+1, option, func(node Node) error {
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}
		if node.AvailableSpaceFor(option) < int64(rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), rp.SameRackCount+1)
		}
		if len(node.Children()) < rp.SameRackCount+1 {
			// a bit faster way to test free racks
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if n.AvailableSpaceFor(option) >= 1 {
				possibleDataNodesCount++
			}
		}
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rackErr != nil {
		return nil, rackErr
	}

	//find main rack and other racks
	mainServer, otherServers, serverErr := mainRack.(*Rack).PickNodesByWeight(rp.SameRackCount+1, option, func(node Node) error {
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}
		if node.AvailableSpaceFor(option) < 1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), 1)
		}
		return nil
	})
	if serverErr != nil {
		return nil, serverErr
	}

	servers = append(servers, mainServer.(*DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Int63n(rack.AvailableSpaceFor(option))
		if server, e := rack.ReserveOneVolume(r, option); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	for _, datacenter := range otherDataCenters {
		r := rand.Int63n(datacenter.AvailableSpaceFor(option))
		if server, e := datacenter.ReserveOneVolume(r, option); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	return
}

func (vg *VolumeGrowth) grow(grpcDialOption grpc.DialOption, topo *Topology, vid needle.VolumeId, option *VolumeGrowOption, servers ...*DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, grpcDialOption, vid, option); err == nil {
			vi := storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          needle.CurrentVersion,
				DiskType:         string(option.DiskType),
			}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server.NodeImpl.String())
		} else {
			glog.V(0).Infoln("Failed to assign volume", vid, "to", servers, "error", err)
			return fmt.Errorf("Failed to assign %d: %v", vid, err)
		}
	}
	return nil
}
