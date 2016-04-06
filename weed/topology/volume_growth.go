package topology

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowOption struct {
	Collection       string
	ReplicaPlacement *storage.ReplicaPlacement
	Ttl              *storage.TTL
	DataCenter       string
	Rack             string
	DataNode         string
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

func (o *VolumeGrowOption) String() string {
	return fmt.Sprintf("Collection:%s, ReplicaPlacement:%v, Ttl:%v, DataCenter:%s, Rack:%s, DataNode:%s", o.Collection, o.ReplicaPlacement, o.Ttl, o.DataCenter, o.Rack, o.DataNode)
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

func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, topo *Topology) (count int, err error) {
	count, err = vg.GrowByCountAndType(vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount()), option, topo)
	if count > 0 && count%option.ReplicaPlacement.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}
func (vg *VolumeGrowth) GrowByCountAndType(targetCount int, option *VolumeGrowOption, topo *Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if c, e := vg.findAndGrow(topo, option); e == nil {
			counter += c
		} else {
			return counter, e
		}
	}
	return
}

func (vg *VolumeGrowth) findAndGrow(topo *Topology, option *VolumeGrowOption) (int, error) {
	servers, e := FindEmptySlotsForOneVolume(topo, option, nil)
	if e != nil {
		return 0, e
	}
	vid := topo.NextVolumeId()
	err := vg.grow(topo, vid, option, servers...)
	return len(servers), err
}

func (vg *VolumeGrowth) grow(topo *Topology, vid storage.VolumeId, option *VolumeGrowOption, servers ...*DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, vid, option); err == nil {
			vi := &storage.VolumeInfo{
				Id:         vid,
				Size:       0,
				Collection: option.Collection,
				Ttl:        option.Ttl,
				Version:    storage.CurrentVersion,
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

func filterMainDataCenter(option *VolumeGrowOption, node Node) error {
	if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
		return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
	}
	rp := option.ReplicaPlacement
	if len(node.Children()) < rp.DiffRackCount+1 {
		return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
	}
	if node.FreeSpace() < rp.DiffRackCount+rp.SameRackCount+1 {
		return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.DiffRackCount+rp.SameRackCount+1)
	}
	possibleRacksCount := 0
	for _, rack := range node.Children() {
		possibleDataNodesCount := 0
		for _, n := range rack.Children() {
			if n.FreeSpace() >= 1 {
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
}

func filterMainRack(option *VolumeGrowOption, node Node) error {
	if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
		return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
	}
	rp := option.ReplicaPlacement
	if node.FreeSpace() < rp.SameRackCount+1 {
		return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.SameRackCount+1)
	}
	if len(node.Children()) < rp.SameRackCount+1 {
		// a bit faster way to test free racks
		return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
	}
	possibleDataNodesCount := 0
	for _, n := range node.Children() {
		if n.FreeSpace() >= 1 {
			possibleDataNodesCount++
		}
	}
	if possibleDataNodesCount < rp.SameRackCount+1 {
		return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
	}
	return nil
}

func makeExceptNodeFilter(nodes []Node) FilterNodeFn {
	m := make(map[NodeId]bool)
	for _, n := range nodes {
		m[n.Id()] = true
	}
	return func(dn Node) error {
		if dn.FreeSpace() <= 0 {
			return ErrFilterContinue
		}
		if _, ok := m[dn.Id()]; ok {
			return ErrFilterContinue
		}
		return nil
	}
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
func FindEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption, existsServers *VolumeLocationList) (additionServers []*DataNode, err error) {
	//find main datacenter and other data centers
	pickNodesFn := RandomlyPickNodeFn
	rp := option.ReplicaPlacement

	pickMainAndRestNodes := func(np NodePicker, totalNodeCount int, filterFirstNodeFn FilterNodeFn, existsNodes []Node) (mainNode Node, restNodes []Node, e error) {
		if len(existsNodes) > 0 {
			mainNode = existsNodes[0]
		}

		if mainNode == nil {
			mainNodes, err := np.PickNodes(1, filterFirstNodeFn, pickNodesFn)
			if err != nil {
				return nil, nil, err
			}
			mainNode = mainNodes[0]
			existsNodes = append(existsNodes, mainNode)
		}
		glog.V(3).Infoln(mainNode.Id(), "picked main node:", mainNode.Id())

		restCount := totalNodeCount - len(existsNodes)

		if restCount > 0 {
			restNodes, err = np.PickNodes(restCount,
				makeExceptNodeFilter(existsNodes), pickNodesFn)
			if err != nil {
				return nil, nil, err
			}
		}

		return mainNode, restNodes, nil
	}
	var existsNode []Node
	if existsServers != nil {
		existsNode = existsServers.DiffDataCenters()
	}
	mainDataCenter, otherDataCenters, dc_err := pickMainAndRestNodes(topo, rp.DiffDataCenterCount+1,
		func(node Node) error {
			return filterMainDataCenter(option, node)
		}, existsNode)
	if dc_err != nil {
		return nil, dc_err
	}
	//find main rack and other racks
	if existsServers != nil {
		existsNode = existsServers.DiffRacks(mainDataCenter.(*DataCenter))
	} else {
		existsNode = nil
	}
	mainRack, otherRacks, rack_err := pickMainAndRestNodes(mainDataCenter.(*DataCenter), rp.DiffRackCount+1,
		func(node Node) error {
			return filterMainRack(option, node)
		},
		existsNode,
	)
	if rack_err != nil {
		return nil, rack_err
	}

	//find main server and other servers
	if existsServers != nil {
		existsNode = existsServers.SameServers(mainRack.(*Rack))
	} else {
		existsNode = nil
	}
	mainServer, otherServers, server_err := pickMainAndRestNodes(mainRack.(*Rack), rp.SameRackCount+1,
		func(node Node) error {
			if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
				return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
			}
			if node.FreeSpace() < 1 {
				return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), 1)
			}
			return nil
		},
		existsNode,
	)

	if server_err != nil {
		return nil, server_err
	}
	if existsServers != nil && existsServers.ContainsDataNode(mainServer.(*DataNode)) {
	} else {
		additionServers = append(additionServers, mainServer.(*DataNode))
	}

	for _, server := range otherServers {
		additionServers = append(additionServers, server.(*DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Intn(rack.FreeSpace())
		if server, e := rack.ReserveOneVolume(r); e == nil {
			additionServers = append(additionServers, server)
		} else {
			return additionServers, e
		}
	}
	for _, dc := range otherDataCenters {
		r := rand.Intn(dc.FreeSpace())
		if server, e := dc.ReserveOneVolume(r); e == nil {
			additionServers = append(additionServers, server)
		} else {
			return additionServers, e
		}
	}
	return
}
