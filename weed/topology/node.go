package topology

import (
	"errors"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"math/rand"
	"strings"
	"sync"
)

type NodeId string
type Node interface {
	Id() NodeId
	String() string
	AvailableSpaceFor(option *VolumeGrowOption) int64
	ReserveOneVolume(r int64, option *VolumeGrowOption) (*DataNode, error)
	UpAdjustDiskUsageDelta(deltaDiskUsages *DiskUsages)
	UpAdjustMaxVolumeId(vid needle.VolumeId)
	GetDiskUsages() *DiskUsages

	GetMaxVolumeId() needle.VolumeId
	SetParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64, growThreshold float64)

	IsDataNode() bool
	IsRack() bool
	IsDataCenter() bool
	Children() []Node
	Parent() Node

	GetValue() interface{} //get reference to the topology,dc,rack,datanode
}
type NodeImpl struct {
	diskUsages   *DiskUsages
	id           NodeId
	parent       Node
	sync.RWMutex // lock children
	children     map[NodeId]Node
	maxVolumeId  needle.VolumeId

	//for rack, data center, topology
	nodeType string
	value    interface{}
}

func (n *NodeImpl) GetDiskUsages() *DiskUsages {
	return n.diskUsages
}

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
func (n *NodeImpl) PickNodesByWeight(numberOfNodes int, option *VolumeGrowOption, filterFirstNodeFn func(dn Node) error) (firstNode Node, restNodes []Node, err error) {
	var totalWeights int64
	var errs []string
	n.RLock()
	candidates := make([]Node, 0, len(n.children))
	candidatesWeights := make([]int64, 0, len(n.children))
	//pick nodes which has enough free volumes as candidates, and use free volumes number as node weight.
	for _, node := range n.children {
		if node.AvailableSpaceFor(option) <= 0 {
			continue
		}
		totalWeights += node.AvailableSpaceFor(option)
		candidates = append(candidates, node)
		candidatesWeights = append(candidatesWeights, node.AvailableSpaceFor(option))
	}
	n.RUnlock()
	if len(candidates) < numberOfNodes {
		glog.V(0).Infoln(n.Id(), "failed to pick", numberOfNodes, "from ", len(candidates), "node candidates")
		return nil, nil, errors.New("No enough data node found!")
	}

	//pick nodes randomly by weights, the node picked earlier has higher final weights
	sortedCandidates := make([]Node, 0, len(candidates))
	for i := 0; i < len(candidates); i++ {
		weightsInterval := rand.Int63n(totalWeights)
		lastWeights := int64(0)
		for k, weights := range candidatesWeights {
			if (weightsInterval >= lastWeights) && (weightsInterval < lastWeights+weights) {
				sortedCandidates = append(sortedCandidates, candidates[k])
				candidatesWeights[k] = 0
				totalWeights -= weights
				break
			}
			lastWeights += weights
		}
	}

	restNodes = make([]Node, 0, numberOfNodes-1)
	ret := false
	n.RLock()
	for k, node := range sortedCandidates {
		if err := filterFirstNodeFn(node); err == nil {
			firstNode = node
			if k >= numberOfNodes-1 {
				restNodes = sortedCandidates[:numberOfNodes-1]
			} else {
				restNodes = append(restNodes, sortedCandidates[:k]...)
				restNodes = append(restNodes, sortedCandidates[k+1:numberOfNodes]...)
			}
			ret = true
			break
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	n.RUnlock()
	if !ret {
		return nil, nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	return
}

func (n *NodeImpl) IsDataNode() bool {
	return n.nodeType == "DataNode"
}
func (n *NodeImpl) IsRack() bool {
	return n.nodeType == "Rack"
}
func (n *NodeImpl) IsDataCenter() bool {
	return n.nodeType == "DataCenter"
}
func (n *NodeImpl) String() string {
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id)
	}
	return string(n.id)
}
func (n *NodeImpl) Id() NodeId {
	return n.id
}
func (n *NodeImpl) getOrCreateDisk(diskType types.DiskType) *DiskUsageCounts {
	return n.diskUsages.getOrCreateDisk(diskType)
}
func (n *NodeImpl) AvailableSpaceFor(option *VolumeGrowOption) int64 {
	t := n.getOrCreateDisk(option.DiskType)
	freeVolumeSlotCount := t.maxVolumeCount + t.remoteVolumeCount - t.volumeCount
	if t.ecShardCount > 0 {
		freeVolumeSlotCount = freeVolumeSlotCount - t.ecShardCount/erasure_coding.DataShardsCount - 1
	}
	return freeVolumeSlotCount
}
func (n *NodeImpl) SetParent(node Node) {
	n.parent = node
}
func (n *NodeImpl) Children() (ret []Node) {
	n.RLock()
	defer n.RUnlock()
	for _, c := range n.children {
		ret = append(ret, c)
	}
	return ret
}
func (n *NodeImpl) Parent() Node {
	return n.parent
}
func (n *NodeImpl) GetValue() interface{} {
	return n.value
}
func (n *NodeImpl) ReserveOneVolume(r int64, option *VolumeGrowOption) (assignedNode *DataNode, err error) {
	n.RLock()
	defer n.RUnlock()
	for _, node := range n.children {
		freeSpace := node.AvailableSpaceFor(option)
		// fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
			if node.IsDataNode() && node.AvailableSpaceFor(option) > 0 {
				// fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return node.(*DataNode), nil
			}
			assignedNode, err = node.ReserveOneVolume(r, option)
			if err == nil {
				return
			}
		}
	}
	return nil, errors.New("No free volume slot found!")
}

func (n *NodeImpl) UpAdjustDiskUsageDelta(deltaDiskUsages *DiskUsages) { //can be negative
	for diskType, diskUsage := range deltaDiskUsages.usages {
		existingDisk := n.getOrCreateDisk(diskType)
		existingDisk.addDiskUsageCounts(diskUsage)
	}
	if n.parent != nil {
		n.parent.UpAdjustDiskUsageDelta(deltaDiskUsages)
	}
}
func (n *NodeImpl) UpAdjustMaxVolumeId(vid needle.VolumeId) { //can be negative
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}
func (n *NodeImpl) GetMaxVolumeId() needle.VolumeId {
	return n.maxVolumeId
}

func (n *NodeImpl) LinkChildNode(node Node) {
	n.Lock()
	defer n.Unlock()
	n.doLinkChildNode(node)
}

func (n *NodeImpl) doLinkChildNode(node Node) {
	if n.children[node.Id()] == nil {
		n.children[node.Id()] = node
		n.UpAdjustDiskUsageDelta(node.GetDiskUsages())
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		node.SetParent(n)
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	n.Lock()
	defer n.Unlock()
	node := n.children[nodeId]
	if node != nil {
		node.SetParent(nil)
		delete(n.children, node.Id())
		n.UpAdjustDiskUsageDelta(node.GetDiskUsages().negative())
		glog.V(0).Infoln(n, "removes", node.Id())
	}
}

func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64, growThreshold float64) {
	if n.IsRack() {
		for _, c := range n.Children() {
			dn := c.(*DataNode) //can not cast n to DataNode
			for _, v := range dn.GetVolumes() {
				if v.Size >= volumeSizeLimit {
					//fmt.Println("volume",v.Id,"size",v.Size,">",volumeSizeLimit)
					n.GetTopology().chanFullVolumes <- v
				}else if float64(v.Size) > float64(volumeSizeLimit) * growThreshold {
					n.GetTopology().chanCrowdedVolumes <- v
				}
			}
		}
	} else {
		for _, c := range n.Children() {
			c.CollectDeadNodeAndFullVolumes(freshThreshHold, volumeSizeLimit, growThreshold)
		}
	}
}

func (n *NodeImpl) GetTopology() *Topology {
	var p Node
	p = n
	for p.Parent() != nil {
		p = p.Parent()
	}
	return p.GetValue().(*Topology)
}
