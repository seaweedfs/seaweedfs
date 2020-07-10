package topology

import (
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

type NodeId string
type Node interface {
	Id() NodeId
	String() string
	FreeSpace() int64
	ReserveOneVolume(r int64) (*DataNode, error)
	UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int64)
	UpAdjustVolumeCountDelta(volumeCountDelta int64)
	UpAdjustRemoteVolumeCountDelta(remoteVolumeCountDelta int64)
	UpAdjustEcShardCountDelta(ecShardCountDelta int64)
	UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int64)
	UpAdjustMaxVolumeId(vid needle.VolumeId)

	GetVolumeCount() int64
	GetEcShardCount() int64
	GetActiveVolumeCount() int64
	GetRemoteVolumeCount() int64
	GetMaxVolumeCount() int64
	GetMaxVolumeId() needle.VolumeId
	SetParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64)

	IsDataNode() bool
	IsRack() bool
	IsDataCenter() bool
	Children() []Node
	Parent() Node

	GetValue() interface{} //get reference to the topology,dc,rack,datanode
}
type NodeImpl struct {
	volumeCount       int64
	remoteVolumeCount int64
	activeVolumeCount int64
	ecShardCount      int64
	maxVolumeCount    int64
	id                NodeId
	parent            Node
	sync.RWMutex      // lock children
	children          map[NodeId]Node
	maxVolumeId       needle.VolumeId

	//for rack, data center, topology
	nodeType string
	value    interface{}
}

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
func (n *NodeImpl) PickNodesByWeight(numberOfNodes int, filterFirstNodeFn func(dn Node) error) (firstNode Node, restNodes []Node, err error) {
	var totalWeights int64
	var errs []string
	n.RLock()
	candidates := make([]Node, 0, len(n.children))
	candidatesWeights := make([]int64, 0, len(n.children))
	//pick nodes which has enough free volumes as candidates, and use free volumes number as node weight.
	for _, node := range n.children {
		if node.FreeSpace() <= 0 {
			continue
		}
		totalWeights += node.FreeSpace()
		candidates = append(candidates, node)
		candidatesWeights = append(candidatesWeights, node.FreeSpace())
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
func (n *NodeImpl) FreeSpace() int64 {
	freeVolumeSlotCount := n.maxVolumeCount + n.remoteVolumeCount - n.volumeCount
	if n.ecShardCount > 0 {
		freeVolumeSlotCount = freeVolumeSlotCount - n.ecShardCount/erasure_coding.DataShardsCount - 1
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
func (n *NodeImpl) ReserveOneVolume(r int64) (assignedNode *DataNode, err error) {
	n.RLock()
	defer n.RUnlock()
	for _, node := range n.children {
		freeSpace := node.FreeSpace()
		// fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
			if node.IsDataNode() && node.FreeSpace() > 0 {
				// fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return node.(*DataNode), nil
			}
			assignedNode, err = node.ReserveOneVolume(r)
			if err == nil {
				return
			}
		}
	}
	return nil, errors.New("No free volume slot found!")
}

func (n *NodeImpl) UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int64) { //can be negative
	if maxVolumeCountDelta == 0 {
		return
	}
	atomic.AddInt64(&n.maxVolumeCount, maxVolumeCountDelta)
	if n.parent != nil {
		n.parent.UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustVolumeCountDelta(volumeCountDelta int64) { //can be negative
	if volumeCountDelta == 0 {
		return
	}
	atomic.AddInt64(&n.volumeCount, volumeCountDelta)
	if n.parent != nil {
		n.parent.UpAdjustVolumeCountDelta(volumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustRemoteVolumeCountDelta(remoteVolumeCountDelta int64) { //can be negative
	if remoteVolumeCountDelta == 0 {
		return
	}
	atomic.AddInt64(&n.remoteVolumeCount, remoteVolumeCountDelta)
	if n.parent != nil {
		n.parent.UpAdjustRemoteVolumeCountDelta(remoteVolumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustEcShardCountDelta(ecShardCountDelta int64) { //can be negative
	if ecShardCountDelta == 0 {
		return
	}
	atomic.AddInt64(&n.ecShardCount, ecShardCountDelta)
	if n.parent != nil {
		n.parent.UpAdjustEcShardCountDelta(ecShardCountDelta)
	}
}
func (n *NodeImpl) UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int64) { //can be negative
	if activeVolumeCountDelta == 0 {
		return
	}
	atomic.AddInt64(&n.activeVolumeCount, activeVolumeCountDelta)
	if n.parent != nil {
		n.parent.UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta)
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
func (n *NodeImpl) GetVolumeCount() int64 {
	return n.volumeCount
}
func (n *NodeImpl) GetEcShardCount() int64 {
	return n.ecShardCount
}
func (n *NodeImpl) GetRemoteVolumeCount() int64 {
	return n.remoteVolumeCount
}
func (n *NodeImpl) GetActiveVolumeCount() int64 {
	return n.activeVolumeCount
}
func (n *NodeImpl) GetMaxVolumeCount() int64 {
	return n.maxVolumeCount
}

func (n *NodeImpl) LinkChildNode(node Node) {
	n.Lock()
	defer n.Unlock()
	if n.children[node.Id()] == nil {
		n.children[node.Id()] = node
		n.UpAdjustMaxVolumeCountDelta(node.GetMaxVolumeCount())
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		n.UpAdjustVolumeCountDelta(node.GetVolumeCount())
		n.UpAdjustRemoteVolumeCountDelta(node.GetRemoteVolumeCount())
		n.UpAdjustEcShardCountDelta(node.GetEcShardCount())
		n.UpAdjustActiveVolumeCountDelta(node.GetActiveVolumeCount())
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
		n.UpAdjustVolumeCountDelta(-node.GetVolumeCount())
		n.UpAdjustRemoteVolumeCountDelta(-node.GetRemoteVolumeCount())
		n.UpAdjustEcShardCountDelta(-node.GetEcShardCount())
		n.UpAdjustActiveVolumeCountDelta(-node.GetActiveVolumeCount())
		n.UpAdjustMaxVolumeCountDelta(-node.GetMaxVolumeCount())
		glog.V(0).Infoln(n, "removes", node.Id())
	}
}

func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64) {
	if n.IsRack() {
		for _, c := range n.Children() {
			dn := c.(*DataNode) //can not cast n to DataNode
			for _, v := range dn.GetVolumes() {
				if uint64(v.Size) >= volumeSizeLimit {
					//fmt.Println("volume",v.Id,"size",v.Size,">",volumeSizeLimit)
					n.GetTopology().chanFullVolumes <- v
				}
			}
		}
	} else {
		for _, c := range n.Children() {
			c.CollectDeadNodeAndFullVolumes(freshThreshHold, volumeSizeLimit)
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
