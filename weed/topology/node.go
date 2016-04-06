package topology

import (
	"errors"
	"math/rand"

	"sort"

	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type NodeId string
type Node interface {
	Id() NodeId
	String() string
	FreeSpace() int
	ReserveOneVolume(r int) (*DataNode, error)
	UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int)
	UpAdjustVolumeCountDelta(volumeCountDelta int)
	UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int)
	UpAdjustPlannedVolumeCountDelta(delta int)
	UpAdjustMaxVolumeId(vid storage.VolumeId)

	GetVolumeCount() int
	GetActiveVolumeCount() int
	GetMaxVolumeCount() int
	GetPlannedVolumeCount() int
	GetMaxVolumeId() storage.VolumeId
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
	id                 NodeId
	volumeCount        int
	activeVolumeCount  int
	maxVolumeCount     int
	plannedVolumeCount int
	parent             Node
	children           map[NodeId]Node
	maxVolumeId        storage.VolumeId

	//for rack, data center, topology
	nodeType string
	value    interface{}
	mutex    sync.RWMutex
}

type NodePicker interface {
	PickNodes(numberOfNodes int, filterNodeFn FilterNodeFn, pickFn PickNodesFn) (nodes []Node, err error)
}

var ErrFilterContinue = errors.New("continue")

type FilterNodeFn func(dn Node) error
type PickNodesFn func(nodes []Node, count int) []Node

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
func (n *NodeImpl) PickNodes(numberOfNodes int, filterNodeFn FilterNodeFn, pickFn PickNodesFn) (nodes []Node, err error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	candidates := make([]Node, 0, len(n.children))
	var errs []string
	for _, node := range n.children {
		if err := filterNodeFn(node); err == nil {
			candidates = append(candidates, node)
		} else if err == ErrFilterContinue {
			continue
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	if len(candidates) < numberOfNodes {
		return nil, errors.New("Not enough data node found!")
		// 	return nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	return pickFn(candidates, numberOfNodes), nil
}

func RandomlyPickNodeFn(nodes []Node, count int) []Node {
	if len(nodes) < count {
		return nil
	}
	for i := range nodes {
		j := rand.Intn(i + 1)
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	return nodes[:count]
}

func (n *NodeImpl) RandomlyPickNodes(numberOfNodes int, filterFirstNodeFn FilterNodeFn) (nodes []Node, err error) {
	return n.PickNodes(numberOfNodes, filterFirstNodeFn, RandomlyPickNodeFn)
}

type nodeList []Node

func (s nodeList) Len() int           { return len(s) }
func (s nodeList) Less(i, j int) bool { return s[i].FreeSpace() < s[j].FreeSpace() }
func (s nodeList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func PickLowUsageNodeFn(nodes []Node, count int) []Node {
	if len(nodes) < count {
		return nil
	}
	sort.Sort(sort.Reverse(nodeList(nodes)))
	return nodes[:count]
}

func (n *NodeImpl) PickLowUsageNodes(numberOfNodes int, filterFirstNodeFn FilterNodeFn) (nodes []Node, err error) {
	return n.PickNodes(numberOfNodes, filterFirstNodeFn, PickLowUsageNodeFn)
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
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.toString()
}

func (n *NodeImpl) toString() string {
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id)
	}
	return string(n.id)
}

func (n *NodeImpl) Id() NodeId {
	return n.id
}
func (n *NodeImpl) FreeSpace() int {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.maxVolumeCount - n.volumeCount - n.plannedVolumeCount
}

func (n *NodeImpl) SetParent(node Node) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.parent = node
}

func (n *NodeImpl) GetChildren(id NodeId) Node {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.children[id]
}

func (n *NodeImpl) SetChildren(c Node) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.children[c.Id()] = c
}

func (n *NodeImpl) DeleteChildren(id NodeId) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	delete(n.children, id)
}

func (n *NodeImpl) FindChildren(filter func(Node) bool) Node {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	for _, c := range n.children {
		if filter(c) {
			return c
		}
	}
	return nil
}

func (n *NodeImpl) Children() (children []Node) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	children = make([]Node, 0, len(n.children))
	for _, c := range n.children {
		children = append(children, c)
	}
	return
}
func (n *NodeImpl) Parent() Node {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.parent
}
func (n *NodeImpl) GetValue() interface{} {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.value
}
func (n *NodeImpl) ReserveOneVolume(r int) (assignedNode *DataNode, err error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
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
				// fmt.Println("assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return node.(*DataNode), nil
			}
			assignedNode, err = node.ReserveOneVolume(r)
			if err != nil {
				return
			}
		}
	}
	return
}

func (n *NodeImpl) UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int) { //can be negative
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.maxVolumeCount += maxVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustVolumeCountDelta(volumeCountDelta int) { //can be negative
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.volumeCount += volumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustVolumeCountDelta(volumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int) { //can be negative
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.activeVolumeCount += activeVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta)
	}
}

func (n *NodeImpl) UpAdjustPlannedVolumeCountDelta(delta int) { //can be negative
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.plannedVolumeCount += delta
	if n.parent != nil {
		n.parent.UpAdjustPlannedVolumeCountDelta(delta)
	}
}

func (n *NodeImpl) UpAdjustMaxVolumeId(vid storage.VolumeId) { //can be negative
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}
func (n *NodeImpl) GetMaxVolumeId() storage.VolumeId {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.maxVolumeId
}
func (n *NodeImpl) GetVolumeCount() int {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.volumeCount
}
func (n *NodeImpl) GetActiveVolumeCount() int {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.activeVolumeCount
}
func (n *NodeImpl) GetMaxVolumeCount() int {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.maxVolumeCount
}

func (n *NodeImpl) GetPlannedVolumeCount() int {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.plannedVolumeCount
}

func (n *NodeImpl) LinkChildNode(node Node) {
	if n.GetChildren(node.Id()) == nil {
		n.SetChildren(node)
		n.UpAdjustMaxVolumeCountDelta(node.GetMaxVolumeCount())
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		n.UpAdjustVolumeCountDelta(node.GetVolumeCount())
		n.UpAdjustActiveVolumeCountDelta(node.GetActiveVolumeCount())
		node.SetParent(n)
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	node := n.GetChildren(nodeId)
	node.SetParent(nil)
	if node != nil {
		n.DeleteChildren(node.Id())
		n.UpAdjustVolumeCountDelta(-node.GetVolumeCount())
		n.UpAdjustActiveVolumeCountDelta(-node.GetActiveVolumeCount())
		n.UpAdjustMaxVolumeCountDelta(-node.GetMaxVolumeCount())
		glog.V(0).Infoln(n, "removes", node, "volumeCount =", n.activeVolumeCount)
	}
}

func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64) {
	if n.IsRack() {
		for _, c := range n.Children() {
			dn := c.(*DataNode) //can not cast n to DataNode
			if dn.LastSeen() < freshThreshHold {
				if !dn.IsDead() {
					dn.SetDead(true)
					n.GetTopology().chanDeadDataNodes <- dn
				}
			}
			for _, v := range dn.Volumes() {
				if uint64(v.Size) >= volumeSizeLimit {
					//fmt.Println("volume",v.Id,"size",v.Size,">",volumeSizeLimit)
					n.GetTopology().chanFullVolumes <- *v
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
