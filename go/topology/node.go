package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"sort"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/storage"
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
	UpAdjustMaxVolumeId(vid storage.VolumeId)

	GetVolumeCount() int
	GetActiveVolumeCount() int
	GetMaxVolumeCount() int
	GetMaxVolumeId() storage.VolumeId
	SetParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64)

	IsDataNode() bool
	IsRack() bool
	IsDataCenter() bool
	Children() map[NodeId]Node
	Parent() Node

	GetValue() interface{} //get reference to the topology,dc,rack,datanode
}
type NodeImpl struct {
	id                NodeId
	volumeCount       int
	activeVolumeCount int
	maxVolumeCount    int
	parent            Node
	children          map[NodeId]Node
	maxVolumeId       storage.VolumeId

	//for rack, data center, topology
	nodeType string
	value    interface{}
}

type NodePicker interface {
	PickNodes(numberOfNodes int, filterNodeFn FilterNodeFn, pickFn PickNodesFn) (nodes []Node, err error)
}


var ErrFilterContinue = errors.New("continue")

type FilterNodeFn func(dn Node) error
type PickNodesFn func(nodes []Node, count int) []Node

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
func (n *NodeImpl) PickNodes(numberOfNodes int, filterNodeFn FilterNodeFn, pickFn PickNodesFn) (nodes []Node, err error) {
	candidates := make([]Node, 0, len(n.children))
	var errs []string
	for _, node := range n.children {
		if err := filterNodeFn(node); err == nil {
			candidates = append(candidates, node)
		}else if err == ErrFilterContinue{
			continue
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	if len(candidates) < numberOfNodes{
		return nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	return pickFn(candidates, numberOfNodes), nil


	glog.V(2).Infoln(n.Id(), "picked main node:", firstNode.Id())

	candidates = candidates[:0]
	for _, node := range n.children {
		if node.Id() == firstNode.Id() {
			continue
		}
		if node.FreeSpace() <= 0 {
			continue
		}
		glog.V(2).Infoln("select rest node candidate:", node.Id())
		candidates = append(candidates, node)
	}
	glog.V(2).Infoln(n.Id(), "picking", numberOfNodes-1, "from rest", len(candidates), "node candidates")
	restNodes = pickFn(candidates, numberOfNodes-1)
	if restNodes == nil {
		glog.V(2).Infoln(n.Id(), "failed to pick", numberOfNodes-1, "from rest", len(candidates), "node candidates")
		err = errors.New("Not enough data node found!")
	}
	return
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

func (n *NodeImpl) RandomlyPickNodes(numberOfNodes int, filterFirstNodeFn FilterNodeFn) (firstNode Node, restNodes []Node, err error) {
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

func (n *NodeImpl) PickLowUsageNodes(numberOfNodes int, filterFirstNodeFn FilterNodeFn) (firstNode Node, restNodes []Node, err error) {
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
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id)
	}
	return string(n.id)
}
func (n *NodeImpl) Id() NodeId {
	return n.id
}
func (n *NodeImpl) FreeSpace() int {
	return n.maxVolumeCount - n.volumeCount
}
func (n *NodeImpl) SetParent(node Node) {
	n.parent = node
}
func (n *NodeImpl) Children() map[NodeId]Node {
	return n.children
}
func (n *NodeImpl) Parent() Node {
	return n.parent
}
func (n *NodeImpl) GetValue() interface{} {
	return n.value
}
func (n *NodeImpl) ReserveOneVolume(r int) (assignedNode *DataNode, err error) {
	for _, node := range n.children {
		freeSpace := node.FreeSpace()
		fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
			if node.IsDataNode() && node.FreeSpace() > 0 {
				fmt.Println("assigned to node =", node, ", freeSpace =", node.FreeSpace())
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
	n.maxVolumeCount += maxVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustVolumeCountDelta(volumeCountDelta int) { //can be negative
	n.volumeCount += volumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustVolumeCountDelta(volumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int) { //can be negative
	n.activeVolumeCount += activeVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta)
	}
}
func (n *NodeImpl) UpAdjustMaxVolumeId(vid storage.VolumeId) { //can be negative
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}
func (n *NodeImpl) GetMaxVolumeId() storage.VolumeId {
	return n.maxVolumeId
}
func (n *NodeImpl) GetVolumeCount() int {
	return n.volumeCount
}
func (n *NodeImpl) GetActiveVolumeCount() int {
	return n.activeVolumeCount
}
func (n *NodeImpl) GetMaxVolumeCount() int {
	return n.maxVolumeCount
}

func (n *NodeImpl) LinkChildNode(node Node) {
	if n.children[node.Id()] == nil {
		n.children[node.Id()] = node
		n.UpAdjustMaxVolumeCountDelta(node.GetMaxVolumeCount())
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		n.UpAdjustVolumeCountDelta(node.GetVolumeCount())
		n.UpAdjustActiveVolumeCountDelta(node.GetActiveVolumeCount())
		node.SetParent(n)
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	node := n.children[nodeId]
	node.SetParent(nil)
	if node != nil {
		delete(n.children, node.Id())
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
			if dn.LastSeen < freshThreshHold {
				if !dn.Dead {
					dn.Dead = true
					n.GetTopology().chanDeadDataNodes <- dn
				}
			}
			for _, v := range dn.volumes {
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
