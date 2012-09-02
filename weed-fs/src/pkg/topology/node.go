package topology

import (
	"fmt"
	"pkg/storage"
)

type NodeId string
type Node interface {
	Id() NodeId
	String() string
	FreeSpace() int
	ReserveOneVolume(r int, vid storage.VolumeId) (bool, *Server)
	UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int)
	UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int)
	UpAdjustMaxVolumeId(vid storage.VolumeId)
	GetActiveVolumeCount() int
	GetMaxVolumeCount() int
	GetMaxVolumeId() storage.VolumeId
	setParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	
	IsServer() bool
}
type NodeImpl struct {
	id                NodeId
	activeVolumeCount int
	maxVolumeCount    int
	parent            Node
	children          map[NodeId]Node
	maxVolumeId       storage.VolumeId

	//for rack, data center, topology
	nodeType string
}

func (n *NodeImpl) IsServer() bool {
	return n.nodeType == "Server"
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
	return n.maxVolumeCount - n.activeVolumeCount
}
func (n *NodeImpl) setParent(node Node) {
	n.parent = node
}
func (n *NodeImpl) ReserveOneVolume(r int, vid storage.VolumeId) (bool, *Server) {
	ret := false
	var assignedNode *Server
	for _, node := range n.children {
		freeSpace := node.FreeSpace()
		fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
		  if node.IsServer() && node.FreeSpace()>0 {
		    fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
		    return true, node.(*Server)
		  }
			ret, assignedNode = node.ReserveOneVolume(r, vid)
			if ret {
				break
			}
		}
	}
	return ret, assignedNode
}

func (n *NodeImpl) UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int) { //can be negative
	n.maxVolumeCount += maxVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta)
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
func (n *NodeImpl) GetActiveVolumeCount() int {
	return n.activeVolumeCount
}
func (n *NodeImpl) GetMaxVolumeCount() int {
	return n.maxVolumeCount
}

func (n *NodeImpl) LinkChildNode(node Node) {
	if n.children[node.Id()] == nil {
		n.children[node.Id()] = node
		n.activeVolumeCount += node.GetActiveVolumeCount()
		n.maxVolumeCount += node.GetMaxVolumeCount()
		node.setParent(n)
		if n.maxVolumeId < node.GetMaxVolumeId() {
			n.maxVolumeId = node.GetMaxVolumeId()
		}
		fmt.Println(n, "adds", node, "volumeCount =", n.activeVolumeCount)
	}
}

func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	node := n.children[nodeId]
	node.setParent(nil)
	if node != nil {
		delete(n.children, node.Id())
		n.UpAdjustActiveVolumeCountDelta(-node.GetActiveVolumeCount())
		n.UpAdjustMaxVolumeCountDelta(-node.GetMaxVolumeCount())
		fmt.Println(n, "removes", node, "volumeCount =", n.activeVolumeCount)
	}
}
