package topology

import (
	"fmt"
	"pkg/storage"
)

type NodeId string
type Node struct {
	Id                  NodeId
	activeVolumeCount   int
	reservedVolumeCount int
	maxVolumeCount      int
	parent              *Node
	children            map[NodeId]*Node
	maxVolumeId         storage.VolumeId
}

func NewNode() *Node {
	n := &Node{}
	n.children = make(map[NodeId]*Node)
	return n
}
func (n *Node) String() string {
  if n.parent!=nil {
    return n.parent.String()+":"+string(n.Id)
  }
  return string(n.Id)
}

func (n *Node) ReserveOneVolume(r int, vid storage.VolumeId) (bool, *Node) {
  if n.children == nil {
    return true, n
  }
  ret := false
  var assignedNode *Node
	for _, node := range n.children {
		freeSpace := node.maxVolumeCount - node.activeVolumeCount - node.reservedVolumeCount
    fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
    if freeSpace <= 0 {
      continue
    }
		if r >= freeSpace {
			r -= freeSpace
		} else {
		  ret, assignedNode = node.ReserveOneVolume(r, vid)
		  if ret {
		    break
		  }
		}
	}
	if ret {
	  n.reservedVolumeCount++
	}
	return ret, assignedNode
}

func (n *Node) AddVolume(v *storage.VolumeInfo) {
	if n.maxVolumeId < v.Id {
		n.maxVolumeId = v.Id
	}
	n.activeVolumeCount++
	fmt.Println(n.Id, "adds 1, volumeCount =", n.activeVolumeCount)
	if n.reservedVolumeCount > 0 { //if reserved
		n.reservedVolumeCount--
	}
	if n.parent != nil {
		n.parent.AddVolume(v)
	}
}
func (n *Node) AddMaxVolumeCount(maxVolumeCount int) {//can be negative
  n.maxVolumeCount += maxVolumeCount
  if n.parent != nil {
    n.parent.AddMaxVolumeCount(maxVolumeCount)
  }
}

func (n *Node) GetMaxVolumeId() storage.VolumeId {
	return n.maxVolumeId
}

func (n *Node) AddNode(node *Node) {
	if n.children[node.Id] == nil {
		n.children[node.Id] = node
		n.activeVolumeCount += node.activeVolumeCount
		n.reservedVolumeCount += node.reservedVolumeCount
		n.maxVolumeCount += node.maxVolumeCount
		fmt.Println(n.Id, "adds", node.Id, "volumeCount =", n.activeVolumeCount)
	}
}

func (n *Node) RemoveNode(nodeId NodeId) {
	node := n.children[nodeId]
	if node != nil {
		delete(n.children, node.Id)
		n.activeVolumeCount -= node.activeVolumeCount
		n.maxVolumeCount -= node.maxVolumeCount
		n.reservedVolumeCount -= node.reservedVolumeCount
		p := n.parent
		for p != nil {
			p.activeVolumeCount -= node.activeVolumeCount
			p.maxVolumeCount -= node.maxVolumeCount
			p.reservedVolumeCount -= node.reservedVolumeCount
			p = p.parent
		}
		fmt.Println(n.Id, "removes", node.Id, "volumeCount =", n.activeVolumeCount)
	}
}
