package topology

import (
	"fmt"
	"pkg/storage"
)

type NodeId string
type Node struct {
	Id                  NodeId
	countVolumeCount    int
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

func (n *Node) ReserveOneVolume(r int, vid storage.VolumeId) bool {
	for _, node := range n.children {
		freeSpace := node.maxVolumeCount - node.countVolumeCount - node.reservedVolumeCount
		if r > freeSpace {
			r -= freeSpace
		} else {
			if node.ReserveOneVolume(r, vid) {
				node.reservedVolumeCount++
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (n *Node) AddVolume(v *storage.VolumeInfo) {
	if n.maxVolumeId < v.Id {
		n.maxVolumeId = v.Id
	}
	n.countVolumeCount++
	fmt.Println(n.Id, "adds 1, volumeCount =", n.countVolumeCount)
	if n.reservedVolumeCount > 0 { //if reserved
		n.reservedVolumeCount--
	}
	if n.parent != nil {
		n.parent.AddVolume(v)
	}
}

func (n *Node) GetMaxVolumeId() storage.VolumeId {
	return n.maxVolumeId
}

func (n *Node) AddNode(node *Node) {
	if n.children[node.Id] == nil {
		n.children[node.Id] = node
		n.countVolumeCount += node.countVolumeCount
		n.maxVolumeCount += node.maxVolumeCount
		fmt.Println(n.Id, "adds", node.Id, "volumeCount =", n.countVolumeCount)
	}
}

func (n *Node) RemoveNode(node *Node) {
	if n.children[node.Id] != nil {
		delete(n.children, node.Id)
		n.countVolumeCount -= node.countVolumeCount
		n.maxVolumeCount -= node.maxVolumeCount
		fmt.Println(n.Id, "removes", node.Id, "volumeCount =", n.countVolumeCount)
	}
}
