package topology

import (
	"pkg/storage"
)

type NodeId string
type Node struct {
	id                  NodeId
	countVolumeCount    int
	reservedVolumeCount int
	maxVolumeCount      int
	parent              *Node
	children            map[NodeId]*Node
	isLeaf              bool
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
	n.countVolumeCount++
	if n.reservedVolumeCount > 0 { //if reserved
		n.reservedVolumeCount--
	}
	if n.parent != nil {
		n.parent.AddVolume(v)
	}
}

func (n *Node) AddNode(node *Node) {
	n.children[node.id] = node
	n.countVolumeCount += node.countVolumeCount
	n.maxVolumeCount += node.maxVolumeCount
	if n.parent != nil {
		n.parent.AddNode(node)
	}
}

func (n *Node) RemoveNode(node *Node) {
	delete(n.children, node.id)
	n.countVolumeCount -= node.countVolumeCount
	n.maxVolumeCount -= node.maxVolumeCount
	if n.parent != nil {
		n.parent.RemoveNode(node)
	}
}
