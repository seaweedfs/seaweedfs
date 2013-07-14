package topology

import (
	"code.google.com/p/weed-fs/go/storage"
	"log"
	"math/rand"
)

type NodeList struct {
	nodes  map[NodeId]Node
	except map[string]Node
}

func NewNodeList(nodes map[NodeId]Node, except map[string]Node) *NodeList {
	m := make(map[NodeId]Node, len(nodes)-len(except))
	for _, n := range nodes {
		if except[n.String()] == nil {
			m[n.Id()] = n
		}
	}
	nl := &NodeList{nodes: m}
	return nl
}

func (nl *NodeList) FreeSpace() int {
	freeSpace := 0
	for _, n := range nl.nodes {
		freeSpace += n.FreeSpace()
	}
	return freeSpace
}

func (nl *NodeList) RandomlyPickN(count int, minSpace int, firstNodeName string) ([]Node, bool) {
	var list []Node
	var preferredNode *Node
	if firstNodeName != "" {
		for _, n := range nl.nodes {
			if n.Id() == NodeId(firstNodeName) && n.FreeSpace() >= minSpace {
				preferredNode = &n
				break
			}
		}
		if preferredNode == nil {
			return list, false
		}
	}

	for _, n := range nl.nodes {
		if n.FreeSpace() >= minSpace && n.Id() != NodeId(firstNodeName) {
			list = append(list, n)
		}
	}
	if count > len(list) || count == len(list) && firstNodeName != "" {
		return nil, false
	}
	for i := len(list); i > 0; i-- {
		r := rand.Intn(i)
		list[r], list[i-1] = list[i-1], list[r]
	}
	if firstNodeName != "" {
		list[0] = *preferredNode
	}
	return list[:count], true
}

func (nl *NodeList) ReserveOneVolume(randomVolumeIndex int, vid storage.VolumeId) (bool, *DataNode) {
	for _, node := range nl.nodes {
		freeSpace := node.FreeSpace()
		if randomVolumeIndex >= freeSpace {
			randomVolumeIndex -= freeSpace
		} else {
			if node.IsDataNode() && node.FreeSpace() > 0 {
				log.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return true, node.(*DataNode)
			}
			children := node.Children()
			newNodeList := NewNodeList(children, nl.except)
			return newNodeList.ReserveOneVolume(randomVolumeIndex, vid)
		}
	}
	return false, nil

}
