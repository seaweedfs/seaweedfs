package topology

import (
	"fmt"
	"math/rand"
	"pkg/storage"
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

func (nl *NodeList) RandomlyPickN(n int) ([]Node, bool) {
	var list []Node
	for _, n := range nl.nodes {
		if n.FreeSpace() > 0 {
			list = append(list, n)
		}
	}
	if n > len(list){
	  return nil,false
	}
	for i := n; i > 0; i-- {
	  r := rand.Intn(i)
	  t := list[r]
	  list[r] = list[i-1]
	  list[i-1] = t
	}
	return list[len(list)-n:], true
}

func (nl *NodeList) ReserveOneVolume(randomVolumeIndex int, vid storage.VolumeId) (bool, *Server) {
	for _, node := range nl.nodes {
		freeSpace := node.FreeSpace()
		if randomVolumeIndex >= freeSpace {
			randomVolumeIndex -= freeSpace
		} else {
			if node.IsServer() && node.FreeSpace() > 0 {
				fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return true, node.(*Server)
			}
			children := node.Children()
			newNodeList := NewNodeList(children, nl.except)
			return newNodeList.ReserveOneVolume(randomVolumeIndex, vid)
		}
	}
	return false, nil

}
