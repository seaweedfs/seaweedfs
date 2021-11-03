package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"sync"
)

type NodeType int

const (
	filerNodeType NodeType = iota
)

type ClusterNode struct {
	address pb.ServerAddress
	version string
	counter int
}

type Cluster struct {
	filers     map[pb.ServerAddress]*ClusterNode
	filersLock sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		filers: make(map[pb.ServerAddress]*ClusterNode),
	}
}

func (cluster *Cluster) AddClusterNode(nodeType string, address pb.ServerAddress, version string) {
	switch nodeType {
	case "filer":
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		if existingNode, found := cluster.filers[address]; found {
			existingNode.counter++
			return
		}
		cluster.filers[address] = &ClusterNode{
			address: address,
			version: version,
			counter: 1,
		}
	case "master":
	}
}

func (cluster *Cluster) RemoveClusterNode(nodeType string, address pb.ServerAddress) {
	switch nodeType {
	case "filer":
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		if existingNode, found := cluster.filers[address]; !found {
			return
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(cluster.filers, address)
			}
		}
	case "master":
	}
}

func (cluster *Cluster) ListClusterNode(nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case "filer":
		cluster.filersLock.RLock()
		defer cluster.filersLock.RUnlock()
		for _, node := range cluster.filers {
			nodes = append(nodes, node)
		}
	case "master":
	}
	return
}
