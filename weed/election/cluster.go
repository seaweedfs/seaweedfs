package election

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"math"
	"sync"
	"time"
)

type ClusterNode struct {
	Address   pb.ServerAddress
	Version   string
	counter   int
	createdTs time.Time
}

type Leaders struct {
	leaders [3]pb.ServerAddress
}

type Cluster struct {
	nodes     map[pb.ServerAddress]*ClusterNode
	nodesLock sync.RWMutex
	leaders   *Leaders
}

func NewCluster() *Cluster {
	return &Cluster{
		nodes:   make(map[pb.ServerAddress]*ClusterNode),
		leaders: &Leaders{},
	}
}

func (cluster *Cluster) AddClusterNode(nodeType string, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	switch nodeType {
	case "filer":
		cluster.nodesLock.Lock()
		defer cluster.nodesLock.Unlock()
		if existingNode, found := cluster.nodes[address]; found {
			existingNode.counter++
			return nil
		}
		cluster.nodes[address] = &ClusterNode{
			Address:   address,
			Version:   version,
			counter:   1,
			createdTs: time.Now(),
		}
		return cluster.ensureLeader(true, nodeType, address)
	case "master":
	}
	return nil
}

func (cluster *Cluster) RemoveClusterNode(nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	switch nodeType {
	case "filer":
		cluster.nodesLock.Lock()
		defer cluster.nodesLock.Unlock()
		if existingNode, found := cluster.nodes[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(cluster.nodes, address)
				return cluster.ensureLeader(false, nodeType, address)
			}
		}
	case "master":
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case "filer":
		cluster.nodesLock.RLock()
		defer cluster.nodesLock.RUnlock()
		for _, node := range cluster.nodes {
			nodes = append(nodes, node)
		}
	case "master":
	}
	return
}

func (cluster *Cluster) IsOneLeader(address pb.ServerAddress) bool {
	return cluster.leaders.isOneLeader(address)
}

func (cluster *Cluster) ensureLeader(isAdd bool, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if cluster.leaders.addLeaderIfVacant(address) {
			// has added the address as one leader
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsLeader: true,
					IsAdd:    true,
				},
			})
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsLeader: false,
					IsAdd:    true,
				},
			})
		}
	} else {
		if cluster.leaders.removeLeaderIfExists(address) {

			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsLeader: true,
					IsAdd:    false,
				},
			})

			// pick the freshest one, since it is less likely to go away
			var shortestDuration int64 = math.MaxInt64
			now := time.Now()
			var candidateAddress pb.ServerAddress
			for _, node := range cluster.nodes {
				if cluster.leaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.createdTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				cluster.leaders.addLeaderIfVacant(candidateAddress)
				// added a new leader
				result = append(result, &master_pb.KeepConnectedResponse{
					ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
						NodeType: nodeType,
						Address:  string(candidateAddress),
						IsLeader: true,
						IsAdd:    true,
					},
				})
			}
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsLeader: false,
					IsAdd:    false,
				},
			})
		}
	}
	return
}

func (leaders *Leaders) addLeaderIfVacant(address pb.ServerAddress) (hasChanged bool) {
	if leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == "" {
			leaders.leaders[i] = address
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) removeLeaderIfExists(address pb.ServerAddress) (hasChanged bool) {
	if !leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			leaders.leaders[i] = ""
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) isOneLeader(address pb.ServerAddress) bool {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			return true
		}
	}
	return false
}
func (leaders *Leaders) GetLeaders() (addresses []pb.ServerAddress) {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] != "" {
			addresses = append(addresses, leaders.leaders[i])
		}
	}
	return
}
