package cluster

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"math"
	"sync"
	"time"
)

const (
	MasterType       = "master"
	VolumeServerType = "volumeServer"
	FilerType        = "filer"
	BrokerType       = "broker"
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
	filers       map[pb.ServerAddress]*ClusterNode
	filersLock   sync.RWMutex
	filerLeaders *Leaders
	brokers      map[pb.ServerAddress]*ClusterNode
	brokersLock  sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		filers:       make(map[pb.ServerAddress]*ClusterNode),
		filerLeaders: &Leaders{},
		brokers:      make(map[pb.ServerAddress]*ClusterNode),
	}
}

func (cluster *Cluster) AddClusterNode(nodeType string, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	switch nodeType {
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		if existingNode, found := cluster.filers[address]; found {
			existingNode.counter++
			return nil
		}
		cluster.filers[address] = &ClusterNode{
			Address:   address,
			Version:   version,
			counter:   1,
			createdTs: time.Now(),
		}
		return cluster.ensureFilerLeaders(true, nodeType, address)
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()
		if existingNode, found := cluster.brokers[address]; found {
			existingNode.counter++
			return nil
		}
		cluster.brokers[address] = &ClusterNode{
			Address:   address,
			Version:   version,
			counter:   1,
			createdTs: time.Now(),
		}
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) RemoveClusterNode(nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	switch nodeType {
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		if existingNode, found := cluster.filers[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(cluster.filers, address)
				return cluster.ensureFilerLeaders(false, nodeType, address)
			}
		}
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()
		if existingNode, found := cluster.brokers[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(cluster.brokers, address)
				return []*master_pb.KeepConnectedResponse{
					{
						ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
							NodeType: nodeType,
							Address:  string(address),
							IsAdd:    false,
						},
					},
				}
			}
		}
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    false,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case FilerType:
		cluster.filersLock.RLock()
		defer cluster.filersLock.RUnlock()
		for _, node := range cluster.filers {
			nodes = append(nodes, node)
		}
	case BrokerType:
		cluster.brokersLock.RLock()
		defer cluster.brokersLock.RUnlock()
		for _, node := range cluster.brokers {
			nodes = append(nodes, node)
		}
	case MasterType:
	}
	return
}

func (cluster *Cluster) IsOneLeader(address pb.ServerAddress) bool {
	return cluster.filerLeaders.isOneLeader(address)
}

func (cluster *Cluster) ensureFilerLeaders(isAdd bool, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if cluster.filerLeaders.addLeaderIfVacant(address) {
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
		if cluster.filerLeaders.removeLeaderIfExists(address) {

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
			for _, node := range cluster.filers {
				if cluster.filerLeaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.createdTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				cluster.filerLeaders.addLeaderIfVacant(candidateAddress)
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
